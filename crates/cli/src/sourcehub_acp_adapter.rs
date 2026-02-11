//! Adapter to bridge ACP policy operations to HTTP's AcpOperations trait via SourceHub.
//!
//! Policies are created on-chain via SourceHub transactions, then cached locally
//! in the ZanzibarStore for reads (list/get). This matches the FFI pattern.

use std::sync::Arc;

use async_trait::async_trait;

use acp::{Policy, StorePolicyOptions, ZanzibarStore};
use defra_http::router::{AcpOperations, PolicyInfo};

/// Adapter that implements AcpOperations with SourceHub for writes and local store for reads.
pub struct SourceHubAcpAdapter {
    sourcehub_acp: Arc<sourcehub::SourceHubDocumentACP>,
    local_store: Arc<dyn ZanzibarStore>,
}

impl SourceHubAcpAdapter {
    pub fn new(
        sourcehub_acp: Arc<sourcehub::SourceHubDocumentACP>,
        local_store: Arc<dyn ZanzibarStore>,
    ) -> Self {
        Self {
            sourcehub_acp,
            local_store,
        }
    }

    pub fn new_arc(
        sourcehub_acp: Arc<sourcehub::SourceHubDocumentACP>,
        local_store: Arc<dyn ZanzibarStore>,
    ) -> Arc<dyn AcpOperations> {
        Arc::new(Self::new(sourcehub_acp, local_store))
    }
}

fn policy_to_info(policy: &Policy) -> PolicyInfo {
    let resources = serde_json::to_value(&policy.resources).ok();
    PolicyInfo {
        id: policy.id.clone(),
        name: Some(policy.name.clone()),
        description: policy.attributes.get("description").cloned(),
        resources,
        actor: None,
        creation_time: None,
    }
}

#[async_trait]
impl AcpOperations for SourceHubAcpAdapter {
    async fn add_policy(&self, yaml: &str) -> Result<String, String> {
        // Validate locally first (same checks as local adapter)
        acp::policy_yaml::check_duplicate_yaml_keys(yaml)?;

        let parsed = acp::policy_yaml::parse_policy_yaml(yaml)?;
        if parsed.name.is_empty() {
            return Err("name required".to_string());
        }
        acp::policy_yaml::validate_policy_expressions(&parsed)?;

        let policy = Policy::from_yaml(yaml).map_err(|e| format!("invalid policy: {}", e))?;

        let options = StorePolicyOptions::new()
            .with_validation()
            .with_dpi_enforcement();

        // DPI validation before submitting on-chain
        self.local_store
            .store_policy_with_options(&policy, &options)
            .await
            .map_err(|e| format!("failed to validate policy: {}", e))?;

        // Submit on-chain via SourceHub
        let policy_id = self
            .sourcehub_acp
            .add_policy("", yaml)
            .await
            .map_err(|e| format!("SourceHub create policy failed: {}", e))?;

        Ok(policy_id)
    }

    async fn list_policies(&self) -> Result<Vec<PolicyInfo>, String> {
        let policies = self
            .local_store
            .list_policies()
            .await
            .map_err(|e| format!("failed to list policies: {}", e))?;

        Ok(policies.iter().map(policy_to_info).collect())
    }

    async fn get_policy(&self, id: &str) -> Result<Option<PolicyInfo>, String> {
        let policy = self
            .local_store
            .get_policy(id)
            .await
            .map_err(|e| format!("failed to get policy: {}", e))?;

        Ok(policy.as_ref().map(policy_to_info))
    }
}
