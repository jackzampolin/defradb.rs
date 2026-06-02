//! Adapter to bridge ACP policy operations to HTTP's AcpOperations trait via SourceHub.
//!
//! Policies are created on-chain via SourceHub transactions, then cached locally
//! in the ZanzibarStore for reads (list/get). This matches the FFI pattern.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;

use acp::{Policy, StorePolicyOptions, ZanzibarStore};
use defra_http::router::{AcpLightClientStatus, AcpOperations, PolicyInfo};

/// Adapter that implements AcpOperations with SourceHub for writes and local store for reads.
pub struct SourceHubAcpAdapter {
    sourcehub_acp: Arc<sourcehub::SourceHubDocumentACP>,
    local_store: Arc<dyn ZanzibarStore>,
    counter: AtomicU64,
    nac_checker: Arc<dyn db::NodeAccessChecker>,
}

impl SourceHubAcpAdapter {
    pub fn new(
        sourcehub_acp: Arc<sourcehub::SourceHubDocumentACP>,
        local_store: Arc<dyn ZanzibarStore>,
        nac_checker: Arc<dyn db::NodeAccessChecker>,
    ) -> Self {
        Self {
            sourcehub_acp,
            local_store,
            counter: AtomicU64::new(1),
            nac_checker,
        }
    }

    pub fn new_arc(
        sourcehub_acp: Arc<sourcehub::SourceHubDocumentACP>,
        local_store: Arc<dyn ZanzibarStore>,
        nac_checker: Arc<dyn db::NodeAccessChecker>,
    ) -> Arc<dyn AcpOperations> {
        Arc::new(Self::new(sourcehub_acp, local_store, nac_checker))
    }

    async fn get_or_cache_policy(&self, policy_id: &str) -> Result<Option<Policy>, String> {
        if let Some(policy) = self
            .local_store
            .get_policy(policy_id)
            .await
            .map_err(|e| format!("failed to get policy from local cache: {}", e))?
        {
            return Ok(Some(policy));
        }

        let Some(policy) = self
            .sourcehub_acp
            .get_policy(policy_id)
            .await
            .map_err(|e| format!("failed to query SourceHub policy: {}", e))?
        else {
            return Ok(None);
        };

        let options = StorePolicyOptions::new()
            .with_validation()
            .with_dpi_enforcement();
        self.local_store
            .store_policy_with_options(&policy, &options)
            .await
            .map_err(|e| format!("failed to cache SourceHub policy: {}", e))?;

        Ok(Some(policy))
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
        self.nac_checker
            .check_node_access(acp::nac::NodePermission::DacPolicyAdd)
            .await
            .map_err(|e| e.to_string())?;

        // Validate locally first (same checks as local adapter)
        acp::policy_yaml::check_duplicate_yaml_keys(yaml)?;

        let parsed = acp::policy_yaml::parse_policy_yaml(yaml)?;
        if parsed.name.is_empty() {
            return Err("name required".to_string());
        }
        acp::policy_yaml::validate_policy_expressions(&parsed)?;

        let counter = self.counter.fetch_add(1, Ordering::SeqCst);
        let policy = acp::policy_yaml::build_policy(&parsed, counter)
            .map_err(|e| format!("invalid policy: {}", e))?;

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

        // Re-store the policy under the on-chain ID so relationship validation
        // can find it. The initial store used a locally-computed SHA256 ID, but
        // the schema references the on-chain ID. Go doesn't cache locally at all
        // (it queries Source Hub on-demand), but since our doc_acp_adapter validates
        // against the local store, we need the policy indexed by on-chain ID.
        if policy_id != policy.id {
            let mut on_chain_policy = policy;
            on_chain_policy.id = policy_id.clone();
            self.local_store
                .store_policy_with_options(&on_chain_policy, &options)
                .await
                .map_err(|e| format!("failed to cache policy with on-chain ID: {}", e))?;
        }

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

    async fn validate_resource_interface(
        &self,
        policy_id: &str,
        resource_name: &str,
    ) -> Result<(), String> {
        let policy = self.get_or_cache_policy(policy_id).await.map_err(|e| {
            format!(
                "policy validation failed with acp: {}. PolicyID: {}",
                e, policy_id
            )
        })?;
        acp::validate_resource_interface(policy_id, resource_name, policy.as_ref())
    }

    async fn get_light_client_status(&self) -> Result<AcpLightClientStatus, String> {
        let status = self
            .sourcehub_acp
            .acp_light_client_status()
            .map_err(|e| format!("failed to get ACP light client status: {}", e))?;

        Ok(AcpLightClientStatus {
            height: status.height,
            module_state_root: status.module_state_root,
            cache_entries: status.cache_entries,
            last_invalidation_height: status.last_invalidation_height,
            connected: status.connected,
        })
    }
}
