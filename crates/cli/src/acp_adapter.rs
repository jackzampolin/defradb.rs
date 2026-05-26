//! Adapter to bridge ACP policy operations to HTTP's AcpOperations trait.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;

use acp::{Policy, StorePolicyOptions, ZanzibarStore};
use defra_http::router::{AcpOperations, PolicyInfo};

/// Adapter that implements AcpOperations backed by a ZanzibarStore.
pub struct AcpAdapter {
    store: Arc<dyn ZanzibarStore>,
    counter: AtomicU64,
}

impl AcpAdapter {
    pub fn new(store: Arc<dyn ZanzibarStore>) -> Self {
        Self {
            store,
            counter: AtomicU64::new(1),
        }
    }

    pub fn new_arc(store: Arc<dyn ZanzibarStore>) -> Arc<dyn AcpOperations> {
        Arc::new(Self::new(store))
    }
}

fn allow_full_zanzibar_policies() -> bool {
    std::env::var("DEFRA_ACP_ALLOW_FULL_ZANZIBAR_POLICIES")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

fn policy_store_options() -> StorePolicyOptions {
    let options = StorePolicyOptions::new().with_validation();
    if allow_full_zanzibar_policies() {
        options
    } else {
        options.with_dpi_enforcement()
    }
}

/// Convert a Zanzibar Policy to an HTTP PolicyInfo.
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
impl AcpOperations for AcpAdapter {
    async fn add_policy(&self, yaml: &str) -> Result<String, String> {
        acp::policy_yaml::check_duplicate_yaml_keys(yaml)?;

        let parsed = acp::policy_yaml::parse_policy_yaml(yaml)?;
        if parsed.name.is_empty() {
            return Err("name required".to_string());
        }
        acp::policy_yaml::validate_policy_expressions(&parsed)?;

        let counter = self.counter.fetch_add(1, Ordering::SeqCst);
        let policy = acp::policy_yaml::build_policy(&parsed, counter)
            .map_err(|e| format!("invalid policy: {}", e))?;
        let policy_id = policy.id.clone();

        let options = policy_store_options();

        self.store
            .store_policy_with_options(&policy, &options)
            .await
            .map_err(|e| format!("failed to store policy: {}", e))?;

        Ok(policy_id)
    }

    async fn list_policies(&self) -> Result<Vec<PolicyInfo>, String> {
        let policies = self
            .store
            .list_policies()
            .await
            .map_err(|e| format!("failed to list policies: {}", e))?;

        Ok(policies.iter().map(policy_to_info).collect())
    }

    async fn get_policy(&self, id: &str) -> Result<Option<PolicyInfo>, String> {
        let policy = self
            .store
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
        let policy = self.store.get_policy(policy_id).await.map_err(|e| {
            format!(
                "policy validation failed with acp: {}. PolicyID: {}",
                e, policy_id
            )
        })?;
        acp::validate_resource_interface(policy_id, resource_name, policy.as_ref())
    }
}
