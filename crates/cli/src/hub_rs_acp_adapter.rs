use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;

use acp::{Policy, StorePolicyOptions, ZanzibarStore};
use defra_http::router::{AcpOperations, PolicyInfo};

pub struct HubRsAcpAdapter {
    hub_acp: Arc<sourcehub::HubRsDocumentACP>,
    local_store: Arc<dyn ZanzibarStore>,
    counter: AtomicU64,
}

impl HubRsAcpAdapter {
    pub fn new(
        hub_acp: Arc<sourcehub::HubRsDocumentACP>,
        local_store: Arc<dyn ZanzibarStore>,
    ) -> Self {
        Self {
            hub_acp,
            local_store,
            counter: AtomicU64::new(1),
        }
    }

    pub fn new_arc(
        hub_acp: Arc<sourcehub::HubRsDocumentACP>,
        local_store: Arc<dyn ZanzibarStore>,
    ) -> Arc<dyn AcpOperations> {
        Arc::new(Self::new(hub_acp, local_store))
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
impl AcpOperations for HubRsAcpAdapter {
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

        let options = StorePolicyOptions::new()
            .with_validation()
            .with_dpi_enforcement();

        self.local_store
            .store_policy_with_options(&policy, &options)
            .await
            .map_err(|e| format!("failed to validate policy: {}", e))?;

        let policy_id = self
            .hub_acp
            .add_policy("", yaml)
            .await
            .map_err(|e| format!("hub.rs create policy failed: {}", e))?;

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
}
