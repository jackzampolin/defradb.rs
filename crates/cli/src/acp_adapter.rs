//! Adapter to bridge ACP policy operations to HTTP's AcpOperations trait.

use std::collections::HashMap;
use std::sync::Arc;

use std::sync::RwLock;

use async_trait::async_trait;
use sha2::{Digest, Sha256};

use defra_http::router::{AcpOperations, PolicyInfo};

/// Adapter that implements AcpOperations with an in-memory policy store.
pub struct AcpAdapter {
    policies: RwLock<HashMap<String, PolicyInfo>>,
}

impl Default for AcpAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl AcpAdapter {
    pub fn new() -> Self {
        Self {
            policies: RwLock::new(HashMap::new()),
        }
    }

    pub fn new_arc() -> Arc<dyn AcpOperations> {
        Arc::new(Self::new())
    }
}

/// Generate a deterministic policy ID from the policy content.
fn policy_id_from_content(content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    let hash = hasher.finalize();
    hex::encode(hash)
}

#[async_trait]
impl AcpOperations for AcpAdapter {
    async fn add_policy(&self, policy: &str) -> Result<String, String> {
        let policy_id = policy_id_from_content(policy);

        // Parse YAML to extract metadata
        let parsed: serde_yaml::Value =
            serde_yaml::from_str(policy).map_err(|e| format!("invalid policy YAML: {}", e))?;

        let name = parsed
            .get("name")
            .and_then(|v| v.as_str())
            .map(String::from);

        let description = parsed
            .get("description")
            .and_then(|v| v.as_str())
            .map(String::from);

        let resources = parsed
            .get("resources")
            .map(|v| serde_json::to_value(v).unwrap_or(serde_json::Value::Null));

        let info = PolicyInfo {
            id: policy_id.clone(),
            name,
            description,
            resources,
            actor: None,
            creation_time: None,
        };

        self.policies
            .write()
            .unwrap()
            .insert(policy_id.clone(), info);

        Ok(policy_id)
    }

    async fn list_policies(&self) -> Result<Vec<PolicyInfo>, String> {
        let policies: Vec<PolicyInfo> = self.policies.read().unwrap().values().cloned().collect();
        Ok(policies)
    }

    async fn get_policy(&self, id: &str) -> Result<Option<PolicyInfo>, String> {
        Ok(self.policies.read().unwrap().get(id).cloned())
    }
}
