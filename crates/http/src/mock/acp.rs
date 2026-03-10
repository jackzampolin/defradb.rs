//! Mock ACP operations for testing ACP handlers.

use async_trait::async_trait;
use std::sync::{Arc, RwLock};

use crate::router::{AcpLightClientStatus, AcpOperations, PolicyInfo};

/// Mock ACP operations for testing ACP handlers.
#[derive(Debug)]
pub struct MockAcpOperations {
    policies: Arc<RwLock<Vec<PolicyInfo>>>,
    next_id: Arc<RwLock<u64>>,
    light_client_status: Arc<RwLock<Option<AcpLightClientStatus>>>,
}

impl Clone for MockAcpOperations {
    fn clone(&self) -> Self {
        Self {
            policies: Arc::clone(&self.policies),
            next_id: Arc::clone(&self.next_id),
            light_client_status: Arc::clone(&self.light_client_status),
        }
    }
}

impl Default for MockAcpOperations {
    fn default() -> Self {
        Self::new()
    }
}

impl MockAcpOperations {
    /// Create a new mock ACP operations instance.
    pub fn new() -> Self {
        Self {
            policies: Arc::new(RwLock::new(vec![])),
            next_id: Arc::new(RwLock::new(1)),
            light_client_status: Arc::new(RwLock::new(None)),
        }
    }

    /// Create with a pre-existing policy.
    pub fn with_policy(self, id: &str, name: Option<&str>) -> Self {
        self.policies.write().unwrap().push(PolicyInfo {
            id: id.to_string(),
            name: name.map(|s| s.to_string()),
            description: None,
            resources: None,
            actor: None,
            creation_time: Some("2024-01-01T00:00:00Z".to_string()),
        });
        self
    }

    /// Create with a pre-existing ACP light client status.
    pub fn with_light_client_status(self, status: AcpLightClientStatus) -> Self {
        *self.light_client_status.write().unwrap() = Some(status);
        self
    }
}

#[async_trait]
impl AcpOperations for MockAcpOperations {
    async fn add_policy(&self, _policy: &str) -> Result<String, String> {
        let mut id = self.next_id.write().unwrap();
        let policy_id = format!("policy-{:04}", *id);
        *id += 1;

        self.policies.write().unwrap().push(PolicyInfo {
            id: policy_id.clone(),
            name: Some("Test Policy".to_string()),
            description: Some("A test policy".to_string()),
            resources: None,
            actor: None,
            creation_time: Some("2024-01-01T00:00:00Z".to_string()),
        });

        Ok(policy_id)
    }

    async fn list_policies(&self) -> Result<Vec<PolicyInfo>, String> {
        Ok(self.policies.read().unwrap().clone())
    }

    async fn get_policy(&self, id: &str) -> Result<Option<PolicyInfo>, String> {
        let policies = self.policies.read().unwrap();
        Ok(policies.iter().find(|p| p.id == id).cloned())
    }

    async fn get_light_client_status(&self) -> Result<AcpLightClientStatus, String> {
        self.light_client_status
            .read()
            .unwrap()
            .clone()
            .ok_or_else(|| "ACP light client status is not configured in this mock".to_string())
    }
}

/// Mock ACP operations that always fails with a configurable error.
#[derive(Debug, Clone)]
pub struct FailingMockAcpOperations {
    error: String,
}

impl FailingMockAcpOperations {
    /// Create a new failing mock with the given error message.
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
        }
    }
}

#[async_trait]
impl AcpOperations for FailingMockAcpOperations {
    async fn add_policy(&self, _policy: &str) -> Result<String, String> {
        Err(self.error.clone())
    }

    async fn list_policies(&self) -> Result<Vec<PolicyInfo>, String> {
        Err(self.error.clone())
    }

    async fn get_policy(&self, _id: &str) -> Result<Option<PolicyInfo>, String> {
        Err(self.error.clone())
    }

    async fn get_light_client_status(&self) -> Result<AcpLightClientStatus, String> {
        Err(self.error.clone())
    }
}
