//! Mock document ACP operations for testing document relationship handlers.

use async_trait::async_trait;
use identity::Did;

use crate::router::DocumentAcpOperations;

/// Mock document ACP operations for testing document relationship handlers.
#[derive(Debug, Clone)]
pub struct MockDocumentAcpOperations {
    allowed: bool,
}

impl MockDocumentAcpOperations {
    pub fn new() -> Self {
        Self { allowed: true }
    }

    pub fn with_allowed(allowed: bool) -> Self {
        Self { allowed }
    }
}

impl Default for MockDocumentAcpOperations {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DocumentAcpOperations for MockDocumentAcpOperations {
    async fn check_doc_access(
        &self,
        _actor: &Did,
        _permission: acp::DocumentPermission,
        _policy_id: &str,
        _resource_name: &str,
        _doc_id: &str,
    ) -> Result<bool, String> {
        Ok(self.allowed)
    }

    async fn add_doc_relationship(
        &self,
        _requestor: &Did,
        _target_actor: &str,
        _collection: &str,
        _doc_id: &str,
        _relation: &str,
    ) -> Result<bool, String> {
        Ok(true)
    }

    async fn delete_doc_relationship(
        &self,
        _requestor: &Did,
        _target_actor: &str,
        _collection: &str,
        _doc_id: &str,
        _relation: &str,
    ) -> Result<bool, String> {
        Ok(true)
    }
}
