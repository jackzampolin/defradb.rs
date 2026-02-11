//! Mock document ACP operations for testing document relationship handlers.

use async_trait::async_trait;
use identity::Did;

use crate::router::DocumentAcpOperations;

/// Mock document ACP operations for testing document relationship handlers.
#[derive(Debug, Clone, Default)]
pub struct MockDocumentAcpOperations;

impl MockDocumentAcpOperations {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl DocumentAcpOperations for MockDocumentAcpOperations {
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
