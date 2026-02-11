//! Mock collection management operations for testing.

use async_trait::async_trait;
use serde_json::json;

use crate::router::CollectionManagementOperations;

/// Mock collection management operations for testing.
#[derive(Debug, Clone, Default)]
pub struct MockCollectionManagementOperations;

impl MockCollectionManagementOperations {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl CollectionManagementOperations for MockCollectionManagementOperations {
    async fn patch_collection(
        &self,
        collection_name: &str,
        _patch: &str,
    ) -> Result<serde_json::Value, String> {
        Ok(json!({"name": collection_name, "version": "v2"}))
    }

    async fn set_active_version(&self, _version_id: &str) -> Result<(), String> {
        Ok(())
    }

    async fn truncate_collection(&self, _name: &str) -> Result<(), String> {
        Ok(())
    }

    async fn purge(&self) -> Result<(), String> {
        Ok(())
    }
}
