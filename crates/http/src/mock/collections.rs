//! Mock collection management operations for testing.

use async_trait::async_trait;
use serde_json::json;

use crate::router::CollectionManagementOperations;

fn mock_collection_version(name: &str) -> schema::CollectionVersion {
    serde_json::from_value(json!({
        "Name": name,
        "VersionID": "mock-version-id",
        "CollectionID": "mock-collection-id",
    }))
    .expect("mock collection version should deserialize")
}

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
    async fn list_actions(&self) -> Result<Vec<defra_core::ActionExecution>, String> {
        Ok(Vec::new())
    }

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

    async fn get_collection_by_name(
        &self,
        name: &str,
    ) -> Result<Option<schema::CollectionVersion>, String> {
        Ok(Some(mock_collection_version(name)))
    }

    async fn has_collection(&self, _name: &str) -> Result<bool, String> {
        Ok(true)
    }

    async fn find_collection_by_id(
        &self,
        _collection_id: &str,
    ) -> Result<Option<schema::CollectionVersion>, String> {
        Ok(Some(mock_collection_version("MockCollection")))
    }

    async fn get_collection_by_version_id(
        &self,
        _version_id: &str,
    ) -> Result<Option<schema::CollectionVersion>, String> {
        Ok(Some(mock_collection_version("MockCollection")))
    }

    async fn delete_collection_versions(&self, _version_ids: Vec<String>) -> Result<(), String> {
        Ok(())
    }

    async fn get_all_collections(&self) -> Result<Vec<schema::CollectionVersion>, String> {
        Ok(vec![mock_collection_version("MockCollection")])
    }

    async fn delete_collection(&self, _name: &str) -> Result<(), String> {
        Ok(())
    }

    async fn delete_collections(
        &self,
        _names: Vec<String>,
        _active_only: bool,
    ) -> Result<(), String> {
        Ok(())
    }
}
