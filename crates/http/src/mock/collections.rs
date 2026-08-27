//! Mock collection management operations for testing.

use async_trait::async_trait;
use serde_json::json;
use std::sync::{Arc, Mutex};

use crate::router::{CollectionManagementOperations, CollectionVersionOperations};

type TruncateCall = (String, Option<serde_json::Value>);

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
pub struct MockCollectionManagementOperations {
    last_migration: Arc<Mutex<Option<lens::LensConfig>>>,
    /// Names passed to `delete_collection`, so a test can tell a filtered
    /// document delete from a collection drop. A no-op mock cannot: it lets a
    /// route wired back to the drop keep every assertion green.
    dropped_collections: Arc<Mutex<Vec<String>>>,
    truncated_collections: Arc<Mutex<Vec<TruncateCall>>>,
}

impl MockCollectionManagementOperations {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn last_migration(&self) -> Option<lens::LensConfig> {
        self.last_migration.lock().unwrap().clone()
    }

    /// Collections dropped through `delete_collection`.
    pub fn dropped_collections(&self) -> Vec<String> {
        self.dropped_collections.lock().unwrap().clone()
    }

    pub fn truncated_collections(&self) -> Vec<TruncateCall> {
        self.truncated_collections.lock().unwrap().clone()
    }
}

#[async_trait]
impl CollectionVersionOperations for MockCollectionManagementOperations {
    async fn get_all_collections(&self) -> Result<Vec<schema::CollectionVersion>, String> {
        Ok(vec![mock_collection_version("MockCollection")])
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
        migration: Option<lens::LensConfig>,
    ) -> Result<serde_json::Value, String> {
        *self.last_migration.lock().unwrap() = migration;
        Ok(json!({"name": collection_name, "version": "v2"}))
    }

    async fn set_active_version(&self, _version_id: &str) -> Result<(), String> {
        Ok(())
    }

    async fn truncate_collection(
        &self,
        name: &str,
        filter: Option<serde_json::Value>,
    ) -> Result<(), String> {
        self.truncated_collections
            .lock()
            .unwrap()
            .push((name.to_string(), filter));
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
        CollectionVersionOperations::get_all_collections(self).await
    }

    async fn delete_collection(&self, name: &str) -> Result<(), String> {
        self.dropped_collections
            .lock()
            .unwrap()
            .push(name.to_string());
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
