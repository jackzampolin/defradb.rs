//! Adapter to bridge database collection management operations to HTTP's
//! CollectionManagementOperations trait.

use std::sync::Arc;

use async_trait::async_trait;

use defra_http::router::CollectionManagementOperations;
use storage::corekv::Store;

/// Adapter that implements CollectionManagementOperations using the database.
pub struct CollectionManagementAdapter<S: Store> {
    database: Arc<db::DB<S>>,
}

impl<S: Store + 'static> CollectionManagementAdapter<S> {
    /// Create a new adapter wrapping the given database.
    pub fn new(database: Arc<db::DB<S>>) -> Self {
        Self { database }
    }

    /// Create an Arc-wrapped adapter.
    pub fn new_arc(database: Arc<db::DB<S>>) -> Arc<dyn CollectionManagementOperations> {
        Arc::new(Self::new(database))
    }
}

#[async_trait]
impl<S: Store + 'static> CollectionManagementOperations for CollectionManagementAdapter<S> {
    async fn patch_collection(
        &self,
        collection_name: &str,
        patch: &str,
    ) -> Result<serde_json::Value, String> {
        let version = self
            .database
            .patch_collection(collection_name, patch)
            .await
            .map_err(|e| format!("{}", e))?;

        serde_json::to_value(&version)
            .map_err(|e| format!("failed to serialize collection version: {}", e))
    }

    async fn set_active_version(&self, version_id: &str) -> Result<(), String> {
        self.database
            .set_active_collection_version(version_id)
            .await
            .map_err(|e| format!("{}", e))
    }

    async fn truncate_collection(&self, name: &str) -> Result<(), String> {
        self.database
            .truncate_collection(name)
            .await
            .map_err(|e| format!("{}", e))
    }

    async fn purge(&self) -> Result<(), String> {
        let collections = self
            .database
            .list_collections()
            .map_err(|e| format!("{}", e))?;
        for name in &collections {
            self.database
                .delete_collection(name)
                .await
                .map_err(|e| format!("{}", e))?;
        }
        Ok(())
    }

    async fn get_collection_by_name(
        &self,
        name: &str,
    ) -> Result<Option<schema::CollectionVersion>, String> {
        self.database
            .get_collection(name)
            .map(|opt| opt.map(|c| c.schema().clone()))
            .map_err(|e| format!("{}", e))
    }

    async fn has_collection(&self, name: &str) -> Result<bool, String> {
        self.database
            .has_collection(name)
            .map_err(|e| format!("{}", e))
    }

    async fn find_collection_by_id(
        &self,
        collection_id: &str,
    ) -> Result<Option<schema::CollectionVersion>, String> {
        self.database
            .find_collection_by_id(collection_id)
            .map(|opt| opt.map(|c| c.schema().clone()))
            .map_err(|e| format!("{}", e))
    }

    async fn get_collection_by_version_id(
        &self,
        version_id: &str,
    ) -> Result<Option<schema::CollectionVersion>, String> {
        self.database
            .get_collection_by_version_id_full(version_id)
            .await
            .map(|opt| opt.map(|c| c.schema().clone()))
            .map_err(|e| format!("{}", e))
    }

    async fn delete_collection_versions(&self, version_ids: Vec<String>) -> Result<(), String> {
        self.database
            .delete_collection_versions_batch(version_ids)
            .await
            .map_err(|e| format!("{}", e))
    }
}
