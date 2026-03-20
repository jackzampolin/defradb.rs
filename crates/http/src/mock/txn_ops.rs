//! Mock transaction operations for testing.

use async_trait::async_trait;
use serde_json::json;

use crate::router::TransactionOperations;

fn mock_collection_version(name: &str) -> schema::CollectionVersion {
    serde_json::from_value(json!({
        "Name": name,
        "VersionID": "mock-version-id",
        "CollectionID": "mock-collection-id",
    }))
    .expect("mock collection version should deserialize")
}

/// Mock transaction operations for testing.
#[derive(Debug, Clone, Default)]
pub struct MockTransactionOperations;

impl MockTransactionOperations {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl TransactionOperations for MockTransactionOperations {
    async fn set_migration_in_txn(&self, _txn_id: &str, _config: &str) -> Result<String, String> {
        Ok("mock-transform-id".to_string())
    }

    async fn get_collections_in_txn(
        &self,
        _txn_id: &str,
    ) -> Result<Vec<schema::CollectionVersion>, String> {
        Ok(vec![mock_collection_version("MockCollection")])
    }

    async fn add_schema_in_txn(
        &self,
        _txn_id: &str,
        _sdl: &str,
    ) -> Result<Vec<schema::CollectionVersion>, String> {
        Ok(vec![mock_collection_version("MockCollection")])
    }
}
