use async_trait::async_trait;

use crate::router::{EncryptedIndexInfo, EncryptedIndexOperations};

/// Mock encrypted index operations for testing.
#[derive(Debug, Clone, Default)]
pub struct MockEncryptedIndexOperations;

impl MockEncryptedIndexOperations {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl EncryptedIndexOperations for MockEncryptedIndexOperations {
    async fn add_encrypted_index(
        &self,
        _collection: &str,
        field_name: &str,
    ) -> Result<EncryptedIndexInfo, String> {
        Ok(EncryptedIndexInfo {
            collection: _collection.to_string(),
            field_name: field_name.to_string(),
            index_type: "equality".to_string(),
        })
    }

    async fn list_encrypted_indexes(
        &self,
        _collection: Option<&str>,
    ) -> Result<Vec<EncryptedIndexInfo>, String> {
        Ok(vec![])
    }

    async fn delete_encrypted_index(
        &self,
        _collection: &str,
        _field_name: &str,
    ) -> Result<(), String> {
        Ok(())
    }
}
