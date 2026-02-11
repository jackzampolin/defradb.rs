//! Mock encrypted index operations for testing.

use async_trait::async_trait;
use std::sync::{Arc, RwLock};

use crate::router::{EncryptedIndexInfo, EncryptedIndexOperations};

/// Mock encrypted index operations backed by in-memory storage.
#[derive(Debug)]
pub struct MockEncryptedIndexOperations {
    indexes: Arc<RwLock<Vec<(String, EncryptedIndexInfo)>>>,
}

impl Clone for MockEncryptedIndexOperations {
    fn clone(&self) -> Self {
        Self {
            indexes: Arc::clone(&self.indexes),
        }
    }
}

impl Default for MockEncryptedIndexOperations {
    fn default() -> Self {
        Self::new()
    }
}

impl MockEncryptedIndexOperations {
    pub fn new() -> Self {
        Self {
            indexes: Arc::new(RwLock::new(vec![])),
        }
    }
}

#[async_trait]
impl EncryptedIndexOperations for MockEncryptedIndexOperations {
    async fn create_encrypted_index(
        &self,
        collection: &str,
        field_name: &str,
    ) -> Result<EncryptedIndexInfo, String> {
        let mut indexes = self.indexes.write().unwrap();

        let exists = indexes
            .iter()
            .any(|(col, idx)| col == collection && idx.field_name == field_name);
        if exists {
            return Err(format!(
                "encrypted index already exists on this field. Field: {}",
                field_name
            ));
        }

        let info = EncryptedIndexInfo {
            collection: collection.to_string(),
            field_name: field_name.to_string(),
            index_type: "equality".to_string(),
        };
        indexes.push((collection.to_string(), info.clone()));
        Ok(info)
    }

    async fn list_encrypted_indexes(
        &self,
        collection: Option<&str>,
    ) -> Result<Vec<EncryptedIndexInfo>, String> {
        let indexes = self.indexes.read().unwrap();
        Ok(indexes
            .iter()
            .filter(|(col, _)| collection.map_or(true, |name| col == name))
            .map(|(col, idx)| {
                let mut info = idx.clone();
                info.collection = col.clone();
                info
            })
            .collect())
    }

    async fn delete_encrypted_index(
        &self,
        collection: &str,
        field_name: &str,
    ) -> Result<(), String> {
        let mut indexes = self.indexes.write().unwrap();
        let initial_len = indexes.len();
        indexes.retain(|(col, idx)| !(col == collection && idx.field_name == field_name));
        if indexes.len() < initial_len {
            Ok(())
        } else {
            Err(format!(
                "encrypted index does not exist on this field. Field: {}",
                field_name
            ))
        }
    }
}
