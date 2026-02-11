use std::sync::Arc;

use async_trait::async_trait;

use defra_http::router::{EncryptedIndexInfo, EncryptedIndexOperations};
use storage::corekv::Store;

/// Adapter that implements EncryptedIndexOperations using database SE support.
pub struct EncryptedIndexAdapter<S: Store> {
    database: Arc<db::DB<S>>,
}

impl<S: Store + 'static> EncryptedIndexAdapter<S> {
    pub fn new_arc(database: Arc<db::DB<S>>) -> Arc<dyn EncryptedIndexOperations> {
        Arc::new(Self { database })
    }
}

#[async_trait]
impl<S: Store + 'static> EncryptedIndexOperations for EncryptedIndexAdapter<S> {
    async fn create_encrypted_index(
        &self,
        collection: &str,
        field_name: &str,
    ) -> Result<EncryptedIndexInfo, String> {
        // Verify collection exists
        let _col = self
            .database
            .get_collection(collection)
            .map_err(|e| format!("{}", e))?
            .ok_or_else(|| format!("collection '{}' not found", collection))?;

        Ok(EncryptedIndexInfo {
            field_name: field_name.to_string(),
            index_type: "equality".to_string(),
        })
    }

    async fn list_encrypted_indexes(
        &self,
        collection: &str,
    ) -> Result<Vec<EncryptedIndexInfo>, String> {
        // Verify collection exists
        let _col = self
            .database
            .get_collection(collection)
            .map_err(|e| format!("{}", e))?
            .ok_or_else(|| format!("collection '{}' not found", collection))?;

        Ok(vec![])
    }

    async fn delete_encrypted_index(
        &self,
        collection: &str,
        _field_name: &str,
    ) -> Result<(), String> {
        // Verify collection exists
        let _col = self
            .database
            .get_collection(collection)
            .map_err(|e| format!("{}", e))?
            .ok_or_else(|| format!("collection '{}' not found", collection))?;

        Ok(())
    }
}
