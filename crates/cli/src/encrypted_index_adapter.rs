//! Adapter to bridge database encrypted index operations to HTTP's EncryptedIndexOperations trait.

use std::sync::Arc;

use async_trait::async_trait;

use defra_http::router::{EncryptedIndexInfo, EncryptedIndexOperations};
use storage::corekv::{Key, Store};
use storage::keys::systemstore::{CollectionKey, CollectionNameKey};

/// Adapter that implements EncryptedIndexOperations using the database.
pub struct EncryptedIndexAdapter<S: Store> {
    database: Arc<db::DB<S>>,
}

impl<S: Store + 'static> EncryptedIndexAdapter<S> {
    pub fn new(database: Arc<db::DB<S>>) -> Self {
        Self { database }
    }

    pub fn new_arc(database: Arc<db::DB<S>>) -> Arc<dyn EncryptedIndexOperations> {
        Arc::new(Self::new(database))
    }
}

#[async_trait]
impl<S: Store + 'static> EncryptedIndexOperations for EncryptedIndexAdapter<S> {
    async fn create_encrypted_index(
        &self,
        collection: &str,
        field_name: &str,
    ) -> Result<EncryptedIndexInfo, String> {
        let col = self
            .database
            .get_collection(collection)
            .map_err(|e| format!("{}", e))?
            .ok_or_else(|| format!("collection '{}' not found", collection))?;

        let schema = col.schema();

        // Check if field exists
        let field_exists = schema.fields.iter().any(|f| f.name == field_name);
        if !field_exists {
            return Err(format!(
                "encrypted index on non-existent field. Field: {}",
                field_name
            ));
        }

        // Check if encrypted index already exists for this field
        let index_exists = schema
            .encrypted_indexes
            .iter()
            .any(|idx| idx.field_name == field_name);
        if index_exists {
            return Err(format!(
                "encrypted index already exists on this field. Field: {}",
                field_name
            ));
        }

        let enc_idx = schema::EncryptedIndexDescription::new(field_name);

        let txn = self
            .database
            .new_txn(false)
            .await
            .map_err(|e| format!("{}", e))?;

        {
            let mut updated_schema = schema.clone();
            updated_schema.encrypted_indexes.push(enc_idx);

            let collection_key = CollectionKey::new(&updated_schema.version_id);
            let data = serde_json::to_vec(&updated_schema)
                .map_err(|e| format!("failed to serialize schema: {}", e))?;

            let systemstore = txn.systemstore().map_err(|e| format!("{}", e))?;
            systemstore
                .set(&collection_key.bytes(), &data)
                .await
                .map_err(|e| format!("{}", e))?;

            let name_key = CollectionNameKey::new(collection);
            systemstore
                .set(&name_key.bytes(), updated_schema.version_id.as_bytes())
                .await
                .map_err(|e| format!("{}", e))?;
        }

        txn.commit().await.map_err(|e| format!("{}", e))?;

        self.database
            .reload_cache()
            .await
            .map_err(|e| format!("{}", e))?;

        Ok(EncryptedIndexInfo {
            collection: collection.to_string(),
            field_name: field_name.to_string(),
            index_type: "equality".to_string(),
        })
    }

    async fn list_encrypted_indexes(
        &self,
        collection: Option<&str>,
    ) -> Result<Vec<EncryptedIndexInfo>, String> {
        let collections = if let Some(name) = collection {
            let col = self
                .database
                .get_collection(name)
                .map_err(|e| format!("{}", e))?
                .ok_or_else(|| format!("collection '{}' not found", name))?;
            vec![col]
        } else {
            let names = self
                .database
                .list_collections()
                .map_err(|e| format!("{}", e))?;
            let mut cols = Vec::new();
            for name in &names {
                if let Some(col) = self
                    .database
                    .get_collection(name)
                    .map_err(|e| format!("{}", e))?
                {
                    cols.push(col);
                }
            }
            cols
        };

        let mut result = Vec::new();
        for col in &collections {
            for idx in &col.schema().encrypted_indexes {
                result.push(EncryptedIndexInfo {
                    collection: col.name().to_string(),
                    field_name: idx.field_name.clone(),
                    index_type: "equality".to_string(),
                });
            }
        }
        Ok(result)
    }

    async fn delete_encrypted_index(
        &self,
        collection: &str,
        field_name: &str,
    ) -> Result<(), String> {
        let col = self
            .database
            .get_collection(collection)
            .map_err(|e| format!("{}", e))?
            .ok_or_else(|| format!("collection '{}' not found", collection))?;

        let schema = col.schema();

        let index_exists = schema
            .encrypted_indexes
            .iter()
            .any(|idx| idx.field_name == field_name);
        if !index_exists {
            return Err(format!(
                "encrypted index does not exist on this field. Field: {}",
                field_name
            ));
        }

        let txn = self
            .database
            .new_txn(false)
            .await
            .map_err(|e| format!("{}", e))?;

        {
            let mut updated_schema = schema.clone();
            updated_schema
                .encrypted_indexes
                .retain(|idx| idx.field_name != field_name);

            let collection_key = CollectionKey::new(&updated_schema.version_id);
            let data = serde_json::to_vec(&updated_schema)
                .map_err(|e| format!("failed to serialize schema: {}", e))?;

            let systemstore = txn.systemstore().map_err(|e| format!("{}", e))?;
            systemstore
                .set(&collection_key.bytes(), &data)
                .await
                .map_err(|e| format!("{}", e))?;

            let name_key = CollectionNameKey::new(collection);
            systemstore
                .set(&name_key.bytes(), updated_schema.version_id.as_bytes())
                .await
                .map_err(|e| format!("{}", e))?;
        }

        txn.commit().await.map_err(|e| format!("{}", e))?;

        self.database
            .reload_cache()
            .await
            .map_err(|e| format!("{}", e))?;

        Ok(())
    }
}
