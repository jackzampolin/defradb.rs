use std::sync::Arc;

use async_trait::async_trait;

use defra_http::router::{EncryptedIndexInfo, EncryptedIndexOperations};
use storage::corekv::{Key, Store};

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
    async fn add_encrypted_index(
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

        let field_exists = schema.fields.iter().any(|f| f.name == field_name);
        if !field_exists {
            return Err(format!(
                "encrypted index on non-existent field. Field: {}",
                field_name
            ));
        }

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
            .map_err(|e| format!("failed to create transaction: {}", e))?;

        {
            let mut updated_schema = schema.clone();
            updated_schema.encrypted_indexes.push(enc_idx.clone());

            let collection_key =
                storage::keys::systemstore::CollectionKey::new(&updated_schema.version_id);
            let schema_data = serde_json::to_vec(&updated_schema)
                .map_err(|e| format!("failed to serialize schema: {}", e))?;

            let systemstore = txn
                .systemstore()
                .map_err(|e| format!("failed to get systemstore: {}", e))?;

            systemstore
                .set(&collection_key.bytes(), &schema_data)
                .await
                .map_err(|e| format!("failed to save schema: {}", e))?;

            let name_key = storage::keys::systemstore::CollectionNameKey::new(collection);
            systemstore
                .set(&name_key.bytes(), updated_schema.version_id.as_bytes())
                .await
                .map_err(|e| format!("failed to save name mapping: {}", e))?;
        }

        txn.commit()
            .await
            .map_err(|e| format!("failed to commit: {}", e))?;

        self.database
            .reload_cache()
            .await
            .map_err(|e| format!("failed to reload cache: {}", e))?;

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
        match collection {
            Some(name) => {
                let col = self
                    .database
                    .get_collection(name)
                    .map_err(|e| format!("{}", e))?
                    .ok_or_else(|| format!("collection '{}' not found", name))?;

                Ok(col
                    .schema()
                    .encrypted_indexes
                    .iter()
                    .map(|ei| EncryptedIndexInfo {
                        collection: name.to_string(),
                        field_name: ei.field_name.clone(),
                        index_type: "equality".to_string(),
                    })
                    .collect())
            }
            None => {
                let names = self
                    .database
                    .list_collections()
                    .map_err(|e| format!("{}", e))?;

                let mut result = Vec::new();
                for name in names {
                    if let Ok(Some(col)) = self.database.get_collection(&name) {
                        for ei in &col.schema().encrypted_indexes {
                            result.push(EncryptedIndexInfo {
                                collection: name.clone(),
                                field_name: ei.field_name.clone(),
                                index_type: "equality".to_string(),
                            });
                        }
                    }
                }
                Ok(result)
            }
        }
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
            .map_err(|e| format!("failed to create transaction: {}", e))?;

        {
            let mut updated_schema = schema.clone();
            updated_schema
                .encrypted_indexes
                .retain(|idx| idx.field_name != field_name);

            let collection_key =
                storage::keys::systemstore::CollectionKey::new(&updated_schema.version_id);
            let schema_data = serde_json::to_vec(&updated_schema)
                .map_err(|e| format!("failed to serialize schema: {}", e))?;

            let systemstore = txn
                .systemstore()
                .map_err(|e| format!("failed to get systemstore: {}", e))?;

            systemstore
                .set(&collection_key.bytes(), &schema_data)
                .await
                .map_err(|e| format!("failed to save schema: {}", e))?;

            let name_key = storage::keys::systemstore::CollectionNameKey::new(collection);
            systemstore
                .set(&name_key.bytes(), updated_schema.version_id.as_bytes())
                .await
                .map_err(|e| format!("failed to save name mapping: {}", e))?;
        }

        txn.commit()
            .await
            .map_err(|e| format!("failed to commit: {}", e))?;

        self.database
            .reload_cache()
            .await
            .map_err(|e| format!("failed to reload cache: {}", e))?;

        Ok(())
    }
}
