//! Adapter to bridge database index operations to HTTP's IndexOperations trait.

use std::sync::Arc;

use async_trait::async_trait;

use db::IndexManager;
use defra_http::router::{IndexFieldInfo, IndexInfo, IndexOperations};
use schema::IndexedFieldDescription;
use storage::corekv::{Key, Store};
use storage::keys::systemstore::CollectionKey;

/// Adapter that implements IndexOperations using the database.
pub struct IndexAdapter<S: Store> {
    database: Arc<db::DB<S>>,
}

impl<S: Store + 'static> IndexAdapter<S> {
    pub fn new(database: Arc<db::DB<S>>) -> Self {
        Self { database }
    }

    pub fn new_arc(database: Arc<db::DB<S>>) -> Arc<dyn IndexOperations> {
        Arc::new(Self::new(database))
    }
}

#[async_trait]
impl<S: Store + 'static> IndexOperations for IndexAdapter<S> {
    async fn create_index(
        &self,
        collection: &str,
        fields: Vec<String>,
        name: Option<&str>,
        unique: bool,
    ) -> Result<IndexInfo, String> {
        self.database
            .check_node_access(None, acp::nac::NodePermission::IndexCreate)
            .await
            .map_err(|e| format!("{}", e))?;

        let col = self
            .database
            .require_collection(collection)
            .map_err(|e| format!("{}", e))?;

        let schema = col.schema().clone();
        let short_id = schema.resolved_root_id();

        let mut index_manager =
            IndexManager::from_collection(short_id, &schema).map_err(|e| format!("{}", e))?;

        let indexed_fields: Vec<IndexedFieldDescription> = fields
            .iter()
            .map(|f| IndexedFieldDescription {
                name: f.clone(),
                descending: false,
            })
            .collect();

        let txn = self
            .database
            .new_txn(false)
            .await
            .map_err(|e| format!("{}", e))?;

        // Scope the systemstore so its Arc<SharedTxn> is dropped before commit
        let (index_desc, updated_schema) = {
            let systemstore = txn.systemstore().map_err(|e| format!("{}", e))?;

            let index_desc = index_manager
                .create_index(
                    &systemstore,
                    collection,
                    name.unwrap_or("").to_string(),
                    indexed_fields,
                    unique,
                    &schema.fields,
                )
                .await
                .map_err(|e| format!("{}", e))?;

            let mut updated_schema = schema;
            updated_schema.indexes.push(index_desc.clone());

            let collection_key = CollectionKey::new(&updated_schema.version_id);
            let data = serde_json::to_vec(&updated_schema)
                .map_err(|e| format!("failed to serialize schema: {}", e))?;
            systemstore
                .set(&collection_key.bytes(), &data)
                .await
                .map_err(|e| format!("{}", e))?;

            (index_desc, updated_schema)
        };

        txn.commit().await.map_err(|e| format!("{}", e))?;

        self.database
            .add_collection_to_cache(updated_schema)
            .map_err(|e| format!("{}", e))?;

        Ok(IndexInfo {
            name: index_desc.name,
            collection: collection.to_string(),
            fields: index_desc
                .fields
                .into_iter()
                .map(|f| IndexFieldInfo {
                    name: f.name,
                    direction: Some(if f.descending {
                        "DESC".to_string()
                    } else {
                        "ASC".to_string()
                    }),
                })
                .collect(),
            unique: index_desc.unique,
        })
    }

    async fn list_indexes(&self, collection: Option<&str>) -> Result<Vec<IndexInfo>, String> {
        self.database
            .check_node_access(None, acp::nac::NodePermission::IndexList)
            .await
            .map_err(|e| format!("{}", e))?;

        let collections = if let Some(name) = collection {
            let col = self
                .database
                .require_collection(name)
                .map_err(|e| format!("{}", e))?;
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
            for idx in col.get_indexes() {
                result.push(IndexInfo {
                    name: idx.name.clone(),
                    collection: col.name().to_string(),
                    fields: idx
                        .fields
                        .iter()
                        .map(|f| IndexFieldInfo {
                            name: f.name.clone(),
                            direction: Some(if f.descending {
                                "DESC".to_string()
                            } else {
                                "ASC".to_string()
                            }),
                        })
                        .collect(),
                    unique: idx.unique,
                });
            }
        }

        Ok(result)
    }

    async fn delete_index(&self, collection: &str, name: &str) -> Result<(), String> {
        self.database
            .check_node_access(None, acp::nac::NodePermission::IndexDelete)
            .await
            .map_err(|e| format!("{}", e))?;

        let col = self
            .database
            .require_collection(collection)
            .map_err(|e| format!("{}", e))?;

        let schema = col.schema().clone();
        let short_id = schema.resolved_root_id();

        let mut index_manager =
            IndexManager::from_collection(short_id, &schema).map_err(|e| format!("{}", e))?;

        let txn = self
            .database
            .new_txn(false)
            .await
            .map_err(|e| format!("{}", e))?;

        // Scope the systemstore so its Arc<SharedTxn> is dropped before commit
        let updated_schema = {
            let systemstore = txn.systemstore().map_err(|e| format!("{}", e))?;

            let existed = index_manager
                .delete_index(&systemstore, name)
                .await
                .map_err(|e| format!("{}", e))?;

            if !existed {
                return Err(format!(
                    "index '{}' not found on collection '{}'",
                    name, collection
                ));
            }

            let mut updated_schema = schema;
            updated_schema.indexes.retain(|idx| idx.name != name);

            let collection_key = CollectionKey::new(&updated_schema.version_id);
            let data = serde_json::to_vec(&updated_schema)
                .map_err(|e| format!("failed to serialize schema: {}", e))?;
            systemstore
                .set(&collection_key.bytes(), &data)
                .await
                .map_err(|e| format!("{}", e))?;

            updated_schema
        };

        txn.commit().await.map_err(|e| format!("{}", e))?;

        self.database
            .add_collection_to_cache(updated_schema)
            .map_err(|e| format!("{}", e))?;

        Ok(())
    }
}
