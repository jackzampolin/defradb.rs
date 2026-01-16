/// Index manager for DefraDB collections.
///
/// Handles index lifecycle operations:
/// - Creating new indexes (with ID generation and bulk indexing)
/// - Dropping indexes (with entry cleanup)
/// - Loading index instances from schema
/// - Maintaining indexes during document mutations
use crate::error::{Error, Result};
use datastore::NamespaceView;
use document::{Document, NormalValue};
use schema::{CollectionVersion, IndexDescription, IndexedFieldDescription};
use std::collections::HashMap;
use storage::corekv::Key;
use storage::index::IndexType;
use storage::keys::IndexIDSequenceKey;

/// Manages indexes for a collection.
///
/// Provides operations for creating, dropping, and maintaining secondary indexes.
/// Indexes are stored in the collection schema and persisted to storage.
pub struct IndexManager {
    /// Collection short ID for index key generation
    collection_short_id: u32,
    /// Active index instances keyed by index name
    indexes: HashMap<String, IndexType>,
}

impl IndexManager {
    /// Create a new empty IndexManager.
    pub fn new(collection_short_id: u32) -> Self {
        Self {
            collection_short_id,
            indexes: HashMap::new(),
        }
    }

    /// Load indexes from a collection schema.
    pub fn from_collection(collection_short_id: u32, schema: &CollectionVersion) -> Self {
        let mut manager = Self::new(collection_short_id);
        for desc in &schema.indexes {
            let index = IndexType::new(collection_short_id, desc.clone());
            manager.indexes.insert(desc.name.clone(), index);
        }
        manager
    }

    /// Get the collection short ID.
    pub fn collection_short_id(&self) -> u32 {
        self.collection_short_id
    }

    /// Get all index descriptions.
    pub fn get_indexes(&self) -> Vec<&IndexDescription> {
        self.indexes.values().map(|idx| idx.description()).collect()
    }

    /// Get an index by name.
    pub fn get_index(&self, name: &str) -> Option<&IndexType> {
        self.indexes.get(name)
    }

    /// Check if an index exists.
    pub fn has_index(&self, name: &str) -> bool {
        self.indexes.contains_key(name)
    }

    /// Create a new index.
    ///
    /// This creates the index definition but does NOT bulk-index existing documents.
    /// Call `bulk_index` separately to index existing documents.
    ///
    /// Returns the IndexDescription with the generated ID.
    pub async fn create_index(
        &mut self,
        datastore: &NamespaceView,
        name: String,
        fields: Vec<IndexedFieldDescription>,
        unique: bool,
    ) -> Result<IndexDescription> {
        // Check if index already exists
        if self.indexes.contains_key(&name) {
            return Err(Error::Other(format!(
                "index '{}' already exists on collection",
                name
            )));
        }

        // Validate fields
        if fields.is_empty() {
            return Err(Error::Other(
                "index must have at least one field".to_string(),
            ));
        }

        // Generate a new index ID
        let index_id = self.next_index_id(datastore).await?;

        // Create the index description
        let desc = IndexDescription {
            name: name.clone(),
            id: index_id,
            fields,
            unique,
        };

        // Create the index instance
        let index = IndexType::new(self.collection_short_id, desc.clone());
        self.indexes.insert(name, index);

        Ok(desc)
    }

    /// Drop an index by name.
    ///
    /// Removes the index from the manager and clears all index entries from storage.
    pub async fn drop_index(&mut self, datastore: &NamespaceView, name: &str) -> Result<bool> {
        match self.indexes.remove(name) {
            Some(index) => {
                // Create a mutable namespace view for deletion
                // We need to use the underlying store operations
                index
                    .remove_all(&mut datastore.clone())
                    .await
                    .map_err(Error::Storage)?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Get the next available index ID for this collection.
    async fn next_index_id(&self, datastore: &NamespaceView) -> Result<u32> {
        let seq_key = IndexIDSequenceKey::new(format!("{}", self.collection_short_id));
        let key_bytes = seq_key.bytes();

        // Get current sequence value
        let current = match datastore.get(&key_bytes).await.map_err(Error::Storage)? {
            Some(bytes) => {
                if bytes.len() == 4 {
                    u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
                } else {
                    0
                }
            }
            None => 0,
        };

        // Increment and store
        let next_id = current + 1;
        datastore
            .set(&key_bytes, &next_id.to_be_bytes())
            .await
            .map_err(Error::Storage)?;

        Ok(next_id)
    }

    /// Bulk index all existing documents in a collection.
    ///
    /// This should be called after creating a new index to populate it with
    /// existing document data.
    pub async fn bulk_index(
        &self,
        datastore: &NamespaceView,
        index_name: &str,
        documents: &[Document],
        schema: &CollectionVersion,
    ) -> Result<usize> {
        let index = self
            .indexes
            .get(index_name)
            .ok_or_else(|| Error::Other(format!("index '{}' not found", index_name)))?;

        let mut indexed_count = 0;
        let mut mutable_datastore = datastore.clone();

        for doc in documents {
            let doc_id = match doc.id() {
                Some(id) => id.to_string(),
                None => continue,
            };

            // Extract field values for the index
            let values = self.extract_index_values(doc, index.description(), schema)?;

            // Save to index
            index
                .save(&mut mutable_datastore, &doc_id, &values)
                .await
                .map_err(Error::Storage)?;

            indexed_count += 1;
        }

        Ok(indexed_count)
    }

    /// Update indexes when a document is created.
    pub async fn on_document_create(
        &self,
        datastore: &NamespaceView,
        doc: &Document,
        schema: &CollectionVersion,
    ) -> Result<()> {
        let doc_id = doc
            .id()
            .ok_or_else(|| Error::InvalidDocument("document must have an ID".to_string()))?
            .to_string();

        let mut mutable_datastore = datastore.clone();

        for index in self.indexes.values() {
            let values = self.extract_index_values(doc, index.description(), schema)?;
            index
                .save(&mut mutable_datastore, &doc_id, &values)
                .await
                .map_err(Error::Storage)?;
        }

        Ok(())
    }

    /// Update indexes when a document is updated.
    pub async fn on_document_update(
        &self,
        datastore: &NamespaceView,
        old_doc: &Document,
        new_doc: &Document,
        schema: &CollectionVersion,
    ) -> Result<()> {
        let doc_id = new_doc
            .id()
            .ok_or_else(|| Error::InvalidDocument("document must have an ID".to_string()))?
            .to_string();

        let mut mutable_datastore = datastore.clone();

        for index in self.indexes.values() {
            let old_values = self.extract_index_values(old_doc, index.description(), schema)?;
            let new_values = self.extract_index_values(new_doc, index.description(), schema)?;

            // Only update if values changed
            if old_values != new_values {
                index
                    .update(&mut mutable_datastore, &doc_id, &old_values, &new_values)
                    .await
                    .map_err(Error::Storage)?;
            }
        }

        Ok(())
    }

    /// Update indexes when a document is deleted.
    pub async fn on_document_delete(
        &self,
        datastore: &NamespaceView,
        doc: &Document,
        schema: &CollectionVersion,
    ) -> Result<()> {
        let doc_id = doc
            .id()
            .ok_or_else(|| Error::InvalidDocument("document must have an ID".to_string()))?
            .to_string();

        let mut mutable_datastore = datastore.clone();

        for index in self.indexes.values() {
            let values = self.extract_index_values(doc, index.description(), schema)?;
            index
                .delete(&mut mutable_datastore, &doc_id, &values)
                .await
                .map_err(Error::Storage)?;
        }

        Ok(())
    }

    /// Extract field values from a document for indexing.
    fn extract_index_values(
        &self,
        doc: &Document,
        index_desc: &IndexDescription,
        _schema: &CollectionVersion,
    ) -> Result<Vec<NormalValue>> {
        let mut values = Vec::with_capacity(index_desc.fields.len());

        for field in &index_desc.fields {
            let value = doc.get(&field.name).cloned().unwrap_or(NormalValue::Null);
            values.push(value);
        }

        Ok(values)
    }

    /// Get all index names.
    pub fn index_names(&self) -> Vec<&str> {
        self.indexes.keys().map(|s| s.as_str()).collect()
    }

    /// Get the number of indexes.
    pub fn index_count(&self) -> usize {
        self.indexes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::DB;
    use schema::FieldDescription;
    use schema::FieldKind;
    use storage::backends::MemoryStore;

    fn test_schema() -> CollectionVersion {
        CollectionVersion::new(
            "users",
            "v1",
            "col-users",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "age", FieldKind::int()),
                FieldDescription::new("4", "email", FieldKind::string()),
            ],
        )
    }

    #[tokio::test]
    async fn test_create_index() {
        let store = MemoryStore::new();
        let db = DB::new(store);
        let txn = db.new_txn(false).await.unwrap();
        let datastore = txn.datastore().unwrap();

        let mut manager = IndexManager::new(1);

        let fields = vec![IndexedFieldDescription {
            name: "name".to_string(),
            descending: false,
        }];

        let desc = manager
            .create_index(&datastore, "idx_name".to_string(), fields, false)
            .await
            .unwrap();

        assert_eq!(desc.name, "idx_name");
        assert_eq!(desc.id, 1);
        assert!(!desc.unique);
        assert!(manager.has_index("idx_name"));
    }

    #[tokio::test]
    async fn test_create_unique_index() {
        let store = MemoryStore::new();
        let db = DB::new(store);
        let txn = db.new_txn(false).await.unwrap();
        let datastore = txn.datastore().unwrap();

        let mut manager = IndexManager::new(1);

        let fields = vec![IndexedFieldDescription {
            name: "email".to_string(),
            descending: false,
        }];

        let desc = manager
            .create_index(&datastore, "idx_email".to_string(), fields, true)
            .await
            .unwrap();

        assert_eq!(desc.name, "idx_email");
        assert!(desc.unique);
    }

    #[tokio::test]
    async fn test_create_duplicate_index_fails() {
        let store = MemoryStore::new();
        let db = DB::new(store);
        let txn = db.new_txn(false).await.unwrap();
        let datastore = txn.datastore().unwrap();

        let mut manager = IndexManager::new(1);

        let fields = vec![IndexedFieldDescription {
            name: "name".to_string(),
            descending: false,
        }];

        manager
            .create_index(&datastore, "idx_name".to_string(), fields.clone(), false)
            .await
            .unwrap();

        let result = manager
            .create_index(&datastore, "idx_name".to_string(), fields, false)
            .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));
    }

    #[tokio::test]
    async fn test_create_empty_fields_fails() {
        let store = MemoryStore::new();
        let db = DB::new(store);
        let txn = db.new_txn(false).await.unwrap();
        let datastore = txn.datastore().unwrap();

        let mut manager = IndexManager::new(1);

        let result = manager
            .create_index(&datastore, "idx_empty".to_string(), vec![], false)
            .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("at least one field"));
    }

    #[tokio::test]
    async fn test_drop_index() {
        let store = MemoryStore::new();
        let db = DB::new(store);
        let txn = db.new_txn(false).await.unwrap();
        let datastore = txn.datastore().unwrap();

        let mut manager = IndexManager::new(1);

        let fields = vec![IndexedFieldDescription {
            name: "name".to_string(),
            descending: false,
        }];

        manager
            .create_index(&datastore, "idx_name".to_string(), fields, false)
            .await
            .unwrap();

        assert!(manager.has_index("idx_name"));

        let dropped = manager.drop_index(&datastore, "idx_name").await.unwrap();
        assert!(dropped);
        assert!(!manager.has_index("idx_name"));
    }

    #[tokio::test]
    async fn test_drop_nonexistent_index() {
        let store = MemoryStore::new();
        let db = DB::new(store);
        let txn = db.new_txn(false).await.unwrap();
        let datastore = txn.datastore().unwrap();

        let mut manager = IndexManager::new(1);

        let dropped = manager.drop_index(&datastore, "nonexistent").await.unwrap();
        assert!(!dropped);
    }

    #[tokio::test]
    async fn test_get_indexes() {
        let store = MemoryStore::new();
        let db = DB::new(store);
        let txn = db.new_txn(false).await.unwrap();
        let datastore = txn.datastore().unwrap();

        let mut manager = IndexManager::new(1);

        manager
            .create_index(
                &datastore,
                "idx1".to_string(),
                vec![IndexedFieldDescription {
                    name: "name".to_string(),
                    descending: false,
                }],
                false,
            )
            .await
            .unwrap();

        manager
            .create_index(
                &datastore,
                "idx2".to_string(),
                vec![IndexedFieldDescription {
                    name: "email".to_string(),
                    descending: false,
                }],
                true,
            )
            .await
            .unwrap();

        let indexes = manager.get_indexes();
        assert_eq!(indexes.len(), 2);
    }

    #[tokio::test]
    async fn test_from_collection_with_indexes() {
        let mut schema = test_schema();
        schema.indexes = vec![
            IndexDescription {
                name: "idx_name".to_string(),
                id: 1,
                fields: vec![IndexedFieldDescription {
                    name: "name".to_string(),
                    descending: false,
                }],
                unique: false,
            },
            IndexDescription {
                name: "idx_email".to_string(),
                id: 2,
                fields: vec![IndexedFieldDescription {
                    name: "email".to_string(),
                    descending: false,
                }],
                unique: true,
            },
        ];

        let manager = IndexManager::from_collection(1, &schema);

        assert_eq!(manager.index_count(), 2);
        assert!(manager.has_index("idx_name"));
        assert!(manager.has_index("idx_email"));
    }

    #[tokio::test]
    async fn test_on_document_create() {
        let store = MemoryStore::new();
        let db = DB::new(store);
        let txn = db.new_txn(false).await.unwrap();

        let schema = test_schema();
        let mut manager = IndexManager::new(1);

        {
            let datastore = txn.datastore().unwrap();

            manager
                .create_index(
                    &datastore,
                    "idx_name".to_string(),
                    vec![IndexedFieldDescription {
                        name: "name".to_string(),
                        descending: false,
                    }],
                    false,
                )
                .await
                .unwrap();

            let mut doc = Document::new();
            doc.generate_and_set_doc_id().unwrap();
            doc.set("name", NormalValue::String("Alice".to_string()));
            doc.set("age", NormalValue::Int(30));

            manager
                .on_document_create(&datastore, &doc, &schema)
                .await
                .unwrap();
        }
        // datastore is dropped here, releasing the Arc<SharedTxn>

        txn.commit().await.unwrap();
    }

    #[tokio::test]
    async fn test_index_id_sequence() {
        let store = MemoryStore::new();
        let db = DB::new(store);
        let txn = db.new_txn(false).await.unwrap();
        let datastore = txn.datastore().unwrap();

        let mut manager = IndexManager::new(1);

        // Create multiple indexes and verify IDs increment
        let desc1 = manager
            .create_index(
                &datastore,
                "idx1".to_string(),
                vec![IndexedFieldDescription {
                    name: "name".to_string(),
                    descending: false,
                }],
                false,
            )
            .await
            .unwrap();

        let desc2 = manager
            .create_index(
                &datastore,
                "idx2".to_string(),
                vec![IndexedFieldDescription {
                    name: "age".to_string(),
                    descending: false,
                }],
                false,
            )
            .await
            .unwrap();

        let desc3 = manager
            .create_index(
                &datastore,
                "idx3".to_string(),
                vec![IndexedFieldDescription {
                    name: "email".to_string(),
                    descending: false,
                }],
                false,
            )
            .await
            .unwrap();

        assert_eq!(desc1.id, 1);
        assert_eq!(desc2.id, 2);
        assert_eq!(desc3.id, 3);
    }
}
