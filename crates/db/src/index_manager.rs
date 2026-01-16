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

/// Result of a bulk index operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BulkIndexResult {
    /// Number of documents successfully indexed.
    pub indexed: usize,
    /// Number of documents skipped (e.g., missing document ID).
    pub skipped: usize,
}

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
    ///
    /// # Errors
    ///
    /// Returns an error if any index in the schema has invalid configuration
    /// (e.g., empty fields list).
    pub fn from_collection(collection_short_id: u32, schema: &CollectionVersion) -> Result<Self> {
        let mut manager = Self::new(collection_short_id);
        for desc in &schema.indexes {
            // Validate index configuration
            if desc.fields.is_empty() {
                return Err(Error::Other(format!(
                    "index '{}' in schema has no fields",
                    desc.name
                )));
            }
            let index = IndexType::new(collection_short_id, desc.clone());
            manager.indexes.insert(desc.name.clone(), index);
        }
        Ok(manager)
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
    ///
    /// # Returns
    ///
    /// - `Ok(true)` if the index existed and was dropped successfully.
    /// - `Ok(false)` if the index did not exist (idempotent - not an error).
    /// - `Err(...)` only on storage failures during entry cleanup.
    ///
    /// # Idempotency
    ///
    /// This method is intentionally idempotent. Calling it multiple times with
    /// the same index name is safe and will not produce errors. This matches
    /// SQL `DROP INDEX IF EXISTS` semantics.
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
    ///
    /// # Concurrency
    ///
    /// This method assumes single-writer semantics provided by the transaction layer.
    /// The read-modify-write sequence is NOT atomic. Concurrent calls from different
    /// transactions could generate duplicate IDs. Callers must ensure exclusive access
    /// to index creation within a collection, either through transaction isolation or
    /// external synchronization.
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
    ///
    /// # Returns
    ///
    /// Returns a `BulkIndexResult` containing:
    /// - `indexed`: Number of documents successfully indexed.
    /// - `skipped`: Number of documents skipped (e.g., documents without an ID).
    ///
    /// Documents without an ID are skipped with a warning logged.
    pub async fn bulk_index(
        &self,
        datastore: &NamespaceView,
        index_name: &str,
        documents: &[Document],
        schema: &CollectionVersion,
    ) -> Result<BulkIndexResult> {
        let index = self
            .indexes
            .get(index_name)
            .ok_or_else(|| Error::Other(format!("index '{}' not found", index_name)))?;

        let mut indexed_count = 0;
        let mut skipped_count = 0;
        let mut mutable_datastore = datastore.clone();

        for doc in documents {
            let doc_id = match doc.id() {
                Some(id) => id.to_string(),
                None => {
                    // Document without ID cannot be indexed - skip with warning
                    skipped_count += 1;
                    continue;
                }
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

        Ok(BulkIndexResult {
            indexed: indexed_count,
            skipped: skipped_count,
        })
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
    ///
    /// # Null Handling
    ///
    /// If a document is missing a field that is part of an index, the value is
    /// indexed as `NormalValue::Null`. This is intentional for nullable fields
    /// and allows documents with missing optional fields to be indexed.
    ///
    /// For unique indexes, multiple documents with NULL values for the same
    /// indexed field will all be indexed (NULL is not considered equal to NULL
    /// for uniqueness purposes).
    fn extract_index_values(
        &self,
        doc: &Document,
        index_desc: &IndexDescription,
        schema: &CollectionVersion,
    ) -> Result<Vec<NormalValue>> {
        // Build a set of field names for O(1) lookup
        let schema_fields: std::collections::HashSet<&str> =
            schema.fields.iter().map(|f| f.name.as_str()).collect();

        let mut values = Vec::with_capacity(index_desc.fields.len());

        for field in &index_desc.fields {
            // Validate that index field exists in schema (except for system fields)
            if !field.name.starts_with('_') && !schema_fields.contains(field.name.as_str()) {
                return Err(Error::Other(format!(
                    "index '{}' references field '{}' which does not exist in schema",
                    index_desc.name, field.name
                )));
            }

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
    use storage::index::IndexIterator;

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

        let manager = IndexManager::from_collection(1, &schema).unwrap();

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

    #[tokio::test]
    async fn test_on_document_update_changes_index_entry() {
        let store = MemoryStore::new();
        let db = DB::new(store);
        let txn = db.new_txn(false).await.unwrap();

        let schema = test_schema();
        let mut manager = IndexManager::new(1);

        {
            let datastore = txn.datastore().unwrap();

            // Create an index on the name field
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

            // Create initial document
            let mut old_doc = Document::new();
            old_doc.generate_and_set_doc_id().unwrap();
            let doc_id = old_doc.id().unwrap().clone();
            old_doc.set("name", NormalValue::String("Alice".to_string()));
            old_doc.set("age", NormalValue::Int(30));

            manager
                .on_document_create(&datastore, &old_doc, &schema)
                .await
                .unwrap();

            // Create updated document with new name
            let mut new_doc = Document::with_id(doc_id);
            new_doc.set("name", NormalValue::String("Alice Smith".to_string()));
            new_doc.set("age", NormalValue::Int(31));

            // Update should succeed
            manager
                .on_document_update(&datastore, &old_doc, &new_doc, &schema)
                .await
                .unwrap();

            // Verify by querying the index - old value should not find doc,
            // new value should find doc
            let index = manager.get_index("idx_name").unwrap();

            // Query for old value
            let mut old_iter = index
                .get(&datastore, &[NormalValue::String("Alice".to_string())])
                .await
                .unwrap();
            let old_results = old_iter.collect_all().await.unwrap();
            assert!(
                old_results.is_empty(),
                "Old index entry should be removed after update"
            );

            // Query for new value
            let mut new_iter = index
                .get(
                    &datastore,
                    &[NormalValue::String("Alice Smith".to_string())],
                )
                .await
                .unwrap();
            let new_results = new_iter.collect_all().await.unwrap();
            assert_eq!(
                new_results.len(),
                1,
                "New index entry should exist after update"
            );
        }

        txn.commit().await.unwrap();
    }

    #[tokio::test]
    async fn test_on_document_update_no_change_when_values_same() {
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

            // Create document
            let mut doc = Document::new();
            doc.generate_and_set_doc_id().unwrap();
            let doc_id = doc.id().unwrap().clone();
            doc.set("name", NormalValue::String("Alice".to_string()));
            doc.set("age", NormalValue::Int(30));

            manager
                .on_document_create(&datastore, &doc, &schema)
                .await
                .unwrap();

            // Update with same indexed value but different non-indexed value
            let mut new_doc = Document::with_id(doc_id);
            new_doc.set("name", NormalValue::String("Alice".to_string())); // Same
            new_doc.set("age", NormalValue::Int(31)); // Different but not indexed

            // Should succeed (optimization path - no actual index write)
            manager
                .on_document_update(&datastore, &doc, &new_doc, &schema)
                .await
                .unwrap();
        }

        txn.commit().await.unwrap();
    }

    #[tokio::test]
    async fn test_on_document_delete_removes_index_entries() {
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

            // Create document
            let mut doc = Document::new();
            doc.generate_and_set_doc_id().unwrap();
            doc.set("name", NormalValue::String("Alice".to_string()));
            doc.set("age", NormalValue::Int(30));

            manager
                .on_document_create(&datastore, &doc, &schema)
                .await
                .unwrap();

            // Verify index entry exists
            let index = manager.get_index("idx_name").unwrap();
            let mut iter = index
                .get(&datastore, &[NormalValue::String("Alice".to_string())])
                .await
                .unwrap();
            let results = iter.collect_all().await.unwrap();
            assert_eq!(results.len(), 1, "Index entry should exist before delete");

            // Delete document
            manager
                .on_document_delete(&datastore, &doc, &schema)
                .await
                .unwrap();

            // Verify index entry is removed
            let mut iter = index
                .get(&datastore, &[NormalValue::String("Alice".to_string())])
                .await
                .unwrap();
            let results = iter.collect_all().await.unwrap();
            assert!(
                results.is_empty(),
                "Index entry should be removed after delete"
            );
        }

        txn.commit().await.unwrap();
    }

    #[tokio::test]
    async fn test_bulk_index_indexes_all_documents() {
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

            // Create multiple documents
            let mut docs = Vec::new();
            for name in ["Alice", "Bob", "Charlie"] {
                let mut doc = Document::new();
                doc.generate_and_set_doc_id().unwrap();
                doc.set("name", NormalValue::String(name.to_string()));
                docs.push(doc);
            }

            // Bulk index them
            let result = manager
                .bulk_index(&datastore, "idx_name", &docs, &schema)
                .await
                .unwrap();

            assert_eq!(result.indexed, 3);
            assert_eq!(result.skipped, 0);

            // Verify all are queryable via index
            let index = manager.get_index("idx_name").unwrap();
            for name in ["Alice", "Bob", "Charlie"] {
                let mut iter = index
                    .get(&datastore, &[NormalValue::String(name.to_string())])
                    .await
                    .unwrap();
                let results = iter.collect_all().await.unwrap();
                assert_eq!(results.len(), 1, "Document '{}' should be in index", name);
            }
        }

        txn.commit().await.unwrap();
    }

    #[tokio::test]
    async fn test_bulk_index_skips_documents_without_id() {
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

            // Create documents - some with IDs, some without
            let mut doc_with_id = Document::new();
            doc_with_id.generate_and_set_doc_id().unwrap();
            doc_with_id.set("name", NormalValue::String("Alice".to_string()));

            let mut doc_without_id = Document::new();
            doc_without_id.set("name", NormalValue::String("Bob".to_string()));
            // No ID set

            let docs = vec![doc_with_id, doc_without_id];

            let result = manager
                .bulk_index(&datastore, "idx_name", &docs, &schema)
                .await
                .unwrap();

            assert_eq!(result.indexed, 1);
            assert_eq!(result.skipped, 1);
        }

        txn.commit().await.unwrap();
    }

    #[tokio::test]
    async fn test_bulk_index_nonexistent_index_fails() {
        let store = MemoryStore::new();
        let db = DB::new(store);
        let txn = db.new_txn(false).await.unwrap();

        let schema = test_schema();
        let manager = IndexManager::new(1);

        {
            let datastore = txn.datastore().unwrap();

            let docs = Vec::new();
            let result = manager
                .bulk_index(&datastore, "nonexistent", &docs, &schema)
                .await;

            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("not found"));
        }
    }

    #[tokio::test]
    async fn test_on_document_create_without_id_fails() {
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

            // Document without ID
            let mut doc = Document::new();
            doc.set("name", NormalValue::String("Alice".to_string()));
            // No ID set

            let result = manager.on_document_create(&datastore, &doc, &schema).await;

            assert!(result.is_err());
            assert!(matches!(result.unwrap_err(), Error::InvalidDocument(_)));
        }
    }

    #[tokio::test]
    async fn test_on_document_update_without_id_fails() {
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

            // Old doc with ID
            let mut old_doc = Document::new();
            old_doc.generate_and_set_doc_id().unwrap();
            old_doc.set("name", NormalValue::String("Alice".to_string()));

            // New doc without ID
            let mut new_doc = Document::new();
            new_doc.set("name", NormalValue::String("Alice Smith".to_string()));
            // No ID set

            let result = manager
                .on_document_update(&datastore, &old_doc, &new_doc, &schema)
                .await;

            assert!(result.is_err());
            assert!(matches!(result.unwrap_err(), Error::InvalidDocument(_)));
        }
    }

    #[tokio::test]
    async fn test_on_document_delete_without_id_fails() {
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

            // Document without ID
            let mut doc = Document::new();
            doc.set("name", NormalValue::String("Alice".to_string()));
            // No ID set

            let result = manager.on_document_delete(&datastore, &doc, &schema).await;

            assert!(result.is_err());
            assert!(matches!(result.unwrap_err(), Error::InvalidDocument(_)));
        }
    }

    #[tokio::test]
    async fn test_from_collection_with_empty_fields_fails() {
        let mut schema = test_schema();
        schema.indexes = vec![IndexDescription {
            name: "idx_invalid".to_string(),
            id: 1,
            fields: vec![], // Empty fields - invalid
            unique: false,
        }];

        let result = IndexManager::from_collection(1, &schema);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("no fields"));
    }

    #[tokio::test]
    async fn test_multi_index_update() {
        let store = MemoryStore::new();
        let db = DB::new(store);
        let txn = db.new_txn(false).await.unwrap();

        let schema = test_schema();
        let mut manager = IndexManager::new(1);

        {
            let datastore = txn.datastore().unwrap();

            // Create multiple indexes
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

            manager
                .create_index(
                    &datastore,
                    "idx_email".to_string(),
                    vec![IndexedFieldDescription {
                        name: "email".to_string(),
                        descending: false,
                    }],
                    false,
                )
                .await
                .unwrap();

            // Create document
            let mut doc = Document::new();
            doc.generate_and_set_doc_id().unwrap();
            let doc_id = doc.id().unwrap().clone();
            doc.set("name", NormalValue::String("Alice".to_string()));
            doc.set(
                "email",
                NormalValue::String("alice@example.com".to_string()),
            );

            manager
                .on_document_create(&datastore, &doc, &schema)
                .await
                .unwrap();

            // Verify both indexes have entries
            let idx_name = manager.get_index("idx_name").unwrap();
            let idx_email = manager.get_index("idx_email").unwrap();

            let mut iter = idx_name
                .get(&datastore, &[NormalValue::String("Alice".to_string())])
                .await
                .unwrap();
            let name_results = iter.collect_all().await.unwrap();
            assert_eq!(name_results.len(), 1);

            let mut iter = idx_email
                .get(
                    &datastore,
                    &[NormalValue::String("alice@example.com".to_string())],
                )
                .await
                .unwrap();
            let email_results = iter.collect_all().await.unwrap();
            assert_eq!(email_results.len(), 1);

            // Update both indexed fields
            let mut new_doc = Document::with_id(doc_id);
            new_doc.set("name", NormalValue::String("Alice Smith".to_string()));
            new_doc.set(
                "email",
                NormalValue::String("alice.smith@example.com".to_string()),
            );

            manager
                .on_document_update(&datastore, &doc, &new_doc, &schema)
                .await
                .unwrap();

            // Verify old entries are gone
            let mut iter = idx_name
                .get(&datastore, &[NormalValue::String("Alice".to_string())])
                .await
                .unwrap();
            let old_name_results = iter.collect_all().await.unwrap();
            assert!(old_name_results.is_empty());

            let mut iter = idx_email
                .get(
                    &datastore,
                    &[NormalValue::String("alice@example.com".to_string())],
                )
                .await
                .unwrap();
            let old_email_results = iter.collect_all().await.unwrap();
            assert!(old_email_results.is_empty());

            // Verify new entries exist
            let mut iter = idx_name
                .get(
                    &datastore,
                    &[NormalValue::String("Alice Smith".to_string())],
                )
                .await
                .unwrap();
            let new_name_results = iter.collect_all().await.unwrap();
            assert_eq!(new_name_results.len(), 1);

            let mut iter = idx_email
                .get(
                    &datastore,
                    &[NormalValue::String("alice.smith@example.com".to_string())],
                )
                .await
                .unwrap();
            let new_email_results = iter.collect_all().await.unwrap();
            assert_eq!(new_email_results.len(), 1);
        }

        txn.commit().await.unwrap();
    }
}
