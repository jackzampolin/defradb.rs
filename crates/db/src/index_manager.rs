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

