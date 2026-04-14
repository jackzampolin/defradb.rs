/// Index manager for DefraDB collections.
///
/// Handles index lifecycle operations:
/// - Creating new indexes (with ID generation and bulk indexing)
/// - Dropping indexes (with entry cleanup)
/// - Loading index instances from schema
/// - Maintaining indexes during document mutations
mod value_extraction;

use crate::error::{Error, Result};
use datastore::NamespaceView;
use document::Document;
use schema::{CollectionVersion, FieldDescription, IndexDescription, IndexedFieldDescription};
use std::collections::HashMap;
use storage::corekv::Key;
use storage::index::{FullTextIndex, IndexType};
use storage::keys::IndexIDSequenceKey;

/// Result of a bulk index operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BulkIndexResult {
    /// Number of documents successfully indexed.
    pub indexed: usize,
    /// Number of documents skipped (e.g., missing document ID).
    pub skipped: usize,
}

/// Generate an index name matching Go's `{Col}_{firstField}_ASC` pattern.
///
/// If the base name already exists, appends `_2`, `_3`, etc. to avoid collisions.
fn generate_index_name(
    collection_name: &str,
    first_field: &str,
    existing_names: &[String],
) -> String {
    let base = format!("{}_{}_ASC", collection_name, first_field);
    if !existing_names.contains(&base) {
        return base;
    }
    let mut suffix = 2u32;
    loop {
        let candidate = format!("{}_{}", base, suffix);
        if !existing_names.contains(&candidate) {
            return candidate;
        }
        suffix += 1;
    }
}

/// Check if an index name is valid per Go's rules.
///
/// Must start with a letter or underscore, and contain only alphanumeric characters
/// and underscores. Matches Go's `isValidIndexName()`.
fn is_valid_index_name(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    let mut chars = name.chars();
    let first = chars.next().unwrap();
    if !first.is_ascii_alphabetic() && first != '_' {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Generate the internal map key used for full-text indexes.
///
/// This key is intentionally invalid for public index creation APIs because
/// full-text indexes are synthesized from `@fulltext` schema directives and do
/// not share the same namespace as user-defined secondary indexes.
pub fn fulltext_index_name(field_name: &str) -> String {
    format!("__fulltext__:{field_name}")
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
            if desc.fields.is_empty() {
                return Err(Error::Other(format!(
                    "index '{}' in schema has no fields",
                    desc.name
                )));
            }
            let index = IndexType::new(collection_short_id, desc.clone());
            manager.indexes.insert(desc.name.clone(), index);
        }
        // Load full-text indexes.
        // IDs are derived deterministically from the field name to avoid collisions
        // with regular indexes (which use the IndexIDSequenceKey mechanism).
        // The high-bit (0x8000_0000) separates the namespace from regular index IDs.
        for ft_desc in &schema.fulltext_indexes {
            let idx_name = fulltext_index_name(&ft_desc.field_name);
            let idx_id = {
                // FNV-1a: stable across Rust versions unlike DefaultHasher
                let mut h: u32 = 2166136261;
                for byte in ft_desc.field_name.as_bytes() {
                    h ^= *byte as u32;
                    h = h.wrapping_mul(16777619);
                }
                h | 0x8000_0000
            };
            let desc = IndexDescription {
                name: idx_name.clone(),
                id: idx_id,
                fields: vec![IndexedFieldDescription {
                    name: ft_desc.field_name.clone(),
                    descending: false,
                }],
                unique: false,
                auto_generated: false,
            };
            let ft_index = FullTextIndex::new(collection_short_id, desc, ft_desc.clone());
            manager
                .indexes
                .insert(idx_name, IndexType::FullText(ft_index));
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
        collection_name: &str,
        name: String,
        fields: Vec<IndexedFieldDescription>,
        unique: bool,
        schema_fields: &[FieldDescription],
    ) -> Result<IndexDescription> {
        let name = if name.is_empty() {
            let first_field = fields.first().map(|f| f.name.as_str()).unwrap_or("unknown");
            let existing: Vec<String> = self.indexes.keys().cloned().collect();
            generate_index_name(collection_name, first_field, &existing)
        } else {
            name
        };

        if !is_valid_index_name(&name) {
            return Err(Error::Other(format!(
                "index with invalid name. Name: {}",
                name
            )));
        }

        if self.indexes.contains_key(&name) {
            return Err(Error::Other(format!(
                "index with name already exists. Name: {}",
                name
            )));
        }

        if fields.is_empty() {
            return Err(Error::Other(
                "index must have at least one field".to_string(),
            ));
        }

        for field in &fields {
            if let Some(schema_field) = schema_fields.iter().find(|f| f.name == field.name) {
                if schema_field.crdt_type.is_counter() {
                    return Err(Error::Other(format!(
                        "indexing accumulated CRDT fields is not yet supported. Field: {}, CRDTType: {}",
                        field.name, schema_field.crdt_type.to_string().to_lowercase()
                    )));
                }
            }
        }

        let index_id = self.next_index_id(datastore).await?;

        let desc = IndexDescription {
            name: name.clone(),
            id: index_id,
            fields,
            unique,
            auto_generated: false,
        };

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
    /// SQL `DELETE INDEX IF EXISTS` semantics.
    pub async fn delete_index(&mut self, datastore: &NamespaceView, name: &str) -> Result<bool> {
        match self.indexes.remove(name) {
            Some(index) => {
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
                    skipped_count += 1;
                    continue;
                }
            };

            let value_sets = self.extract_index_values(doc, index.description(), schema)?;

            for values in &value_sets {
                index
                    .save(&mut mutable_datastore, &doc_id, values)
                    .await
                    .map_err(Error::Storage)?;
            }

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
            let value_sets = self.extract_index_values(doc, index.description(), schema)?;
            for values in &value_sets {
                index
                    .save(&mut mutable_datastore, &doc_id, values)
                    .await
                    .map_err(Error::Storage)?;
            }
        }

        Ok(())
    }

    /// Update indexes when a document is created via blind create.
    ///
    /// "Blind" means the document existence check was skipped (content-addressed IDs
    /// are unique by construction). However, unique index constraints on field values
    /// are still enforced — two different documents can share a content-addressed ID
    /// scheme but have duplicate field values.
    pub async fn on_document_create_blind(
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
            let value_sets = self.extract_index_values(doc, index.description(), schema)?;
            for values in &value_sets {
                index
                    .save(&mut mutable_datastore, &doc_id, values)
                    .await
                    .map_err(Error::Storage)?;
            }
        }

        Ok(())
    }

    /// Update indexes when a document is updated.
    ///
    /// For array fields, this deletes all old index entries and creates new ones.
    /// The update is performed by deleting all old value combinations and saving
    /// all new value combinations.
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
            let old_value_sets = self.extract_index_values(old_doc, index.description(), schema)?;
            let new_value_sets = self.extract_index_values(new_doc, index.description(), schema)?;

            if old_value_sets != new_value_sets {
                for old_values in &old_value_sets {
                    index
                        .delete(&mut mutable_datastore, &doc_id, old_values)
                        .await
                        .map_err(Error::Storage)?;
                }
                for new_values in &new_value_sets {
                    index
                        .save(&mut mutable_datastore, &doc_id, new_values)
                        .await
                        .map_err(Error::Storage)?;
                }
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
            let value_sets = self.extract_index_values(doc, index.description(), schema)?;
            for values in &value_sets {
                index
                    .delete(&mut mutable_datastore, &doc_id, values)
                    .await
                    .map_err(Error::Storage)?;
            }
        }

        Ok(())
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
