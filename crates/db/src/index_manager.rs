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
use schema::{CollectionVersion, FieldDescription, IndexDescription, IndexedFieldDescription};
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
        collection_name: &str,
        name: String,
        fields: Vec<IndexedFieldDescription>,
        unique: bool,
        schema_fields: &[FieldDescription],
    ) -> Result<IndexDescription> {
        // Auto-generate name if empty (matches Go behavior)
        let name = if name.is_empty() {
            let first_field = fields.first().map(|f| f.name.as_str()).unwrap_or("unknown");
            let existing: Vec<String> = self.indexes.keys().cloned().collect();
            generate_index_name(collection_name, first_field, &existing)
        } else {
            name
        };

        // Validate index name
        if !is_valid_index_name(&name) {
            return Err(Error::Other(format!(
                "index with invalid name. Name: {}",
                name
            )));
        }

        // Check if index already exists
        if self.indexes.contains_key(&name) {
            return Err(Error::Other(format!(
                "index with name already exists. Name: {}",
                name
            )));
        }

        // Validate fields
        if fields.is_empty() {
            return Err(Error::Other(
                "index must have at least one field".to_string(),
            ));
        }

        // Reject CRDT counter fields (matches Go's NewCollectionIndex validation)
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

            // Extract field values for the index (may return multiple value sets for arrays)
            let value_sets = self.extract_index_values(doc, index.description(), schema)?;

            // Save all value sets to index (one entry per array element combination)
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
            // Save all value sets (one entry per array element combination)
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

            // Only update if value sets changed
            if old_value_sets != new_value_sets {
                // Delete all old entries
                for old_values in &old_value_sets {
                    index
                        .delete(&mut mutable_datastore, &doc_id, old_values)
                        .await
                        .map_err(Error::Storage)?;
                }
                // Save all new entries
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
            // Delete all value set entries (one per array element combination)
            for values in &value_sets {
                index
                    .delete(&mut mutable_datastore, &doc_id, values)
                    .await
                    .map_err(Error::Storage)?;
            }
        }

        Ok(())
    }

    /// Extract field values from a document for indexing.
    ///
    /// # Multi-Value Indexing (Arrays)
    ///
    /// When a field contains an array, multiple index entries are created - one per
    /// array element. For composite indexes with multiple array fields, the Cartesian
    /// product of all array elements is generated.
    ///
    /// Example: For document `{tags: ["a", "b"], categories: ["x", "y"]}` with a
    /// composite index on `(tags, categories)`, four index entries are created:
    /// `("a", "x")`, `("a", "y")`, `("b", "x")`, `("b", "y")`
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
    ) -> Result<Vec<Vec<NormalValue>>> {
        // Build a set of field names for O(1) lookup
        let schema_fields: std::collections::HashSet<&str> =
            schema.fields.iter().map(|f| f.name.as_str()).collect();

        // Collect expanded values for each field (arrays become multiple values)
        let mut field_value_sets: Vec<Vec<NormalValue>> =
            Vec::with_capacity(index_desc.fields.len());

        for field in &index_desc.fields {
            // Validate that index field exists in schema (except for system fields)
            if !field.name.starts_with('_') && !schema_fields.contains(field.name.as_str()) {
                return Err(Error::Other(format!(
                    "index '{}' references field '{}' which does not exist in schema",
                    index_desc.name, field.name
                )));
            }

            let value = doc.get(&field.name).cloned().unwrap_or(NormalValue::Null);

            // Expand arrays into multiple values; scalars become single-element sets
            let expanded = Self::expand_value_for_indexing(value);
            field_value_sets.push(expanded);
        }

        // Compute Cartesian product of all field value sets
        Ok(Self::cartesian_product(field_value_sets))
    }

    /// Expand a value for multi-value indexing.
    ///
    /// Arrays are expanded into their elements. Empty arrays result in a single
    /// NULL value to ensure the document is still indexed.
    fn expand_value_for_indexing(value: NormalValue) -> Vec<NormalValue> {
        // Helper macro to handle array expansion with conversion to NormalValue
        macro_rules! expand_array {
            ($arr:expr, $variant:ident) => {
                if $arr.is_empty() {
                    vec![NormalValue::Null]
                } else {
                    $arr.iter()
                        .map(|v| NormalValue::$variant(v.clone()))
                        .collect()
                }
            };
        }

        // Helper macro for nillable arrays (Some/None)
        macro_rules! expand_nillable_array {
            ($opt:expr, $variant:ident) => {
                match $opt {
                    Some(arr) => {
                        if arr.is_empty() {
                            vec![NormalValue::Null]
                        } else {
                            arr.iter()
                                .map(|v| NormalValue::$variant(v.clone()))
                                .collect()
                        }
                    }
                    None => vec![NormalValue::Null],
                }
            };
        }

        // Helper macro for arrays with nillable elements
        macro_rules! expand_nillable_element_array {
            ($arr:expr, $variant:ident) => {
                if $arr.is_empty() {
                    vec![NormalValue::Null]
                } else {
                    $arr.iter()
                        .map(|v| match v {
                            Some(val) => NormalValue::$variant(val.clone()),
                            None => NormalValue::Null,
                        })
                        .collect()
                }
            };
        }

        match value {
            // JSON values are expanded via leaf traversal
            NormalValue::Json(_) => {
                let leaves = value.json_leaves();
                if leaves.is_empty() {
                    // Empty JSON object/array produces single NULL entry
                    vec![NormalValue::Null]
                } else {
                    leaves
                }
            }

            // Non-array scalar types - single element
            NormalValue::Null
            | NormalValue::Bool(_)
            | NormalValue::Int(_)
            | NormalValue::Float64(_)
            | NormalValue::Float32(_)
            | NormalValue::String(_)
            | NormalValue::Bytes(_)
            | NormalValue::Time(_)
            | NormalValue::Document(_)
            | NormalValue::JsonLeaf(_)
            | NormalValue::NillableBool(_)
            | NormalValue::NillableInt(_)
            | NormalValue::NillableFloat64(_)
            | NormalValue::NillableFloat32(_)
            | NormalValue::NillableString(_)
            | NormalValue::NillableBytes(_)
            | NormalValue::NillableTime(_)
            | NormalValue::NillableDocument(_) => vec![value],

            // Typed array types - expand to elements
            NormalValue::BoolArray(ref arr) => expand_array!(arr, Bool),
            NormalValue::IntArray(ref arr) => expand_array!(arr, Int),
            NormalValue::Float64Array(ref arr) => expand_array!(arr, Float64),
            NormalValue::Float32Array(ref arr) => expand_array!(arr, Float32),
            NormalValue::StringArray(ref arr) => expand_array!(arr, String),
            NormalValue::BytesArray(ref arr) => expand_array!(arr, Bytes),
            NormalValue::TimeArray(ref arr) => expand_array!(arr, Time),
            NormalValue::DocumentArray(ref arr) => {
                if arr.is_empty() {
                    vec![NormalValue::Null]
                } else {
                    arr.iter()
                        .map(|v| NormalValue::Document(Box::new(v.clone())))
                        .collect()
                }
            }
            NormalValue::JsonArray(ref arr) => expand_array!(arr, Json),

            // Nillable arrays (whole array can be null)
            NormalValue::NillableBoolArray(ref opt) => expand_nillable_array!(opt, Bool),
            NormalValue::NillableIntArray(ref opt) => expand_nillable_array!(opt, Int),
            NormalValue::NillableFloat64Array(ref opt) => expand_nillable_array!(opt, Float64),
            NormalValue::NillableFloat32Array(ref opt) => expand_nillable_array!(opt, Float32),
            NormalValue::NillableStringArray(ref opt) => expand_nillable_array!(opt, String),
            NormalValue::NillableBytesArray(ref opt) => expand_nillable_array!(opt, Bytes),
            NormalValue::NillableTimeArray(ref opt) => expand_nillable_array!(opt, Time),
            NormalValue::NillableDocumentArray(ref opt) => match opt {
                Some(arr) => {
                    if arr.is_empty() {
                        vec![NormalValue::Null]
                    } else {
                        arr.iter()
                            .map(|v| NormalValue::Document(Box::new(v.clone())))
                            .collect()
                    }
                }
                None => vec![NormalValue::Null],
            },

            // Arrays with nillable elements
            NormalValue::NillableBoolElementArray(ref arr) => {
                expand_nillable_element_array!(arr, Bool)
            }
            NormalValue::NillableIntElementArray(ref arr) => {
                expand_nillable_element_array!(arr, Int)
            }
            NormalValue::NillableFloat64ElementArray(ref arr) => {
                expand_nillable_element_array!(arr, Float64)
            }
            NormalValue::NillableFloat32ElementArray(ref arr) => {
                expand_nillable_element_array!(arr, Float32)
            }
            NormalValue::NillableStringElementArray(ref arr) => {
                expand_nillable_element_array!(arr, String)
            }
            NormalValue::NillableBytesElementArray(ref arr) => {
                expand_nillable_element_array!(arr, Bytes)
            }
            NormalValue::NillableTimeElementArray(ref arr) => {
                expand_nillable_element_array!(arr, Time)
            }
            NormalValue::NillableDocumentElementArray(ref arr) => {
                if arr.is_empty() {
                    vec![NormalValue::Null]
                } else {
                    arr.iter()
                        .map(|v| match v {
                            Some(doc) => NormalValue::Document(Box::new(doc.clone())),
                            None => NormalValue::Null,
                        })
                        .collect()
                }
            }
        }
    }

    /// Compute Cartesian product of field value sets.
    ///
    /// Given `[[a, b], [x, y]]`, produces `[[a, x], [a, y], [b, x], [b, y]]`.
    fn cartesian_product(sets: Vec<Vec<NormalValue>>) -> Vec<Vec<NormalValue>> {
        if sets.is_empty() {
            return vec![vec![]];
        }

        let mut result: Vec<Vec<NormalValue>> = vec![vec![]];

        for set in sets {
            let mut new_result = Vec::with_capacity(result.len() * set.len());
            for combo in &result {
                for val in &set {
                    let mut new_combo = combo.clone();
                    new_combo.push(val.clone());
                    new_result.push(new_combo);
                }
            }
            result = new_result;
        }

        result
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
