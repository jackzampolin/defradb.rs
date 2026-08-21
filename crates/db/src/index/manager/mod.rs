/// Index manager for DefraDB collections.
///
/// Handles index lifecycle operations:
/// - Creating new indexes (with ID generation and bulk indexing)
/// - Dropping indexes (with entry cleanup)
/// - Loading index instances from schema
/// - Maintaining indexes during document mutations
mod value_extraction;

use crate::index::error::{Error, Result};
use datastore::NamespaceView;
use document::Document;
use schema::{
    CollectionVersion, FieldDescription, IndexDescription, IndexKind, IndexedFieldDescription,
    OrderedIndexDescription,
};
use std::collections::HashMap;
use storage::corekv::Key;
use storage::index::FullTextIndex;

mod index_type;
pub use index_type::IndexType;
use storage::keys::IndexIDSequenceKey;

pub mod doc_source;
pub use doc_source::{DocumentSource, SliceSource};

/// How a live unique conflict was resolved during merge (#1111).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeConflictOutcome {
    /// No conflict, or the stale-entry heal handled it.
    Clean,
    /// The incoming document won the deterministic pick and now holds the
    /// unique entry; the previous holder is no longer indexed.
    IncomingIndexed,
    /// The existing holder won; the incoming document is persisted but not
    /// indexed for the conflicting values.
    IncomingUnindexed,
}

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

/// Reject the unset doc short ID (0): index maintenance requires a mapped document.
fn require_doc_short_id(doc_short_id: u64) -> Result<()> {
    if doc_short_id == 0 {
        return Err(Error::InvalidDocument(
            "document must have a doc short ID".to_string(),
        ));
    }
    Ok(())
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
    /// Collection ID for document/deletion-marker key generation. Unique
    /// enforcement needs it to ask whether the document a stale index entry
    /// points at is still alive (#1111/#700). Empty when constructed without a
    /// schema, in which case stale-entry healing is disabled.
    collection_id: String,
    /// Active index instances keyed by index name
    indexes: HashMap<String, IndexType>,
}

impl IndexManager {
    fn validate_unique_value_sets(
        index: &IndexType,
        value_sets: &[Vec<document::NormalValue>],
    ) -> Result<()> {
        if !matches!(index, IndexType::Unique(_)) {
            return Ok(());
        }

        for (position, values) in value_sets.iter().enumerate() {
            if values.iter().any(document::NormalValue::is_nil) {
                continue;
            }
            if value_sets[..position].contains(values) {
                return Err(Error::Storage(
                    storage::corekv::Error::UniqueConstraintViolation,
                ));
            }
        }
        Ok(())
    }

    /// Create a new empty IndexManager.
    pub fn new(collection_short_id: u32) -> Self {
        Self {
            collection_short_id,
            collection_id: String::new(),
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
        Self::from_indexes(collection_short_id, schema, &schema.indexes)
    }

    /// Load a selected set of regular indexes plus all full-text indexes from a collection schema.
    pub fn from_indexes(
        collection_short_id: u32,
        schema: &CollectionVersion,
        indexes: &[IndexDescription],
    ) -> Result<Self> {
        let mut manager = Self::new(collection_short_id);
        manager.collection_id = schema.collection_id.clone();
        for desc in indexes {
            if desc.fields.is_empty() {
                return Err(Error::Other(format!(
                    "index '{}' in schema has no fields",
                    desc.name
                )));
            }
            // `try_new` rather than `new`: a stored vector description that
            // cannot be built must say so, not quietly load as an ordered index
            // over the raw vector field.
            let index = IndexType::try_new(collection_short_id, desc.clone())?;
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
                kind: None,
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
        self.create_index_of_kind(
            datastore,
            collection_name,
            name,
            fields,
            IndexKind::Ordered(OrderedIndexDescription { unique }),
            schema_fields,
        )
        .await
    }

    /// Creates an index of any kind.
    ///
    /// [`create_index`](Self::create_index) is this with an ordered kind. The
    /// kind is a single argument rather than a `unique` flag plus a config,
    /// because those two can disagree and the resulting index would be in a
    /// state that means nothing.
    pub async fn create_index_of_kind(
        &mut self,
        datastore: &NamespaceView,
        collection_name: &str,
        name: String,
        fields: Vec<IndexedFieldDescription>,
        kind: IndexKind,
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
            unique: false,
            kind: Some(kind),
            auto_generated: false,
        }
        .normalized();

        // `try_new`, so a vector index with out-of-range parameters is refused
        // here rather than becoming an ordered index over the vector field.
        let index = IndexType::try_new(self.collection_short_id, desc.clone())?;
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
            Some(bytes) if bytes.len() == 4 => {
                u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
            }
            _ => 0,
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
    /// existing document data. Each document is paired with its node-local
    /// doc short ID.
    ///
    /// # Returns
    ///
    /// Returns a `BulkIndexResult` containing:
    /// - `indexed`: Number of documents successfully indexed.
    /// - `skipped`: Number of documents skipped (unset short ID).
    pub async fn bulk_index(
        &self,
        datastore: &NamespaceView,
        index_name: &str,
        documents: &[(u64, Document)],
        schema: &CollectionVersion,
    ) -> Result<BulkIndexResult> {
        let mut source = SliceSource::new(documents);
        self.bulk_index_from(datastore, index_name, &mut source, schema)
            .await
    }

    /// [`bulk_index`](Self::bulk_index) over a pull-based source, holding one
    /// document at a time rather than the whole collection.
    pub async fn bulk_index_from<S: DocumentSource + ?Sized>(
        &self,
        datastore: &NamespaceView,
        index_name: &str,
        source: &mut S,
        schema: &CollectionVersion,
    ) -> Result<BulkIndexResult> {
        let index = self
            .indexes
            .get(index_name)
            .ok_or_else(|| Error::Other(format!("index '{}' not found", index_name)))?;

        let mut indexed_count = 0;
        let mut skipped_count = 0;
        let mut mutable_datastore = datastore.clone();

        while let Some((doc_short_id, doc)) = source.next().await? {
            if doc_short_id == 0 {
                skipped_count += 1;
                continue;
            }

            let value_sets = self.extract_index_values(&doc, index.description(), schema)?;

            for values in &value_sets {
                index
                    .save(&mut mutable_datastore, doc_short_id, values)
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

    /// Save an index entry, healing stale unique conflicts (#1111/#700).
    ///
    /// A unique violation whose existing entry points at a **deleted or
    /// missing** document is not a real conflict — it is damage from an era
    /// when merge/delete paths did not maintain indexes (four production
    /// stores were wounded this way, and such an entry otherwise blocks the
    /// value forever with no repair affordance). Deletion is terminal in
    /// DefraDB, so the stale holder can never become live again: reclaim the
    /// entry and proceed.
    ///
    /// A violation whose holder is alive is a genuine conflict and propagates
    /// unchanged — local-write semantics stay strict (Go parity).
    async fn save_healing_stale_unique(
        &self,
        datastore: &mut NamespaceView,
        index: &IndexType,
        doc_short_id: u64,
        values: &[document::NormalValue],
    ) -> std::result::Result<(), storage::corekv::Error> {
        match index.save(datastore, doc_short_id, values).await {
            Err(storage::corekv::Error::UniqueConstraintViolation) => {
                let IndexType::Unique(unique) = index else {
                    return Err(storage::corekv::Error::UniqueConstraintViolation);
                };
                if self.collection_id.is_empty() {
                    return Err(storage::corekv::Error::UniqueConstraintViolation);
                }
                let Some(holder) = unique.conflicting_doc_id(datastore, values).await? else {
                    return Err(storage::corekv::Error::UniqueConstraintViolation);
                };
                if holder == doc_short_id {
                    // Re-indexing the same document with the same values is
                    // idempotent, not a conflict (content-addressed docIDs make
                    // identical-content recreates land on the same id).
                    return unique.save_blind(datastore, doc_short_id, values).await;
                }
                if doc_is_live(datastore, &self.collection_id, holder).await? {
                    return Err(storage::corekv::Error::UniqueConstraintViolation);
                }
                tracing::info!(
                    collection_id = %self.collection_id,
                    index = %index.description().name,
                    stale_doc_short_id = holder,
                    new_doc_short_id = doc_short_id,
                    "reclaimed stale unique index entry pointing at a deleted or missing document"
                );
                unique.save_blind(datastore, doc_short_id, values).await
            }
            other => other,
        }
    }

    /// Save an index entry on the MERGE path, resolving live unique conflicts
    /// deterministically instead of failing the merge (#1111).
    ///
    /// A CRDT merge cannot preserve cross-replica uniqueness: two nodes that
    /// each locally accepted the same unique value must still converge when
    /// they sync. Failing the merge (the previous behavior, and Go's current
    /// behavior) wedges the document's entire forward history on both sides —
    /// permanent retry, permanent divergence. Instead, both documents persist
    /// and the unique entry goes to a deterministic winner, so every replica
    /// converges to the same index no matter the merge order:
    ///
    /// **the lexicographically smallest docID wins.**
    ///
    /// docIDs are immutable and totally ordered, so the pick is stable across
    /// time and identical on every replica — no version/height state needed.
    /// The losing document remains fully readable by non-index (scan) queries
    /// and is reported loudly; applications keep their own reconcile policy
    /// (source-inc/gents#694 already does).
    async fn save_resolving_unique_conflict(
        &self,
        datastore: &mut NamespaceView,
        systemstore: &NamespaceView,
        index: &IndexType,
        doc_short_id: u64,
        doc_id: &str,
        values: &[document::NormalValue],
    ) -> std::result::Result<MergeConflictOutcome, storage::corekv::Error> {
        match self
            .save_healing_stale_unique(datastore, index, doc_short_id, values)
            .await
        {
            Ok(()) => Ok(MergeConflictOutcome::Clean),
            Err(storage::corekv::Error::UniqueConstraintViolation) => {
                let IndexType::Unique(unique) = index else {
                    return Err(storage::corekv::Error::UniqueConstraintViolation);
                };
                let Some(holder) = unique.conflicting_doc_id(datastore, values).await? else {
                    return Err(storage::corekv::Error::UniqueConstraintViolation);
                };
                // The deterministic winner is decided on the PUBLIC DocID, which
                // is byte-identical on every replica. The holder is stored as a
                // node-local short id (different per node), so it must be resolved
                // back to its public DocID before comparison — comparing short ids
                // would let replicas pick different winners and silently diverge.
                let Some(holder_doc_id) = holder_public_doc_id(systemstore, holder).await? else {
                    return Err(storage::corekv::Error::UniqueConstraintViolation);
                };
                if doc_id < holder_doc_id.as_str() {
                    unique.save_blind(datastore, doc_short_id, values).await?;
                    tracing::error!(
                        collection_id = %self.collection_id,
                        index = %index.description().name,
                        winner_doc_id = %doc_id,
                        unindexed_doc_id = %holder_doc_id,
                        "unique index conflict during merge: incoming document wins the \
                         deterministic pick; the previous holder is no longer indexed"
                    );
                    Ok(MergeConflictOutcome::IncomingIndexed)
                } else {
                    tracing::error!(
                        collection_id = %self.collection_id,
                        index = %index.description().name,
                        winner_doc_id = %holder_doc_id,
                        unindexed_doc_id = %doc_id,
                        "unique index conflict during merge: existing holder wins the \
                         deterministic pick; the incoming document is persisted unindexed"
                    );
                    Ok(MergeConflictOutcome::IncomingUnindexed)
                }
            }
            Err(other) => Err(other),
        }
    }

    /// Merge-path variant of [`Self::on_document_create`]: resolves live
    /// unique conflicts deterministically instead of erroring (#1111).
    pub async fn on_document_create_merge(
        &self,
        datastore: &NamespaceView,
        systemstore: &NamespaceView,
        doc: &Document,
        doc_short_id: u64,
        schema: &CollectionVersion,
    ) -> Result<()> {
        require_doc_short_id(doc_short_id)?;
        let doc_id = doc
            .id()
            .ok_or_else(|| Error::InvalidDocument("document must have an ID".to_string()))?
            .to_string();
        let mut mutable_datastore = datastore.clone();
        for index in self.indexes.values() {
            let value_sets = self.extract_index_values(doc, index.description(), schema)?;
            for values in &value_sets {
                self.save_resolving_unique_conflict(
                    &mut mutable_datastore,
                    systemstore,
                    index,
                    doc_short_id,
                    &doc_id,
                    values,
                )
                .await
                .map_err(Error::Storage)?;
            }
        }
        Ok(())
    }

    /// Merge-path variant of [`Self::on_document_update`]: resolves live
    /// unique conflicts deterministically instead of erroring (#1111).
    pub async fn on_document_update_merge(
        &self,
        datastore: &NamespaceView,
        systemstore: &NamespaceView,
        old_doc: &Document,
        new_doc: &Document,
        doc_short_id: u64,
        schema: &CollectionVersion,
    ) -> Result<()> {
        require_doc_short_id(doc_short_id)?;
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
                        .delete(&mut mutable_datastore, doc_short_id, old_values)
                        .await
                        .map_err(Error::Storage)?;
                }
                for new_values in &new_value_sets {
                    self.save_resolving_unique_conflict(
                        &mut mutable_datastore,
                        systemstore,
                        index,
                        doc_short_id,
                        &doc_id,
                        new_values,
                    )
                    .await
                    .map_err(Error::Storage)?;
                }
            }
        }
        Ok(())
    }

    /// Update indexes when a document is created.
    pub async fn on_document_create(
        &self,
        datastore: &NamespaceView,
        doc: &Document,
        doc_short_id: u64,
        schema: &CollectionVersion,
    ) -> Result<()> {
        require_doc_short_id(doc_short_id)?;

        let mut mutable_datastore = datastore.clone();

        for index in self.indexes.values() {
            let value_sets = self.extract_index_values(doc, index.description(), schema)?;
            Self::validate_unique_value_sets(index, &value_sets)?;
            for values in &value_sets {
                // Local creates stay strict but self-heal stale unique entries
                // pointing at a deleted or missing document (#1111/#700).
                self.save_healing_stale_unique(&mut mutable_datastore, index, doc_short_id, values)
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
        doc_short_id: u64,
        schema: &CollectionVersion,
    ) -> Result<()> {
        require_doc_short_id(doc_short_id)?;

        let mut mutable_datastore = datastore.clone();

        for index in self.indexes.values() {
            let old_value_sets = self.extract_index_values(old_doc, index.description(), schema)?;
            let new_value_sets = self.extract_index_values(new_doc, index.description(), schema)?;

            if old_value_sets != new_value_sets {
                Self::validate_unique_value_sets(index, &new_value_sets)?;
                for old_values in &old_value_sets {
                    index
                        .delete(&mut mutable_datastore, doc_short_id, old_values)
                        .await
                        .map_err(Error::Storage)?;
                }
                for new_values in &new_value_sets {
                    self.save_healing_stale_unique(
                        &mut mutable_datastore,
                        index,
                        doc_short_id,
                        new_values,
                    )
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
        doc_short_id: u64,
        schema: &CollectionVersion,
    ) -> Result<()> {
        require_doc_short_id(doc_short_id)?;

        let mut mutable_datastore = datastore.clone();

        for index in self.indexes.values() {
            let value_sets = self.extract_index_values(doc, index.description(), schema)?;
            for values in &value_sets {
                index
                    .delete(&mut mutable_datastore, doc_short_id, values)
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

/// Whether the document a unique index entry points at is still alive: its
/// body exists and it carries no logical-deletion marker. Runs against the
/// same transactional view as the index write, so the answer is consistent
/// with the mutation being attempted.
async fn doc_is_live(
    datastore: &NamespaceView,
    collection_id: &str,
    doc_short_id: u64,
) -> std::result::Result<bool, storage::corekv::Error> {
    let body = storage::keys::doc_key(collection_id, doc_short_id);
    if !datastore.has(&body).await? {
        return Ok(false);
    }
    let deleted = storage::keys::deleted_doc_key(collection_id, doc_short_id);
    Ok(!datastore.has(&deleted).await?)
}

/// Resolve a unique entry holder's node-local short ID to its public DocID via
/// the systemstore doc-ID index.
///
/// The deterministic merge winner MUST be chosen on this public DocID, which is
/// byte-identical on every replica; the short id is node-local and differs per
/// node, so comparing short ids would break cross-replica convergence.
async fn holder_public_doc_id(
    systemstore: &NamespaceView,
    doc_short_id: u64,
) -> std::result::Result<Option<String>, storage::corekv::Error> {
    let key = storage::keys::DocShortIDToDocIDKey::new(doc_short_id).bytes();
    match systemstore.get(&key).await? {
        Some(bytes) => {
            Ok(Some(String::from_utf8(bytes).map_err(|e| {
                storage::corekv::Error::Other(e.to_string())
            })?))
        }
        None => Ok(None),
    }
}
