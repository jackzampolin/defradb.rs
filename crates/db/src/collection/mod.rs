/// Collection struct for DefraDB matching Go's internal/db/collection.go.
///
/// A Collection represents a set of documents that share the same schema.
/// It provides CRUD operations for documents.
///
/// # Transaction Semantics
///
/// Methods that perform both document storage and index updates (e.g., `create_with_indexes`,
/// `update_with_indexes`, `delete_with_indexes`) are NOT atomic within a single operation.
/// If the document write succeeds but the index update fails, the caller MUST discard the
/// transaction (do not commit) to maintain consistency. The underlying transaction will
/// roll back both operations when discarded.
use crate::error::{Error, Result};
use crate::index::IndexManager;
use datastore::NamespaceView;
use defra_core::ActionStatus;
use document::{DocID, Document, NormalValue};
use schema::{
    legacy_collection_short_id, CollectionVersion, FieldKind, IndexDescription, ScalarArrayKind,
    ScalarKind,
};
use storage::corekv::{IterOptions, Key};
use storage::keys::doc_id_index::encode_doc_short_id;
use storage::keys::systemstore::{CollectionID, CollectionIDSequenceKey};

pub mod acp;
pub mod cache;
mod crud;
mod index;
pub mod loader;
pub(crate) mod locks;
pub mod name;
pub(crate) mod ops;
pub(crate) mod provider;
pub mod retriever;
pub mod selector;
pub mod snapshot;
pub mod stream;
mod truncator;
pub mod validation;

pub use truncator::DbCollectionTruncator;

/// Derive the legacy short ID from a collection_id string.
///
/// This is retained only for compatibility with older metadata and tests. Store-backed code
/// should resolve or require the persisted `root_id` instead.
#[deprecated(note = "use persisted collection root IDs instead")]
pub fn collection_short_id(collection_id: &str) -> u32 {
    legacy_collection_short_id(collection_id)
}

/// Load the persisted short ID for a collection if one exists.
pub async fn load_persisted_collection_short_id(
    systemstore: &NamespaceView,
    collection_id: &str,
) -> Result<Option<u32>> {
    let short_id_key = CollectionID::new(collection_id);
    let Some(short_id_bytes) = systemstore
        .get(&short_id_key.bytes())
        .await
        .map_err(Error::Storage)?
    else {
        return Ok(None);
    };

    let Ok(short_id_str) = String::from_utf8(short_id_bytes.to_vec()) else {
        return Ok(None);
    };

    Ok(short_id_str.parse::<u32>().ok())
}

/// Load the persisted root ID for a collection, returning an error if the mapping is missing.
pub async fn require_persisted_collection_short_id(
    systemstore: &NamespaceView,
    collection_id: &str,
) -> Result<u32> {
    load_persisted_collection_short_id(systemstore, collection_id)
        .await?
        .ok_or_else(|| {
            Error::Other(format!(
                "missing persisted collection root_id for collection_id '{}'",
                collection_id
            ))
        })
}

/// Load the persisted root ID for a collection, allocating one if it does not exist yet.
pub async fn ensure_persisted_collection_short_id(
    systemstore: &NamespaceView,
    collection_id: &str,
) -> Result<u32> {
    if let Some(short_id) = load_persisted_collection_short_id(systemstore, collection_id).await? {
        return Ok(short_id);
    }

    let seq_key = CollectionIDSequenceKey;
    let key_bytes = seq_key.bytes();
    let current: u32 = match systemstore.get(&key_bytes).await.map_err(Error::Storage)? {
        Some(bytes) if bytes.len() == 4 => {
            u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
        }
        _ => 0,
    };
    let next = current + 1;
    systemstore
        .set(&key_bytes, &next.to_be_bytes())
        .await
        .map_err(Error::Storage)?;

    let short_id_key = CollectionID::new(collection_id);
    systemstore
        .set(&short_id_key.bytes(), next.to_string().as_bytes())
        .await
        .map_err(Error::Storage)?;

    Ok(next)
}

/// Populate a deserialized collection schema with its persisted root ID.
pub async fn populate_collection_root_id(
    systemstore: &NamespaceView,
    schema: &mut CollectionVersion,
) -> Result<()> {
    if schema.root_id == 0 {
        schema.root_id =
            require_persisted_collection_short_id(systemstore, &schema.collection_id).await?;
    }
    Ok(())
}

/// Key prefix for document data in datastore.
pub(super) const DOC_KEY_PREFIX: &[u8] = b"/d/";

/// Key prefix for per-document schema versions in datastore.
pub(super) const VERSION_KEY_PREFIX: &[u8] = b"/v/";

/// Marker byte indicating a document is deleted (matches Go's DeletedObjectMarker).
pub(super) const DELETED_MARKER: u8 = 0x01;

/// A collection of documents with a shared schema.
#[derive(Debug, Clone)]
pub struct Collection {
    /// The collection schema definition.
    def: CollectionVersion,
    write_indexes: Vec<IndexDescription>,
    queryable_indexes: Vec<IndexDescription>,
}

impl Collection {
    /// Create a new collection with the given schema definition.
    pub fn new(def: CollectionVersion) -> Self {
        let indexes = def.indexes.clone();
        Self {
            def,
            write_indexes: indexes.clone(),
            queryable_indexes: indexes,
        }
    }

    pub(crate) fn with_index_actions(
        def: CollectionVersion,
        actions: &std::collections::HashMap<u32, ActionStatus>,
    ) -> Self {
        let write_indexes = def
            .indexes
            .iter()
            .filter(|index| actions.get(&index.id) != Some(&ActionStatus::ERRORED))
            .cloned()
            .collect();
        let queryable_indexes = def
            .indexes
            .iter()
            .filter(|index| !actions.contains_key(&index.id))
            .cloned()
            .collect();
        Self {
            def,
            write_indexes,
            queryable_indexes,
        }
    }

    /// Get the collection name.
    pub fn name(&self) -> &str {
        &self.def.name
    }

    /// Get the collection ID.
    pub fn collection_id(&self) -> &str {
        &self.def.collection_id
    }

    /// Get the collection version ID (content hash of this schema version).
    ///
    /// Go uses `VersionID()` for the `collectionVersionID` field in CRDT delta blocks.
    pub fn version_id(&self) -> &str {
        &self.def.version_id
    }

    /// Get the collection schema.
    pub fn schema(&self) -> &CollectionVersion {
        &self.def
    }

    /// Get the storage prefix ID used for indexes, heads, and view cache keys.
    pub fn resolved_root_id(&self) -> u32 {
        self.def.resolved_root_id()
    }

    /// Get all indexes defined on this collection.
    pub fn get_indexes(&self) -> &[IndexDescription] {
        &self.def.indexes
    }

    pub(crate) fn write_indexes(&self) -> &[IndexDescription] {
        &self.write_indexes
    }

    pub(crate) fn queryable_indexes(&self) -> &[IndexDescription] {
        &self.queryable_indexes
    }

    pub(crate) fn schema_for_queries(&self) -> CollectionVersion {
        let mut schema = self.def.clone();
        schema.indexes.clone_from(&self.queryable_indexes);
        schema
    }

    /// Check if an index exists on this collection.
    pub fn has_index(&self, name: &str) -> bool {
        self.def.indexes.iter().any(|idx| idx.name == name)
    }

    /// Get an index by name.
    pub fn get_index(&self, name: &str) -> Option<&IndexDescription> {
        self.def.indexes.iter().find(|idx| idx.name == name)
    }

    /// Generate the storage key for a document blob.
    ///
    /// Keyed by the node-local doc short ID: iteration order over a
    /// collection is allocation (insertion) order, matching Go v1.0.0's
    /// short-ID-keyed datastore (#4838). Delegates to the shared key helper
    /// so the write, merge, and index layers agree on the layout (#1111).
    pub fn doc_key(&self, doc_short_id: u64) -> Vec<u8> {
        storage::keys::doc_key(&self.def.collection_id, doc_short_id)
    }

    /// Generate the storage key for a document's deletion marker.
    pub(crate) fn deleted_key(&self, doc_short_id: u64) -> Vec<u8> {
        storage::keys::deleted_doc_key(&self.def.collection_id, doc_short_id)
    }

    /// Generate the storage key for a document's schema version.
    ///
    /// The version is stored separately from the document data to enable
    /// efficient version checks without deserializing the full document.
    ///
    /// Key format: /v/<collection_id>/<doc_short_id> — a prefix distinct
    /// from doc blobs so blob scans never have to filter version keys.
    pub(crate) fn version_key(&self, doc_short_id: u64) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend_from_slice(VERSION_KEY_PREFIX);
        key.extend_from_slice(self.def.collection_id.as_bytes());
        key.push(b'/');
        key.extend_from_slice(&encode_doc_short_id(doc_short_id));
        key
    }

    /// Generate the key prefix for iterating collection documents.
    pub(crate) fn collection_key_prefix(&self) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend_from_slice(DOC_KEY_PREFIX);
        key.extend_from_slice(self.def.collection_id.as_bytes());
        key.push(b'/');
        key
    }

    /// Resolve a public DocID to this collection's doc short ID.
    ///
    /// Returns `None` for unknown documents or documents belonging to a
    /// different collection.
    pub async fn resolve_doc_short_id(
        &self,
        systemstore: &NamespaceView,
        doc_id: &DocID,
    ) -> Result<Option<u64>> {
        crate::docid::map::get_doc_short_id(
            systemstore,
            self.resolved_root_id(),
            &doc_id.to_string(),
        )
        .await
    }

    /// Resolve an input DocID or alias to its local short ID and canonical
    /// genesis-derived DocID.
    pub(crate) async fn resolve_doc_identity(
        &self,
        systemstore: &NamespaceView,
        doc_id: &DocID,
    ) -> Result<Option<(u64, DocID)>> {
        let Some(doc_short_id) = self.resolve_doc_short_id(systemstore, doc_id).await? else {
            return Ok(None);
        };
        let canonical = crate::docid::map::get_doc_id(systemstore, doc_short_id)
            .await?
            .ok_or_else(|| {
                Error::InvalidDocument(format!(
                    "document short ID {doc_short_id} has no canonical DocID"
                ))
            })?
            .parse::<DocID>()?;
        Ok(Some((doc_short_id, canonical)))
    }

    /// Resolve an input DocID or alias, returning the canonical identity.
    pub(crate) async fn require_doc_identity(
        &self,
        systemstore: &NamespaceView,
        doc_id: &DocID,
    ) -> Result<(u64, DocID)> {
        self.resolve_doc_identity(systemstore, doc_id)
            .await?
            .ok_or_else(|| Error::DocumentNotFound(doc_id.to_string()))
    }

    /// Store the schema version for a document.
    pub(crate) async fn store_version(
        &self,
        datastore: &NamespaceView,
        doc_short_id: u64,
    ) -> Result<()> {
        let key = self.version_key(doc_short_id);
        let version = self.def.version_id.as_bytes();
        datastore.set(&key, version).await.map_err(Error::Storage)
    }

    /// Load the schema version for a document.
    pub(crate) async fn load_version(
        &self,
        datastore: &NamespaceView,
        doc_short_id: u64,
    ) -> Result<Option<String>> {
        let key = self.version_key(doc_short_id);

        match datastore.get(&key).await.map_err(Error::Storage)? {
            Some(bytes) => {
                let version = String::from_utf8(bytes.into())
                    .map_err(|e| Error::text_decode("invalid version encoding", e))?;
                Ok(Some(version))
            }
            None => Ok(None),
        }
    }

    /// Delete the schema version for a document.
    pub(crate) async fn delete_version(
        &self,
        datastore: &NamespaceView,
        doc_short_id: u64,
    ) -> Result<()> {
        let key = self.version_key(doc_short_id);
        datastore.delete(&key).await.map_err(Error::Storage)
    }
}
