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
mod crud;
mod crud_datastore;
mod index_ops;
mod validation;

use crate::error::{Error, Result};
use crate::index_manager::IndexManager;
use crate::txn::DbTxn;
use datastore::NamespaceView;
use document::{DocID, Document, NormalValue};
use schema::{
    legacy_collection_short_id, CollectionVersion, FieldKind, IndexDescription, ScalarArrayKind,
    ScalarKind,
};
use storage::corekv::{IterOptions, Key, Store};
use storage::keys::systemstore::{CollectionID, CollectionIDSequenceKey};

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

/// Key prefix for document deletion markers in datastore.
/// Deleted documents have their data stored at /d/ and a marker at /del/
pub(super) const DELETED_KEY_PREFIX: &[u8] = b"/del/";

/// Marker byte indicating a document is deleted (matches Go's DeletedObjectMarker).
pub(super) const DELETED_MARKER: u8 = 0x01;

/// A collection of documents with a shared schema.
#[derive(Debug, Clone)]
pub struct Collection {
    /// The collection schema definition.
    def: CollectionVersion,
}

impl Collection {
    /// Create a new collection with the given schema definition.
    pub fn new(def: CollectionVersion) -> Self {
        Self { def }
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

    /// Check if an index exists on this collection.
    pub fn has_index(&self, name: &str) -> bool {
        self.def.indexes.iter().any(|idx| idx.name == name)
    }

    /// Get an index by name.
    pub fn get_index(&self, name: &str) -> Option<&IndexDescription> {
        self.def.indexes.iter().find(|idx| idx.name == name)
    }

    /// Generate the storage key for a document.
    pub(crate) fn doc_key(&self, doc_id: &DocID) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend_from_slice(DOC_KEY_PREFIX);
        key.extend_from_slice(self.def.collection_id.as_bytes());
        key.push(b'/');
        key.extend_from_slice(doc_id.to_string().as_bytes());
        key
    }

    /// Generate the storage key for a document's deletion marker.
    pub(crate) fn deleted_key(&self, doc_id: &DocID) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend_from_slice(DELETED_KEY_PREFIX);
        key.extend_from_slice(self.def.collection_id.as_bytes());
        key.push(b'/');
        key.extend_from_slice(doc_id.to_string().as_bytes());
        key
    }

    /// Generate the key prefix for all deletion markers in this collection.
    #[allow(dead_code)]
    pub(crate) fn deleted_key_prefix(&self) -> Vec<u8> {
        let mut prefix = Vec::new();
        prefix.extend_from_slice(DELETED_KEY_PREFIX);
        prefix.extend_from_slice(self.def.collection_id.as_bytes());
        prefix.push(b'/');
        prefix
    }

    /// Generate the storage key for a document's schema version.
    ///
    /// The version is stored separately from the document data to enable
    /// efficient version checks without deserializing the full document.
    ///
    /// Key format: /d/<collection_id>/<doc_id>/v
    pub(crate) fn version_key(&self, doc_id: &DocID) -> Vec<u8> {
        let mut key = self.doc_key(doc_id);
        key.push(b'/');
        key.push(b'v'); // DATASTORE_DOC_VERSION_FIELD_ID
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

    /// Store the schema version for a document.
    pub(crate) async fn store_version(
        &self,
        datastore: &NamespaceView,
        doc_id: &DocID,
    ) -> Result<()> {
        let key = self.version_key(doc_id);
        let version = self.def.version_id.as_bytes();
        datastore.set(&key, version).await.map_err(Error::Storage)
    }

    /// Load the schema version for a document.
    pub(crate) async fn load_version(
        &self,
        datastore: &NamespaceView,
        doc_id: &DocID,
    ) -> Result<Option<String>> {
        let key = self.version_key(doc_id);

        match datastore.get(&key).await.map_err(Error::Storage)? {
            Some(bytes) => {
                let version = String::from_utf8(bytes)
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
        doc_id: &DocID,
    ) -> Result<()> {
        let key = self.version_key(doc_id);
        datastore.delete(&key).await.map_err(Error::Storage)
    }
}
