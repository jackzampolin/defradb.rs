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
use std::hash::{Hash, Hasher};

use crate::error::{Error, Result};
use crate::index_manager::IndexManager;
use crate::txn::DbTxn;
use datastore::NamespaceView;
use document::{DocID, Document, NormalValue};
use schema::{CollectionVersion, FieldKind, IndexDescription, ScalarArrayKind, ScalarKind};
use storage::corekv::{IterOptions, Store};

/// Derive a short u32 ID from a collection_id string.
/// Uses a simple hash to ensure determinism and uniqueness.
pub fn collection_short_id(collection_id: &str) -> u32 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    collection_id.hash(&mut hasher);
    hasher.finish() as u32
}

/// Key prefix for document data in datastore.
const DOC_KEY_PREFIX: &[u8] = b"/d/";

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

    /// Get the collection schema.
    pub fn schema(&self) -> &CollectionVersion {
        &self.def
    }

    // =========================================================================
    // Index Methods
    // =========================================================================

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

    // =========================================================================
    // Document Methods with Index Maintenance
    // =========================================================================

    /// Create a new document and update all indexes.
    ///
    /// This method wraps the standard create operation with index maintenance.
    pub async fn create_with_indexes(
        &self,
        datastore: &NamespaceView,
        doc: &Document,
        index_manager: &IndexManager,
    ) -> Result<DocID> {
        // Validate document against schema
        self.validate_document(doc)?;

        // Generate document ID if not present
        let doc_id = doc
            .id()
            .cloned()
            .ok_or_else(|| Error::InvalidDocument("Document must have an ID".into()))?;

        // Check if document already exists
        let key = self.doc_key(&doc_id);
        if datastore.has(&key).await.map_err(Error::Storage)? {
            return Err(Error::InvalidDocument(format!(
                "Document with ID {} already exists",
                doc_id
            )));
        }

        // Serialize document to CBOR
        let data = doc
            .to_cbor()
            .map_err(|e| Error::Serialization(e.to_string()))?;

        // Store document
        datastore.set(&key, &data).await.map_err(Error::Storage)?;

        // Store schema version for lens migration support
        self.store_version(datastore, &doc_id).await?;

        // Update indexes
        index_manager
            .on_document_create(datastore, doc, &self.def)
            .await?;

        Ok(doc_id)
    }

    /// Update an existing document and maintain all indexes.
    ///
    /// This method wraps the standard update operation with index maintenance.
    pub async fn update_with_indexes(
        &self,
        datastore: &NamespaceView,
        doc: &Document,
        index_manager: &IndexManager,
    ) -> Result<()> {
        // Validate document against schema
        self.validate_document(doc)?;

        let doc_id = doc
            .id()
            .ok_or_else(|| Error::InvalidDocument("Document must have an ID".into()))?;

        let key = self.doc_key(doc_id);

        // Get old document for index update
        let old_doc = match datastore.get(&key).await.map_err(Error::Storage)? {
            Some(bytes) => {
                let mut d =
                    Document::from_cbor(&bytes).map_err(|e| Error::Serialization(e.to_string()))?;
                d.set_id(doc_id.clone());
                d
            }
            None => return Err(Error::DocumentNotFound(doc_id.to_string())),
        };

        // Serialize and store
        let data = doc
            .to_cbor()
            .map_err(|e| Error::Serialization(e.to_string()))?;

        datastore.set(&key, &data).await.map_err(Error::Storage)?;

        // Update indexes
        index_manager
            .on_document_update(datastore, &old_doc, doc, &self.def)
            .await?;

        Ok(())
    }

    /// Delete a document and update all indexes.
    ///
    /// This method wraps the standard delete operation with index maintenance.
    pub async fn delete_with_indexes(
        &self,
        datastore: &NamespaceView,
        doc_id: &DocID,
        index_manager: &IndexManager,
    ) -> Result<bool> {
        let key = self.doc_key(doc_id);

        // Get the document for index cleanup
        let doc = match datastore.get(&key).await.map_err(Error::Storage)? {
            Some(bytes) => {
                let mut d =
                    Document::from_cbor(&bytes).map_err(|e| Error::Serialization(e.to_string()))?;
                d.set_id(doc_id.clone());
                d
            }
            None => return Ok(false),
        };

        // Delete document
        datastore.delete(&key).await.map_err(Error::Storage)?;

        // Update indexes
        index_manager
            .on_document_delete(datastore, &doc, &self.def)
            .await?;

        Ok(true)
    }

    // =========================================================================
    // Standard Document Methods (without index maintenance)
    // =========================================================================

    /// Create a new document in this collection.
    ///
    /// The document must have an ID set before calling this method.
    /// The document will be validated against the collection schema.
    /// The document's schema version will be set to the current collection version.
    pub async fn create<S: Store>(&self, txn: &DbTxn<S>, doc: &Document) -> Result<DocID> {
        // Validate document against schema
        self.validate_document(doc)?;

        // Generate document ID if not present
        let doc_id = doc
            .id()
            .cloned()
            .ok_or_else(|| Error::InvalidDocument("Document must have an ID".into()))?;

        let datastore = txn.datastore()?;

        // Check if document already exists
        let key = self.doc_key(&doc_id);
        if datastore.has(&key).await.map_err(Error::Storage)? {
            return Err(Error::InvalidDocument(format!(
                "Document with ID {} already exists",
                doc_id
            )));
        }

        // Serialize document to CBOR
        let data = doc
            .to_cbor()
            .map_err(|e| Error::Serialization(e.to_string()))?;

        // Store document
        datastore.set(&key, &data).await.map_err(Error::Storage)?;

        // Store schema version
        self.store_version(&datastore, &doc_id).await?;

        Ok(doc_id)
    }

    /// Get a document by ID.
    ///
    /// The returned document will have its schema version set if stored.
    pub async fn get<S: Store>(&self, txn: &DbTxn<S>, doc_id: &DocID) -> Result<Option<Document>> {
        let datastore = txn.datastore()?;
        let key = self.doc_key(doc_id);
        let data = datastore.get(&key).await.map_err(Error::Storage)?;

        match data {
            Some(bytes) => {
                let mut doc =
                    Document::from_cbor(&bytes).map_err(|e| Error::Serialization(e.to_string()))?;
                // Set the document ID (stored as part of the key, not in the serialized document)
                doc.set_id(doc_id.clone());

                // Load and set the schema version
                if let Some(version) = self.load_version(&datastore, doc_id).await? {
                    doc.set_schema_version_id(version);
                }

                Ok(Some(doc))
            }
            None => Ok(None),
        }
    }

    /// Update an existing document.
    ///
    /// The document will be validated against the collection schema.
    pub async fn update<S: Store>(&self, txn: &DbTxn<S>, doc: &Document) -> Result<()> {
        // Validate document against schema
        self.validate_document(doc)?;

        let doc_id = doc
            .id()
            .ok_or_else(|| Error::InvalidDocument("Document must have an ID".into()))?;

        let key = self.doc_key(doc_id);

        // Check document exists
        if !txn.datastore()?.has(&key).await.map_err(Error::Storage)? {
            return Err(Error::DocumentNotFound(doc_id.to_string()));
        }

        // Serialize and store
        let data = doc
            .to_cbor()
            .map_err(|e| Error::Serialization(e.to_string()))?;

        txn.datastore()?
            .set(&key, &data)
            .await
            .map_err(Error::Storage)?;

        Ok(())
    }

    /// Delete a document by ID.
    ///
    /// Also deletes the document's schema version.
    pub async fn delete<S: Store>(&self, txn: &DbTxn<S>, doc_id: &DocID) -> Result<bool> {
        let datastore = txn.datastore()?;
        let key = self.doc_key(doc_id);

        // Check if document exists
        if !datastore.has(&key).await.map_err(Error::Storage)? {
            return Ok(false);
        }

        // Delete document
        datastore.delete(&key).await.map_err(Error::Storage)?;

        // Delete schema version
        self.delete_version(&datastore, doc_id).await?;

        Ok(true)
    }

    /// Check if a document exists.
    pub async fn exists<S: Store>(&self, txn: &DbTxn<S>, doc_id: &DocID) -> Result<bool> {
        let key = self.doc_key(doc_id);
        txn.datastore()?.has(&key).await.map_err(Error::Storage)
    }

    /// Save a document (create or update).
    ///
    /// The document will be validated against the collection schema.
    pub async fn save<S: Store>(&self, txn: &DbTxn<S>, doc: &Document) -> Result<DocID> {
        // Validate document against schema
        self.validate_document(doc)?;

        let doc_id = doc
            .id()
            .cloned()
            .ok_or_else(|| Error::InvalidDocument("Document must have an ID".into()))?;

        let key = self.doc_key(&doc_id);

        // Serialize and store (upsert)
        let data = doc
            .to_cbor()
            .map_err(|e| Error::Serialization(e.to_string()))?;

        txn.datastore()?
            .set(&key, &data)
            .await
            .map_err(Error::Storage)?;

        Ok(doc_id)
    }

    /// Iterate over all documents in the collection.
    pub async fn iterate<S: Store, F, Fut>(&self, txn: &DbTxn<S>, mut callback: F) -> Result<()>
    where
        F: FnMut(Document) -> Fut,
        Fut: std::future::Future<Output = Result<bool>>,
    {
        let prefix = self.collection_key_prefix();
        let opts = IterOptions::new().with_prefix(prefix);

        let mut iter = txn
            .datastore()?
            .iterator(opts)
            .await
            .map_err(Error::Storage)?;

        while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
            let mut doc = Document::from_cbor(&pair.value).map_err(|e| {
                Error::Serialization(format!(
                    "failed to deserialize document at key {:?}: {}",
                    String::from_utf8_lossy(&pair.key),
                    e
                ))
            })?;

            // Extract doc_id from key (format: /d/<collection_id>/<doc_id>)
            // Find the last '/' and extract the doc_id string after it
            if let Some(pos) = pair.key.iter().rposition(|&b| b == b'/') {
                let doc_id_str = String::from_utf8_lossy(&pair.key[pos + 1..]);
                if let Ok(doc_id) = doc_id_str.parse::<DocID>() {
                    doc.set_id(doc_id);
                }
            }

            // Callback returns true to continue, false to stop
            if !callback(doc).await? {
                break;
            }
        }

        iter.close().await.map_err(Error::Storage)?;
        Ok(())
    }

    /// Get all documents in the collection.
    pub async fn get_all<S: Store>(&self, txn: &DbTxn<S>) -> Result<Vec<Document>> {
        let mut docs = Vec::new();

        self.iterate(txn, |doc| {
            docs.push(doc);
            async { Ok(true) }
        })
        .await?;

        Ok(docs)
    }

    // =========================================================================
    // Methods that take NamespaceView directly (for Send-safe async contexts)
    // =========================================================================

    /// Get a document by ID using a NamespaceView directly.
    ///
    /// This method takes `NamespaceView` instead of `&DbTxn` to allow
    /// use in async contexts where `Send` futures are required.
    /// The returned document will have its schema version set if stored.
    pub async fn get_with_datastore(
        &self,
        datastore: &NamespaceView,
        doc_id: &DocID,
    ) -> Result<Option<Document>> {
        let key = self.doc_key(doc_id);
        let data = datastore.get(&key).await.map_err(Error::Storage)?;

        match data {
            Some(bytes) => {
                let mut doc =
                    Document::from_cbor(&bytes).map_err(|e| Error::Serialization(e.to_string()))?;
                // Set the document ID (stored as part of the key, not in the serialized document)
                doc.set_id(doc_id.clone());

                // Load and set the schema version
                if let Some(version) = self.load_version(datastore, doc_id).await? {
                    doc.set_schema_version_id(version);
                }

                Ok(Some(doc))
            }
            None => Ok(None),
        }
    }

    /// Get all documents in the collection using a NamespaceView directly.
    ///
    /// This method takes `NamespaceView` instead of `&DbTxn` to allow
    /// use in async contexts where `Send` futures are required.
    /// Each returned document will have its schema version set if stored.
    pub async fn get_all_with_datastore(&self, datastore: &NamespaceView) -> Result<Vec<Document>> {
        let prefix = self.collection_key_prefix();
        let opts = IterOptions::new().with_prefix(prefix);

        let mut iter = datastore.iterator(opts).await.map_err(Error::Storage)?;

        let mut docs = Vec::new();
        while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
            // Skip version keys (end with /v)
            if pair.key.ends_with(b"/v") {
                continue;
            }

            let mut doc = Document::from_cbor(&pair.value).map_err(|e| {
                Error::Serialization(format!(
                    "failed to deserialize document at key {:?}: {}",
                    String::from_utf8_lossy(&pair.key),
                    e
                ))
            })?;

            // Extract doc_id from key (format: /d/<collection_id>/<doc_id>)
            // Find the last '/' and extract the doc_id string after it
            if let Some(pos) = pair.key.iter().rposition(|&b| b == b'/') {
                let doc_id_str = String::from_utf8_lossy(&pair.key[pos + 1..]);
                if let Ok(doc_id) = doc_id_str.parse::<DocID>() {
                    doc.set_id(doc_id.clone());

                    // Load and set the schema version
                    if let Some(version) = self.load_version(datastore, &doc_id).await? {
                        doc.set_schema_version_id(version);
                    }
                }
            }

            docs.push(doc);
        }

        iter.close().await.map_err(Error::Storage)?;
        Ok(docs)
    }

    /// Create a new document using a NamespaceView directly.
    ///
    /// This method takes `NamespaceView` instead of `&DbTxn` to allow
    /// use in async contexts where `Send` futures are required.
    /// The document's schema version will be set to the current collection version.
    pub async fn create_with_datastore(
        &self,
        datastore: &NamespaceView,
        doc: &Document,
    ) -> Result<DocID> {
        // Validate document against schema
        self.validate_document(doc)?;

        // Require document ID
        let doc_id = doc
            .id()
            .cloned()
            .ok_or_else(|| Error::InvalidDocument("Document must have an ID".into()))?;

        // Check if document already exists
        let key = self.doc_key(&doc_id);
        if datastore.has(&key).await.map_err(Error::Storage)? {
            return Err(Error::InvalidDocument(format!(
                "Document with ID {} already exists",
                doc_id
            )));
        }

        // Serialize document to CBOR
        let data = doc
            .to_cbor()
            .map_err(|e| Error::Serialization(e.to_string()))?;

        // Store document
        datastore.set(&key, &data).await.map_err(Error::Storage)?;

        // Store schema version
        self.store_version(datastore, &doc_id).await?;

        Ok(doc_id)
    }

    /// Update an existing document using a NamespaceView directly.
    ///
    /// This method takes `NamespaceView` instead of `&DbTxn` to allow
    /// use in async contexts where `Send` futures are required.
    pub async fn update_with_datastore(
        &self,
        datastore: &NamespaceView,
        doc: &Document,
    ) -> Result<()> {
        // Validate document against schema
        self.validate_document(doc)?;

        let doc_id = doc
            .id()
            .ok_or_else(|| Error::InvalidDocument("Document must have an ID".into()))?;

        let key = self.doc_key(doc_id);

        // Check document exists
        if !datastore.has(&key).await.map_err(Error::Storage)? {
            return Err(Error::DocumentNotFound(doc_id.to_string()));
        }

        // Serialize and store
        let data = doc
            .to_cbor()
            .map_err(|e| Error::Serialization(e.to_string()))?;

        datastore.set(&key, &data).await.map_err(Error::Storage)?;

        Ok(())
    }

    /// Delete a document by ID using a NamespaceView directly.
    ///
    /// This method takes `NamespaceView` instead of `&DbTxn` to allow
    /// use in async contexts where `Send` futures are required.
    /// Also deletes the document's schema version.
    pub async fn delete_with_datastore(
        &self,
        datastore: &NamespaceView,
        doc_id: &DocID,
    ) -> Result<bool> {
        let key = self.doc_key(doc_id);

        // Check if document exists
        if !datastore.has(&key).await.map_err(Error::Storage)? {
            return Ok(false);
        }

        // Delete document
        datastore.delete(&key).await.map_err(Error::Storage)?;

        // Delete schema version
        self.delete_version(datastore, doc_id).await?;

        Ok(true)
    }

    /// Check if a document exists using a NamespaceView directly.
    ///
    /// This method takes `NamespaceView` instead of `&DbTxn` to allow
    /// use in async contexts where `Send` futures are required.
    pub async fn exists_with_datastore(
        &self,
        datastore: &NamespaceView,
        doc_id: &DocID,
    ) -> Result<bool> {
        let key = self.doc_key(doc_id);
        datastore.has(&key).await.map_err(Error::Storage)
    }

    /// Save a document (create or update) using a NamespaceView directly.
    ///
    /// This method takes `NamespaceView` instead of `&DbTxn` to allow
    /// use in async contexts where `Send` futures are required.
    ///
    /// Unlike `create_with_datastore`, this performs an upsert - it will
    /// create the document if it doesn't exist, or update it if it does.
    ///
    /// Note: Validation is skipped for P2P-synced documents since they
    /// may have been created with a different schema version. The document's
    /// schema version is preserved if set, otherwise the collection's version is used.
    pub async fn save_with_datastore(
        &self,
        datastore: &NamespaceView,
        doc: &Document,
    ) -> Result<DocID> {
        let doc_id = doc
            .id()
            .cloned()
            .ok_or_else(|| Error::InvalidDocument("Document must have an ID".into()))?;

        let key = self.doc_key(&doc_id);

        // Serialize and store (upsert)
        let data = doc
            .to_cbor()
            .map_err(|e| Error::Serialization(e.to_string()))?;

        datastore.set(&key, &data).await.map_err(Error::Storage)?;

        // Store schema version - preserve document's version if set, otherwise use collection's
        let version_key = self.version_key(&doc_id);
        let version = doc
            .schema_version_id()
            .unwrap_or(&self.def.version_id);
        datastore
            .set(&version_key, version.as_bytes())
            .await
            .map_err(Error::Storage)?;

        Ok(doc_id)
    }

    /// Generate the storage key for a document.
    fn doc_key(&self, doc_id: &DocID) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend_from_slice(DOC_KEY_PREFIX);
        key.extend_from_slice(self.def.collection_id.as_bytes());
        key.push(b'/');
        key.extend_from_slice(doc_id.to_string().as_bytes());
        key
    }

    /// Generate the storage key for a document's schema version.
    ///
    /// The version is stored separately from the document data to enable
    /// efficient version checks without deserializing the full document.
    ///
    /// Key format: /d/<collection_id>/<doc_id>/v
    fn version_key(&self, doc_id: &DocID) -> Vec<u8> {
        let mut key = self.doc_key(doc_id);
        key.push(b'/');
        key.push(b'v'); // DATASTORE_DOC_VERSION_FIELD_ID
        key
    }

    /// Generate the key prefix for iterating collection documents.
    fn collection_key_prefix(&self) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend_from_slice(DOC_KEY_PREFIX);
        key.extend_from_slice(self.def.collection_id.as_bytes());
        key.push(b'/');
        key
    }

    /// Store the schema version for a document.
    async fn store_version(&self, datastore: &NamespaceView, doc_id: &DocID) -> Result<()> {
        let key = self.version_key(doc_id);
        let version = self.def.version_id.as_bytes();
        datastore.set(&key, version).await.map_err(Error::Storage)
    }

    /// Load the schema version for a document.
    async fn load_version(&self, datastore: &NamespaceView, doc_id: &DocID) -> Result<Option<String>> {
        let key = self.version_key(doc_id);

        match datastore.get(&key).await.map_err(Error::Storage)? {
            Some(bytes) => {
                let version = String::from_utf8(bytes).map_err(|e| {
                    Error::Serialization(format!("Invalid version encoding: {}", e))
                })?;
                Ok(Some(version))
            }
            None => Ok(None),
        }
    }

    /// Delete the schema version for a document.
    async fn delete_version(&self, datastore: &NamespaceView, doc_id: &DocID) -> Result<()> {
        let key = self.version_key(doc_id);
        datastore.delete(&key).await.map_err(Error::Storage)
    }

    /// Validate a document against this collection's schema.
    ///
    /// Returns an error if the document contains fields with incorrect types.
    /// Unknown fields (not in schema) are allowed for flexibility.
    fn validate_document(&self, doc: &Document) -> Result<()> {
        for field_def in &self.def.fields {
            // Skip _docID field - it's handled separately
            if field_def.name == "_docID" {
                continue;
            }

            // Get the value for this field (if present)
            if let Some(value) = doc.get(&field_def.name) {
                // Validate the value type matches the schema
                if !is_value_compatible_with_kind(value, &field_def.kind) {
                    return Err(Error::InvalidDocument(format!(
                        "Field '{}' has incompatible type: expected {:?}, got {:?}",
                        field_def.name, field_def.kind, value
                    )));
                }
            }
            // Missing fields are allowed (nullable by default in DefraDB)
        }
        Ok(())
    }
}

/// Check if a NormalValue is compatible with a FieldKind.
fn is_value_compatible_with_kind(value: &NormalValue, kind: &FieldKind) -> bool {
    // Null is compatible with all nillable types (which is everything in DefraDB)
    if value.is_nil() {
        return true;
    }

    match kind {
        FieldKind::Scalar(scalar) => is_value_compatible_with_scalar(value, *scalar),
        FieldKind::ScalarArray(array) => is_value_compatible_with_array(value, *array),
        // Relations are stored as document IDs (strings) or nested documents
        FieldKind::Relation { is_array, .. }
        | FieldKind::SelfRef { is_array, .. }
        | FieldKind::Named { is_array, .. } => {
            if *is_array {
                matches!(
                    value,
                    NormalValue::StringArray(_) | NormalValue::DocumentArray(_)
                )
            } else {
                matches!(value, NormalValue::String(_) | NormalValue::Document(_))
            }
        }
    }
}

/// Check if a NormalValue is compatible with a ScalarKind.
fn is_value_compatible_with_scalar(value: &NormalValue, scalar: ScalarKind) -> bool {
    match scalar {
        ScalarKind::None => true,
        ScalarKind::DocID => matches!(value, NormalValue::String(_)),
        ScalarKind::Bool => matches!(value, NormalValue::Bool(_) | NormalValue::NillableBool(_)),
        ScalarKind::Int => matches!(value, NormalValue::Int(_) | NormalValue::NillableInt(_)),
        ScalarKind::Float64 => {
            matches!(
                value,
                NormalValue::Float64(_) | NormalValue::NillableFloat64(_)
            )
        }
        ScalarKind::Float32 => {
            matches!(
                value,
                NormalValue::Float32(_) | NormalValue::NillableFloat32(_)
            )
        }
        ScalarKind::DateTime => {
            matches!(value, NormalValue::Time(_) | NormalValue::NillableTime(_))
        }
        ScalarKind::String => {
            matches!(
                value,
                NormalValue::String(_) | NormalValue::NillableString(_)
            )
        }
        ScalarKind::Blob => {
            matches!(value, NormalValue::Bytes(_) | NormalValue::NillableBytes(_))
        }
        ScalarKind::Json => matches!(value, NormalValue::Json(_)),
    }
}

/// Check if a NormalValue is compatible with a ScalarArrayKind.
fn is_value_compatible_with_array(value: &NormalValue, array: ScalarArrayKind) -> bool {
    match array {
        ScalarArrayKind::BoolArray => matches!(value, NormalValue::BoolArray(_)),
        ScalarArrayKind::IntArray => matches!(value, NormalValue::IntArray(_)),
        ScalarArrayKind::Float64Array => matches!(value, NormalValue::Float64Array(_)),
        ScalarArrayKind::Float32Array => matches!(value, NormalValue::Float32Array(_)),
        ScalarArrayKind::StringArray => matches!(value, NormalValue::StringArray(_)),
        ScalarArrayKind::NillableBoolArray => {
            matches!(
                value,
                NormalValue::NillableBoolArray(_) | NormalValue::NillableBoolElementArray(_)
            )
        }
        ScalarArrayKind::NillableIntArray => {
            matches!(
                value,
                NormalValue::NillableIntArray(_) | NormalValue::NillableIntElementArray(_)
            )
        }
        ScalarArrayKind::NillableFloat64Array => {
            matches!(
                value,
                NormalValue::NillableFloat64Array(_) | NormalValue::NillableFloat64ElementArray(_)
            )
        }
        ScalarArrayKind::NillableFloat32Array => {
            matches!(
                value,
                NormalValue::NillableFloat32Array(_) | NormalValue::NillableFloat32ElementArray(_)
            )
        }
        ScalarArrayKind::NillableStringArray => {
            matches!(
                value,
                NormalValue::NillableStringArray(_) | NormalValue::NillableStringElementArray(_)
            )
        }
    }
}
