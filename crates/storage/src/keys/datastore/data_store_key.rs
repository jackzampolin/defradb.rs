use super::super::doc_id_index::encode_doc_short_id;
use super::super::utils::{encode_uvarint_ascending, InstanceType, SEPARATOR};
use super::DATASTORE_DOC_VERSION_FIELD_ID;
use crate::corekv::Key;

/// DataStoreKey: Main key for storing field values in documents
///
/// Structure: /[CollectionShortID]/[InstanceType]/[DocShortID uvarint]/[FieldID]
/// Example: /1/v/\x2a/fieldname
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataStoreKey {
    /// Collection short ID (varint-encoded)
    pub collection_id: u32,
    /// Instance type (value, priority, or deleted)
    pub instance_type: InstanceType,
    /// Document short ID (varint-encoded)
    pub doc_short_id: u64,
    /// Field ID (variable length string)
    pub field_id: String,
}

impl DataStoreKey {
    /// Create a new DataStoreKey
    pub fn new(
        collection_id: u32,
        instance_type: InstanceType,
        doc_short_id: u64,
        field_id: impl Into<String>,
    ) -> Self {
        Self {
            collection_id,
            instance_type,
            doc_short_id,
            field_id: field_id.into(),
        }
    }

    /// Create a prefix for all keys in a collection
    pub fn collection_prefix(collection_id: u32) -> Vec<u8> {
        let mut buf = vec![SEPARATOR];
        buf = encode_uvarint_ascending(buf, collection_id as u64);
        buf.push(SEPARATOR);
        buf
    }

    /// Create a prefix for all documents in a collection with a specific instance type
    pub fn collection_instance_prefix(collection_id: u32, instance_type: InstanceType) -> Vec<u8> {
        let mut buf = Self::collection_prefix(collection_id);
        buf.push(instance_type.as_byte());
        buf.push(SEPARATOR);
        buf
    }

    /// Create a prefix for all fields in a specific document
    pub fn document_prefix(
        collection_id: u32,
        instance_type: InstanceType,
        doc_short_id: u64,
    ) -> Vec<u8> {
        let mut buf = Self::collection_instance_prefix(collection_id, instance_type);
        buf.extend_from_slice(&encode_doc_short_id(doc_short_id));
        buf.push(SEPARATOR);
        buf
    }

    /// Create a key for storing the document's schema version.
    ///
    /// The version field uses the special field ID "v" (DATASTORE_DOC_VERSION_FIELD_ID).
    /// This is used by the lens migration system to track which schema version
    /// a document was stored with.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let key = DataStoreKey::version_key(1, InstanceType::Value, 42);
    /// // Results in key: /1/v/\x2a/v
    /// ```
    pub fn version_key(
        collection_id: u32,
        instance_type: InstanceType,
        doc_short_id: u64,
    ) -> Self {
        Self::new(
            collection_id,
            instance_type,
            doc_short_id,
            DATASTORE_DOC_VERSION_FIELD_ID,
        )
    }

    /// Check if this key represents a document version field.
    pub fn is_version_field(&self) -> bool {
        self.field_id == DATASTORE_DOC_VERSION_FIELD_ID
    }
}

impl Key for DataStoreKey {
    fn bytes(&self) -> Vec<u8> {
        let mut buf = Self::document_prefix(self.collection_id, self.instance_type, self.doc_short_id);
        buf.extend_from_slice(self.field_id.as_bytes());
        buf
    }

    fn to_string(&self) -> String {
        format!(
            "/{}/{}/{}/{}",
            self.collection_id,
            self.instance_type.as_str(),
            self.doc_short_id,
            self.field_id
        )
    }
}

/// PrimaryDataStoreKey: Maps documents to their primary keys
///
/// Structure: /[CollectionShortID]/pk/[DocShortID uvarint]
/// Example: /1/pk/\x2a
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimaryDataStoreKey {
    /// Collection short ID (varint-encoded)
    pub collection_id: u32,
    /// Document short ID (varint-encoded)
    pub doc_short_id: u64,
}

impl PrimaryDataStoreKey {
    /// Create a new PrimaryDataStoreKey
    pub fn new(collection_id: u32, doc_short_id: u64) -> Self {
        Self {
            collection_id,
            doc_short_id,
        }
    }

    /// Create a prefix for all primary keys in a collection
    pub fn collection_prefix(collection_id: u32) -> Vec<u8> {
        let mut buf = vec![SEPARATOR];
        buf = encode_uvarint_ascending(buf, collection_id as u64);
        buf.extend_from_slice(b"/pk/");
        buf
    }

    /// Convert to the corresponding DataStoreKey (no instance type or field).
    pub fn to_data_store_key(&self, instance_type: InstanceType) -> DataStoreKey {
        DataStoreKey::new(self.collection_id, instance_type, self.doc_short_id, "")
    }
}

impl Key for PrimaryDataStoreKey {
    fn bytes(&self) -> Vec<u8> {
        let mut buf = Self::collection_prefix(self.collection_id);
        buf.extend_from_slice(&encode_doc_short_id(self.doc_short_id));
        buf
    }

    fn to_string(&self) -> String {
        format!("/{}/pk/{}", self.collection_id, self.doc_short_id)
    }
}
