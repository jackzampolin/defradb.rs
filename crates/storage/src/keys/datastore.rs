/// Datastore keys for document and collection data
///
/// These keys are prefixed with 'd' at the store level and handle:
/// - Document field values
/// - Primary key mappings
/// - Secondary indexes
/// - Search engine artifacts
/// - View caching

use super::utils::{
    encode_uvarint_ascending, InstanceType, SEPARATOR,
};
use crate::corekv::Key;

/// DataStoreKey: Main key for storing field values in documents
///
/// Structure: /[CollectionRootID]/[InstanceType]/[DocID]/[FieldID]
/// Example: /1/v/bae123456789abcdef0123456789abcdef012345/fieldname
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataStoreKey {
    /// Collection short ID (varint-encoded)
    pub collection_id: u32,
    /// Instance type (value, priority, or deleted)
    pub instance_type: InstanceType,
    /// Document ID (40 character string)
    pub doc_id: String,
    /// Field ID (variable length string)
    pub field_id: String,
}

impl DataStoreKey {
    /// Create a new DataStoreKey
    pub fn new(
        collection_id: u32,
        instance_type: InstanceType,
        doc_id: impl Into<String>,
        field_id: impl Into<String>,
    ) -> Self {
        Self {
            collection_id,
            instance_type,
            doc_id: doc_id.into(),
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
        doc_id: impl Into<String>,
    ) -> Vec<u8> {
        let doc_id = doc_id.into();
        let mut buf = Self::collection_instance_prefix(collection_id, instance_type);
        buf.extend_from_slice(doc_id.as_bytes());
        buf.push(SEPARATOR);
        buf
    }
}

impl Key for DataStoreKey {
    fn bytes(&self) -> Vec<u8> {
        let mut buf = vec![SEPARATOR];
        buf = encode_uvarint_ascending(buf, self.collection_id as u64);
        buf.push(SEPARATOR);
        buf.push(self.instance_type.as_byte());
        buf.push(SEPARATOR);
        buf.extend_from_slice(self.doc_id.as_bytes());
        buf.push(SEPARATOR);
        buf.extend_from_slice(self.field_id.as_bytes());
        buf
    }

    fn to_string(&self) -> String {
        format!(
            "/{}/{}/{}/{}",
            self.collection_id,
            self.instance_type.as_str(),
            self.doc_id,
            self.field_id
        )
    }
}

/// PrimaryDataStoreKey: Maps documents to their primary keys
///
/// Structure: /[CollectionID]/pk/[DocID]
/// Example: /1/pk/bae123456789abcdef0123456789abcdef012345
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimaryDataStoreKey {
    /// Collection short ID (decimal format, not varint)
    pub collection_id: u32,
    /// Document ID
    pub doc_id: String,
}

impl PrimaryDataStoreKey {
    /// Create a new PrimaryDataStoreKey
    pub fn new(collection_id: u32, doc_id: impl Into<String>) -> Self {
        Self {
            collection_id,
            doc_id: doc_id.into(),
        }
    }

    /// Create a prefix for all primary keys in a collection
    pub fn collection_prefix(collection_id: u32) -> Vec<u8> {
        let prefix = format!("/{}/pk/", collection_id);
        prefix.into_bytes()
    }
}

impl Key for PrimaryDataStoreKey {
    fn bytes(&self) -> Vec<u8> {
        let s = format!("/{}/pk/{}", self.collection_id, self.doc_id);
        s.into_bytes()
    }

    fn to_string(&self) -> String {
        format!("/{}/pk/{}", self.collection_id, self.doc_id)
    }
}

/// IndexDataStoreKey: Stores indexed field values for secondary indexes
///
/// Structure: /[CollectionID]/[IndexID]/[FieldValue1](/[FieldValue2]...)
/// Example: /1/2/valueA/valueB
#[derive(Debug, Clone, PartialEq)]
pub struct IndexDataStoreKey {
    /// Collection short ID (varint-encoded)
    pub collection_id: u32,
    /// Index ID (varint-encoded)
    pub index_id: u32,
    /// Indexed field values (variable length)
    pub field_values: Vec<Vec<u8>>,
}

impl IndexDataStoreKey {
    /// Create a new IndexDataStoreKey
    pub fn new(collection_id: u32, index_id: u32, field_values: Vec<Vec<u8>>) -> Self {
        Self {
            collection_id,
            index_id,
            field_values,
        }
    }

    /// Create a prefix for all entries in an index
    pub fn index_prefix(collection_id: u32, index_id: u32) -> Vec<u8> {
        let mut buf = vec![SEPARATOR];
        buf = encode_uvarint_ascending(buf, collection_id as u64);
        buf.push(SEPARATOR);
        buf = encode_uvarint_ascending(buf, index_id as u64);
        buf.push(SEPARATOR);
        buf
    }
}

impl Key for IndexDataStoreKey {
    fn bytes(&self) -> Vec<u8> {
        let mut buf = vec![SEPARATOR];
        buf = encode_uvarint_ascending(buf, self.collection_id as u64);
        buf.push(SEPARATOR);
        buf = encode_uvarint_ascending(buf, self.index_id as u64);
        buf.push(SEPARATOR);

        // Append field values (already encoded)
        for (i, value) in self.field_values.iter().enumerate() {
            if i > 0 {
                buf.push(SEPARATOR);
            }
            buf.extend_from_slice(value);
        }

        buf
    }

    fn to_string(&self) -> String {
        let values_str = self
            .field_values
            .iter()
            .map(|v| hex::encode(v))
            .collect::<Vec<_>>()
            .join("/");
        format!("/{}/{}/{}", self.collection_id, self.index_id, values_str)
    }
}

/// DatastoreSE: Stores search engine index artifacts
///
/// Structure: /se/[CollectionID]/[IndexID]/[SearchTagHex]/[DocID]
/// Example: /se/col1/idx1/a1b2c3d4e5f6/docid123
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatastoreSE {
    /// Collection identifier (hex-encoded)
    pub collection_id: String,
    /// Index identifier (hex-encoded)
    pub index_id: String,
    /// Search tag (raw bytes, hex-encoded in key)
    pub search_tag: Vec<u8>,
    /// Document identifier
    pub doc_id: String,
}

impl DatastoreSE {
    /// Create a new DatastoreSE key
    pub fn new(
        collection_id: impl Into<String>,
        index_id: impl Into<String>,
        search_tag: Vec<u8>,
        doc_id: impl Into<String>,
    ) -> Self {
        Self {
            collection_id: collection_id.into(),
            index_id: index_id.into(),
            search_tag,
            doc_id: doc_id.into(),
        }
    }

    /// Create a prefix for all search engine entries
    pub fn se_prefix() -> Vec<u8> {
        b"/se/".to_vec()
    }

    /// Create a prefix for a specific collection's search engine entries
    pub fn collection_prefix(collection_id: impl Into<String>) -> Vec<u8> {
        let collection_id = collection_id.into();
        format!("/se/{}/", collection_id).into_bytes()
    }
}

impl Key for DatastoreSE {
    fn bytes(&self) -> Vec<u8> {
        let search_tag_hex = hex::encode(&self.search_tag);
        let s = format!(
            "/se/{}/{}/{}/{}",
            self.collection_id, self.index_id, search_tag_hex, self.doc_id
        );
        s.into_bytes()
    }

    fn to_string(&self) -> String {
        let search_tag_hex = hex::encode(&self.search_tag);
        format!(
            "/se/{}/{}/{}/{}",
            self.collection_id, self.index_id, search_tag_hex, self.doc_id
        )
    }
}

/// ViewCacheKey: Caches view/query result items
///
/// Structure: /collection/vi/[CollectionRootID]/[ItemID]
/// Example: /collection/vi/1/5
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewCacheKey {
    /// Collection short ID (varint-encoded)
    pub collection_id: u32,
    /// Item ID (varint-encoded)
    pub item_id: u64,
}

impl ViewCacheKey {
    /// Create a new ViewCacheKey
    pub fn new(collection_id: u32, item_id: u64) -> Self {
        Self {
            collection_id,
            item_id,
        }
    }

    /// Create a prefix for all view cache entries
    pub fn view_cache_prefix() -> Vec<u8> {
        b"/collection/vi/".to_vec()
    }

    /// Create a prefix for a specific collection's view cache
    pub fn collection_prefix(collection_id: u32) -> Vec<u8> {
        let mut buf = Self::view_cache_prefix();
        buf = encode_uvarint_ascending(buf, collection_id as u64);
        buf.push(SEPARATOR);
        buf
    }
}

impl Key for ViewCacheKey {
    fn bytes(&self) -> Vec<u8> {
        let mut buf = Self::view_cache_prefix();
        buf = encode_uvarint_ascending(buf, self.collection_id as u64);
        buf.push(SEPARATOR);
        buf = encode_uvarint_ascending(buf, self.item_id);
        buf
    }

    fn to_string(&self) -> String {
        format!("/collection/vi/{}/{}", self.collection_id, self.item_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datastore_key() {
        let key = DataStoreKey::new(
            1,
            InstanceType::Value,
            "bae123456789abcdef0123456789abcdef012345",
            "fieldname",
        );

        let bytes = key.bytes();
        assert!(!bytes.is_empty());
        assert!(bytes[0] == SEPARATOR);

        let string = key.to_string();
        assert!(string.contains("/1/v/"));
        assert!(string.contains("fieldname"));
    }

    #[test]
    fn test_primary_datastore_key() {
        let key = PrimaryDataStoreKey::new(1, "bae123456789abcdef0123456789abcdef012345");

        let bytes = key.bytes();
        let string = key.to_string();

        assert_eq!(string, "/1/pk/bae123456789abcdef0123456789abcdef012345");
        assert_eq!(bytes, string.as_bytes());
    }

    #[test]
    fn test_index_datastore_key() {
        let key = IndexDataStoreKey::new(1, 2, vec![b"valueA".to_vec(), b"valueB".to_vec()]);

        let bytes = key.bytes();
        assert!(!bytes.is_empty());
        assert!(bytes[0] == SEPARATOR);
    }

    #[test]
    fn test_datastore_se() {
        let key = DatastoreSE::new("col1", "idx1", vec![0xa1, 0xb2, 0xc3], "docid123");

        let string = key.to_string();
        assert!(string.starts_with("/se/"));
        assert!(string.contains("col1"));
        assert!(string.contains("idx1"));
        assert!(string.contains("a1b2c3")); // hex encoded
        assert!(string.contains("docid123"));
    }

    #[test]
    fn test_view_cache_key() {
        let key = ViewCacheKey::new(1, 5);

        let string = key.to_string();
        assert_eq!(string, "/collection/vi/1/5");

        let bytes = key.bytes();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_datastore_key_prefixes() {
        let prefix = DataStoreKey::collection_prefix(1);
        assert!(!prefix.is_empty());

        let prefix = DataStoreKey::collection_instance_prefix(1, InstanceType::Value);
        assert!(!prefix.is_empty());

        let prefix = DataStoreKey::document_prefix(1, InstanceType::Value, "docid");
        assert!(!prefix.is_empty());
    }
}
