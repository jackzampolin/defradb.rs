/// Datastore keys for document and collection data
///
/// These keys are prefixed with 'd' at the store level and handle:
/// - Document field values
/// - Primary key mappings
/// - Secondary indexes
/// - Search engine artifacts
/// - View caching
use super::utils::{encode_uvarint_ascending, InstanceType, SEPARATOR};
use crate::corekv::Key;

/// Special field ID for storing document schema version.
///
/// Documents store their schema version ID as a field with this ID.
/// This allows the lens migration system to determine if a document
/// needs to be transformed to match the current collection schema.
///
/// Storage key format: `/{collectionShortID}/v/{docID}/v`
///
/// Matches Go's `keys.DATASTORE_DOC_VERSION_FIELD_ID`.
pub const DATASTORE_DOC_VERSION_FIELD_ID: &str = "v";

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

    /// Create a key for storing the document's schema version.
    ///
    /// The version field uses the special field ID "v" (DATASTORE_DOC_VERSION_FIELD_ID).
    /// This is used by the lens migration system to track which schema version
    /// a document was stored with.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let key = DataStoreKey::version_key(1, InstanceType::Value, "bae123...");
    /// // Results in key: /1/v/bae123.../v
    /// ```
    pub fn version_key(
        collection_id: u32,
        instance_type: InstanceType,
        doc_id: impl Into<String>,
    ) -> Self {
        Self::new(
            collection_id,
            instance_type,
            doc_id,
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

/// An indexed field with value and sort direction.
///
/// Matches Go's keys.IndexedField
#[derive(Debug, Clone)]
pub struct IndexedField {
    /// The field value
    pub value: document::NormalValue,
    /// Whether this field is indexed in descending order
    pub descending: bool,
}

impl IndexedField {
    /// Create a new indexed field
    pub fn new(value: document::NormalValue, descending: bool) -> Self {
        Self { value, descending }
    }

    /// Create an ascending indexed field
    pub fn ascending(value: document::NormalValue) -> Self {
        Self {
            value,
            descending: false,
        }
    }

    /// Create a descending indexed field
    pub fn descending(value: document::NormalValue) -> Self {
        Self {
            value,
            descending: true,
        }
    }
}

impl PartialEq for IndexedField {
    fn eq(&self, other: &Self) -> bool {
        self.descending == other.descending && self.value == other.value
    }
}

/// IndexDataStoreKey: Stores indexed field values for secondary indexes
///
/// Structure: /[CollectionShortID]/[IndexID]/[EncodedFieldValue1][EncodedFieldValue2]...
/// Example: /1/2/<encoded value A><encoded value B>
///
/// Note: Field values are encoded using order-preserving encoding that
/// maintains sort order when compared as byte sequences.
#[derive(Debug, Clone)]
pub struct IndexDataStoreKey {
    /// Collection short ID (varint-encoded in key bytes)
    pub collection_short_id: u32,
    /// Index ID (varint-encoded in key bytes)
    pub index_id: u32,
    /// Indexed fields with values and sort direction
    pub fields: Vec<IndexedField>,
}

impl PartialEq for IndexDataStoreKey {
    fn eq(&self, other: &Self) -> bool {
        self.collection_short_id == other.collection_short_id
            && self.index_id == other.index_id
            && self.fields == other.fields
    }
}

impl IndexDataStoreKey {
    /// Create a new IndexDataStoreKey
    pub fn new(collection_short_id: u32, index_id: u32, fields: Vec<IndexedField>) -> Self {
        Self {
            collection_short_id,
            index_id,
            fields,
        }
    }

    /// Create a prefix for all entries in an index
    pub fn index_prefix(collection_short_id: u32, index_id: u32) -> Vec<u8> {
        let mut buf = vec![SEPARATOR];
        buf = encode_uvarint_ascending(buf, collection_short_id as u64);
        buf.push(SEPARATOR);
        buf = encode_uvarint_ascending(buf, index_id as u64);
        buf.push(SEPARATOR);
        buf
    }

    /// Create a prefix for all entries in a collection's indexes
    pub fn collection_prefix(collection_short_id: u32) -> Vec<u8> {
        let mut buf = vec![SEPARATOR];
        buf = encode_uvarint_ascending(buf, collection_short_id as u64);
        buf.push(SEPARATOR);
        buf
    }

    /// Convert the key to bytes, returning an error if encoding fails.
    ///
    /// Use this method when you need to handle encoding errors (e.g., unsupported
    /// field types or timestamp overflow).
    pub fn try_bytes(&self) -> crate::corekv::Result<Vec<u8>> {
        let mut buf = vec![SEPARATOR];
        buf = encode_uvarint_ascending(buf, self.collection_short_id as u64);
        buf.push(SEPARATOR);
        buf = encode_uvarint_ascending(buf, self.index_id as u64);
        buf.push(SEPARATOR);

        for field in &self.fields {
            buf = crate::field_value::encode_field_value(buf, &field.value, field.descending)?;
        }

        Ok(buf)
    }
}

impl Key for IndexDataStoreKey {
    fn bytes(&self) -> Vec<u8> {
        // Note: Prefer try_bytes() for proper error handling. This implementation
        // panics on encoding errors (e.g., unsupported field types, timestamp overflow).
        match self.try_bytes() {
            Ok(bytes) => bytes,
            Err(e) => {
                panic!(
                    "IndexDataStoreKey encoding failed for collection={}, index={}: {}. \
                     Use try_bytes() for proper error handling.",
                    self.collection_short_id, self.index_id, e
                )
            }
        }
    }

    fn to_string(&self) -> String {
        let values_str = self
            .fields
            .iter()
            .map(|f| {
                format!(
                    "{:?}({})",
                    f.value,
                    if f.descending { "desc" } else { "asc" }
                )
            })
            .collect::<Vec<_>>()
            .join("/");
        format!(
            "/{}/{}/{}",
            self.collection_short_id, self.index_id, values_str
        )
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
        use document::NormalValue;

        let key = IndexDataStoreKey::new(
            1,
            2,
            vec![
                IndexedField::ascending(NormalValue::String("valueA".to_string())),
                IndexedField::ascending(NormalValue::String("valueB".to_string())),
            ],
        );

        let bytes = key.bytes();
        assert!(!bytes.is_empty());
        assert!(bytes[0] == SEPARATOR);

        let string = key.to_string();
        assert!(string.contains("/1/2/"));
    }

    #[test]
    fn test_index_datastore_key_sort_order() {
        use document::NormalValue;

        // Test that keys with different values maintain sort order
        let key1 = IndexDataStoreKey::new(1, 1, vec![IndexedField::ascending(NormalValue::Int(1))]);
        let key2 = IndexDataStoreKey::new(1, 1, vec![IndexedField::ascending(NormalValue::Int(2))]);
        let key3 = IndexDataStoreKey::new(1, 1, vec![IndexedField::ascending(NormalValue::Int(3))]);

        let bytes1 = key1.bytes();
        let bytes2 = key2.bytes();
        let bytes3 = key3.bytes();

        assert!(bytes1 < bytes2, "key1 should be < key2");
        assert!(bytes2 < bytes3, "key2 should be < key3");
    }

    #[test]
    fn test_index_datastore_key_descending() {
        use document::NormalValue;

        // Test that descending order reverses sort
        let key1 =
            IndexDataStoreKey::new(1, 1, vec![IndexedField::descending(NormalValue::Int(1))]);
        let key2 =
            IndexDataStoreKey::new(1, 1, vec![IndexedField::descending(NormalValue::Int(2))]);

        let bytes1 = key1.bytes();
        let bytes2 = key2.bytes();

        // In descending order, larger value should have smaller key bytes
        assert!(bytes1 > bytes2, "descending: key1 should be > key2");
    }

    #[test]
    fn test_index_datastore_key_composite() {
        use document::NormalValue;

        // Test composite index (multiple fields)
        let key = IndexDataStoreKey::new(
            1,
            1,
            vec![
                IndexedField::ascending(NormalValue::String("alice".to_string())),
                IndexedField::descending(NormalValue::Int(25)),
            ],
        );

        let bytes = key.bytes();
        assert!(!bytes.is_empty());

        // Same first field, different second field
        let key_a = IndexDataStoreKey::new(
            1,
            1,
            vec![
                IndexedField::ascending(NormalValue::String("alice".to_string())),
                IndexedField::descending(NormalValue::Int(30)),
            ],
        );
        let key_b = IndexDataStoreKey::new(
            1,
            1,
            vec![
                IndexedField::ascending(NormalValue::String("alice".to_string())),
                IndexedField::descending(NormalValue::Int(20)),
            ],
        );

        // With descending second field: 30 should come before 20 in sort order
        assert!(key_a.bytes() < key_b.bytes());
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

    #[test]
    fn test_version_key() {
        let key = DataStoreKey::version_key(
            1,
            InstanceType::Value,
            "bae123456789abcdef0123456789abcdef012345",
        );

        assert!(key.is_version_field());
        assert_eq!(key.field_id, DATASTORE_DOC_VERSION_FIELD_ID);
        assert_eq!(key.field_id, "v");

        let string = key.to_string();
        assert!(string.ends_with("/v"), "Version key should end with /v");
        assert!(string.contains("/1/v/"));
    }

    #[test]
    fn test_is_version_field() {
        let version_key = DataStoreKey::new(
            1,
            InstanceType::Value,
            "docid",
            DATASTORE_DOC_VERSION_FIELD_ID,
        );
        assert!(version_key.is_version_field());

        let regular_key = DataStoreKey::new(1, InstanceType::Value, "docid", "name");
        assert!(!regular_key.is_version_field());
    }

    #[test]
    fn test_version_field_id_constant() {
        // Verify the constant matches Go's DATASTORE_DOC_VERSION_FIELD_ID
        assert_eq!(DATASTORE_DOC_VERSION_FIELD_ID, "v");
    }
}
