use super::super::utils::{encode_uvarint_ascending, SEPARATOR};
use crate::corekv::Key;

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
