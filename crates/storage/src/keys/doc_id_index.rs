use super::utils::{decode_uvarint_ascending, encode_uvarint_ascending};
use crate::corekv::{Error, Key, Result};
use crate::encoding::{
    decode_uvarint_ascending as decode_go_uvarint_ascending,
    encode_uvarint_ascending as encode_go_uvarint_ascending,
};

/// Systemstore root prefix for the doc-ID index keyspace.
pub const DOC_ID_INDEX: &str = "/d";
/// Key for the document short-ID allocation sequence.
pub const DOC_SHORT_ID_SEQ: &str = "/seq/doc";

const DOC_SHORT_ID_TO_DOC_ID: &str = "s";
const DOC_ID_TO_DOC_REF: &str = "p";
const DOC_SHORT_ID_TO_DOC_ID_ALIAS: &str = "r";
const BLOCK_CID_TO_DOC_ID: &str = "b";

/// Encode a document short ID as order-preserving uvarint bytes.
///
/// Returns an empty vector for 0: short IDs start at 1 and 0 marks
/// "unset" (Go parity).
pub fn encode_doc_short_id(doc_short_id: u64) -> Vec<u8> {
    if doc_short_id == 0 {
        return Vec::new();
    }
    encode_uvarint_ascending(Vec::new(), doc_short_id)
}

/// Decode a document short ID, requiring the buffer to be fully consumed.
pub fn decode_doc_short_id(data: &[u8]) -> Result<u64> {
    let (rest, doc_short_id) = decode_doc_short_id_prefix(data)?;
    if !rest.is_empty() {
        return Err(Error::Other("trailing bytes after doc short ID".into()));
    }
    Ok(doc_short_id)
}

/// Decode a document short ID prefix, returning the remaining bytes.
pub fn decode_doc_short_id_prefix(data: &[u8]) -> Result<(&[u8], u64)> {
    let (rest, doc_short_id) = decode_uvarint_ascending(data)?;
    if doc_short_id == 0 {
        return Err(Error::Other("doc short ID cannot be 0".into()));
    }
    Ok((rest, doc_short_id))
}

/// Reference from a public DocID to its node-local storage identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DocRef {
    /// Collection short ID.
    pub collection_short_id: u32,
    /// Document short ID.
    pub doc_short_id: u64,
}

impl DocRef {
    /// Create a new DocRef.
    pub fn new(collection_short_id: u32, doc_short_id: u64) -> Self {
        Self {
            collection_short_id,
            doc_short_id,
        }
    }

    /// Encode as uvarint(collection short ID) || uvarint(doc short ID).
    ///
    /// Returns an empty vector when either component is 0 (Go parity).
    pub fn encode(&self) -> Vec<u8> {
        if self.collection_short_id == 0 || self.doc_short_id == 0 {
            return Vec::new();
        }
        let buf = encode_go_uvarint_ascending(Vec::new(), self.collection_short_id as u64);
        encode_go_uvarint_ascending(buf, self.doc_short_id)
    }

    /// Decode from the encoded form, requiring full consumption.
    pub fn decode(data: &[u8]) -> Result<Self> {
        let (rest, collection_short_id) = decode_go_uvarint_ascending(data)?;
        if collection_short_id == 0 || collection_short_id > u32::MAX as u64 {
            return Err(Error::Other("invalid collection short ID in DocRef".into()));
        }
        let (rest, doc_short_id) = decode_go_uvarint_ascending(rest)?;
        if doc_short_id == 0 {
            return Err(Error::Other("doc short ID cannot be 0".into()));
        }
        if !rest.is_empty() {
            return Err(Error::Other("trailing bytes after DocRef".into()));
        }
        Ok(Self {
            collection_short_id: collection_short_id as u32,
            doc_short_id,
        })
    }
}

fn doc_id_index_key(segments: &[&[u8]]) -> Vec<u8> {
    let mut result = DOC_ID_INDEX.as_bytes().to_vec();
    for segment in segments {
        if !segment.is_empty() {
            result.push(b'/');
            result.extend_from_slice(segment);
        }
    }
    result
}

/// DocShortIDToDocIDKey: Maps a document short ID to its public DocID.
///
/// Structure: /d/s/[DocShortID uvarint]  →  DocID string
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocShortIDToDocIDKey {
    /// Document short ID.
    pub doc_short_id: u64,
}

impl DocShortIDToDocIDKey {
    /// Create a new DocShortIDToDocIDKey.
    pub fn new(doc_short_id: u64) -> Self {
        Self { doc_short_id }
    }
}

impl Key for DocShortIDToDocIDKey {
    fn bytes(&self) -> Vec<u8> {
        doc_id_index_key(&[
            DOC_SHORT_ID_TO_DOC_ID.as_bytes(),
            &encode_doc_short_id(self.doc_short_id),
        ])
    }

    fn to_string(&self) -> String {
        format!("/d/s/{}", self.doc_short_id)
    }
}

/// DocIDToDocRefKey: Maps a public DocID to its DocRef.
///
/// Structure: /d/p/[DocID]  →  DocRef bytes
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocIDToDocRefKey {
    /// Public document ID string.
    pub doc_id: String,
}

impl DocIDToDocRefKey {
    /// Create a new DocIDToDocRefKey.
    pub fn new(doc_id: impl Into<String>) -> Self {
        Self {
            doc_id: doc_id.into(),
        }
    }
}

impl Key for DocIDToDocRefKey {
    fn bytes(&self) -> Vec<u8> {
        doc_id_index_key(&[DOC_ID_TO_DOC_REF.as_bytes(), self.doc_id.as_bytes()])
    }

    fn to_string(&self) -> String {
        format!("/d/p/{}", self.doc_id)
    }
}

/// DocShortIDToDocIDAliasKey: Set of DocIDs referenced by a short ID.
///
/// Supports enumeration of every DocID mapping to clean up when a
/// document is purged.
///
/// Structure: /d/r/[DocShortID uvarint]/[DocID]  →  DocID string
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocShortIDToDocIDAliasKey {
    /// Document short ID.
    pub doc_short_id: u64,
    /// Public document ID string.
    pub doc_id: String,
}

impl DocShortIDToDocIDAliasKey {
    /// Create a new DocShortIDToDocIDAliasKey.
    pub fn new(doc_short_id: u64, doc_id: impl Into<String>) -> Self {
        Self {
            doc_short_id,
            doc_id: doc_id.into(),
        }
    }

    /// Prefix covering every alias entry of a short ID (trailing '/').
    pub fn short_id_prefix(doc_short_id: u64) -> Vec<u8> {
        let mut prefix = doc_id_index_key(&[
            DOC_SHORT_ID_TO_DOC_ID_ALIAS.as_bytes(),
            &encode_doc_short_id(doc_short_id),
        ]);
        prefix.push(b'/');
        prefix
    }
}

impl Key for DocShortIDToDocIDAliasKey {
    fn bytes(&self) -> Vec<u8> {
        doc_id_index_key(&[
            DOC_SHORT_ID_TO_DOC_ID_ALIAS.as_bytes(),
            &encode_doc_short_id(self.doc_short_id),
            self.doc_id.as_bytes(),
        ])
    }

    fn to_string(&self) -> String {
        format!("/d/r/{}/{}", self.doc_short_id, self.doc_id)
    }
}

/// BlockCIDToDocIDKey: Records that a block belongs to a document.
///
/// Field blocks can be byte-identical across documents, so ownership is
/// a set keyed by (block CID, DocID).
///
/// Structure: /d/b/[BlockCID]/[DocID]  →  (empty)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockCIDToDocIDKey {
    /// Block CID string form.
    pub block_cid: String,
    /// Public document ID string.
    pub doc_id: String,
}

impl BlockCIDToDocIDKey {
    /// Create a new BlockCIDToDocIDKey.
    pub fn new(block_cid: impl Into<String>, doc_id: impl Into<String>) -> Self {
        Self {
            block_cid: block_cid.into(),
            doc_id: doc_id.into(),
        }
    }

    /// Prefix covering every DocID owning a block (trailing '/').
    pub fn block_prefix(block_cid: &str) -> Vec<u8> {
        let mut prefix = doc_id_index_key(&[BLOCK_CID_TO_DOC_ID.as_bytes(), block_cid.as_bytes()]);
        prefix.push(b'/');
        prefix
    }
}

impl Key for BlockCIDToDocIDKey {
    fn bytes(&self) -> Vec<u8> {
        doc_id_index_key(&[
            BLOCK_CID_TO_DOC_ID.as_bytes(),
            self.block_cid.as_bytes(),
            self.doc_id.as_bytes(),
        ])
    }

    fn to_string(&self) -> String {
        format!("/d/b/{}/{}", self.block_cid, self.doc_id)
    }
}

/// DocShortIDSequenceKey: Monotonic allocation sequence for doc short IDs.
///
/// Structure: /seq/doc
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DocShortIDSequenceKey;

impl DocShortIDSequenceKey {
    /// Create a new DocShortIDSequenceKey.
    pub fn new() -> Self {
        Self
    }
}

impl Key for DocShortIDSequenceKey {
    fn bytes(&self) -> Vec<u8> {
        DOC_SHORT_ID_SEQ.as_bytes().to_vec()
    }

    fn to_string(&self) -> String {
        DOC_SHORT_ID_SEQ.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_doc_short_id_roundtrip() {
        for id in [1u64, 47, 239, 240, 2287, 2288, u64::MAX] {
            let encoded = encode_doc_short_id(id);
            assert!(!encoded.is_empty());
            assert_eq!(decode_doc_short_id(&encoded).unwrap(), id);
        }
    }

    #[test]
    fn test_doc_short_id_zero_invalid() {
        assert!(encode_doc_short_id(0).is_empty());
        assert!(decode_doc_short_id(&[0x00]).is_err());
        assert!(decode_doc_short_id(&[]).is_err());
    }

    #[test]
    fn test_doc_short_id_ordering_preserved() {
        let mut prev = encode_doc_short_id(1);
        for id in [2u64, 100, 239, 240, 1000, 2287, 2288, 70000, u64::MAX] {
            let next = encode_doc_short_id(id);
            assert!(prev < next, "ordering violated at {}", id);
            prev = next;
        }
    }

    #[test]
    fn test_doc_ref_roundtrip() {
        let doc_ref = DocRef::new(7, 1234);
        let encoded = doc_ref.encode();
        assert_eq!(DocRef::decode(&encoded).unwrap(), doc_ref);
    }

    #[test]
    fn test_doc_ref_matches_go_encoding() {
        assert_eq!(DocRef::new(1, 1).encode(), vec![0x89, 0x89]);
        assert_eq!(DocRef::new(7, 1234).encode(), vec![0x8f, 0xf7, 0x04, 0xd2]);
    }

    #[test]
    fn test_doc_ref_zero_components_encode_empty() {
        assert!(DocRef::new(0, 5).encode().is_empty());
        assert!(DocRef::new(5, 0).encode().is_empty());
        assert!(DocRef::decode(&[]).is_err());
    }

    #[test]
    fn test_key_layouts() {
        let key = DocShortIDToDocIDKey::new(1);
        assert_eq!(key.bytes(), [b"/d/s/".as_slice(), &[0x01]].concat());

        let key = DocIDToDocRefKey::new("bae-x");
        assert_eq!(key.bytes(), b"/d/p/bae-x");

        let key = DocShortIDToDocIDAliasKey::new(1, "bae-x");
        assert_eq!(
            key.bytes(),
            [b"/d/r/".as_slice(), &[0x01], b"/bae-x"].concat()
        );

        let key = BlockCIDToDocIDKey::new("bafyx", "bae-x");
        assert_eq!(key.bytes(), b"/d/b/bafyx/bae-x");
        assert_eq!(BlockCIDToDocIDKey::block_prefix("bafyx"), b"/d/b/bafyx/");

        assert_eq!(DocShortIDSequenceKey::new().bytes(), b"/seq/doc");
    }

    #[test]
    fn test_alias_prefix() {
        assert_eq!(
            DocShortIDToDocIDAliasKey::short_id_prefix(1),
            [b"/d/r/".as_slice(), &[0x01], b"/"].concat()
        );
    }
}
