//! Wire-format structs for DocSync and BranchableSync over pubsub_rpc.
//!
//! These mirror Go's type layouts byte-for-byte so a Rust node's
//! `doc-sync` / `sync-branchable` traffic is decodable by a Go peer and
//! vice versa. In particular:
//!
//! - Field names use Go's lowercase JSON tags (`docIDs`, `docID`, `heads`,
//!   `results`, `sender`, `collectionID`) — `fxamacker/cbor` honours JSON
//!   tags by default, so the CBOR wire keys match.
//! - No `MetaData` fields. Authentication for these protocols comes from
//!   the pubsub layer (`MessageAuthenticity::Signed` on the gossipsub
//!   behaviour), not from an in-message signature envelope.
//! - Byte arrays use `serde_bytes` so serde_cbor emits CBOR major-type-2
//!   rather than CBOR arrays of integers.
//!
//! References:
//! - Go DocSync: `defradb/internal/db/p2p/sync_doc.go:40-54`
//! - Go Branchable: `defradb/internal/db/p2p/sync_branchable_col.go:35-50`

use serde::{Deserialize, Serialize};

/// Upper bound on document IDs per DocSync request. Prevents memory
/// exhaustion from malicious peers; matches the `MAX_DOC_IDS` constant
/// already used on the two-stream path.
pub const MAX_DOC_IDS: usize = 1000;

// ---------------------------------------------------------------------------
// DocSync
// ---------------------------------------------------------------------------

/// DocSync request published on the `doc-sync` topic.
///
/// Go: `docSyncRequest{ DocIDs []string \`json:"docIDs"\` }`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocSyncRequest {
    #[serde(rename = "docIDs")]
    pub doc_ids: Vec<String>,
}

impl DocSyncRequest {
    pub fn new(doc_ids: Vec<String>) -> Self {
        Self { doc_ids }
    }
}

/// DocSync reply published on the `doc-sync/<caller>/_response` sub-topic
/// (wrapped in an [`super::InternalResponse`] envelope).
///
/// Go: `docSyncReply{ Results []docSyncItem \`json:"results"\`; Sender string \`json:"sender"\` }`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DocSyncReply {
    #[serde(rename = "results", default)]
    pub results: Vec<DocSyncItem>,
    #[serde(rename = "sender", default)]
    pub sender: String,
}

/// Per-document sync entry.
///
/// Go: `docSyncItem{ DocID string \`json:"docID"\`; Heads [][]byte \`json:"heads"\` }`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DocSyncItem {
    #[serde(rename = "docID")]
    pub doc_id: String,
    #[serde(rename = "heads", with = "serde_bytes_vec")]
    pub heads: Vec<Vec<u8>>,
}

// ---------------------------------------------------------------------------
// BranchableSync
// ---------------------------------------------------------------------------

/// BranchableCollection sync request published on the `sync-branchable` topic.
///
/// Go: `syncBranchableCollectionRequest{ CollectionID string \`json:"collectionID"\` }`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchableSyncRequest {
    #[serde(rename = "collectionID")]
    pub collection_id: String,
}

impl BranchableSyncRequest {
    pub fn new(collection_id: String) -> Self {
        Self { collection_id }
    }
}

/// BranchableCollection sync reply wrapped in an [`super::InternalResponse`].
///
/// Go: `syncBranchableCollectionReply{
///     CollectionID string  \`json:"collectionID"\`;
///     Heads        [][]byte \`json:"heads"\`;
///     Sender       string   \`json:"sender"\`;
/// }`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BranchableSyncReply {
    #[serde(rename = "collectionID")]
    pub collection_id: String,
    #[serde(rename = "heads", with = "serde_bytes_vec", default)]
    pub heads: Vec<Vec<u8>>,
    #[serde(rename = "sender", default)]
    pub sender: String,
}

/// Custom serde module for `Vec<Vec<u8>>` to force CBOR byte-string elements
/// rather than CBOR arrays-of-integers. `serde_bytes` handles `Vec<u8>`
/// directly; the outer `Vec<_>` needs its own adapter.
mod serde_bytes_vec {
    use serde::de::{SeqAccess, Visitor};
    use serde::ser::SerializeSeq;
    use serde::{Deserializer, Serializer};
    use std::fmt;

    pub fn serialize<S: Serializer>(value: &[Vec<u8>], ser: S) -> Result<S::Ok, S::Error> {
        let mut seq = ser.serialize_seq(Some(value.len()))?;
        for v in value {
            seq.serialize_element(serde_bytes::Bytes::new(v))?;
        }
        seq.end()
    }

    struct BytesVecVisitor;
    impl<'de> Visitor<'de> for BytesVecVisitor {
        type Value = Vec<Vec<u8>>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("sequence of byte strings")
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0));
            while let Some(bytes) = seq.next_element::<serde_bytes::ByteBuf>()? {
                out.push(bytes.into_vec());
            }
            Ok(out)
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<Vec<Vec<u8>>, D::Error> {
        de.deserialize_seq(BytesVecVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bin(bytes: &str) -> Vec<u8> {
        hex::decode(bytes).expect("hex")
    }

    // Build CBOR bytes and scan for the Go-shaped field names; we don't
    // pre-compute whole-message fixtures because Go's CBOR output is
    // deterministic only up to map-ordering, which fxamacker/cbor
    // preserves as declared — same as serde_cbor — so the field-name
    // offsets are the parity signal.
    #[test]
    fn doc_sync_request_uses_lowercase_docids_key() {
        let req = DocSyncRequest::new(vec!["docA".into(), "docB".into()]);
        let bytes = serde_cbor::to_vec(&req).expect("encode");
        assert!(
            find_text(&bytes, "docIDs").is_some(),
            "wire key must be `docIDs` (Go JSON tag), not PascalCase"
        );
        assert!(
            find_text(&bytes, "DocIDs").is_none(),
            "must not emit the old PascalCase key"
        );
    }

    #[test]
    fn doc_sync_reply_uses_lowercase_keys() {
        let reply = DocSyncReply {
            results: vec![DocSyncItem {
                doc_id: "bafyreid".into(),
                heads: vec![bin("00ff")],
            }],
            sender: "12D3KooW".into(),
        };
        let bytes = serde_cbor::to_vec(&reply).expect("encode");
        for k in ["results", "sender", "docID", "heads"] {
            assert!(
                find_text(&bytes, k).is_some(),
                "DocSyncReply must carry key `{k}` on the wire"
            );
        }
        for k in ["Results", "Sender", "DocID", "Heads"] {
            assert!(
                find_text(&bytes, k).is_none(),
                "DocSyncReply must not carry PascalCase key `{k}` on the wire"
            );
        }
    }

    #[test]
    fn branchable_sync_request_uses_lowercase_collection_id() {
        let req = BranchableSyncRequest::new("col-1".into());
        let bytes = serde_cbor::to_vec(&req).expect("encode");
        assert!(find_text(&bytes, "collectionID").is_some());
        assert!(find_text(&bytes, "CollectionID").is_none());
    }

    #[test]
    fn branchable_sync_reply_uses_lowercase_keys() {
        let reply = BranchableSyncReply {
            collection_id: "col-1".into(),
            heads: vec![bin("0102")],
            sender: "peer".into(),
        };
        let bytes = serde_cbor::to_vec(&reply).expect("encode");
        for k in ["collectionID", "heads", "sender"] {
            assert!(find_text(&bytes, k).is_some(), "missing wire key `{k}`");
        }
    }

    #[test]
    fn heads_use_cbor_byte_strings_not_arrays_of_ints() {
        let reply = BranchableSyncReply {
            collection_id: "x".into(),
            heads: vec![bin("deadbeef")],
            sender: "".into(),
        };
        let bytes = serde_cbor::to_vec(&reply).expect("encode");
        // CBOR byte-string with length 4 is 0x44 0xde 0xad 0xbe 0xef.
        // CBOR array-of-ints would be 0x84 0x18 0xde 0x18 0xad ...
        // Check that the raw head bytes appear consecutively somewhere.
        let needle = [0x44_u8, 0xde, 0xad, 0xbe, 0xef];
        assert!(
            bytes.windows(5).any(|w| w == needle),
            "heads must serialize as CBOR byte strings"
        );
    }

    #[test]
    fn round_trip_doc_sync_reply() {
        let original = DocSyncReply {
            results: vec![DocSyncItem {
                doc_id: "bafy".into(),
                heads: vec![bin("00"), bin("01ff")],
            }],
            sender: "peerA".into(),
        };
        let bytes = serde_cbor::to_vec(&original).expect("encode");
        let decoded: DocSyncReply = serde_cbor::from_slice(&bytes).expect("decode");
        assert_eq!(decoded, original);
    }

    fn find_text(haystack: &[u8], needle: &str) -> Option<usize> {
        haystack
            .windows(needle.len())
            .position(|w| w == needle.as_bytes())
    }
}
