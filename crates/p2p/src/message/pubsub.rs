//! Wire-format structs for DocSync and BranchableSync over
//! [`crate::pubsub_rpc`].
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
//! - Byte arrays use `serde_bytes` so the encoder emits CBOR major-type-2
//!   rather than CBOR arrays of integers.
//! - Encoding is ciborium-backed (declaration-order keys, definite-length
//!   maps) to match Go's `cbor.Marshal` (fxamacker/cbor v2, default opts).
//! - Optional slice fields that Go declares as `nil` (e.g. `results` before
//!   any items are appended) emit CBOR null (`0xf6`), not empty-array
//!   (`0x80`).
//!
//! Kept out of the `pubsub_rpc` primitive so the primitive stays
//! transport-only; these types are DefraDB-specific message shapes that
//! happen to ride on top of it.
//!
//! References:
//! - Go DocSync: `defradb/internal/db/p2p/sync_doc.go:40-54`
//! - Go Branchable: `defradb/internal/db/p2p/sync_branchable_col.go:35-50`

use serde::{Deserialize, Serialize};

/// Upper bound on document IDs per DocSync request / items per reply.
/// Prevents memory exhaustion from malicious peers; matches the
/// default `MAX_DOC_IDS` constant used on the two-stream path.
pub const MAX_DOC_IDS: usize = 1000;

/// Upper bound on the number of head CIDs per document in a DocSync reply.
/// Realistic values are 1-few; `64` gives room for highly-concurrent
/// writers while still blocking malicious payloads.
pub const MAX_HEADS_PER_DOC: usize = 64;

/// Upper bound on the number of head CIDs in a BranchableSync reply.
/// A branchable collection can have many concurrent branches; pick a
/// generous cap that still prevents unbounded allocation.
pub const MAX_BRANCH_HEADS: usize = 1024;

// ---------------------------------------------------------------------------
// DocSync
// ---------------------------------------------------------------------------

/// DocSync request published on the `doc-sync` topic.
///
/// Go: `docSyncRequest{ DocIDs []string \`json:"docIDs"\` }`
///
/// Deserialization enforces [`MAX_DOC_IDS`] so a malicious peer cannot force
/// an unbounded allocation by sending an oversized `docIDs` array.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DocSyncRequest {
    #[serde(rename = "docIDs")]
    pub doc_ids: Vec<String>,
}

impl DocSyncRequest {
    pub fn new(doc_ids: Vec<String>) -> Self {
        Self { doc_ids }
    }
}

impl<'de> Deserialize<'de> for DocSyncRequest {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::{Error, MapAccess, Visitor};
        use std::fmt;

        struct DocSyncRequestVisitor;
        impl<'de> Visitor<'de> for DocSyncRequestVisitor {
            type Value = DocSyncRequest;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a docSyncRequest map with a `docIDs` field")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut doc_ids: Option<Vec<String>> = None;
                while let Some(key) = map.next_key::<String>()? {
                    if key == "docIDs" {
                        if doc_ids.is_some() {
                            return Err(Error::duplicate_field("docIDs"));
                        }
                        let seq: BoundedStrings = map.next_value::<BoundedDocIds>()?.into();
                        doc_ids = Some(seq.0);
                    } else {
                        let _: serde::de::IgnoredAny = map.next_value()?;
                    }
                }
                Ok(DocSyncRequest {
                    doc_ids: doc_ids.unwrap_or_default(),
                })
            }
        }

        deserializer.deserialize_map(DocSyncRequestVisitor)
    }
}

/// DocSync reply published on the `doc-sync/<caller>/_response` sub-topic
/// (wrapped in an [`crate::pubsub_rpc::InternalResponse`] envelope).
///
/// Go: `docSyncReply{ Results []docSyncItem \`json:"results"\`; Sender string \`json:"sender"\` }`
///
/// Deserialization caps `results` at [`MAX_DOC_IDS`]. An empty `results`
/// serializes as CBOR null (matching Go's `var results []docSyncItem`
/// default, see `sync_doc.go:286`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
pub struct DocSyncReply {
    #[serde(rename = "results", serialize_with = "nil_or_seq::serialize")]
    pub results: Vec<DocSyncItem>,
    #[serde(rename = "sender")]
    pub sender: String,
}

impl<'de> Deserialize<'de> for DocSyncReply {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::{Error, MapAccess, Visitor};
        use std::fmt;

        struct DocSyncReplyVisitor;
        impl<'de> Visitor<'de> for DocSyncReplyVisitor {
            type Value = DocSyncReply;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a docSyncReply map")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut results: Option<Vec<DocSyncItem>> = None;
                let mut sender: Option<String> = None;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "results" => {
                            if results.is_some() {
                                return Err(Error::duplicate_field("results"));
                            }
                            let bounded = map.next_value::<BoundedResults>()?;
                            results = Some(bounded.0);
                        }
                        "sender" => {
                            if sender.is_some() {
                                return Err(Error::duplicate_field("sender"));
                            }
                            sender = Some(map.next_value()?);
                        }
                        _ => {
                            let _: serde::de::IgnoredAny = map.next_value()?;
                        }
                    }
                }
                Ok(DocSyncReply {
                    results: results.unwrap_or_default(),
                    sender: sender.unwrap_or_default(),
                })
            }
        }

        deserializer.deserialize_map(DocSyncReplyVisitor)
    }
}

/// Per-document sync entry.
///
/// Go: `docSyncItem{ DocID string \`json:"docID"\`; Heads [][]byte \`json:"heads"\` }`
///
/// Go always includes at least one head when emitting the item (see
/// `sync_doc.go:293`), so `heads` never takes the nil-semantics path.
/// Deserialization caps `heads` at [`MAX_HEADS_PER_DOC`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
pub struct DocSyncItem {
    #[serde(rename = "docID")]
    pub doc_id: String,
    #[serde(rename = "heads", serialize_with = "serde_bytes_vec::serialize")]
    pub heads: Vec<Vec<u8>>,
}

impl<'de> Deserialize<'de> for DocSyncItem {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::{Error, MapAccess, Visitor};
        use std::fmt;

        struct DocSyncItemVisitor;
        impl<'de> Visitor<'de> for DocSyncItemVisitor {
            type Value = DocSyncItem;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a docSyncItem map")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut doc_id: Option<String> = None;
                let mut heads: Option<Vec<Vec<u8>>> = None;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "docID" => {
                            if doc_id.is_some() {
                                return Err(Error::duplicate_field("docID"));
                            }
                            doc_id = Some(map.next_value()?);
                        }
                        "heads" => {
                            if heads.is_some() {
                                return Err(Error::duplicate_field("heads"));
                            }
                            let bounded = map.next_value::<BoundedHeadsPerDoc>()?;
                            heads = Some(bounded.0);
                        }
                        _ => {
                            let _: serde::de::IgnoredAny = map.next_value()?;
                        }
                    }
                }
                Ok(DocSyncItem {
                    doc_id: doc_id.unwrap_or_default(),
                    heads: heads.unwrap_or_default(),
                })
            }
        }

        deserializer.deserialize_map(DocSyncItemVisitor)
    }
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

/// BranchableCollection sync reply wrapped in an
/// [`crate::pubsub_rpc::InternalResponse`].
///
/// Go: `syncBranchableCollectionReply{
///     CollectionID string  \`json:"collectionID"\`;
///     Heads        [][]byte \`json:"heads"\`;
///     Sender       string   \`json:"sender"\`;
/// }`
///
/// Deserialization caps `heads` at [`MAX_BRANCH_HEADS`]. An empty `heads`
/// serializes as CBOR null.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
pub struct BranchableSyncReply {
    #[serde(rename = "collectionID")]
    pub collection_id: String,
    #[serde(rename = "heads", serialize_with = "nil_or_bytes_seq::serialize")]
    pub heads: Vec<Vec<u8>>,
    #[serde(rename = "sender")]
    pub sender: String,
}

impl<'de> Deserialize<'de> for BranchableSyncReply {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::{Error, MapAccess, Visitor};
        use std::fmt;

        struct BranchableSyncReplyVisitor;
        impl<'de> Visitor<'de> for BranchableSyncReplyVisitor {
            type Value = BranchableSyncReply;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a syncBranchableCollectionReply map")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut collection_id: Option<String> = None;
                let mut heads: Option<Vec<Vec<u8>>> = None;
                let mut sender: Option<String> = None;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "collectionID" => {
                            if collection_id.is_some() {
                                return Err(Error::duplicate_field("collectionID"));
                            }
                            collection_id = Some(map.next_value()?);
                        }
                        "heads" => {
                            if heads.is_some() {
                                return Err(Error::duplicate_field("heads"));
                            }
                            let bounded = map.next_value::<BoundedBranchHeads>()?;
                            heads = Some(bounded.0);
                        }
                        "sender" => {
                            if sender.is_some() {
                                return Err(Error::duplicate_field("sender"));
                            }
                            sender = Some(map.next_value()?);
                        }
                        _ => {
                            let _: serde::de::IgnoredAny = map.next_value()?;
                        }
                    }
                }
                Ok(BranchableSyncReply {
                    collection_id: collection_id.unwrap_or_default(),
                    heads: heads.unwrap_or_default(),
                    sender: sender.unwrap_or_default(),
                })
            }
        }

        deserializer.deserialize_map(BranchableSyncReplyVisitor)
    }
}

// ---------------------------------------------------------------------------
// Bounded deserializers
// ---------------------------------------------------------------------------

// Each wrapper is a dedicated newtype so the label can be baked into the
// error message without needing `&'static str` const generics (unstable).
// The structure is the same across all four; the macros generate the visitor
// boilerplate.

struct BoundedStrings(Vec<String>);

macro_rules! bounded_strings {
    ($ty:ident, $label:literal, $max:expr) => {
        struct $ty(Vec<String>);

        impl From<$ty> for BoundedStrings {
            fn from(v: $ty) -> Self {
                BoundedStrings(v.0)
            }
        }

        impl<'de> serde::Deserialize<'de> for $ty {
            fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
                use serde::de::{Error, SeqAccess, Visitor};
                use std::fmt;

                struct V;
                impl<'de> Visitor<'de> for V {
                    type Value = $ty;

                    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                        write!(f, "a sequence of at most {} `{}` strings", $max, $label)
                    }

                    fn visit_seq<A: SeqAccess<'de>>(
                        self,
                        mut seq: A,
                    ) -> Result<Self::Value, A::Error> {
                        let hint = seq.size_hint().unwrap_or(0);
                        if hint > $max {
                            return Err(Error::custom(format!(
                                "{} exceeds limit ({} > {})",
                                $label, hint, $max
                            )));
                        }
                        let mut out = Vec::with_capacity(hint.min($max));
                        while let Some(s) = seq.next_element::<String>()? {
                            if out.len() >= $max {
                                return Err(Error::custom(format!(
                                    "{} exceeds limit (>{})",
                                    $label, $max
                                )));
                            }
                            out.push(s);
                        }
                        Ok($ty(out))
                    }
                }

                de.deserialize_seq(V)
            }
        }
    };
}

bounded_strings!(BoundedDocIds, "docIDs", MAX_DOC_IDS);

macro_rules! bounded_bytes_vec {
    ($ty:ident, $label:literal, $max:expr, null_ok = $null_ok:expr) => {
        struct $ty(Vec<Vec<u8>>);

        impl<'de> serde::Deserialize<'de> for $ty {
            fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
                use serde::de::{Error, SeqAccess, Visitor};
                use std::fmt;

                struct V;
                impl<'de> Visitor<'de> for V {
                    type Value = $ty;

                    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                        if $null_ok {
                            write!(
                                f,
                                "null or a sequence of at most {} `{}` byte strings",
                                $max, $label
                            )
                        } else {
                            write!(
                                f,
                                "a sequence of at most {} `{}` byte strings",
                                $max, $label
                            )
                        }
                    }

                    fn visit_unit<E: Error>(self) -> Result<Self::Value, E> {
                        if $null_ok {
                            Ok($ty(Vec::new()))
                        } else {
                            Err(E::custom(format!("{} may not be null", $label)))
                        }
                    }

                    fn visit_none<E: Error>(self) -> Result<Self::Value, E> {
                        self.visit_unit()
                    }

                    fn visit_some<D: serde::Deserializer<'de>>(
                        self,
                        d: D,
                    ) -> Result<Self::Value, D::Error> {
                        <$ty as serde::Deserialize>::deserialize(d)
                    }

                    fn visit_seq<A: SeqAccess<'de>>(
                        self,
                        mut seq: A,
                    ) -> Result<Self::Value, A::Error> {
                        let hint = seq.size_hint().unwrap_or(0);
                        if hint > $max {
                            return Err(Error::custom(format!(
                                "{} exceeds limit ({} > {})",
                                $label, hint, $max
                            )));
                        }
                        let mut out = Vec::with_capacity(hint.min($max));
                        while let Some(b) = seq.next_element::<serde_bytes::ByteBuf>()? {
                            if out.len() >= $max {
                                return Err(Error::custom(format!(
                                    "{} exceeds limit (>{})",
                                    $label, $max
                                )));
                            }
                            out.push(b.into_vec());
                        }
                        Ok($ty(out))
                    }
                }

                de.deserialize_any(V)
            }
        }
    };
}

bounded_bytes_vec!(
    BoundedHeadsPerDoc,
    "heads-per-doc",
    MAX_HEADS_PER_DOC,
    null_ok = false
);

bounded_bytes_vec!(
    BoundedBranchHeads,
    "heads-per-branchable-collection",
    MAX_BRANCH_HEADS,
    null_ok = true
);

// Generic seq wrapper for `Vec<DocSyncItem>` with null-as-empty.
struct BoundedResults(Vec<DocSyncItem>);

impl<'de> Deserialize<'de> for BoundedResults {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        use serde::de::{Error, SeqAccess, Visitor};
        use std::fmt;

        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = BoundedResults;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(
                    f,
                    "null or a sequence of at most {MAX_DOC_IDS} `results` items"
                )
            }

            fn visit_unit<E: Error>(self) -> Result<Self::Value, E> {
                Ok(BoundedResults(Vec::new()))
            }

            fn visit_none<E: Error>(self) -> Result<Self::Value, E> {
                Ok(BoundedResults(Vec::new()))
            }

            fn visit_some<D: serde::Deserializer<'de>>(
                self,
                d: D,
            ) -> Result<Self::Value, D::Error> {
                <BoundedResults as Deserialize>::deserialize(d)
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let hint = seq.size_hint().unwrap_or(0);
                if hint > MAX_DOC_IDS {
                    return Err(Error::custom(format!(
                        "results exceeds limit ({hint} > {MAX_DOC_IDS})"
                    )));
                }
                let mut out = Vec::with_capacity(hint.min(MAX_DOC_IDS));
                while let Some(item) = seq.next_element::<DocSyncItem>()? {
                    if out.len() >= MAX_DOC_IDS {
                        return Err(Error::custom(format!(
                            "results exceeds limit (>{MAX_DOC_IDS})"
                        )));
                    }
                    out.push(item);
                }
                Ok(BoundedResults(out))
            }
        }

        de.deserialize_any(V)
    }
}

// ---------------------------------------------------------------------------
// Nil-semantics serde helpers
// ---------------------------------------------------------------------------

/// Serialize `Vec<DocSyncItem>` as CBOR null when empty, otherwise as a
/// sequence. Matches Go's implicit behavior when `[]T` is nil vs `[]T{}`.
/// Deserialization is handled by the parent struct's hand-written `Deserialize`
/// impl, which uses [`BoundedResults`] for the size cap.
mod nil_or_seq {
    use super::DocSyncItem;
    use serde::ser::{SerializeSeq, Serializer};

    pub fn serialize<S: Serializer>(v: &[DocSyncItem], ser: S) -> Result<S::Ok, S::Error> {
        if v.is_empty() {
            return ser.serialize_none();
        }
        let mut seq = ser.serialize_seq(Some(v.len()))?;
        for item in v {
            seq.serialize_element(item)?;
        }
        seq.end()
    }
}

/// Serialize `Vec<Vec<u8>>` as CBOR null when empty, otherwise as a sequence
/// of byte strings. Deserialization goes through [`BoundedBranchHeads`].
mod nil_or_bytes_seq {
    use serde::ser::{SerializeSeq, Serializer};

    pub fn serialize<S: Serializer>(v: &[Vec<u8>], ser: S) -> Result<S::Ok, S::Error> {
        if v.is_empty() {
            return ser.serialize_none();
        }
        let mut seq = ser.serialize_seq(Some(v.len()))?;
        for b in v {
            seq.serialize_element(serde_bytes::Bytes::new(b))?;
        }
        seq.end()
    }
}

/// Serialize `Vec<Vec<u8>>` as a CBOR array of byte strings (no null fallback).
/// Deserialization goes through [`BoundedHeadsPerDoc`].
mod serde_bytes_vec {
    use serde::ser::{SerializeSeq, Serializer};

    pub fn serialize<S: Serializer>(value: &[Vec<u8>], ser: S) -> Result<S::Ok, S::Error> {
        let mut seq = ser.serialize_seq(Some(value.len()))?;
        for v in value {
            seq.serialize_element(serde_bytes::Bytes::new(v))?;
        }
        seq.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Byte fixtures produced by `testdata/gen_message_fixtures/main.go`, which
    // runs `cbor.Marshal(...)` from `github.com/fxamacker/cbor/v2` with default
    // opts — the same pipeline as `defradb/internal/db/p2p/sync_doc.go:112,
    // :303` and `sync_branchable_col.go:107, :271`.
    //
    // To regenerate:
    //   cd testdata/gen_message_fixtures && go run main.go
    const GO_DOC_SYNC_REQUEST_TWO_IDS_HEX: &str = "a166646f634944738264646f634164646f6342";
    const GO_DOC_SYNC_REQUEST_EMPTY_HEX: &str = "a166646f6349447380";
    const GO_DOC_SYNC_ITEM_HEX: &str =
        "a265646f6349446b626166792d646f632d6964656865616473824301020344ffeeddcc";
    const GO_DOC_SYNC_REPLY_HEX: &str = "a267726573756c747382a265646f63494466626166792d316568656164738144deadbeefa265646f63494466626166792d32656865616473814200116673656e6465726c313244334b6f6f5750656572";
    const GO_DOC_SYNC_REPLY_EMPTY_HEX: &str = "a267726573756c7473f66673656e6465726470656572";
    const GO_BRANCHABLE_SYNC_REQUEST_HEX: &str =
        "a16c636f6c6c656374696f6e49446f626166792d636f6c6c656374696f6e";
    const GO_BRANCHABLE_SYNC_REPLY_HEX: &str = "a36c636f6c6c656374696f6e49446f626166792d636f6c6c656374696f6e6568656164738243aabbcc4299886673656e6465726c313244334b6f6f5750656572";
    const GO_BRANCHABLE_SYNC_REPLY_EMPTY_HEADS_HEX: &str =
        "a36c636f6c6c656374696f6e49446f626166792d636f6c6c656374696f6e656865616473f66673656e6465726470656572";

    // ---------- encode parity ----------

    #[test]
    fn doc_sync_request_two_ids_matches_go_fixture() {
        let req = DocSyncRequest::new(vec!["docA".into(), "docB".into()]);
        assert_hex_eq(encode(&req), GO_DOC_SYNC_REQUEST_TWO_IDS_HEX);
    }

    #[test]
    fn doc_sync_request_empty_matches_go_fixture() {
        let req = DocSyncRequest::new(vec![]);
        assert_hex_eq(encode(&req), GO_DOC_SYNC_REQUEST_EMPTY_HEX);
    }

    #[test]
    fn doc_sync_item_matches_go_fixture() {
        let item = DocSyncItem {
            doc_id: "bafy-doc-id".into(),
            heads: vec![vec![0x01, 0x02, 0x03], vec![0xff, 0xee, 0xdd, 0xcc]],
        };
        assert_hex_eq(encode(&item), GO_DOC_SYNC_ITEM_HEX);
    }

    #[test]
    fn doc_sync_reply_matches_go_fixture() {
        let reply = DocSyncReply {
            results: vec![
                DocSyncItem {
                    doc_id: "bafy-1".into(),
                    heads: vec![vec![0xde, 0xad, 0xbe, 0xef]],
                },
                DocSyncItem {
                    doc_id: "bafy-2".into(),
                    heads: vec![vec![0x00, 0x11]],
                },
            ],
            sender: "12D3KooWPeer".into(),
        };
        assert_hex_eq(encode(&reply), GO_DOC_SYNC_REPLY_HEX);
    }

    #[test]
    fn doc_sync_reply_empty_results_emits_null_like_go() {
        let reply = DocSyncReply {
            results: vec![],
            sender: "peer".into(),
        };
        assert_hex_eq(encode(&reply), GO_DOC_SYNC_REPLY_EMPTY_HEX);
    }

    #[test]
    fn branchable_sync_request_matches_go_fixture() {
        let req = BranchableSyncRequest::new("bafy-collection".into());
        assert_hex_eq(encode(&req), GO_BRANCHABLE_SYNC_REQUEST_HEX);
    }

    #[test]
    fn branchable_sync_reply_matches_go_fixture() {
        let reply = BranchableSyncReply {
            collection_id: "bafy-collection".into(),
            heads: vec![vec![0xaa, 0xbb, 0xcc], vec![0x99, 0x88]],
            sender: "12D3KooWPeer".into(),
        };
        assert_hex_eq(encode(&reply), GO_BRANCHABLE_SYNC_REPLY_HEX);
    }

    #[test]
    fn branchable_sync_reply_empty_heads_emits_null_like_go() {
        let reply = BranchableSyncReply {
            collection_id: "bafy-collection".into(),
            heads: vec![],
            sender: "peer".into(),
        };
        assert_hex_eq(encode(&reply), GO_BRANCHABLE_SYNC_REPLY_EMPTY_HEADS_HEX);
    }

    // ---------- decode parity ----------

    #[test]
    fn decodes_go_doc_sync_request_two_ids() {
        let bytes = hex::decode(GO_DOC_SYNC_REQUEST_TWO_IDS_HEX).unwrap();
        let decoded: DocSyncRequest = ciborium::from_reader(bytes.as_slice()).unwrap();
        assert_eq!(decoded.doc_ids, vec!["docA", "docB"]);
    }

    #[test]
    fn decodes_go_doc_sync_reply_with_items() {
        let bytes = hex::decode(GO_DOC_SYNC_REPLY_HEX).unwrap();
        let decoded: DocSyncReply = ciborium::from_reader(bytes.as_slice()).unwrap();
        assert_eq!(decoded.results.len(), 2);
        assert_eq!(decoded.results[0].doc_id, "bafy-1");
        assert_eq!(decoded.results[0].heads, vec![vec![0xde, 0xad, 0xbe, 0xef]]);
        assert_eq!(decoded.sender, "12D3KooWPeer");
    }

    #[test]
    fn decodes_go_doc_sync_reply_null_as_empty_vec() {
        let bytes = hex::decode(GO_DOC_SYNC_REPLY_EMPTY_HEX).unwrap();
        let decoded: DocSyncReply = ciborium::from_reader(bytes.as_slice()).unwrap();
        assert!(decoded.results.is_empty());
        assert_eq!(decoded.sender, "peer");
    }

    #[test]
    fn decodes_go_branchable_sync_reply_null_heads_as_empty() {
        let bytes = hex::decode(GO_BRANCHABLE_SYNC_REPLY_EMPTY_HEADS_HEX).unwrap();
        let decoded: BranchableSyncReply = ciborium::from_reader(bytes.as_slice()).unwrap();
        assert_eq!(decoded.collection_id, "bafy-collection");
        assert!(decoded.heads.is_empty());
        assert_eq!(decoded.sender, "peer");
    }

    // ---------- bounded-vec enforcement ----------

    #[test]
    fn doc_sync_request_rejects_oversized_doc_ids() {
        #[derive(serde::Serialize)]
        struct Raw {
            #[serde(rename = "docIDs")]
            doc_ids: Vec<String>,
        }
        let raw = Raw {
            doc_ids: (0..=MAX_DOC_IDS).map(|i| format!("doc-{i}")).collect(),
        };
        let bytes = encode(&raw);
        let err = ciborium::from_reader::<DocSyncRequest, _>(bytes.as_slice())
            .expect_err("must reject oversized payload");
        assert!(err.to_string().contains("docIDs"), "{err}");
    }

    #[test]
    fn doc_sync_request_accepts_boundary_doc_ids() {
        #[derive(serde::Serialize)]
        struct Raw {
            #[serde(rename = "docIDs")]
            doc_ids: Vec<String>,
        }
        let raw = Raw {
            doc_ids: (0..MAX_DOC_IDS).map(|i| format!("doc-{i}")).collect(),
        };
        let bytes = encode(&raw);
        let decoded: DocSyncRequest =
            ciborium::from_reader(bytes.as_slice()).expect("exactly MAX_DOC_IDS must decode");
        assert_eq!(decoded.doc_ids.len(), MAX_DOC_IDS);
    }

    #[test]
    fn doc_sync_reply_rejects_oversized_results() {
        #[derive(serde::Serialize)]
        struct Raw {
            #[serde(rename = "results")]
            results: Vec<DocSyncItem>,
            #[serde(rename = "sender")]
            sender: String,
        }
        let raw = Raw {
            results: (0..=MAX_DOC_IDS)
                .map(|i| DocSyncItem {
                    doc_id: format!("doc-{i}"),
                    heads: vec![vec![0u8]],
                })
                .collect(),
            sender: "peer".into(),
        };
        let bytes = encode(&raw);
        let err = ciborium::from_reader::<DocSyncReply, _>(bytes.as_slice())
            .expect_err("must reject oversized results");
        assert!(err.to_string().contains("results"), "{err}");
    }

    #[test]
    fn doc_sync_item_rejects_oversized_heads() {
        #[derive(serde::Serialize)]
        struct Raw<'a> {
            #[serde(rename = "docID")]
            doc_id: String,
            #[serde(rename = "heads")]
            heads: Vec<&'a serde_bytes::Bytes>,
        }
        let big: Vec<Vec<u8>> = (0..=MAX_HEADS_PER_DOC).map(|_| vec![0u8]).collect();
        let raw = Raw {
            doc_id: "d".into(),
            heads: big.iter().map(|v| serde_bytes::Bytes::new(v)).collect(),
        };
        let bytes = encode(&raw);
        let err = ciborium::from_reader::<DocSyncItem, _>(bytes.as_slice())
            .expect_err("must reject oversized heads");
        assert!(err.to_string().contains("heads-per-doc"), "{err}");
    }

    #[test]
    fn branchable_sync_reply_rejects_oversized_heads() {
        #[derive(serde::Serialize)]
        struct Raw<'a> {
            #[serde(rename = "collectionID")]
            collection_id: String,
            #[serde(rename = "heads")]
            heads: Vec<&'a serde_bytes::Bytes>,
            #[serde(rename = "sender")]
            sender: String,
        }
        let big: Vec<Vec<u8>> = (0..=MAX_BRANCH_HEADS).map(|_| vec![0u8]).collect();
        let raw = Raw {
            collection_id: "c".into(),
            heads: big.iter().map(|v| serde_bytes::Bytes::new(v)).collect(),
            sender: "peer".into(),
        };
        let bytes = encode(&raw);
        let err = ciborium::from_reader::<BranchableSyncReply, _>(bytes.as_slice())
            .expect_err("must reject oversized heads");
        assert!(err.to_string().contains("branchable"), "{err}");
    }

    // ---------- round-trip ----------

    #[test]
    fn round_trip_doc_sync_reply_with_items() {
        let original = DocSyncReply {
            results: vec![DocSyncItem {
                doc_id: "bafy".into(),
                heads: vec![vec![0x00], vec![0x01, 0xff]],
            }],
            sender: "peerA".into(),
        };
        let bytes = encode(&original);
        let decoded: DocSyncReply = ciborium::from_reader(bytes.as_slice()).expect("decode");
        assert_eq!(decoded, original);
    }

    #[test]
    fn round_trip_branchable_sync_reply_with_heads() {
        let original = BranchableSyncReply {
            collection_id: "col".into(),
            heads: vec![vec![0xaa], vec![0xbb, 0xcc]],
            sender: "peer".into(),
        };
        let bytes = encode(&original);
        let decoded: BranchableSyncReply = ciborium::from_reader(bytes.as_slice()).expect("decode");
        assert_eq!(decoded, original);
    }

    // ---------- helpers ----------

    fn encode<T: serde::Serialize>(v: &T) -> Vec<u8> {
        let mut out = Vec::new();
        ciborium::into_writer(v, &mut out).expect("encode");
        out
    }

    fn assert_hex_eq(got: Vec<u8>, expected_hex: &str) {
        let got_hex = hex::encode(&got);
        assert_eq!(
            got_hex,
            expected_hex,
            "byte mismatch vs Go fixture (Rust len={}, Go len={})",
            got.len(),
            expected_hex.len() / 2
        );
    }
}
