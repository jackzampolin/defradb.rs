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
