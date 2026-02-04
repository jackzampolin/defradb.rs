//! Wire message types for DefraDB P2P protocol.
//!
//! Messages are CBOR encoded for wire compatibility with the Go implementation.
//! CBOR byte strings (major type 2) require serde_bytes annotations for Vec<u8>.
//!
//! # CBOR Serialization Guide
//!
//! Go's fxamacker/cbor has specific behaviors for `[]byte` that Rust must match:
//!
//! | Rust Type | Go Equivalent | Serializer | When to Use |
//! |-----------|---------------|------------|-------------|
//! | `Vec<u8>` | `[]byte` (non-nil) | `serde_bytes` | Required bytes field |
//! | `Option<Vec<u8>>` | `[]byte` (nullable) | `optional_bytes` | Optional signature field |
//! | `Vec<u8>` (may be empty) | `[]byte` (nil=null) | `nullable_bytes` | Public key (empty→CBOR null) |
//!
//! ## `serde_bytes`
//! Standard CBOR byte string encoding. Use for `Vec<u8>` fields that always contain data.
//! Example: CID bytes, block data.
//!
//! ## `optional_bytes`
//! Handles `Option<Vec<u8>>` where `None` → CBOR null, `Some(bytes)` → CBOR byte string.
//! Use for fields that are conditionally present (e.g., signature before signing).
//!
//! ## `nullable_bytes`
//! Handles `Vec<u8>` where empty `Vec` → CBOR null (matching Go's `nil []byte`).
//! Use for fields like pubkey where Go sends CBOR null for unset values.
//! WARNING: On round-trip, empty bytes become CBOR null which becomes empty bytes.

use serde::{Deserialize, Serialize};

use crate::protocol::MESSAGE_VERSION;

/// Custom serialization for Option<Vec<u8>> as CBOR byte strings.
///
/// Use this for optional byte fields like signatures that may or may not be present.
/// - `None` serializes to CBOR null
/// - `Some(bytes)` serializes to CBOR byte string
mod optional_bytes {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &Option<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(bytes) => serde_bytes::serialize(bytes, serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Try to deserialize as an Option<serde_bytes::ByteBuf>
        let opt: Option<serde_bytes::ByteBuf> = Option::deserialize(deserializer)?;
        Ok(opt.map(|b| b.into_vec()))
    }
}

/// Custom serialization for Vec<u8> that treats empty as CBOR null.
///
/// Go's fxamacker/cbor sends `nil []byte` as CBOR null instead of empty byte string.
/// This serializer matches that behavior:
/// - Empty `Vec<u8>` serializes to CBOR null
/// - Non-empty `Vec<u8>` serializes to CBOR byte string
/// - CBOR null deserializes to empty `Vec<u8>`
///
/// Use for fields like pubkey where Go may send CBOR null for unset values.
mod nullable_bytes {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Serialize empty Vec as null to match Go's behavior
        // Go's fxamacker/cbor sends nil []byte as CBOR null
        if value.is_empty() {
            serializer.serialize_none()
        } else {
            serde_bytes::serialize(value, serializer)
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Try to deserialize as Option to handle both null and byte arrays
        let opt: Option<serde_bytes::ByteBuf> = Option::deserialize(deserializer)?;
        Ok(opt.map(|b| b.into_vec()).unwrap_or_default())
    }
}

/// Metadata that is part of every P2P message.
///
/// This struct contains common fields for message routing, authentication,
/// and error handling.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MetaData {
    /// DefraDB message version.
    #[serde(rename = "Version")]
    pub version: String,

    /// Unique message identifier. Responses use the same ID as the request.
    #[serde(rename = "MessageID")]
    pub message_id: String,

    /// ID of the sender (PeerID when using libp2p).
    #[serde(rename = "SenderID")]
    pub sender_id: String,

    /// Public key of the node that created the message.
    /// Uses nullable_bytes to handle Go's nil []byte as CBOR null.
    #[serde(rename = "Pubkey", with = "nullable_bytes")]
    pub pubkey: Vec<u8>,

    /// Signature for message authentication.
    /// Uses custom serialization for optional CBOR bytes.
    #[serde(
        rename = "Signature",
        skip_serializing_if = "Option::is_none",
        default,
        with = "optional_bytes"
    )]
    pub signature: Option<Vec<u8>>,

    /// Error message if something went wrong.
    #[serde(rename = "ErrMessage", skip_serializing_if = "Option::is_none")]
    pub err_message: Option<String>,
}

impl MetaData {
    /// Create new metadata with default values.
    pub fn new() -> Self {
        Self {
            version: MESSAGE_VERSION.to_string(),
            ..Default::default()
        }
    }

    /// Set the message version to the current protocol version.
    pub fn set_version(&mut self) {
        self.version = MESSAGE_VERSION.to_string();
    }
}

/// PushLog request message for sending resource updates to peer nodes.
///
/// This is the primary message type for CRDT synchronization between nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushLogRequest {
    /// Message metadata.
    #[serde(flatten)]
    pub metadata: MetaData,

    /// Document ID being updated.
    #[serde(rename = "DocID")]
    pub doc_id: String,

    /// Content ID (CID) of the block.
    /// Uses serde_bytes for CBOR byte string compatibility with Go.
    #[serde(rename = "CID", with = "serde_bytes")]
    pub cid: Vec<u8>,

    /// Collection ID the document belongs to.
    #[serde(rename = "CollectionID")]
    pub collection_id: String,

    /// Creator/author of the update.
    #[serde(rename = "Creator")]
    pub creator: String,

    /// The IPLD block data.
    /// Uses serde_bytes for CBOR byte string compatibility with Go.
    #[serde(rename = "Block", with = "serde_bytes")]
    pub block: Vec<u8>,
}

impl PushLogRequest {
    /// Create a new PushLogRequest.
    pub fn new(
        doc_id: String,
        cid: Vec<u8>,
        collection_id: String,
        creator: String,
        block: Vec<u8>,
    ) -> Self {
        Self {
            metadata: MetaData::new(),
            doc_id,
            cid,
            collection_id,
            creator,
            block,
        }
    }
}

/// PushLog reply message sent in response to a PushLogRequest.
///
/// Note: We don't use `#[serde(flatten)]` because serde_cbor produces
/// indefinite-length maps when flatten is used (CBOR major type 0xbf).
/// Go's fxamacker/cbor produces definite-length maps, causing signature
/// verification to fail. Instead, we duplicate the fields for wire compatibility.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PushLogReply {
    /// DefraDB message version.
    #[serde(rename = "Version")]
    pub version: String,

    /// Unique message identifier. Responses use the same ID as the request.
    #[serde(rename = "MessageID")]
    pub message_id: String,

    /// ID of the sender (PeerID when using libp2p).
    #[serde(rename = "SenderID")]
    pub sender_id: String,

    /// Public key of the node that created the message.
    #[serde(rename = "Pubkey", with = "nullable_bytes")]
    pub pubkey: Vec<u8>,

    /// Signature for message authentication.
    #[serde(
        rename = "Signature",
        skip_serializing_if = "Option::is_none",
        default,
        with = "optional_bytes"
    )]
    pub signature: Option<Vec<u8>>,

    /// Error message if something went wrong.
    #[serde(rename = "ErrMessage", skip_serializing_if = "Option::is_none")]
    pub err_message: Option<String>,
}

impl PushLogReply {
    /// Create a new successful PushLogReply.
    pub fn success(request_message_id: &str) -> Self {
        Self {
            version: MESSAGE_VERSION.to_string(),
            message_id: request_message_id.to_string(),
            sender_id: String::new(),
            pubkey: Vec::new(),
            signature: None,
            err_message: None,
        }
    }

    /// Create a new error PushLogReply.
    pub fn error(request_message_id: &str, err: &str) -> Self {
        Self {
            version: MESSAGE_VERSION.to_string(),
            message_id: request_message_id.to_string(),
            sender_id: String::new(),
            pubkey: Vec::new(),
            signature: None,
            err_message: Some(err.to_string()),
        }
    }
}

/// Trait for types that can be P2P messages.
pub trait Message {
    /// Get the message version.
    fn version(&self) -> &str;

    /// Set the message version.
    fn set_version(&mut self, version: String);

    /// Get the message ID.
    fn message_id(&self) -> &str;

    /// Set the message ID.
    fn set_message_id(&mut self, id: String);

    /// Get the sender ID.
    fn sender_id(&self) -> &str;

    /// Set the sender ID.
    fn set_sender_id(&mut self, id: String);

    /// Get the public key.
    fn pubkey(&self) -> &[u8];

    /// Set the public key.
    fn set_pubkey(&mut self, pubkey: Vec<u8>);

    /// Get the signature if present.
    fn signature(&self) -> Option<&[u8]>;

    /// Set the signature.
    fn set_signature(&mut self, signature: Option<Vec<u8>>);

    /// Get the error message if present.
    fn err_message(&self) -> Option<&str>;
}

impl Message for PushLogRequest {
    fn version(&self) -> &str {
        &self.metadata.version
    }

    fn set_version(&mut self, version: String) {
        self.metadata.version = version;
    }

    fn message_id(&self) -> &str {
        &self.metadata.message_id
    }

    fn set_message_id(&mut self, id: String) {
        self.metadata.message_id = id;
    }

    fn sender_id(&self) -> &str {
        &self.metadata.sender_id
    }

    fn set_sender_id(&mut self, id: String) {
        self.metadata.sender_id = id;
    }

    fn pubkey(&self) -> &[u8] {
        &self.metadata.pubkey
    }

    fn set_pubkey(&mut self, pubkey: Vec<u8>) {
        self.metadata.pubkey = pubkey;
    }

    fn signature(&self) -> Option<&[u8]> {
        self.metadata.signature.as_deref()
    }

    fn set_signature(&mut self, signature: Option<Vec<u8>>) {
        self.metadata.signature = signature;
    }

    fn err_message(&self) -> Option<&str> {
        self.metadata.err_message.as_deref()
    }
}

impl Message for PushLogReply {
    fn version(&self) -> &str {
        &self.version
    }

    fn set_version(&mut self, version: String) {
        self.version = version;
    }

    fn message_id(&self) -> &str {
        &self.message_id
    }

    fn set_message_id(&mut self, id: String) {
        self.message_id = id;
    }

    fn sender_id(&self) -> &str {
        &self.sender_id
    }

    fn set_sender_id(&mut self, id: String) {
        self.sender_id = id;
    }

    fn pubkey(&self) -> &[u8] {
        &self.pubkey
    }

    fn set_pubkey(&mut self, pubkey: Vec<u8>) {
        self.pubkey = pubkey;
    }

    fn signature(&self) -> Option<&[u8]> {
        self.signature.as_deref()
    }

    fn set_signature(&mut self, signature: Option<Vec<u8>>) {
        self.signature = signature;
    }

    fn err_message(&self) -> Option<&str> {
        self.err_message.as_deref()
    }
}

/// Custom serialization for Vec<Vec<u8>> as CBOR byte strings.
///
/// Use this for arrays of byte fields like CID heads in DocSync.
mod vec_of_bytes {
    use serde::{de::SeqAccess, de::Visitor, ser::SerializeSeq, Deserializer, Serializer};

    pub fn serialize<S>(value: &Vec<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(value.len()))?;
        for bytes in value {
            seq.serialize_element(&serde_bytes::Bytes::new(bytes))?;
        }
        seq.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct VecBytesVisitor;

        impl<'de> Visitor<'de> for VecBytesVisitor {
            type Value = Vec<Vec<u8>>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a sequence of byte arrays")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut result = Vec::new();
                while let Some(bytes) = seq.next_element::<serde_bytes::ByteBuf>()? {
                    result.push(bytes.into_vec());
                }
                Ok(result)
            }
        }

        deserializer.deserialize_seq(VecBytesVisitor)
    }
}

/// DocSync request message for pulling specific documents from peers.
///
/// This is used when a node wants to sync specific documents from the network.
/// Unlike replicator sync (push-based), DocSync is pull-based.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocSyncRequest {
    /// Message metadata.
    #[serde(flatten)]
    pub metadata: MetaData,

    /// Document IDs to sync.
    #[serde(rename = "DocIDs")]
    pub doc_ids: Vec<String>,
}

impl DocSyncRequest {
    /// Create a new DocSyncRequest.
    pub fn new(doc_ids: Vec<String>) -> Self {
        Self {
            metadata: MetaData::new(),
            doc_ids,
        }
    }
}

impl Message for DocSyncRequest {
    fn version(&self) -> &str {
        &self.metadata.version
    }

    fn set_version(&mut self, version: String) {
        self.metadata.version = version;
    }

    fn message_id(&self) -> &str {
        &self.metadata.message_id
    }

    fn set_message_id(&mut self, id: String) {
        self.metadata.message_id = id;
    }

    fn sender_id(&self) -> &str {
        &self.metadata.sender_id
    }

    fn set_sender_id(&mut self, id: String) {
        self.metadata.sender_id = id;
    }

    fn pubkey(&self) -> &[u8] {
        &self.metadata.pubkey
    }

    fn set_pubkey(&mut self, pubkey: Vec<u8>) {
        self.metadata.pubkey = pubkey;
    }

    fn signature(&self) -> Option<&[u8]> {
        self.metadata.signature.as_deref()
    }

    fn set_signature(&mut self, signature: Option<Vec<u8>>) {
        self.metadata.signature = signature;
    }

    fn err_message(&self) -> Option<&str> {
        self.metadata.err_message.as_deref()
    }
}

/// Individual document sync result containing head CIDs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocSyncItem {
    /// Document ID.
    #[serde(rename = "DocID")]
    pub doc_id: String,

    /// Head CIDs as byte arrays.
    #[serde(rename = "Heads", with = "vec_of_bytes")]
    pub heads: Vec<Vec<u8>>,
}

/// DocSync reply message sent in response to a DocSyncRequest.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DocSyncReply {
    /// DefraDB message version.
    #[serde(rename = "Version")]
    pub version: String,

    /// Unique message identifier. Responses use the same ID as the request.
    #[serde(rename = "MessageID")]
    pub message_id: String,

    /// ID of the sender (PeerID when using libp2p).
    #[serde(rename = "SenderID")]
    pub sender_id: String,

    /// Public key of the node that created the message.
    #[serde(rename = "Pubkey", with = "nullable_bytes")]
    pub pubkey: Vec<u8>,

    /// Signature for message authentication.
    #[serde(
        rename = "Signature",
        skip_serializing_if = "Option::is_none",
        default,
        with = "optional_bytes"
    )]
    pub signature: Option<Vec<u8>>,

    /// Error message if something went wrong.
    #[serde(rename = "ErrMessage", skip_serializing_if = "Option::is_none")]
    pub err_message: Option<String>,

    /// Results for each requested document.
    #[serde(rename = "Results", default)]
    pub results: Vec<DocSyncItem>,
}

impl DocSyncReply {
    /// Create a new successful DocSyncReply.
    pub fn success(request_message_id: &str, results: Vec<DocSyncItem>) -> Self {
        Self {
            version: crate::protocol::MESSAGE_VERSION.to_string(),
            message_id: request_message_id.to_string(),
            sender_id: String::new(),
            pubkey: Vec::new(),
            signature: None,
            err_message: None,
            results,
        }
    }

    /// Create a new error DocSyncReply.
    pub fn error(request_message_id: &str, err: &str) -> Self {
        Self {
            version: crate::protocol::MESSAGE_VERSION.to_string(),
            message_id: request_message_id.to_string(),
            sender_id: String::new(),
            pubkey: Vec::new(),
            signature: None,
            err_message: Some(err.to_string()),
            results: Vec::new(),
        }
    }
}

impl Message for DocSyncReply {
    fn version(&self) -> &str {
        &self.version
    }

    fn set_version(&mut self, version: String) {
        self.version = version;
    }

    fn message_id(&self) -> &str {
        &self.message_id
    }

    fn set_message_id(&mut self, id: String) {
        self.message_id = id;
    }

    fn sender_id(&self) -> &str {
        &self.sender_id
    }

    fn set_sender_id(&mut self, id: String) {
        self.sender_id = id;
    }

    fn pubkey(&self) -> &[u8] {
        &self.pubkey
    }

    fn set_pubkey(&mut self, pubkey: Vec<u8>) {
        self.pubkey = pubkey;
    }

    fn signature(&self) -> Option<&[u8]> {
        self.signature.as_deref()
    }

    fn set_signature(&mut self, signature: Option<Vec<u8>>) {
        self.signature = signature;
    }

    fn err_message(&self) -> Option<&str> {
        self.err_message.as_deref()
    }
}

/// Branchable collection sync request.
///
/// Sent to peers to ask for their head CIDs for a branchable collection.
/// Uses two-stream protocol (same transport as DocSync).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchableSyncRequest {
    /// Message metadata.
    #[serde(flatten)]
    pub metadata: MetaData,

    /// Collection ID to sync.
    #[serde(rename = "CollectionID")]
    pub collection_id: String,
}

impl BranchableSyncRequest {
    /// Create a new BranchableSyncRequest.
    pub fn new(collection_id: String) -> Self {
        Self {
            metadata: MetaData::new(),
            collection_id,
        }
    }
}

impl Message for BranchableSyncRequest {
    fn version(&self) -> &str {
        &self.metadata.version
    }

    fn set_version(&mut self, version: String) {
        self.metadata.version = version;
    }

    fn message_id(&self) -> &str {
        &self.metadata.message_id
    }

    fn set_message_id(&mut self, id: String) {
        self.metadata.message_id = id;
    }

    fn sender_id(&self) -> &str {
        &self.metadata.sender_id
    }

    fn set_sender_id(&mut self, id: String) {
        self.metadata.sender_id = id;
    }

    fn pubkey(&self) -> &[u8] {
        &self.metadata.pubkey
    }

    fn set_pubkey(&mut self, pubkey: Vec<u8>) {
        self.metadata.pubkey = pubkey;
    }

    fn signature(&self) -> Option<&[u8]> {
        self.metadata.signature.as_deref()
    }

    fn set_signature(&mut self, signature: Option<Vec<u8>>) {
        self.metadata.signature = signature;
    }

    fn err_message(&self) -> Option<&str> {
        self.metadata.err_message.as_deref()
    }
}

/// Branchable collection sync reply with head CIDs.
///
/// Uses flat metadata fields (no `#[serde(flatten)]`) for CBOR wire compatibility.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BranchableSyncReply {
    /// DefraDB message version.
    #[serde(rename = "Version")]
    pub version: String,

    /// Unique message identifier.
    #[serde(rename = "MessageID")]
    pub message_id: String,

    /// ID of the sender.
    #[serde(rename = "SenderID")]
    pub sender_id: String,

    /// Public key of the node.
    #[serde(rename = "Pubkey", with = "nullable_bytes")]
    pub pubkey: Vec<u8>,

    /// Signature for message authentication.
    #[serde(
        rename = "Signature",
        skip_serializing_if = "Option::is_none",
        default,
        with = "optional_bytes"
    )]
    pub signature: Option<Vec<u8>>,

    /// Error message if something went wrong.
    #[serde(rename = "ErrMessage", skip_serializing_if = "Option::is_none")]
    pub err_message: Option<String>,

    /// Collection ID this reply is for.
    #[serde(rename = "CollectionID")]
    pub collection_id: String,

    /// Head CIDs as byte arrays.
    #[serde(rename = "Heads", with = "vec_of_bytes", default)]
    pub heads: Vec<Vec<u8>>,
}

impl BranchableSyncReply {
    /// Create a new successful BranchableSyncReply.
    pub fn success(request_message_id: &str, collection_id: &str, heads: Vec<Vec<u8>>) -> Self {
        Self {
            version: crate::protocol::MESSAGE_VERSION.to_string(),
            message_id: request_message_id.to_string(),
            sender_id: String::new(),
            pubkey: Vec::new(),
            signature: None,
            err_message: None,
            collection_id: collection_id.to_string(),
            heads,
        }
    }

    /// Create a new error BranchableSyncReply.
    pub fn error(request_message_id: &str, collection_id: &str, err: &str) -> Self {
        Self {
            version: crate::protocol::MESSAGE_VERSION.to_string(),
            message_id: request_message_id.to_string(),
            sender_id: String::new(),
            pubkey: Vec::new(),
            signature: None,
            err_message: Some(err.to_string()),
            collection_id: collection_id.to_string(),
            heads: Vec::new(),
        }
    }
}

impl Message for BranchableSyncReply {
    fn version(&self) -> &str {
        &self.version
    }

    fn set_version(&mut self, version: String) {
        self.version = version;
    }

    fn message_id(&self) -> &str {
        &self.message_id
    }

    fn set_message_id(&mut self, id: String) {
        self.message_id = id;
    }

    fn sender_id(&self) -> &str {
        &self.sender_id
    }

    fn set_sender_id(&mut self, id: String) {
        self.sender_id = id;
    }

    fn pubkey(&self) -> &[u8] {
        &self.pubkey
    }

    fn set_pubkey(&mut self, pubkey: Vec<u8>) {
        self.pubkey = pubkey;
    }

    fn signature(&self) -> Option<&[u8]> {
        self.signature.as_deref()
    }

    fn set_signature(&mut self, signature: Option<Vec<u8>>) {
        self.signature = signature;
    }

    fn err_message(&self) -> Option<&str> {
        self.err_message.as_deref()
    }
}

/// PushLog broadcast message for GossipSub publishing.
///
/// This is a lightweight version of PushLogRequest used for pubsub broadcasts.
/// Unlike request-response messages, pubsub messages do NOT include MetaData
/// signing fields - libp2p's GossipSub handles message authentication via
/// `MessageAuthenticity::Signed`.
///
/// # Wire Compatibility
///
/// This matches Go's approach where PushLogRequest is CBOR-encoded WITHOUT
/// the MetaData signing fields for pubsub, since the sender peer ID comes
/// from the pubsub layer itself.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PushLogBroadcast {
    /// Document ID being updated.
    #[serde(rename = "DocID")]
    pub doc_id: String,

    /// Content ID (CID) of the block.
    /// Uses serde_bytes for CBOR byte string compatibility with Go.
    #[serde(rename = "CID", with = "serde_bytes")]
    pub cid: Vec<u8>,

    /// Collection ID the document belongs to.
    #[serde(rename = "CollectionID")]
    pub collection_id: String,

    /// Creator/author of the update.
    #[serde(rename = "Creator")]
    pub creator: String,

    /// The IPLD block data.
    /// Uses serde_bytes for CBOR byte string compatibility with Go.
    #[serde(rename = "Block", with = "serde_bytes")]
    pub block: Vec<u8>,
}

impl PushLogBroadcast {
    /// Create a new PushLogBroadcast.
    pub fn new(
        doc_id: String,
        cid: Vec<u8>,
        collection_id: String,
        creator: String,
        block: Vec<u8>,
    ) -> Self {
        Self {
            doc_id,
            cid,
            collection_id,
            creator,
            block,
        }
    }

    /// Convert from a PushLogRequest (strips metadata).
    pub fn from_request(req: &PushLogRequest) -> Self {
        Self {
            doc_id: req.doc_id.clone(),
            cid: req.cid.clone(),
            collection_id: req.collection_id.clone(),
            creator: req.creator.clone(),
            block: req.block.clone(),
        }
    }

    /// Convert to a PushLogRequest (adds default metadata).
    pub fn to_request(&self) -> PushLogRequest {
        PushLogRequest::new(
            self.doc_id.clone(),
            self.cid.clone(),
            self.collection_id.clone(),
            self.creator.clone(),
            self.block.clone(),
        )
    }
}

// =============================================================================
// Searchable Encryption (SE) Messages
// =============================================================================
//
// These message types are used for searchable encryption artifact replication
// and querying. They match Go's internal/se/dto.go exactly for wire compatibility.

/// SE field query for searching encrypted indexes.
///
/// Used to query a specific encrypted field on a remote node.
/// Matches Go's SEFieldQuery struct.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SEFieldQuery {
    /// Name of the indexed field.
    #[serde(rename = "FieldName")]
    pub field_name: String,

    /// Index identifier (typically the field name).
    #[serde(rename = "IndexID")]
    pub index_id: String,

    /// The search tag computed from the query value.
    #[serde(rename = "SearchTag", with = "serde_bytes")]
    pub search_tag: Vec<u8>,
}

impl SEFieldQuery {
    /// Create a new SE field query.
    pub fn new(field_name: impl Into<String>, index_id: impl Into<String>, search_tag: Vec<u8>) -> Self {
        Self {
            field_name: field_name.into(),
            index_id: index_id.into(),
            search_tag,
        }
    }
}

/// Request to query SE artifacts from a replicator node.
///
/// The client sends search tags computed from query values, and the
/// replicator returns matching document IDs without ever seeing
/// the actual data values.
///
/// Matches Go's QuerySEArtifactsRequest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuerySEArtifactsRequest {
    /// Message metadata.
    #[serde(flatten)]
    pub metadata: MetaData,

    /// Collection identifier.
    #[serde(rename = "CollectionID")]
    pub collection_id: String,

    /// Queries for each encrypted field.
    #[serde(rename = "Queries")]
    pub queries: Vec<SEFieldQuery>,
}

impl QuerySEArtifactsRequest {
    /// Create a new QuerySEArtifactsRequest.
    pub fn new(collection_id: impl Into<String>, queries: Vec<SEFieldQuery>) -> Self {
        Self {
            metadata: MetaData::new(),
            collection_id: collection_id.into(),
            queries,
        }
    }
}

impl Message for QuerySEArtifactsRequest {
    fn version(&self) -> &str {
        &self.metadata.version
    }

    fn set_version(&mut self, version: String) {
        self.metadata.version = version;
    }

    fn message_id(&self) -> &str {
        &self.metadata.message_id
    }

    fn set_message_id(&mut self, id: String) {
        self.metadata.message_id = id;
    }

    fn sender_id(&self) -> &str {
        &self.metadata.sender_id
    }

    fn set_sender_id(&mut self, id: String) {
        self.metadata.sender_id = id;
    }

    fn pubkey(&self) -> &[u8] {
        &self.metadata.pubkey
    }

    fn set_pubkey(&mut self, pubkey: Vec<u8>) {
        self.metadata.pubkey = pubkey;
    }

    fn signature(&self) -> Option<&[u8]> {
        self.metadata.signature.as_deref()
    }

    fn set_signature(&mut self, signature: Option<Vec<u8>>) {
        self.metadata.signature = signature;
    }

    fn err_message(&self) -> Option<&str> {
        self.metadata.err_message.as_deref()
    }
}

/// Reply to QuerySEArtifactsRequest with matching document IDs.
///
/// Matches Go's QuerySEArtifactsReply.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QuerySEArtifactsReply {
    /// DefraDB message version.
    #[serde(rename = "Version")]
    pub version: String,

    /// Unique message identifier. Responses use the same ID as the request.
    #[serde(rename = "MessageID")]
    pub message_id: String,

    /// ID of the sender (PeerID when using libp2p).
    #[serde(rename = "SenderID")]
    pub sender_id: String,

    /// Public key of the node that created the message.
    #[serde(rename = "Pubkey", with = "nullable_bytes")]
    pub pubkey: Vec<u8>,

    /// Signature for message authentication.
    #[serde(
        rename = "Signature",
        skip_serializing_if = "Option::is_none",
        default,
        with = "optional_bytes"
    )]
    pub signature: Option<Vec<u8>>,

    /// Error message if something went wrong.
    #[serde(rename = "ErrMessage", skip_serializing_if = "Option::is_none")]
    pub err_message: Option<String>,

    /// Matching document IDs.
    #[serde(rename = "DocIDs", default)]
    pub doc_ids: Vec<String>,
}

impl QuerySEArtifactsReply {
    /// Create a new successful QuerySEArtifactsReply.
    pub fn success(request_message_id: &str, doc_ids: Vec<String>) -> Self {
        Self {
            version: MESSAGE_VERSION.to_string(),
            message_id: request_message_id.to_string(),
            sender_id: String::new(),
            pubkey: Vec::new(),
            signature: None,
            err_message: None,
            doc_ids,
        }
    }

    /// Create a new error QuerySEArtifactsReply.
    pub fn error(request_message_id: &str, err: &str) -> Self {
        Self {
            version: MESSAGE_VERSION.to_string(),
            message_id: request_message_id.to_string(),
            sender_id: String::new(),
            pubkey: Vec::new(),
            signature: None,
            err_message: Some(err.to_string()),
            doc_ids: Vec::new(),
        }
    }
}

impl Message for QuerySEArtifactsReply {
    fn version(&self) -> &str {
        &self.version
    }

    fn set_version(&mut self, version: String) {
        self.version = version;
    }

    fn message_id(&self) -> &str {
        &self.message_id
    }

    fn set_message_id(&mut self, id: String) {
        self.message_id = id;
    }

    fn sender_id(&self) -> &str {
        &self.sender_id
    }

    fn set_sender_id(&mut self, id: String) {
        self.sender_id = id;
    }

    fn pubkey(&self) -> &[u8] {
        &self.pubkey
    }

    fn set_pubkey(&mut self, pubkey: Vec<u8>) {
        self.pubkey = pubkey;
    }

    fn signature(&self) -> Option<&[u8]> {
        self.signature.as_deref()
    }

    fn set_signature(&mut self, signature: Option<Vec<u8>>) {
        self.signature = signature;
    }

    fn err_message(&self) -> Option<&str> {
        self.err_message.as_deref()
    }
}

/// SE artifact to be pushed to a replicator.
///
/// Matches Go's SEArtifact struct.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SEArtifact {
    /// Document identifier.
    #[serde(rename = "DocID")]
    pub doc_id: String,

    /// Index identifier (typically the field name).
    #[serde(rename = "IndexID")]
    pub index_id: String,

    /// The search tag for this document's field value.
    #[serde(rename = "SearchTag", with = "serde_bytes")]
    pub search_tag: Vec<u8>,
}

impl SEArtifact {
    /// Create a new SE artifact.
    pub fn new(doc_id: impl Into<String>, index_id: impl Into<String>, search_tag: Vec<u8>) -> Self {
        Self {
            doc_id: doc_id.into(),
            index_id: index_id.into(),
            search_tag,
        }
    }
}

/// Request to push SE artifacts to a replicator node.
///
/// The producer sends artifacts when documents are created/updated.
/// The replicator stores them for later querying.
///
/// Matches Go's PushSEArtifactsRequest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushSEArtifactsRequest {
    /// Message metadata.
    #[serde(flatten)]
    pub metadata: MetaData,

    /// Collection identifier.
    #[serde(rename = "CollectionID")]
    pub collection_id: String,

    /// Artifacts to push.
    #[serde(rename = "Artifacts")]
    pub artifacts: Vec<SEArtifact>,
}

impl PushSEArtifactsRequest {
    /// Create a new PushSEArtifactsRequest.
    pub fn new(collection_id: impl Into<String>, artifacts: Vec<SEArtifact>) -> Self {
        Self {
            metadata: MetaData::new(),
            collection_id: collection_id.into(),
            artifacts,
        }
    }
}

impl Message for PushSEArtifactsRequest {
    fn version(&self) -> &str {
        &self.metadata.version
    }

    fn set_version(&mut self, version: String) {
        self.metadata.version = version;
    }

    fn message_id(&self) -> &str {
        &self.metadata.message_id
    }

    fn set_message_id(&mut self, id: String) {
        self.metadata.message_id = id;
    }

    fn sender_id(&self) -> &str {
        &self.metadata.sender_id
    }

    fn set_sender_id(&mut self, id: String) {
        self.metadata.sender_id = id;
    }

    fn pubkey(&self) -> &[u8] {
        &self.metadata.pubkey
    }

    fn set_pubkey(&mut self, pubkey: Vec<u8>) {
        self.metadata.pubkey = pubkey;
    }

    fn signature(&self) -> Option<&[u8]> {
        self.metadata.signature.as_deref()
    }

    fn set_signature(&mut self, signature: Option<Vec<u8>>) {
        self.metadata.signature = signature;
    }

    fn err_message(&self) -> Option<&str> {
        self.metadata.err_message.as_deref()
    }
}

/// Reply to PushSEArtifactsRequest acknowledging receipt.
///
/// Matches Go's PushSEArtifactsReply.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PushSEArtifactsReply {
    /// DefraDB message version.
    #[serde(rename = "Version")]
    pub version: String,

    /// Unique message identifier. Responses use the same ID as the request.
    #[serde(rename = "MessageID")]
    pub message_id: String,

    /// ID of the sender (PeerID when using libp2p).
    #[serde(rename = "SenderID")]
    pub sender_id: String,

    /// Public key of the node that created the message.
    #[serde(rename = "Pubkey", with = "nullable_bytes")]
    pub pubkey: Vec<u8>,

    /// Signature for message authentication.
    #[serde(
        rename = "Signature",
        skip_serializing_if = "Option::is_none",
        default,
        with = "optional_bytes"
    )]
    pub signature: Option<Vec<u8>>,

    /// Error message if something went wrong.
    #[serde(rename = "ErrMessage", skip_serializing_if = "Option::is_none")]
    pub err_message: Option<String>,
}

impl PushSEArtifactsReply {
    /// Create a new successful PushSEArtifactsReply.
    pub fn success(request_message_id: &str) -> Self {
        Self {
            version: MESSAGE_VERSION.to_string(),
            message_id: request_message_id.to_string(),
            sender_id: String::new(),
            pubkey: Vec::new(),
            signature: None,
            err_message: None,
        }
    }

    /// Create a new error PushSEArtifactsReply.
    pub fn error(request_message_id: &str, err: &str) -> Self {
        Self {
            version: MESSAGE_VERSION.to_string(),
            message_id: request_message_id.to_string(),
            sender_id: String::new(),
            pubkey: Vec::new(),
            signature: None,
            err_message: Some(err.to_string()),
        }
    }
}

impl Message for PushSEArtifactsReply {
    fn version(&self) -> &str {
        &self.version
    }

    fn set_version(&mut self, version: String) {
        self.version = version;
    }

    fn message_id(&self) -> &str {
        &self.message_id
    }

    fn set_message_id(&mut self, id: String) {
        self.message_id = id;
    }

    fn sender_id(&self) -> &str {
        &self.sender_id
    }

    fn set_sender_id(&mut self, id: String) {
        self.sender_id = id;
    }

    fn pubkey(&self) -> &[u8] {
        &self.pubkey
    }

    fn set_pubkey(&mut self, pubkey: Vec<u8>) {
        self.pubkey = pubkey;
    }

    fn signature(&self) -> Option<&[u8]> {
        self.signature.as_deref()
    }

    fn set_signature(&mut self, signature: Option<Vec<u8>>) {
        self.signature = signature;
    }

    fn err_message(&self) -> Option<&str> {
        self.err_message.as_deref()
    }
}
