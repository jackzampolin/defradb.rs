//! PushLog message types for CRDT synchronization.

use serde::{Deserialize, Serialize};

use super::cbor::{nullable_bytes, optional_bytes};
use super::metadata::MetaData;
use super::traits::Message;
use crate::protocol::MESSAGE_VERSION;

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

    /// Optional sender-issued explicit replay capability for encrypted replay.
    #[serde(
        rename = "ExplicitReplayCapability",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub explicit_replay_capability: Option<String>,
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
            explicit_replay_capability: None,
        }
    }
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
