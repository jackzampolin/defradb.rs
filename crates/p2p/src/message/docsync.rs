//! DocSync message types for pull-based document synchronization.

use serde::{Deserialize, Serialize};

use super::cbor::{nullable_bytes, optional_bytes, vec_of_bytes};
use super::traits::Message;

/// Default maximum number of document IDs allowed in a single DocSyncRequest.
///
/// Coordinators may lower or raise this runtime limit through
/// `SyncConfig::max_doc_sync_request_doc_ids`.
pub const MAX_DOC_IDS: usize = 1000;

/// DocSync request message for pulling specific documents from peers.
///
/// This is used when a node wants to sync specific documents from the network.
/// Unlike replicator sync (push-based), DocSync is pull-based.
///
/// Note: We don't use `#[serde(flatten)]` because serde's flatten produces
/// indefinite-length maps when flatten is used (CBOR major type 0xbf).
/// Go's fxamacker/cbor produces definite-length maps, causing signature
/// verification to fail. Instead, we duplicate the fields for wire compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocSyncRequest {
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

    /// Document IDs to sync.
    #[serde(rename = "DocIDs")]
    pub doc_ids: Vec<String>,
}

impl DocSyncRequest {
    /// Create a new DocSyncRequest.
    pub fn new(doc_ids: Vec<String>) -> Self {
        Self {
            version: crate::protocol::MESSAGE_VERSION.to_string(),
            message_id: String::new(),
            sender_id: String::new(),
            pubkey: Vec::new(),
            signature: None,
            err_message: None,
            doc_ids,
        }
    }
}

impl Message for DocSyncRequest {
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
