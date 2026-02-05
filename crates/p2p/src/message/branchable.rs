//! Branchable collection sync message types.

use serde::{Deserialize, Serialize};

use super::cbor::{nullable_bytes, optional_bytes, vec_of_bytes};
use super::metadata::MetaData;
use super::traits::Message;

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
