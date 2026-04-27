//! Searchable Encryption (SE) message types.
//!
//! These message types are used for searchable encryption artifact replication
//! and querying. They match Go's internal/se/dto.go exactly for wire compatibility.

use serde::{Deserialize, Serialize};

use super::cbor::{nullable_bytes, optional_bytes};
use super::traits::Message;
use crate::protocol::MESSAGE_VERSION;

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
    pub fn new(
        field_name: impl Into<String>,
        index_id: impl Into<String>,
        search_tag: Vec<u8>,
    ) -> Self {
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
///
/// Note: We don't use `#[serde(flatten)]` because serde_cbor produces
/// indefinite-length maps when flatten is used (CBOR major type 0xbf).
/// Go's fxamacker/cbor produces definite-length maps, causing signature
/// verification to fail. Instead, we duplicate the fields for wire compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuerySEArtifactsRequest {
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
            version: crate::protocol::MESSAGE_VERSION.to_string(),
            message_id: String::new(),
            sender_id: String::new(),
            pubkey: Vec::new(),
            signature: None,
            err_message: None,
            collection_id: collection_id.into(),
            queries,
        }
    }
}

impl Message for QuerySEArtifactsRequest {
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
    pub fn new(
        doc_id: impl Into<String>,
        index_id: impl Into<String>,
        search_tag: Vec<u8>,
    ) -> Self {
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
///
/// Note: We don't use `#[serde(flatten)]` because serde_cbor produces
/// indefinite-length maps when flatten is used (CBOR major type 0xbf).
/// Go's fxamacker/cbor produces definite-length maps, causing signature
/// verification to fail. Instead, we duplicate the fields for wire compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushSEArtifactsRequest {
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
            version: crate::protocol::MESSAGE_VERSION.to_string(),
            message_id: String::new(),
            sender_id: String::new(),
            pubkey: Vec::new(),
            signature: None,
            err_message: None,
            collection_id: collection_id.into(),
            artifacts,
        }
    }
}

impl Message for PushSEArtifactsRequest {
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
