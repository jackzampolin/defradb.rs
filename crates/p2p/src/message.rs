// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Wire message types for DefraDB P2P protocol.
//!
//! Messages are CBOR encoded for wire compatibility with the Go implementation.
//! CBOR byte strings (major type 2) require serde_bytes annotations for Vec<u8>.

use serde::{Deserialize, Serialize};

use crate::protocol::MESSAGE_VERSION;

/// Custom serialization for Option<Vec<u8>> as CBOR byte strings.
/// Needed because serde_bytes doesn't directly support Option<Vec<u8>>.
mod optional_bytes {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

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
    /// Uses serde_bytes for CBOR byte string compatibility with Go.
    #[serde(rename = "Pubkey", with = "serde_bytes")]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushLogReply {
    /// Message metadata.
    #[serde(flatten)]
    pub metadata: MetaData,
}

impl PushLogReply {
    /// Create a new successful PushLogReply.
    pub fn success(request_message_id: &str) -> Self {
        let mut metadata = MetaData::new();
        metadata.message_id = request_message_id.to_string();
        Self { metadata }
    }

    /// Create a new error PushLogReply.
    pub fn error(request_message_id: &str, err: &str) -> Self {
        let mut metadata = MetaData::new();
        metadata.message_id = request_message_id.to_string();
        metadata.err_message = Some(err.to_string());
        Self { metadata }
    }
}

/// Trait for types that can be P2P messages.
pub trait Message {
    /// Get the message metadata.
    fn metadata(&self) -> &MetaData;

    /// Get mutable access to message metadata.
    fn metadata_mut(&mut self) -> &mut MetaData;

    /// Get the message version.
    fn version(&self) -> &str {
        &self.metadata().version
    }

    /// Get the message ID.
    fn message_id(&self) -> &str {
        &self.metadata().message_id
    }

    /// Get the sender ID.
    fn sender_id(&self) -> &str {
        &self.metadata().sender_id
    }

    /// Get the public key.
    fn pubkey(&self) -> &[u8] {
        &self.metadata().pubkey
    }

    /// Get the signature if present.
    fn signature(&self) -> Option<&[u8]> {
        self.metadata().signature.as_deref()
    }

    /// Get the error message if present.
    fn err_message(&self) -> Option<&str> {
        self.metadata().err_message.as_deref()
    }
}

impl Message for PushLogRequest {
    fn metadata(&self) -> &MetaData {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut MetaData {
        &mut self.metadata
    }
}

impl Message for PushLogReply {
    fn metadata(&self) -> &MetaData {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut MetaData {
        &mut self.metadata
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
