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

use serde::{Deserialize, Serialize};

use crate::protocol::MESSAGE_VERSION;

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
    #[serde(rename = "Pubkey")]
    pub pubkey: Vec<u8>,

    /// Signature for message authentication.
    #[serde(rename = "Signature", skip_serializing_if = "Option::is_none")]
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
    #[serde(rename = "CID")]
    pub cid: Vec<u8>,

    /// Collection ID the document belongs to.
    #[serde(rename = "CollectionID")]
    pub collection_id: String,

    /// Creator/author of the update.
    #[serde(rename = "Creator")]
    pub creator: String,

    /// The IPLD block data.
    #[serde(rename = "Block")]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pushlog_request_serialization() {
        let request = PushLogRequest::new(
            "doc123".to_string(),
            vec![1, 2, 3, 4],
            "collection1".to_string(),
            "creator1".to_string(),
            vec![5, 6, 7, 8],
        );

        let encoded = serde_cbor::to_vec(&request).expect("failed to encode");
        let decoded: PushLogRequest =
            serde_cbor::from_slice(&encoded).expect("failed to decode");

        assert_eq!(decoded.doc_id, "doc123");
        assert_eq!(decoded.cid, vec![1, 2, 3, 4]);
        assert_eq!(decoded.collection_id, "collection1");
        assert_eq!(decoded.creator, "creator1");
        assert_eq!(decoded.block, vec![5, 6, 7, 8]);
    }

    #[test]
    fn test_pushlog_reply_success() {
        let reply = PushLogReply::success("msg123");
        assert_eq!(reply.metadata.message_id, "msg123");
        assert!(reply.metadata.err_message.is_none());
    }

    #[test]
    fn test_pushlog_reply_error() {
        let reply = PushLogReply::error("msg123", "something went wrong");
        assert_eq!(reply.metadata.message_id, "msg123");
        assert_eq!(
            reply.metadata.err_message,
            Some("something went wrong".to_string())
        );
    }
}
