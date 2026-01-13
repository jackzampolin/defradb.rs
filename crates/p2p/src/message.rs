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

    #[test]
    fn test_metadata_new() {
        let metadata = MetaData::new();
        assert_eq!(metadata.version, MESSAGE_VERSION);
        assert!(metadata.message_id.is_empty());
        assert!(metadata.sender_id.is_empty());
        assert!(metadata.pubkey.is_empty());
        assert!(metadata.signature.is_none());
        assert!(metadata.err_message.is_none());
    }

    #[test]
    fn test_metadata_set_version() {
        let mut metadata = MetaData::default();
        assert!(metadata.version.is_empty());

        metadata.set_version();
        assert_eq!(metadata.version, MESSAGE_VERSION);
    }

    #[test]
    fn test_message_trait_accessors_pushlog_request() {
        let mut request = PushLogRequest::new(
            "doc456".to_string(),
            vec![10, 20],
            "col2".to_string(),
            "creator2".to_string(),
            vec![30, 40],
        );

        // Set metadata fields
        request.metadata.message_id = "test-msg-id".to_string();
        request.metadata.sender_id = "sender-peer-id".to_string();
        request.metadata.pubkey = vec![1, 2, 3, 4, 5];
        request.metadata.signature = Some(vec![6, 7, 8, 9]);
        request.metadata.err_message = Some("test error".to_string());

        // Test trait accessors
        assert_eq!(request.version(), MESSAGE_VERSION);
        assert_eq!(request.message_id(), "test-msg-id");
        assert_eq!(request.sender_id(), "sender-peer-id");
        assert_eq!(request.pubkey(), &[1, 2, 3, 4, 5]);
        assert_eq!(request.signature(), Some(&[6u8, 7, 8, 9][..]));
        assert_eq!(request.err_message(), Some("test error"));

        // Test mutable access
        request.metadata_mut().message_id = "new-msg-id".to_string();
        assert_eq!(request.message_id(), "new-msg-id");
    }

    #[test]
    fn test_message_trait_accessors_pushlog_reply() {
        let mut reply = PushLogReply::success("reply-id");

        // Set additional metadata
        reply.metadata.sender_id = "replier-id".to_string();
        reply.metadata.pubkey = vec![11, 22, 33];

        // Test trait accessors
        assert_eq!(reply.version(), MESSAGE_VERSION);
        assert_eq!(reply.message_id(), "reply-id");
        assert_eq!(reply.sender_id(), "replier-id");
        assert_eq!(reply.pubkey(), &[11, 22, 33]);
        assert!(reply.signature().is_none());
        assert!(reply.err_message().is_none());

        // Test error reply
        let error_reply = PushLogReply::error("error-id", "failed");
        assert_eq!(error_reply.err_message(), Some("failed"));
    }

    #[test]
    fn test_pushlog_request_cbor_field_names() {
        // This test verifies CBOR field names match Go implementation
        let request = PushLogRequest::new(
            "doc789".to_string(),
            vec![1, 2, 3],
            "collection3".to_string(),
            "creator3".to_string(),
            vec![4, 5, 6],
        );

        let encoded = serde_cbor::to_vec(&request).expect("failed to encode");

        // Decode as a generic CBOR value to check field names
        let value: serde_cbor::Value =
            serde_cbor::from_slice(&encoded).expect("failed to decode as Value");

        if let serde_cbor::Value::Map(map) = value {
            // Check that Go-compatible field names are used
            let has_version = map
                .iter()
                .any(|(k, _)| k == &serde_cbor::Value::Text("Version".to_string()));
            let has_doc_id = map
                .iter()
                .any(|(k, _)| k == &serde_cbor::Value::Text("DocID".to_string()));
            let has_cid = map
                .iter()
                .any(|(k, _)| k == &serde_cbor::Value::Text("CID".to_string()));
            let has_collection_id = map
                .iter()
                .any(|(k, _)| k == &serde_cbor::Value::Text("CollectionID".to_string()));
            let has_creator = map
                .iter()
                .any(|(k, _)| k == &serde_cbor::Value::Text("Creator".to_string()));
            let has_block = map
                .iter()
                .any(|(k, _)| k == &serde_cbor::Value::Text("Block".to_string()));

            assert!(has_version, "Missing 'Version' field");
            assert!(has_doc_id, "Missing 'DocID' field");
            assert!(has_cid, "Missing 'CID' field");
            assert!(has_collection_id, "Missing 'CollectionID' field");
            assert!(has_creator, "Missing 'Creator' field");
            assert!(has_block, "Missing 'Block' field");
        } else {
            panic!("Expected CBOR map");
        }
    }

    #[test]
    fn test_pushlog_reply_cbor_field_names() {
        // Verify reply field names for Go compatibility
        let reply = PushLogReply::success("msg123");

        let encoded = serde_cbor::to_vec(&reply).expect("failed to encode");
        let value: serde_cbor::Value =
            serde_cbor::from_slice(&encoded).expect("failed to decode as Value");

        if let serde_cbor::Value::Map(map) = value {
            let has_version = map
                .iter()
                .any(|(k, _)| k == &serde_cbor::Value::Text("Version".to_string()));
            let has_message_id = map
                .iter()
                .any(|(k, _)| k == &serde_cbor::Value::Text("MessageID".to_string()));

            assert!(has_version, "Missing 'Version' field");
            assert!(has_message_id, "Missing 'MessageID' field");
        } else {
            panic!("Expected CBOR map");
        }
    }

    #[test]
    fn test_optional_fields_omitted() {
        // Verify optional fields are omitted when None (matching Go's omitempty)
        let reply = PushLogReply::success("msg123");

        let encoded = serde_cbor::to_vec(&reply).expect("failed to encode");
        let value: serde_cbor::Value =
            serde_cbor::from_slice(&encoded).expect("failed to decode as Value");

        if let serde_cbor::Value::Map(map) = value {
            // Signature and ErrMessage should NOT be present when None
            let has_signature = map
                .iter()
                .any(|(k, _)| k == &serde_cbor::Value::Text("Signature".to_string()));
            let has_err_message = map
                .iter()
                .any(|(k, _)| k == &serde_cbor::Value::Text("ErrMessage".to_string()));

            assert!(!has_signature, "Signature should be omitted when None");
            assert!(!has_err_message, "ErrMessage should be omitted when None");
        } else {
            panic!("Expected CBOR map");
        }
    }

    #[test]
    fn test_optional_fields_included_when_set() {
        // Verify optional fields ARE included when set
        let reply = PushLogReply::error("msg123", "error message");

        let encoded = serde_cbor::to_vec(&reply).expect("failed to encode");
        let value: serde_cbor::Value =
            serde_cbor::from_slice(&encoded).expect("failed to decode as Value");

        if let serde_cbor::Value::Map(map) = value {
            let has_err_message = map
                .iter()
                .any(|(k, _)| k == &serde_cbor::Value::Text("ErrMessage".to_string()));

            assert!(has_err_message, "ErrMessage should be present when set");
        } else {
            panic!("Expected CBOR map");
        }
    }

    #[test]
    fn test_large_block_data() {
        // Test with large block data (realistic scenario)
        let large_block = vec![0xAB; 1024 * 1024]; // 1 MB block

        let request = PushLogRequest::new(
            "large-doc".to_string(),
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            "collection".to_string(),
            "creator".to_string(),
            large_block.clone(),
        );

        let encoded = serde_cbor::to_vec(&request).expect("failed to encode large request");
        let decoded: PushLogRequest =
            serde_cbor::from_slice(&encoded).expect("failed to decode large request");

        assert_eq!(decoded.block.len(), 1024 * 1024);
        assert_eq!(decoded.block, large_block);
    }

    #[test]
    fn test_empty_fields() {
        // Test with empty but valid fields
        let request = PushLogRequest::new(
            "".to_string(),
            vec![],
            "".to_string(),
            "".to_string(),
            vec![],
        );

        let encoded = serde_cbor::to_vec(&request).expect("failed to encode");
        let decoded: PushLogRequest =
            serde_cbor::from_slice(&encoded).expect("failed to decode");

        assert!(decoded.doc_id.is_empty());
        assert!(decoded.cid.is_empty());
        assert!(decoded.collection_id.is_empty());
        assert!(decoded.creator.is_empty());
        assert!(decoded.block.is_empty());
    }

    #[test]
    fn test_pushlog_broadcast_serialization() {
        let broadcast = PushLogBroadcast::new(
            "doc123".to_string(),
            vec![1, 2, 3, 4],
            "collection1".to_string(),
            "creator1".to_string(),
            vec![5, 6, 7, 8],
        );

        let encoded = serde_cbor::to_vec(&broadcast).expect("failed to encode");
        let decoded: PushLogBroadcast =
            serde_cbor::from_slice(&encoded).expect("failed to decode");

        assert_eq!(decoded.doc_id, "doc123");
        assert_eq!(decoded.cid, vec![1, 2, 3, 4]);
        assert_eq!(decoded.collection_id, "collection1");
        assert_eq!(decoded.creator, "creator1");
        assert_eq!(decoded.block, vec![5, 6, 7, 8]);
    }

    #[test]
    fn test_pushlog_broadcast_cbor_field_names() {
        // Verify CBOR field names match Go implementation WITHOUT MetaData fields
        let broadcast = PushLogBroadcast::new(
            "doc789".to_string(),
            vec![1, 2, 3],
            "collection3".to_string(),
            "creator3".to_string(),
            vec![4, 5, 6],
        );

        let encoded = serde_cbor::to_vec(&broadcast).expect("failed to encode");
        let value: serde_cbor::Value =
            serde_cbor::from_slice(&encoded).expect("failed to decode as Value");

        if let serde_cbor::Value::Map(map) = value {
            let field_names: Vec<String> = map
                .iter()
                .filter_map(|(k, _)| {
                    if let serde_cbor::Value::Text(s) = k {
                        Some(s.clone())
                    } else {
                        None
                    }
                })
                .collect();

            // Should have Go-compatible field names
            assert!(field_names.contains(&"DocID".to_string()), "Missing DocID");
            assert!(field_names.contains(&"CID".to_string()), "Missing CID");
            assert!(
                field_names.contains(&"CollectionID".to_string()),
                "Missing CollectionID"
            );
            assert!(field_names.contains(&"Creator".to_string()), "Missing Creator");
            assert!(field_names.contains(&"Block".to_string()), "Missing Block");

            // Should NOT have MetaData fields (pubsub doesn't use them)
            assert!(
                !field_names.contains(&"Version".to_string()),
                "Version should not be present in broadcast"
            );
            assert!(
                !field_names.contains(&"MessageID".to_string()),
                "MessageID should not be present in broadcast"
            );
            assert!(
                !field_names.contains(&"SenderID".to_string()),
                "SenderID should not be present in broadcast"
            );
            assert!(
                !field_names.contains(&"Signature".to_string()),
                "Signature should not be present in broadcast"
            );
        } else {
            panic!("Expected CBOR map");
        }
    }

    #[test]
    fn test_pushlog_broadcast_from_request() {
        let request = PushLogRequest::new(
            "doc456".to_string(),
            vec![10, 20, 30],
            "col2".to_string(),
            "creator2".to_string(),
            vec![40, 50, 60],
        );

        let broadcast = PushLogBroadcast::from_request(&request);

        assert_eq!(broadcast.doc_id, request.doc_id);
        assert_eq!(broadcast.cid, request.cid);
        assert_eq!(broadcast.collection_id, request.collection_id);
        assert_eq!(broadcast.creator, request.creator);
        assert_eq!(broadcast.block, request.block);
    }

    #[test]
    fn test_pushlog_broadcast_to_request() {
        let broadcast = PushLogBroadcast::new(
            "doc789".to_string(),
            vec![70, 80, 90],
            "col3".to_string(),
            "creator3".to_string(),
            vec![100, 110, 120],
        );

        let request = broadcast.to_request();

        assert_eq!(request.doc_id, broadcast.doc_id);
        assert_eq!(request.cid, broadcast.cid);
        assert_eq!(request.collection_id, broadcast.collection_id);
        assert_eq!(request.creator, broadcast.creator);
        assert_eq!(request.block, broadcast.block);
        // Request should have default metadata
        assert_eq!(request.metadata.version, MESSAGE_VERSION);
    }
}
