//! Tests for the wire message types module.

use bytes::Bytes;
use p2p::message::{
    Message, MetaData, PushLogBroadcast, PushLogGossipPayloadEncoding, PushLogReply, PushLogRequest,
};
use p2p::protocol::MESSAGE_VERSION;

fn encode_with_ciborium<T: serde::Serialize>(value: &T) -> Vec<u8> {
    let mut bytes = Vec::new();
    ciborium::into_writer(value, &mut bytes).expect("failed to encode with ciborium");
    bytes
}

fn encode_with_postcard<T: serde::Serialize>(value: &T) -> Vec<u8> {
    postcard::to_allocvec(value).expect("failed to encode with postcard")
}

fn decode_with_ciborium<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> T {
    ciborium::from_reader(bytes).expect("failed to decode with ciborium")
}

fn has_text_key(map: &[(ciborium::Value, ciborium::Value)], key: &str) -> bool {
    map.iter()
        .any(|(candidate, _)| candidate == &ciborium::Value::Text(key.to_string()))
}

#[test]
fn test_pushlog_request_serialization() {
    let request = PushLogRequest::new(
        "doc123".to_string(),
        Bytes::from(vec![1, 2, 3, 4]),
        "collection1".to_string(),
        "creator1".to_string(),
        Bytes::from(vec![5, 6, 7, 8]),
    );

    let encoded = encode_with_ciborium(&request);
    let decoded: PushLogRequest = decode_with_ciborium(&encoded);

    assert_eq!(decoded.doc_id, "doc123");
    assert_eq!(decoded.cid, vec![1, 2, 3, 4]);
    assert_eq!(decoded.collection_id, "collection1");
    assert_eq!(decoded.creator, "creator1");
    assert_eq!(decoded.block, vec![5, 6, 7, 8]);
    assert!(!decoded.supports_same_stream_reply);
}

#[test]
fn test_pushlog_request_same_stream_reply_capability_is_backward_compatible() {
    let mut request = PushLogRequest::new(
        "doc123".to_string(),
        Bytes::from(vec![1, 2, 3, 4]),
        "collection1".to_string(),
        "creator1".to_string(),
        Bytes::from(vec![5, 6, 7, 8]),
    );

    let encoded = encode_with_ciborium(&request);
    let value: ciborium::Value = decode_with_ciborium(&encoded);
    let ciborium::Value::Map(map) = value else {
        panic!("Expected CBOR map");
    };
    assert!(
        !has_text_key(&map, "SupportsSameStreamReply"),
        "false capability should be omitted for older peers"
    );
    let decoded: PushLogRequest = decode_with_ciborium(&encoded);
    assert!(!decoded.supports_same_stream_reply);

    request.supports_same_stream_reply = true;
    let encoded = encode_with_ciborium(&request);
    let value: ciborium::Value = decode_with_ciborium(&encoded);
    let ciborium::Value::Map(map) = value else {
        panic!("Expected CBOR map");
    };
    assert!(has_text_key(&map, "SupportsSameStreamReply"));
    let decoded: PushLogRequest = decode_with_ciborium(&encoded);
    assert!(decoded.supports_same_stream_reply);
}

#[test]
fn test_pushlog_reply_success() {
    let reply = PushLogReply::success("msg123");
    // PushLogReply has flat fields (not nested metadata)
    assert_eq!(reply.message_id, "msg123");
    assert!(reply.err_message.is_none());
}

#[test]
fn test_pushlog_reply_error() {
    let reply = PushLogReply::error("msg123", "something went wrong");
    // PushLogReply has flat fields (not nested metadata)
    assert_eq!(reply.message_id, "msg123");
    assert_eq!(reply.err_message, Some("something went wrong".to_string()));
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
        Bytes::from(vec![10, 20]),
        "col2".to_string(),
        "creator2".to_string(),
        Bytes::from(vec![30, 40]),
    );

    // Set metadata fields directly on the struct
    request.message_id = "test-msg-id".to_string();
    request.sender_id = "sender-peer-id".to_string();
    request.pubkey = vec![1, 2, 3, 4, 5];
    request.signature = Some(vec![6, 7, 8, 9]);
    request.err_message = Some("test error".to_string());

    // Test trait accessors
    assert_eq!(request.version(), MESSAGE_VERSION);
    assert_eq!(request.message_id(), "test-msg-id");
    assert_eq!(request.sender_id(), "sender-peer-id");
    assert_eq!(request.pubkey(), &[1, 2, 3, 4, 5]);
    assert_eq!(request.signature(), Some(&[6u8, 7, 8, 9][..]));
    assert_eq!(request.err_message(), Some("test error"));

    // Test mutable access via direct field
    request.message_id = "new-msg-id".to_string();
    assert_eq!(request.message_id(), "new-msg-id");
}

#[test]
fn test_message_trait_accessors_pushlog_reply() {
    let mut reply = PushLogReply::success("reply-id");

    // Set additional fields directly (PushLogReply uses flat structure, not nested metadata)
    reply.sender_id = "replier-id".to_string();
    reply.pubkey = vec![11, 22, 33];

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
        Bytes::from(vec![1, 2, 3]),
        "collection3".to_string(),
        "creator3".to_string(),
        Bytes::from(vec![4, 5, 6]),
    );

    let encoded = encode_with_ciborium(&request);

    // Decode as a generic CBOR value to check field names
    let value: ciborium::Value = decode_with_ciborium(&encoded);

    if let ciborium::Value::Map(map) = value {
        // Check that Go-compatible field names are used
        let has_version = has_text_key(&map, "Version");
        let has_doc_id = has_text_key(&map, "DocID");
        let has_cid = has_text_key(&map, "CID");
        let has_collection_id = has_text_key(&map, "CollectionID");
        let has_creator = has_text_key(&map, "Creator");
        let has_block = has_text_key(&map, "Block");

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

    let encoded = encode_with_ciborium(&reply);
    let value: ciborium::Value = decode_with_ciborium(&encoded);

    if let ciborium::Value::Map(map) = value {
        let has_version = has_text_key(&map, "Version");
        let has_message_id = has_text_key(&map, "MessageID");

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

    let encoded = encode_with_ciborium(&reply);
    let value: ciborium::Value = decode_with_ciborium(&encoded);

    if let ciborium::Value::Map(map) = value {
        // Signature and ErrMessage should NOT be present when None
        let has_signature = has_text_key(&map, "Signature");
        let has_err_message = has_text_key(&map, "ErrMessage");

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

    let encoded = encode_with_ciborium(&reply);
    let value: ciborium::Value = decode_with_ciborium(&encoded);

    if let ciborium::Value::Map(map) = value {
        let has_err_message = has_text_key(&map, "ErrMessage");

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
        Bytes::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
        "collection".to_string(),
        "creator".to_string(),
        Bytes::from(large_block.clone()),
    );

    let encoded = encode_with_ciborium(&request);
    let decoded: PushLogRequest = decode_with_ciborium(&encoded);

    assert_eq!(decoded.block.len(), 1024 * 1024);
    assert_eq!(decoded.block, large_block);
}

#[test]
fn test_empty_fields() {
    // Test with empty but valid fields
    let request = PushLogRequest::new(
        "".to_string(),
        Bytes::from(vec![]),
        "".to_string(),
        "".to_string(),
        Bytes::from(vec![]),
    );

    let encoded = encode_with_ciborium(&request);
    let decoded: PushLogRequest = decode_with_ciborium(&encoded);

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
        Bytes::from(vec![1, 2, 3, 4]),
        "collection1".to_string(),
        "creator1".to_string(),
        Bytes::from(vec![5, 6, 7, 8]),
    );

    let encoded = encode_with_ciborium(&broadcast);
    let decoded: PushLogBroadcast = decode_with_ciborium(&encoded);

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
        Bytes::from(vec![1, 2, 3]),
        "collection3".to_string(),
        "creator3".to_string(),
        Bytes::from(vec![4, 5, 6]),
    );

    let encoded = encode_with_ciborium(&broadcast);
    let value: ciborium::Value = decode_with_ciborium(&encoded);

    if let ciborium::Value::Map(map) = value {
        let field_names: Vec<String> = map
            .iter()
            .filter_map(|(k, _)| {
                if let ciborium::Value::Text(s) = k {
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
        assert!(
            field_names.contains(&"Creator".to_string()),
            "Missing Creator"
        );
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
        Bytes::from(vec![10, 20, 30]),
        "col2".to_string(),
        "creator2".to_string(),
        Bytes::from(vec![40, 50, 60]),
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
        Bytes::from(vec![70, 80, 90]),
        "col3".to_string(),
        "creator3".to_string(),
        Bytes::from(vec![100, 110, 120]),
    );

    let request = broadcast.to_request();

    assert_eq!(request.doc_id, broadcast.doc_id);
    assert_eq!(request.cid, broadcast.cid);
    assert_eq!(request.collection_id, broadcast.collection_id);
    assert_eq!(request.creator, broadcast.creator);
    assert_eq!(request.block, broadcast.block);
    // Request should have default metadata
    assert_eq!(request.version, MESSAGE_VERSION);
}

// Regression guard for issue #827.
//
// `#[serde(flatten)]` on MetaData causes serde_cbor to emit an indefinite-
// length CBOR map (major type 5, additional info 31 = byte 0xbf) instead of
// a definite-length map (0xa0–0xb7 for 0–23 entries). Go's fxamacker/cbor
// emits definite maps. Since both sides re-serialize for signature
// verification, the byte mismatch breaks Rust↔Go interop.
//
// The fix inlines MetaData fields (same as PushLogReply already does).
// These tests fail on pre-fix code (first byte = 0xbf) and pass after.
mod regression_827 {
    use bytes::Bytes;
    use p2p::message::*;

    fn assert_definite_cbor_map(type_name: &str, bytes: &[u8]) {
        assert!(
            !bytes.is_empty(),
            "{type_name}: serialization produced empty output"
        );
        assert_ne!(
            bytes[0], 0xbf,
            "{type_name}: CBOR map must be definite-length (not 0xbf indefinite). \
             If this fails, #[serde(flatten)] has been reintroduced on MetaData."
        );
        let major = bytes[0] >> 5;
        assert_eq!(
            major, 5,
            "{type_name}: expected CBOR map (major type 5), got major type {major}"
        );
    }

    #[test]
    fn pushlog_request_definite_map() {
        let req = PushLogRequest::new(
            "doc".into(),
            Bytes::from(vec![1]),
            "col".into(),
            "creator".into(),
            Bytes::from(vec![2]),
        );
        let bytes = serde_cbor::to_vec(&req).unwrap();
        assert_definite_cbor_map("PushLogRequest", &bytes);
    }

    #[test]
    fn docsync_request_definite_map() {
        let req = DocSyncRequest::new(vec!["doc1".into()]);
        let bytes = serde_cbor::to_vec(&req).unwrap();
        assert_definite_cbor_map("DocSyncRequest", &bytes);
    }

    #[test]
    fn branchable_request_definite_map() {
        let req = BranchableSyncRequest::new("col1".into());
        let bytes = serde_cbor::to_vec(&req).unwrap();
        assert_definite_cbor_map("BranchableSyncRequest", &bytes);
    }

    #[test]
    fn identity_request_definite_map() {
        let req = IdentityRequest::new("peer1".into());
        let bytes = serde_cbor::to_vec(&req).unwrap();
        assert_definite_cbor_map("IdentityRequest", &bytes);
    }

    #[test]
    fn query_se_request_definite_map() {
        let req = QuerySEArtifactsRequest::new("col1", vec![]);
        let bytes = serde_cbor::to_vec(&req).unwrap();
        assert_definite_cbor_map("QuerySEArtifactsRequest", &bytes);
    }

    #[test]
    fn push_se_request_definite_map() {
        let req = PushSEArtifactsRequest::new("col1", vec![]);
        let bytes = serde_cbor::to_vec(&req).unwrap();
        assert_definite_cbor_map("PushSEArtifactsRequest", &bytes);
    }
}

#[test]
fn test_decode_gossip_payload_from_postcard_broadcast() {
    let broadcast = PushLogBroadcast::new(
        "doc-postcard-broadcast".to_string(),
        Bytes::from(vec![1, 2, 3, 4]),
        "collection-postcard-broadcast".to_string(),
        "creator-postcard-broadcast".to_string(),
        Bytes::from(vec![5, 6, 7, 8]),
    );

    let encoded = encode_with_postcard(&broadcast);
    let (decoded, encoding) =
        PushLogBroadcast::decode_gossip_payload(&encoded).expect("decode failed");

    assert_eq!(encoding, PushLogGossipPayloadEncoding::PostcardBroadcast);
    assert_eq!(decoded, broadcast);
}

#[test]
fn test_encode_gossip_payload_uses_cbor_broadcast() {
    let broadcast = PushLogBroadcast::new(
        "doc-canonical".to_string(),
        Bytes::from(vec![1, 3, 5]),
        "collection-canonical".to_string(),
        "creator-canonical".to_string(),
        Bytes::from(vec![2, 4, 6]),
    );

    let encoded = broadcast
        .encode_gossip_payload()
        .expect("canonical encode should succeed");
    let (decoded, encoding) =
        PushLogBroadcast::decode_gossip_payload(&encoded).expect("decode failed");

    assert_eq!(encoding, PushLogGossipPayloadEncoding::CborBroadcast);
    assert_eq!(decoded, broadcast);
}

#[test]
fn test_decode_gossip_payload_from_cbor_broadcast() {
    let broadcast = PushLogBroadcast::new(
        "doc-cbor-broadcast".to_string(),
        Bytes::from(vec![10, 20, 30]),
        "collection-cbor-broadcast".to_string(),
        "creator-cbor-broadcast".to_string(),
        Bytes::from(vec![40, 50, 60]),
    );

    let encoded = encode_with_ciborium(&broadcast);
    let (decoded, encoding) =
        PushLogBroadcast::decode_gossip_payload(&encoded).expect("decode failed");

    assert_eq!(encoding, PushLogGossipPayloadEncoding::CborBroadcast);
    assert_eq!(decoded, broadcast);
}

#[test]
fn test_decode_gossip_payload_from_cbor_request() {
    let request = PushLogRequest::new(
        "doc-cbor-request".to_string(),
        Bytes::from(vec![11, 22, 33]),
        "collection-cbor-request".to_string(),
        "creator-cbor-request".to_string(),
        Bytes::from(vec![44, 55, 66]),
    );

    let encoded = encode_with_ciborium(&request);
    let (decoded, encoding) =
        PushLogBroadcast::decode_gossip_payload(&encoded).expect("decode failed");

    assert_eq!(encoding, PushLogGossipPayloadEncoding::CborRequest);
    assert_eq!(decoded, PushLogBroadcast::from_request(&request));
}
