
//! Tests for the CBOR codec module.

use std::io;

use futures::io::Cursor;
use libp2p::request_response::Codec;

use p2p::codec::{decode, encode, PushLogCodec, MAX_MESSAGE_SIZE};
use p2p::error::Error;
use p2p::message::{PushLogReply, PushLogRequest};
use p2p::protocol::{rep_request_protocol, rep_response_protocol};

#[tokio::test]
async fn test_codec_roundtrip_request() {
    let mut codec = PushLogCodec::new();
    let protocol = rep_request_protocol();

    let original = PushLogRequest::new(
        "doc123".to_string(),
        vec![1, 2, 3],
        "collection1".to_string(),
        "creator1".to_string(),
        vec![4, 5, 6],
    );

    // Write
    let mut write_buf = Cursor::new(Vec::new());
    codec
        .write_request(&protocol, &mut write_buf, original.clone())
        .await
        .expect("write failed");

    // Read
    let mut read_buf = Cursor::new(write_buf.into_inner());
    let decoded = codec
        .read_request(&protocol, &mut read_buf)
        .await
        .expect("read failed");

    assert_eq!(decoded.doc_id, original.doc_id);
    assert_eq!(decoded.cid, original.cid);
    assert_eq!(decoded.collection_id, original.collection_id);
}

#[tokio::test]
async fn test_codec_roundtrip_response() {
    let mut codec = PushLogCodec::new();
    let protocol = rep_response_protocol();

    let original = PushLogReply::success("msg123");

    // Write
    let mut write_buf = Cursor::new(Vec::new());
    codec
        .write_response(&protocol, &mut write_buf, original.clone())
        .await
        .expect("write failed");

    // Read
    let mut read_buf = Cursor::new(write_buf.into_inner());
    let decoded = codec
        .read_response(&protocol, &mut read_buf)
        .await
        .expect("read failed");

    // PushLogReply uses flat fields (not nested metadata)
    assert_eq!(decoded.message_id, original.message_id);
}

#[tokio::test]
async fn test_codec_invalid_cbor_request() {
    let mut codec = PushLogCodec::new();
    let protocol = rep_request_protocol();

    // Invalid CBOR data
    let mut read_buf = Cursor::new(vec![0xFF, 0xFF, 0xFF, 0xFF]);
    let result = codec.read_request(&protocol, &mut read_buf).await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert!(err.to_string().contains("CBOR deserialization error"));
}

#[tokio::test]
async fn test_codec_invalid_cbor_response() {
    let mut codec = PushLogCodec::new();
    let protocol = rep_response_protocol();

    // Invalid CBOR data
    let mut read_buf = Cursor::new(vec![0xFE, 0xFE, 0xFE]);
    let result = codec.read_response(&protocol, &mut read_buf).await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn test_codec_empty_message() {
    let mut codec = PushLogCodec::new();
    let protocol = rep_request_protocol();

    // Empty data
    let mut read_buf = Cursor::new(Vec::new());
    let result = codec.read_request(&protocol, &mut read_buf).await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
}

#[tokio::test]
async fn test_codec_truncated_cbor() {
    let mut codec = PushLogCodec::new();
    let protocol = rep_request_protocol();

    let original = PushLogRequest::new(
        "doc123".to_string(),
        vec![1, 2, 3],
        "collection1".to_string(),
        "creator1".to_string(),
        vec![4, 5, 6],
    );

    // Encode then truncate
    let full_data = serde_cbor::to_vec(&original).unwrap();
    let truncated = &full_data[..full_data.len() / 2];

    let mut read_buf = Cursor::new(truncated.to_vec());
    let result = codec.read_request(&protocol, &mut read_buf).await;

    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
}

#[test]
fn test_encode_decode_roundtrip() {
    let request = PushLogRequest::new(
        "doc456".to_string(),
        vec![10, 20, 30],
        "col2".to_string(),
        "creator2".to_string(),
        vec![40, 50, 60],
    );

    let encoded = encode(&request).expect("encode failed");
    let decoded: PushLogRequest = decode(&encoded).expect("decode failed");

    assert_eq!(decoded.doc_id, request.doc_id);
    assert_eq!(decoded.cid, request.cid);
    assert_eq!(decoded.collection_id, request.collection_id);
}

#[test]
fn test_decode_invalid_cbor() {
    let invalid_data = vec![0xFF, 0xFF];
    let result: Result<PushLogRequest, Error> = decode(&invalid_data);

    assert!(result.is_err());
    match result {
        Err(Error::CborDeserialization(_)) => {}
        _ => panic!("Expected CborDeserialization error"),
    }
}

#[test]
fn test_max_message_size_constant() {
    // Verify the constant is 16 MB
    assert_eq!(MAX_MESSAGE_SIZE, 16 * 1024 * 1024);
}
