use p2p::{message::PushLogRequest, TwoStreamHandler};

#[test]
fn test_protocol_ids() {
    assert_eq!(
        TwoStreamHandler::request_protocol().as_ref(),
        "/defradb/rep_req/0.0.1"
    );
    assert_eq!(
        TwoStreamHandler::response_protocol().as_ref(),
        "/defradb/rep_resp/0.0.1"
    );
    assert_eq!(
        TwoStreamHandler::se_query_request_protocol().as_ref(),
        "/defradb/se_query_req/0.0.1"
    );
    assert_eq!(
        TwoStreamHandler::se_query_response_protocol().as_ref(),
        "/defradb/se_query_resp/0.0.1"
    );
}

#[test]
fn test_success_reply() {
    let request = PushLogRequest::new(
        "doc123".to_string(),
        bytes::Bytes::from(vec![1, 2, 3]),
        "col123".to_string(),
        "creator".to_string(),
        bytes::Bytes::from(vec![4, 5, 6]),
    );

    let reply = TwoStreamHandler::success_reply(&request);
    // PushLogReply has flat fields (no metadata struct) for CBOR wire compatibility
    assert_eq!(reply.message_id, request.message_id);
    assert!(reply.err_message.is_none());
}

#[test]
fn test_error_reply() {
    let request = PushLogRequest::new(
        "doc123".to_string(),
        bytes::Bytes::from(vec![1, 2, 3]),
        "col123".to_string(),
        "creator".to_string(),
        bytes::Bytes::from(vec![4, 5, 6]),
    );

    let reply = TwoStreamHandler::error_reply(&request, "test error");
    // PushLogReply has flat fields (no metadata struct) for CBOR wire compatibility
    assert_eq!(reply.message_id, request.message_id);
    assert_eq!(reply.err_message, Some("test error".to_string()));
}
