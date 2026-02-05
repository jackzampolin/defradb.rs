//! Two-stream protocol handler for Go compatibility.
//!
//! Go's DefraDB uses a two-stream pattern for request-response:
//! 1. Sender opens stream on `/defradb/rep_req/0.0.1`, sends request, closes stream
//! 2. Receiver processes request, opens NEW stream on `/defradb/rep_resp/0.0.1` to send response
//!
//! This is different from libp2p-rust's request-response which uses bidirectional streams.
//! This module implements Go's pattern for interoperability using libp2p-stream.

mod event;
mod handler;
mod runner;

pub use event::TwoStreamEvent;
pub use handler::TwoStreamHandler;
pub use runner::TwoStreamRunner;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::PushLogRequest;

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
    }

    #[test]
    fn test_success_reply() {
        let request = PushLogRequest::new(
            "doc123".to_string(),
            vec![1, 2, 3],
            "col123".to_string(),
            "creator".to_string(),
            vec![4, 5, 6],
        );

        let reply = TwoStreamHandler::success_reply(&request);
        // PushLogReply has flat fields (no metadata struct) for CBOR wire compatibility
        assert_eq!(reply.message_id, request.metadata.message_id);
        assert!(reply.err_message.is_none());
    }

    #[test]
    fn test_error_reply() {
        let request = PushLogRequest::new(
            "doc123".to_string(),
            vec![1, 2, 3],
            "col123".to_string(),
            "creator".to_string(),
            vec![4, 5, 6],
        );

        let reply = TwoStreamHandler::error_reply(&request, "test error");
        // PushLogReply has flat fields (no metadata struct) for CBOR wire compatibility
        assert_eq!(reply.message_id, request.metadata.message_id);
        assert_eq!(reply.err_message, Some("test error".to_string()));
    }
}
