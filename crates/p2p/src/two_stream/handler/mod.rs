//! Two-stream protocol handler.
//!
//! Go's DefraDB uses a two-stream pattern for request-response:
//! 1. Sender opens stream on `/defradb/rep_req/0.0.1`, sends request, closes stream
//! 2. Receiver processes request, opens NEW stream on `/defradb/rep_resp/0.0.1` to send response
//!
//! This is different from libp2p-rust's request-response which uses bidirectional streams.
//! This module implements Go's pattern for interoperability using libp2p-stream.

mod branchable_se;
mod car;
mod doc_sync;
mod identity;
mod inbound;
mod pushlog;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use libp2p::StreamProtocol;
use libp2p_stream as stream;
use parking_lot::Mutex;
use tokio::sync::oneshot;

use libp2p::PeerId;

use crate::message::{IdentityResponse, PushLogReply, PushLogRequest};
use crate::protocol::{
    CAR_REQUEST_PROTOCOL, CAR_RESPONSE_PROTOCOL, IDENTITY_REQUEST_PROTOCOL,
    IDENTITY_RESPONSE_PROTOCOL, REP_REQUEST_PROTOCOL, REP_RESPONSE_PROTOCOL, SE_REQUEST_PROTOCOL,
    SE_RESPONSE_PROTOCOL,
};

/// Timeout for waiting for a response.
pub(super) const RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

/// Pending response key bound to the expected transport peer.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct PendingResponseKey {
    pub(crate) peer_id: PeerId,
    pub(crate) message_id: String,
}

impl PendingResponseKey {
    pub(crate) fn new(peer_id: PeerId, message_id: impl Into<String>) -> Self {
        Self {
            peer_id,
            message_id: message_id.into(),
        }
    }
}

/// State for tracking pending responses.
#[derive(Default)]
pub(crate) struct PendingResponses {
    /// Map of expected peer + MessageID to response channel.
    pub(crate) channels: HashMap<PendingResponseKey, oneshot::Sender<PushLogReply>>,
    /// Map of expected peer + MessageID to identity response channel.
    pub(crate) identity_channels: HashMap<PendingResponseKey, oneshot::Sender<IdentityResponse>>,
}

/// Two-stream protocol handler.
///
/// Handles Go's two-stream request-response pattern where requests and responses
/// flow on separate streams identified by different protocol IDs.
///
/// Uses `libp2p-stream` for stream management.
pub struct TwoStreamHandler {
    /// Control for the stream behaviour (for opening streams).
    pub(super) control: stream::Control,
    /// Pending response channels keyed by expected peer and MessageID.
    pub(super) pending: Arc<Mutex<PendingResponses>>,
}

impl TwoStreamHandler {
    /// Create a new two-stream handler from a stream::Control.
    pub fn new(control: stream::Control) -> Self {
        Self {
            control,
            pending: Arc::new(Mutex::new(PendingResponses::default())),
        }
    }

    /// Get a clone of the pending responses Arc for lock-free response processing.
    pub(crate) fn pending_responses(&self) -> Arc<Mutex<PendingResponses>> {
        self.pending.clone()
    }

    /// Get the request protocol.
    pub fn request_protocol() -> StreamProtocol {
        StreamProtocol::new(REP_REQUEST_PROTOCOL)
    }

    /// Get the response protocol.
    pub fn response_protocol() -> StreamProtocol {
        StreamProtocol::new(REP_RESPONSE_PROTOCOL)
    }

    /// Get the SE request protocol.
    pub fn se_request_protocol() -> StreamProtocol {
        StreamProtocol::new(SE_REQUEST_PROTOCOL)
    }

    /// Get the SE response protocol.
    pub fn se_response_protocol() -> StreamProtocol {
        StreamProtocol::new(SE_RESPONSE_PROTOCOL)
    }

    /// Get the CAR request protocol.
    pub fn car_request_protocol() -> StreamProtocol {
        StreamProtocol::new(CAR_REQUEST_PROTOCOL)
    }

    /// Get the CAR response protocol.
    pub fn car_response_protocol() -> StreamProtocol {
        StreamProtocol::new(CAR_RESPONSE_PROTOCOL)
    }

    /// Get the identity request protocol.
    pub fn identity_request_protocol() -> StreamProtocol {
        StreamProtocol::new(IDENTITY_REQUEST_PROTOCOL)
    }

    /// Get the identity response protocol.
    pub fn identity_response_protocol() -> StreamProtocol {
        StreamProtocol::new(IDENTITY_RESPONSE_PROTOCOL)
    }

    /// Clean up a pending response channel (used on timeout or cancellation).
    pub fn cleanup_pending(&self, peer_id: PeerId, message_id: &str) {
        let mut pending = self.pending.lock();
        pending
            .channels
            .remove(&PendingResponseKey::new(peer_id, message_id));
    }

    /// Clean up a pending identity response channel (used on timeout or cancellation).
    pub fn cleanup_pending_identity(&self, peer_id: PeerId, message_id: &str) {
        let mut pending = self.pending.lock();
        pending
            .identity_channels
            .remove(&PendingResponseKey::new(peer_id, message_id));
    }

    /// Create a success reply for a request.
    pub fn success_reply(request: &PushLogRequest) -> PushLogReply {
        PushLogReply::success(&request.message_id)
    }

    /// Create an error reply for a request.
    pub fn error_reply(request: &PushLogRequest, error: &str) -> PushLogReply {
        PushLogReply::error(&request.message_id, error)
    }
}
