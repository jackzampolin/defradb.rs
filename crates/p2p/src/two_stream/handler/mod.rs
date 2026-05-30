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
mod se_query;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use libp2p::StreamProtocol;
use libp2p_stream as stream;
use parking_lot::Mutex;
use tokio::sync::oneshot;

use libp2p::PeerId;

use crate::message::{DocSyncReply, IdentityResponse, PushLogReply, PushLogRequest};
use crate::protocol::{
    CAR_REQUEST_PROTOCOL, CAR_RESPONSE_PROTOCOL, IDENTITY_REQUEST_PROTOCOL,
    IDENTITY_RESPONSE_PROTOCOL, REP_REQUEST_PROTOCOL, REP_RESPONSE_PROTOCOL,
    SE_QUERY_REQUEST_PROTOCOL, SE_QUERY_RESPONSE_PROTOCOL, SE_REQUEST_PROTOCOL,
    SE_RESPONSE_PROTOCOL,
};
use crate::{error::Error, message::Message, Result};

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

pub(super) fn ensure_transport_sender<M: Message>(peer_id: &PeerId, msg: &M) -> Result<()> {
    if msg.sender_id() == peer_id.to_string() {
        Ok(())
    } else {
        Err(Error::Transport(format!(
            "transport peer {} did not match signed sender {}",
            peer_id,
            msg.sender_id()
        )))
    }
}

/// State for tracking pending responses.
#[derive(Default)]
pub(crate) struct PendingResponses {
    /// Map of expected peer + MessageID to response channel.
    pub(crate) channels: HashMap<PendingResponseKey, oneshot::Sender<PushLogReply>>,
    /// Map of expected peer + MessageID to identity response channel.
    pub(crate) identity_channels: HashMap<PendingResponseKey, oneshot::Sender<IdentityResponse>>,
    /// Map of expected peer + MessageID to DocSync response channel.
    pub(crate) doc_sync_channels: HashMap<PendingResponseKey, oneshot::Sender<DocSyncReply>>,
    /// Fire-and-forget DocSync requests awaiting an async response event.
    pub(crate) doc_sync_requests: HashMap<PendingResponseKey, Instant>,
    /// Fire-and-forget BranchableSync requests awaiting an async response event.
    pub(crate) branchable_sync_requests: HashMap<PendingResponseKey, Instant>,
}

impl PendingResponses {
    fn prune_expired(&mut self) {
        let now = Instant::now();
        self.doc_sync_requests
            .retain(|_, inserted_at| now.duration_since(*inserted_at) <= RESPONSE_TIMEOUT);
        self.branchable_sync_requests
            .retain(|_, inserted_at| now.duration_since(*inserted_at) <= RESPONSE_TIMEOUT);
    }

    pub(crate) fn register_doc_sync_request(&mut self, key: PendingResponseKey) {
        self.prune_expired();
        self.doc_sync_requests.insert(key, Instant::now());
    }

    pub(crate) fn consume_doc_sync_request(&mut self, key: &PendingResponseKey) -> bool {
        self.prune_expired();
        self.doc_sync_requests.remove(key).is_some()
    }

    pub(crate) fn register_branchable_sync_request(&mut self, key: PendingResponseKey) {
        self.prune_expired();
        self.branchable_sync_requests.insert(key, Instant::now());
    }

    pub(crate) fn consume_branchable_sync_request(&mut self, key: &PendingResponseKey) -> bool {
        self.prune_expired();
        self.branchable_sync_requests.remove(key).is_some()
    }
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

    /// Get the SE query request protocol.
    pub fn se_query_request_protocol() -> StreamProtocol {
        StreamProtocol::new(SE_QUERY_REQUEST_PROTOCOL)
    }

    /// Get the SE query response protocol.
    pub fn se_query_response_protocol() -> StreamProtocol {
        StreamProtocol::new(SE_QUERY_RESPONSE_PROTOCOL)
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

    /// Clean up a pending DocSync request.
    pub fn cleanup_pending_doc_sync(&self, peer_id: PeerId, message_id: &str) {
        let mut pending = self.pending.lock();
        let key = PendingResponseKey::new(peer_id, message_id);
        pending.doc_sync_channels.remove(&key);
        pending.doc_sync_requests.remove(&key);
    }

    /// Clean up a pending BranchableSync request.
    pub fn cleanup_pending_branchable_sync(&self, peer_id: PeerId, message_id: &str) {
        let mut pending = self.pending.lock();
        pending
            .branchable_sync_requests
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doc_sync_pending_requests_are_peer_bound_and_single_use() {
        let peer = PeerId::random();
        let other_peer = PeerId::random();
        let key = PendingResponseKey::new(peer, "doc-sync-1");
        let wrong_peer_key = PendingResponseKey::new(other_peer, "doc-sync-1");
        let mut pending = PendingResponses::default();

        pending.register_doc_sync_request(key.clone());

        assert!(!pending.consume_doc_sync_request(&wrong_peer_key));
        assert!(pending.consume_doc_sync_request(&key));
        assert!(!pending.consume_doc_sync_request(&key));
    }

    #[test]
    fn branchable_sync_pending_requests_are_peer_bound_and_single_use() {
        let peer = PeerId::random();
        let other_peer = PeerId::random();
        let key = PendingResponseKey::new(peer, "branchable-sync-1");
        let wrong_peer_key = PendingResponseKey::new(other_peer, "branchable-sync-1");
        let mut pending = PendingResponses::default();

        pending.register_branchable_sync_request(key.clone());

        assert!(!pending.consume_branchable_sync_request(&wrong_peer_key));
        assert!(pending.consume_branchable_sync_request(&key));
        assert!(!pending.consume_branchable_sync_request(&key));
    }
}
