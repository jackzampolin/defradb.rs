//! Searchable-encryption query correlation.
//!
//! Tracks in-flight `QuerySEArtifactsRequest` message IDs → response channels so
//! inbound [`QuerySEArtifactsReply`] envelopes (delivered asynchronously over the
//! SE query response two-stream protocol) can be routed back to the requester.
//!
//! Mirrors the KMS [`crate::pubsub_rpc::correlator::Correlator`] state machine
//! (shared `Arc<Mutex<HashMap>>` + Drop-cleanup), but is keyed by the message-ID
//! `String` rather than a `Cid`, since SE query message IDs are UUID strings.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::oneshot;
use tracing::debug;

use crate::message::QuerySEArtifactsReply;

/// Outstanding SE-query registry shared between the requester and the event
/// loop that receives replies. Cheap to clone into tasks.
#[derive(Clone, Default)]
pub struct SeQueryCorrelator {
    ongoing: Arc<Mutex<HashMap<String, oneshot::Sender<QuerySEArtifactsReply>>>>,
}

/// Handle for a registered SE query. The correlator slot is removed when this
/// guard is dropped, so a requester that times out (or returns early) does not
/// leak the entry. Await [`PendingSeQuery::recv`] for the reply (keep the guard
/// alive across the await so the slot stays registered).
pub struct PendingSeQuery {
    message_id: String,
    receiver: oneshot::Receiver<QuerySEArtifactsReply>,
    correlator: SeQueryCorrelator,
}

impl PendingSeQuery {
    /// Await the reply for this query. Returns `Err` if the slot was cancelled
    /// (sender dropped) before a reply arrived.
    pub async fn recv(
        &mut self,
    ) -> std::result::Result<QuerySEArtifactsReply, oneshot::error::RecvError> {
        (&mut self.receiver).await
    }
}

impl Drop for PendingSeQuery {
    fn drop(&mut self) {
        self.correlator.ongoing.lock().remove(&self.message_id);
    }
}

impl SeQueryCorrelator {
    /// Create a new, empty correlator.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a slot for `message_id` and return a guard whose `receiver`
    /// resolves when the matching reply arrives. Registering the same
    /// `message_id` twice overwrites the first slot (the first caller then
    /// times out); callers must use unique message IDs (sign-first generates a
    /// fresh UUID per request).
    pub fn register(&self, message_id: String) -> PendingSeQuery {
        let (tx, rx) = oneshot::channel();
        self.ongoing.lock().insert(message_id.clone(), tx);
        PendingSeQuery {
            message_id,
            receiver: rx,
            correlator: self.clone(),
        }
    }

    /// Deliver a reply, routing it to the matching registered slot.
    ///
    /// Returns `true` if a waiting requester received the reply, `false` if the
    /// reply was stale (no matching slot, late arrival, or requester gone).
    pub fn deliver(&self, reply: QuerySEArtifactsReply) -> bool {
        let sender = self.ongoing.lock().remove(&reply.message_id);
        match sender {
            Some(tx) => tx.send(reply).is_ok(),
            None => {
                debug!(message_id = %reply.message_id, "se_query: reply with no matching request dropped");
                false
            }
        }
    }

    /// Drop the slot for `message_id` without waiting for a reply.
    pub fn cancel(&self, message_id: &str) {
        self.ongoing.lock().remove(message_id);
    }

    /// Number of currently in-flight SE queries. For tests/metrics only.
    pub fn in_flight(&self) -> usize {
        self.ongoing.lock().len()
    }
}
