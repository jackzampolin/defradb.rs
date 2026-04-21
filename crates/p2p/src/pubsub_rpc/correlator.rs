//! Outstanding-request correlation.
//!
//! Tracks a map of in-flight request CIDs → response channels so incoming
//! [`InternalResponse`] envelopes (delivered over `<base>/<self>/_response`)
//! can be routed back to the original [`publish`](Correlator::publish) call.
//!
//! Mirrors the state machine in
//! `sourcenetwork/go-libp2p-pubsub-rpc/rpc.go:204-278` minus the direct
//! gossipsub coupling — the gossipsub integration lives in the host layer
//! (see `crate::host::p2p_host::protocols`), so this module stays
//! transport-agnostic for unit testing.

use std::collections::HashMap;
use std::sync::Arc;

use cid::Cid;
use libp2p::PeerId;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use super::envelope::InternalResponse;
use super::id::derive_request_id;

/// A single response delivered back to the caller.
///
/// Mirrors Go's public `rpc.Response` struct (`rpc.go:34-44`). The wire
/// envelope's `From` field is advisory; Go overwrites it with the validated
/// gossipsub sender (`rpc.go:415`) before the response reaches the caller,
/// so we do the same here.
#[derive(Debug, Clone)]
pub struct PubsubResponse {
    /// The request-ID echoed by the responder.
    pub id: Cid,
    /// Responder peer, populated from the verified gossipsub message source.
    pub from: PeerId,
    /// Raw response payload.
    pub data: Vec<u8>,
    /// Error string produced by the responder, if any.
    pub err: Option<String>,
}

/// Options that control how many responses a publish collects and whether
/// the publish retries for newly-joined peers. Mirrors `options.go`.
#[derive(Debug, Clone, Copy, Default)]
pub struct PublishOptions {
    /// Fire-and-forget: don't allocate a correlation slot. Matches Go's
    /// `WithIgnoreResponse(true)`.
    pub ignore_response: bool,
    /// Collect responses from every peer that replies, not just the first.
    pub multi_response: bool,
}

/// Outstanding-request registry shared between the publisher and the
/// subscription listener. Safe to clone into tasks.
#[derive(Clone, Default)]
pub struct Correlator {
    ongoing: Arc<Mutex<HashMap<Cid, Entry>>>,
}

struct Entry {
    sender: mpsc::UnboundedSender<PubsubResponse>,
    multi_response: bool,
}

/// Result of preparing a publish: the request bytes (unchanged), the derived
/// request ID, and a receiver for incoming responses.
///
/// Fire-and-forget calls (`PublishOptions::ignore_response = true`) still
/// receive an ID (so the responder can echo it), but the receiver channel
/// is a dummy that is immediately dropped — no entry is stored in the map.
pub struct PreparedPublish {
    pub id: Cid,
    pub data: Vec<u8>,
    pub responses: mpsc::UnboundedReceiver<PubsubResponse>,
}

impl Correlator {
    /// Create a new, empty correlator.
    pub fn new() -> Self {
        Self::default()
    }

    /// Prepare a publish: derive the request ID, register a correlation
    /// entry if responses are expected, and return the receiver the caller
    /// should drain until the context expires (or the first response arrives
    /// for single-response publishes).
    pub fn publish(&self, data: Vec<u8>, opts: PublishOptions) -> PreparedPublish {
        let id = derive_request_id(&data);
        let (tx, rx) = mpsc::unbounded_channel();
        if !opts.ignore_response {
            let entry = Entry {
                sender: tx,
                multi_response: opts.multi_response,
            };
            self.ongoing.lock().insert(id, entry);
        }
        // For fire-and-forget we still return the receiver, but nothing
        // will ever send on it — caller drops it immediately.
        PreparedPublish {
            id,
            data,
            responses: rx,
        }
    }

    /// Drop the correlation entry for `id`. Should be called when the
    /// caller's context cancels or the single-response receiver yields.
    pub fn cancel(&self, id: &Cid) {
        self.ongoing.lock().remove(id);
    }

    /// Deliver a decoded response envelope. Routes to the matching ongoing
    /// entry if one exists; single-response entries are auto-removed after
    /// one successful delivery.
    ///
    /// Returns `true` if a waiting caller received the response,
    /// `false` if the response was stale (late arrival or fire-and-forget).
    pub fn deliver(&self, from: PeerId, response: InternalResponse) -> bool {
        let Ok(id) = response.id.parse::<Cid>() else {
            return false;
        };
        let response = PubsubResponse {
            id,
            from,
            data: response.data,
            err: if response.err.is_empty() {
                None
            } else {
                Some(response.err)
            },
        };
        let mut map = self.ongoing.lock();
        let Some(entry) = map.get(&id) else {
            return false;
        };
        let multi = entry.multi_response;
        if entry.sender.send(response).is_err() {
            // Receiver dropped — treat as cancelled.
            map.remove(&id);
            return false;
        }
        if !multi {
            map.remove(&id);
        }
        true
    }

    /// Number of currently in-flight requests. Intended for tests and
    /// metrics, not for correctness-critical code paths.
    pub fn in_flight(&self) -> usize {
        self.ongoing.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p::identity::Keypair;

    fn a_peer() -> PeerId {
        PeerId::from_public_key(&Keypair::generate_ed25519().public())
    }

    fn internal_for(id: &Cid, data: &[u8], err: &str) -> InternalResponse {
        InternalResponse {
            id: id.to_string(),
            from: Vec::new(),
            data: data.to_vec(),
            err: err.to_string(),
        }
    }

    #[tokio::test]
    async fn single_response_delivers_and_removes() {
        let c = Correlator::new();
        let prep = c.publish(b"req".to_vec(), PublishOptions::default());
        assert_eq!(c.in_flight(), 1);

        let from = a_peer();
        let delivered = c.deliver(from, internal_for(&prep.id, b"resp", ""));
        assert!(delivered);
        assert_eq!(c.in_flight(), 0, "single-response entry must auto-remove");

        let mut rx = prep.responses;
        let r = rx.recv().await.expect("response");
        assert_eq!(r.id, prep.id);
        assert_eq!(r.from, from);
        assert_eq!(r.data, b"resp");
        assert!(r.err.is_none());
    }

    #[tokio::test]
    async fn multi_response_keeps_entry_open() {
        let c = Correlator::new();
        let prep = c.publish(
            b"req".to_vec(),
            PublishOptions {
                multi_response: true,
                ..Default::default()
            },
        );

        let p1 = a_peer();
        let p2 = a_peer();
        assert!(c.deliver(p1, internal_for(&prep.id, b"r1", "")));
        assert!(c.deliver(p2, internal_for(&prep.id, b"r2", "boom")));
        assert_eq!(
            c.in_flight(),
            1,
            "multi-response entry stays until explicitly cancelled"
        );

        let mut rx = prep.responses;
        let r1 = rx.recv().await.expect("first response");
        let r2 = rx.recv().await.expect("second response");
        assert_eq!(r1.from, p1);
        assert_eq!(r2.from, p2);
        assert_eq!(r1.err, None);
        assert_eq!(r2.err.as_deref(), Some("boom"));

        c.cancel(&prep.id);
        assert_eq!(c.in_flight(), 0);
    }

    #[tokio::test]
    async fn ignore_response_allocates_no_entry() {
        let c = Correlator::new();
        let _prep = c.publish(
            b"req".to_vec(),
            PublishOptions {
                ignore_response: true,
                ..Default::default()
            },
        );
        assert_eq!(c.in_flight(), 0);
    }

    #[test]
    fn late_response_is_ignored() {
        let c = Correlator::new();
        let id = derive_request_id(b"never-sent");
        let delivered = c.deliver(a_peer(), internal_for(&id, b"", ""));
        assert!(
            !delivered,
            "response with no matching ongoing request drops"
        );
    }

    #[test]
    fn malformed_cid_is_dropped() {
        let c = Correlator::new();
        let delivered = c.deliver(
            a_peer(),
            InternalResponse {
                id: "not-a-cid".to_string(),
                from: Vec::new(),
                data: Vec::new(),
                err: String::new(),
            },
        );
        assert!(!delivered);
    }
}
