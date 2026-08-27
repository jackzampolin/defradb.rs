//! Outstanding-request correlation.
//!
//! Tracks a map of in-flight request CIDs → response channels so incoming
//! [`InternalResponse`] envelopes (delivered over `<base>/<self>/_response`)
//! can be routed back to the original publish call.
//!
//! Mirrors the state machine in
//! `sourcenetwork/go-libp2p-pubsub-rpc/rpc.go:204-278` minus the direct
//! gossipsub coupling — the gossipsub integration lives in the host layer
//! (see `crate::host::p2p_host::protocols`), so this module stays
//! transport-agnostic for unit testing.

use std::collections::HashMap;
use std::sync::Arc;

use cid::Cid;
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tracing::debug;

use super::envelope::InternalResponse;
use super::id::derive_request_id;

/// Default channel bound for multi-response publishes. If responses arrive
/// faster than the caller drains them, older messages still block the sender
/// side; pick a value large enough to accommodate burst traffic but small
/// enough to signal backpressure before unbounded memory growth. This
/// mirrors Go's per-topic response channel size (`rpc.go:224`).
pub const DEFAULT_MULTI_RESPONSE_BUFFER: usize = 128;

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
    /// Responder peer, populated from the verified gossip message source. Held
    /// as the transport-native peer-id string (libp2p base58 or iroh hex) since
    /// it is only forwarded to the caller (e.g. for the KMS ECIES AAD), never
    /// used as a correlation key — that is the request [`Cid`].
    pub from: String,
    /// Raw response payload.
    pub data: Vec<u8>,
    /// Error string produced by the responder, if any.
    pub err: Option<String>,
}

/// Options that control how many responses a publish collects.
///
/// Matches a subset of Go's `options.go`. `ignore_response` is expressed by
/// choosing [`Correlator::publish_fire_and_forget`] rather than a flag.
#[derive(Debug, Clone, Copy)]
pub struct PublishOptions {
    /// Collect responses from every peer that replies, not just the first.
    pub multi_response: bool,
    /// Channel bound for multi-response publishes. Ignored when
    /// `multi_response` is false (single-response always uses a 1-slot
    /// bounded channel).
    pub multi_response_buffer: usize,
}

impl Default for PublishOptions {
    fn default() -> Self {
        Self {
            multi_response: false,
            multi_response_buffer: DEFAULT_MULTI_RESPONSE_BUFFER,
        }
    }
}

/// Outstanding-request registry shared between the publisher and the
/// subscription listener. Safe to clone into tasks.
#[derive(Clone, Default)]
pub struct Correlator {
    ongoing: Arc<Mutex<HashMap<Cid, Entry>>>,
}

struct Entry {
    sender: mpsc::Sender<PubsubResponse>,
    multi_response: bool,
}

/// Handle for a request that expects one or more responses.
///
/// The correlator entry is removed automatically when this handle is dropped
/// (single-response entries are also auto-removed after the first delivery).
/// Drain [`PreparedPublish::responses`] until the caller's deadline expires,
/// then drop the handle to release the slot.
pub struct PreparedPublish {
    pub id: Cid,
    pub data: Vec<u8>,
    pub responses: mpsc::Receiver<PubsubResponse>,
    /// Back-reference used by `Drop` to remove the entry if the caller drops
    /// without having drained the channel.
    correlator: Correlator,
}

impl Drop for PreparedPublish {
    fn drop(&mut self) {
        // Always attempt to remove — cheap if the entry was already cleared
        // by a single-response auto-close or an explicit cancel.
        self.correlator.ongoing.lock().remove(&self.id);
    }
}

impl Correlator {
    /// Create a new, empty correlator.
    pub fn new() -> Self {
        Self::default()
    }

    /// Prepare a publish that expects responses.
    ///
    /// Derives the request ID, registers a correlation entry, and returns a
    /// handle whose `Drop` removes the entry automatically.
    ///
    /// # Concurrent identical requests
    ///
    /// The request ID is the CID of `data`, so two callers publishing
    /// **identical** bytes derive the same ID. The second `publish` overwrites
    /// the first's entry in the in-flight map — the first caller's response
    /// channel is silently dropped and that caller times out instead of
    /// receiving the reply. Go has the same behavior: `Topic.Publish` in
    /// `sourcenetwork/go-libp2p-pubsub-rpc/rpc.go:222-230` performs an
    /// unguarded `t.ongoing[msgID] = ongoingMessage{...}`, so this is a Go
    /// parity match by design. Callers that need to multiplex truly identical
    /// requests must serialize them externally or vary the payload (e.g.
    /// add a nonce) so the derived CIDs differ.
    pub fn publish(&self, data: Vec<u8>, opts: PublishOptions) -> PreparedPublish {
        let id = derive_request_id(&data);
        let buffer = if opts.multi_response {
            opts.multi_response_buffer.max(1)
        } else {
            1
        };
        let (tx, rx) = mpsc::channel(buffer);
        let entry = Entry {
            sender: tx,
            multi_response: opts.multi_response,
        };
        self.ongoing.lock().insert(id, entry);
        PreparedPublish {
            id,
            data,
            responses: rx,
            correlator: self.clone(),
        }
    }

    /// Prepare a fire-and-forget publish. Matches Go's
    /// `WithIgnoreResponse(true)`: the request still gets an ID so the
    /// responder can echo it, but no correlation slot is allocated and any
    /// response will be silently dropped.
    pub fn publish_fire_and_forget(&self, data: &[u8]) -> Cid {
        derive_request_id(data)
    }

    /// Drop the correlation entry for `id` without waiting for a response.
    /// Normally not needed — [`PreparedPublish`]'s `Drop` does this — but
    /// exposed for callers that want to cancel early while holding the
    /// handle alive for other purposes (rare).
    pub fn cancel(&self, id: &Cid) {
        self.ongoing.lock().remove(id);
    }

    /// Drop every in-flight response sender and wake waiting publishers.
    ///
    /// Used during coordinator shutdown so callers don't sit on the normal
    /// response timeout after the transport has already started closing.
    pub fn cancel_all(&self) -> usize {
        self.ongoing.lock().drain().count()
    }

    /// Deliver a decoded response envelope. Routes to the matching ongoing
    /// entry if one exists; single-response entries are auto-removed after
    /// one successful delivery.
    ///
    /// Returns `true` if a waiting caller received the response,
    /// `false` if the response was stale (late arrival or fire-and-forget).
    pub fn deliver(&self, from: String, response: InternalResponse) -> bool {
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
        // Use try_send: if the caller has fallen behind the buffer, Go also
        // drops responses rather than blocking the gossipsub loop
        // (`rpc.go:275` uses a non-blocking select with a default-log).
        let send_result = entry.sender.try_send(response);
        match send_result {
            Ok(()) => {
                if !multi {
                    map.remove(&id);
                }
                true
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                // Receiver dropped — treat as cancelled.
                map.remove(&id);
                false
            }
            Err(mpsc::error::TrySendError::Full(dropped)) => {
                // Full: backpressure trap. Keep the entry but report drop.
                debug!(
                    from = %dropped.from,
                    request_id = %id,
                    "pubsub_rpc: response dropped due to full buffer"
                );
                false
            }
        }
    }

    /// Number of currently in-flight requests. Intended for tests and
    /// metrics, not for correctness-critical code paths.
    pub fn in_flight(&self) -> usize {
        self.ongoing.lock().len()
    }
}
