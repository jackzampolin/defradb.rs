//! High-level [`TopicHandler`] that stitches the correlator, envelope, and
//! topic-naming primitives into a transport-agnostic RPC surface.
//!
//! A `TopicHandler` is configured with a base topic string (`doc-sync`,
//! `sync-branchable`, ...) and handles the protocol state for a single
//! `(host, base_topic)` pair. It does *not* own a gossipsub handle — the
//! caller is expected to pump `deliver_gossip_message` whenever a raw
//! subscribed-topic message arrives on the wire, and call
//! [`TopicHandler::prepare_publish`] to package outgoing requests.
//!
//! This layering keeps the primitive testable in isolation (see the unit
//! tests at the bottom of this file, which drive a full request/response
//! round-trip without any swarm or tokio reactor involvement) and lets the
//! host/iroh integration code stay small.

use std::sync::Arc;

use async_trait::async_trait;
use cid::Cid;
use libp2p::PeerId;
use tracing::debug;

use super::correlator::{Correlator, PreparedPublish, PublishOptions};
use super::envelope::InternalResponse;
use super::id::derive_request_id;
use super::topic::{response_topic, response_topic_suffix, strip_response_topic_with_suffix};

/// User-supplied callback invoked for each incoming request on the base
/// topic. Produces the raw reply bytes or an error string that will be
/// forwarded to the caller via [`InternalResponse::err`].
#[async_trait]
pub trait MessageHandler: Send + Sync + 'static {
    async fn handle(&self, from: PeerId, data: Vec<u8>) -> Result<Vec<u8>, String>;
}

/// Implement `MessageHandler` for any async closure that matches the shape
/// `(PeerId, Vec<u8>) -> Future<Output = Result<Vec<u8>, String>>`.
pub struct FnHandler<F>(pub F);

#[async_trait]
impl<F, Fut> MessageHandler for FnHandler<F>
where
    F: Fn(PeerId, Vec<u8>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<Vec<u8>, String>> + Send,
{
    async fn handle(&self, from: PeerId, data: Vec<u8>) -> Result<Vec<u8>, String> {
        (self.0)(from, data).await
    }
}

/// Work item returned by [`TopicHandler::deliver_gossip_message`] when an
/// inbound request produced a reply that must now be published on the
/// response sub-topic.
#[derive(Debug, Clone)]
pub struct OutgoingResponse {
    /// Where to publish: `<base>/<caller-peer>/_response`.
    pub topic: String,
    /// Fully-encoded `InternalResponse` envelope.
    pub bytes: Vec<u8>,
}

/// Outcome of delivering a single gossipsub message to the handler.
///
/// The caller (host integration) typically:
/// - `Forwarded` → no further work.
/// - `OutgoingResponse` → publish the bundled bytes on the named topic.
/// - `Ignored` → message wasn't for us (wrong recipient response, late
///   arrival, etc.); drop it.
#[derive(Debug, Clone)]
pub enum DeliveryOutcome {
    /// Response was routed to a waiting publisher.
    Forwarded,
    /// Request was handled; caller must publish the returned bytes.
    Respond(OutgoingResponse),
    /// Message did not match any ongoing request or handler.
    Ignored,
}

struct Shared {
    base_topic: String,
    self_peer: PeerId,
    /// Pre-formatted `/{self_peer}/_response` suffix for hot-path topic
    /// matching. Avoids re-allocating per delivered message.
    self_response_suffix: String,
    /// Pre-formatted `{base_topic}/{self_peer}/_response` — the topic to
    /// subscribe to for responses addressed to us.
    self_response_topic: String,
    correlator: Correlator,
    handler: Arc<dyn MessageHandler>,
}

/// A pubsub_rpc topic handler.
///
/// Clone-safe: the inner state is shared via `Arc`. Use the same instance
/// across the publish path and the deliver path.
#[derive(Clone)]
pub struct TopicHandler {
    inner: Arc<Shared>,
}

impl TopicHandler {
    /// Build a new handler for `(base_topic, self_peer)`.
    pub fn new(
        base_topic: impl Into<String>,
        self_peer: PeerId,
        handler: Arc<dyn MessageHandler>,
    ) -> Self {
        let base_topic = base_topic.into();
        let self_response_suffix = response_topic_suffix(&self_peer);
        let self_response_topic = format!("{base_topic}{self_response_suffix}");
        Self {
            inner: Arc::new(Shared {
                base_topic,
                self_peer,
                self_response_suffix,
                self_response_topic,
                correlator: Correlator::new(),
                handler,
            }),
        }
    }

    /// Base topic (e.g. `"doc-sync"`).
    pub fn base_topic(&self) -> &str {
        &self.inner.base_topic
    }

    /// The `<base>/<self>/_response` sub-topic the caller should be
    /// subscribed to in order to receive replies.
    pub fn self_response_topic(&self) -> &str {
        &self.inner.self_response_topic
    }

    /// Package an outgoing publish that expects responses.
    pub fn prepare_publish(&self, data: Vec<u8>, opts: PublishOptions) -> PreparedPublish {
        self.inner.correlator.publish(data, opts)
    }

    /// Publish-and-forget variant matching Go's `WithIgnoreResponse(true)`.
    /// Returns the request ID so the caller can log/correlate out-of-band.
    pub fn publish_fire_and_forget(&self, data: &[u8]) -> Cid {
        self.inner.correlator.publish_fire_and_forget(data)
    }

    /// Cancel correlation for a request-in-flight. Normally unneeded —
    /// [`PreparedPublish`] auto-cancels on drop.
    pub fn cancel(&self, id: &Cid) {
        self.inner.correlator.cancel(id);
    }

    /// Number of in-flight requests awaiting responses. Test/metric helper.
    pub fn in_flight(&self) -> usize {
        self.inner.correlator.in_flight()
    }

    /// Dispatch an incoming gossipsub message on `topic` from `from`.
    ///
    /// - If `topic == base_topic`, treats `data` as a request: hands it to
    ///   the [`MessageHandler`] and returns an [`OutgoingResponse`] packaged
    ///   for the caller to publish.
    /// - If `topic` is our `<base>/<self>/_response` sub-topic, decodes the
    ///   envelope and routes it into the correlator.
    /// - Otherwise returns [`DeliveryOutcome::Ignored`].
    ///
    /// This method is `async` so that the user-provided `MessageHandler`
    /// can perform async work (storage lookups, etc.).
    pub async fn deliver_gossip_message(
        &self,
        topic: &str,
        from: PeerId,
        data: Vec<u8>,
    ) -> DeliveryOutcome {
        if topic == self.inner.base_topic {
            // Ignore our own requests echoed back by the pubsub mesh.
            if from == self.inner.self_peer {
                return DeliveryOutcome::Ignored;
            }
            let request_id = derive_request_id(&data);
            let (reply_bytes, err) = match self.inner.handler.handle(from, data).await {
                Ok(bytes) => (bytes, String::new()),
                Err(e) => (Vec::new(), e),
            };
            let envelope = InternalResponse {
                id: request_id.to_string(),
                err,
                data: reply_bytes,
                from: Vec::new(), // filled in by recipient from gossipsub source
            };
            match envelope.to_cbor() {
                Ok(bytes) => DeliveryOutcome::Respond(OutgoingResponse {
                    topic: response_topic(&self.inner.base_topic, &from),
                    bytes,
                }),
                Err(e) => {
                    debug!(
                        base_topic = %self.inner.base_topic,
                        caller = %from,
                        error = %e,
                        "pubsub_rpc: failed to encode response envelope"
                    );
                    DeliveryOutcome::Ignored
                }
            }
        } else if strip_response_topic_with_suffix(topic, &self.inner.self_response_suffix)
            == Some(self.inner.base_topic.as_str())
        {
            let envelope = match InternalResponse::from_cbor(&data) {
                Ok(e) => e,
                Err(e) => {
                    debug!(
                        base_topic = %self.inner.base_topic,
                        from = %from,
                        error = %e,
                        payload_size = data.len(),
                        "pubsub_rpc: failed to decode response envelope"
                    );
                    return DeliveryOutcome::Ignored;
                }
            };
            if self.inner.correlator.deliver(from, envelope) {
                DeliveryOutcome::Forwarded
            } else {
                DeliveryOutcome::Ignored
            }
        } else {
            DeliveryOutcome::Ignored
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::correlator::PubsubResponse;
    use super::*;
    use libp2p::identity::Keypair;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::mpsc;

    fn a_peer() -> PeerId {
        PeerId::from_public_key(&Keypair::generate_ed25519().public())
    }

    struct CountingEcho {
        calls: AtomicUsize,
    }
    #[async_trait]
    impl MessageHandler for CountingEcho {
        async fn handle(&self, _from: PeerId, data: Vec<u8>) -> Result<Vec<u8>, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(data)
        }
    }

    #[tokio::test]
    async fn round_trip_request_reply() {
        let alice = a_peer();
        let bob = a_peer();

        let echo = Arc::new(CountingEcho {
            calls: AtomicUsize::new(0),
        });
        let bob_topic = TopicHandler::new("doc-sync", bob, echo.clone());
        let alice_topic = TopicHandler::new(
            "doc-sync",
            alice,
            Arc::new(FnHandler(|_: PeerId, _: Vec<u8>| async {
                Err::<Vec<u8>, String>("alice is a pure caller".into())
            })),
        );

        let mut prep = alice_topic.prepare_publish(b"hello".to_vec(), PublishOptions::default());
        assert_eq!(alice_topic.in_flight(), 1);

        let outcome = bob_topic
            .deliver_gossip_message("doc-sync", alice, prep.data.clone())
            .await;
        assert_eq!(echo.calls.load(Ordering::SeqCst), 1);
        let response = match outcome {
            DeliveryOutcome::Respond(r) => r,
            other => panic!("expected Respond, got {other:?}"),
        };
        assert_eq!(
            response.topic,
            super::super::topic::response_topic("doc-sync", &alice),
            "bob must reply on alice's response sub-topic"
        );

        let ret = alice_topic
            .deliver_gossip_message(alice_topic.self_response_topic(), bob, response.bytes)
            .await;
        assert!(matches!(ret, DeliveryOutcome::Forwarded));

        let got = prep.responses.recv().await.expect("response arrives");
        assert_eq!(got.from, bob);
        assert_eq!(got.data, b"hello");
        assert_eq!(got.id, prep.id);
        assert!(got.err.is_none());
        assert_eq!(alice_topic.in_flight(), 0, "single-response auto-closes");
    }

    #[tokio::test]
    async fn echoed_own_request_is_ignored() {
        let alice = a_peer();
        let t = TopicHandler::new(
            "doc-sync",
            alice,
            Arc::new(FnHandler(|_: PeerId, _: Vec<u8>| async {
                panic!("handler must not run for own echoed request");
            })),
        );
        let outcome = t
            .deliver_gossip_message("doc-sync", alice, b"x".to_vec())
            .await;
        assert!(matches!(outcome, DeliveryOutcome::Ignored));
    }

    #[tokio::test]
    async fn response_for_other_peer_is_ignored() {
        let alice = a_peer();
        let bob = a_peer();
        let t = TopicHandler::new(
            "doc-sync",
            alice,
            Arc::new(FnHandler(|_: PeerId, _: Vec<u8>| async {
                Ok::<Vec<u8>, String>(Vec::new())
            })),
        );
        let not_my_topic = super::super::topic::response_topic("doc-sync", &bob);
        let outcome = t
            .deliver_gossip_message(&not_my_topic, bob, b"x".to_vec())
            .await;
        assert!(matches!(outcome, DeliveryOutcome::Ignored));
    }

    #[tokio::test]
    async fn handler_error_propagates_as_err_string() {
        let alice = a_peer();
        let bob = a_peer();

        let bob_topic = TopicHandler::new(
            "doc-sync",
            bob,
            Arc::new(FnHandler(|_: PeerId, _: Vec<u8>| async {
                Err::<Vec<u8>, String>("unknown doc".into())
            })),
        );
        let alice_topic = TopicHandler::new(
            "doc-sync",
            alice,
            Arc::new(FnHandler(|_: PeerId, _: Vec<u8>| async {
                Ok::<Vec<u8>, String>(Vec::new())
            })),
        );

        let mut prep = alice_topic.prepare_publish(b"req".to_vec(), PublishOptions::default());
        let resp = match bob_topic
            .deliver_gossip_message("doc-sync", alice, prep.data.clone())
            .await
        {
            DeliveryOutcome::Respond(r) => r,
            other => panic!("expected Respond, got {other:?}"),
        };
        alice_topic
            .deliver_gossip_message(alice_topic.self_response_topic(), bob, resp.bytes)
            .await;

        let got = prep.responses.recv().await.expect("response");
        assert_eq!(got.err.as_deref(), Some("unknown doc"));
        assert!(got.data.is_empty());
    }

    #[tokio::test]
    async fn out_of_band_topic_is_ignored() {
        let alice = a_peer();
        let t = TopicHandler::new(
            "doc-sync",
            alice,
            Arc::new(FnHandler(|_: PeerId, _: Vec<u8>| async {
                panic!("handler must not run for unrelated topic");
            })),
        );
        let outcome = t
            .deliver_gossip_message("some-other-topic", a_peer(), b"x".to_vec())
            .await;
        assert!(matches!(outcome, DeliveryOutcome::Ignored));
    }

    #[tokio::test]
    async fn multi_response_collects_until_dropped() {
        let alice = a_peer();
        let bob = a_peer();
        let carol = a_peer();

        let alice_topic = TopicHandler::new(
            "doc-sync",
            alice,
            Arc::new(FnHandler(|_: PeerId, _: Vec<u8>| async {
                Ok::<Vec<u8>, String>(Vec::new())
            })),
        );

        let mut prep = alice_topic.prepare_publish(
            b"multi".to_vec(),
            PublishOptions {
                multi_response: true,
                ..Default::default()
            },
        );
        let make_envelope = |id: &Cid, data: &[u8]| {
            let env = InternalResponse {
                id: id.to_string(),
                err: String::new(),
                data: data.to_vec(),
                from: Vec::new(),
            };
            env.to_cbor().expect("encode")
        };
        let rt = alice_topic.self_response_topic().to_string();
        assert!(matches!(
            alice_topic
                .deliver_gossip_message(&rt, bob, make_envelope(&prep.id, b"from-bob"))
                .await,
            DeliveryOutcome::Forwarded
        ));
        assert!(matches!(
            alice_topic
                .deliver_gossip_message(&rt, carol, make_envelope(&prep.id, b"from-carol"))
                .await,
            DeliveryOutcome::Forwarded
        ));

        let rx: &mut mpsc::Receiver<PubsubResponse> = &mut prep.responses;
        let first = rx.recv().await.expect("first");
        let second = rx.recv().await.expect("second");
        assert_eq!(
            {
                let s: std::collections::HashSet<PeerId> =
                    [first.from, second.from].into_iter().collect();
                s.len()
            },
            2
        );
        assert_eq!(
            alice_topic.in_flight(),
            1,
            "multi-response stays open until dropped"
        );

        drop(prep);
        assert_eq!(alice_topic.in_flight(), 0);
    }

    #[tokio::test]
    async fn late_response_is_dropped() {
        let alice = a_peer();
        let t = TopicHandler::new(
            "doc-sync",
            alice,
            Arc::new(FnHandler(|_: PeerId, _: Vec<u8>| async {
                Ok::<Vec<u8>, String>(Vec::new())
            })),
        );
        let envelope = InternalResponse {
            id: derive_request_id(b"never").to_string(),
            err: String::new(),
            data: Vec::new(),
            from: Vec::new(),
        };
        let outcome = t
            .deliver_gossip_message(
                t.self_response_topic(),
                a_peer(),
                envelope.to_cbor().unwrap(),
            )
            .await;
        assert!(matches!(outcome, DeliveryOutcome::Ignored));
    }

    #[tokio::test]
    async fn malformed_response_envelope_is_ignored() {
        let alice = a_peer();
        let t = TopicHandler::new(
            "doc-sync",
            alice,
            Arc::new(FnHandler(|_: PeerId, _: Vec<u8>| async {
                Ok::<Vec<u8>, String>(Vec::new())
            })),
        );
        let outcome = t
            .deliver_gossip_message(t.self_response_topic(), a_peer(), vec![0xff, 0xff, 0xff])
            .await;
        assert!(matches!(outcome, DeliveryOutcome::Ignored));
    }
}
