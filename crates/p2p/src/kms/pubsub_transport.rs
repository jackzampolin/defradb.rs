//! Pubsub-gossip transport for KMS request/reply, generic over any
//! [`P2PTransport`] that supports raw gossip (`publish_raw` + `subscribe_raw`
//! + `register_pubsub_rpc_topic`).
//!
//! Wire-compatible with Go's `internal/kms/pubsub.go`, which layers the
//! `sourcenetwork/go-libp2p-pubsub-rpc` request/response protocol over
//! gossipsub:
//!
//! - The requester publishes a bare-CBOR `FetchEncryptionKeyRequest` on topic
//!   `"encryption"` and tracks it by `CIDv1(raw, sha256(request_bytes))`.
//! - The responder runs its handler and publishes the reply NOT on
//!   `"encryption"` but on the requester's per-peer sub-topic
//!   `"encryption/<requester_peer_id>/_response"`, wrapped in an
//!   `internalResponse` dag-cbor envelope `{ID, From, Data, Err}` where `Data`
//!   is the bare-CBOR `FetchEncryptionKeyReply` and `ID` echoes the request CID.
//! - The requester, subscribed to `"encryption/<self>/_response"`, decodes the
//!   envelope, correlates `ID` to the outstanding request, and unwraps the
//!   ECIES reply blocks. The AAD binds the requester's ephemeral pubkey and the
//!   responder's peer id — the latter taken from the verified gossip source of
//!   the `_response` message (Go's `resp.From`).
//!
//! This re-uses the Go-fixture-verified [`crate::pubsub_rpc`] primitive
//! (`Correlator` + envelope + topic naming + request-id derivation) rather than
//! a bespoke single-slot reply path, so concurrent fetches correlate correctly.

use kms::{
    EncodedFetchRequest, FetchEncryptionKeyReply, FetchEncryptionKeyRequest, IncomingHandler,
    KeyTransport, Result as KmsResult, TransportReplyStream,
};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::pubsub_rpc::{response_topic, Correlator, InternalResponse, PublishOptions};
use crate::topics::{DefraTopic, ENCRYPTION_TOPIC};
use crate::transport::P2PTransport;

/// Upper bound on how long `send_request` waits for at least one peer to be
/// known as an encryption-topic subscriber before publishing. gossipsub
/// propagates SUBSCRIBE control messages on its heartbeat (default 1s), so a
/// fetch issued immediately on a key-miss can race subscription propagation.
/// Without this wait, `flood_publish` has zero targets and the publish fails
/// with `InsufficientPeers` — the request never reaches the wire (#976).
const SUBSCRIBER_WAIT_TIMEOUT: Duration = Duration::from_secs(10);

/// Poll interval while waiting for an encryption-topic subscriber to appear.
const SUBSCRIBER_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Gossip-backed `KeyTransport`, generic over the underlying P2P transport.
///
/// Outgoing fetches are correlated by request-CID via [`Correlator`]; replies
/// arrive on the local `encryption/<self>/_response` sub-topic as
/// `internalResponse` envelopes. Inbound requests on `encryption` are answered
/// by publishing an envelope on the caller's `_response` sub-topic.
pub struct PubsubKeyTransport<T: P2PTransport> {
    transport: T,
    handler: RwLock<Option<Arc<dyn IncomingHandler>>>,
    correlator: Correlator,
    /// This node's libp2p peer id (gossip source string form).
    local_peer_id: String,
    /// Pre-formatted `encryption/<self>/_response` topic this node subscribes
    /// to in order to receive replies addressed to it.
    self_response_topic: String,
}

impl<T: P2PTransport> PubsubKeyTransport<T> {
    /// Construct, subscribe to ENCRYPTION_TOPIC and the local `_response`
    /// sub-topic, and register both for raw routing.
    pub async fn new(transport: T) -> KmsResult<Arc<Self>> {
        let local_peer_id = transport.local_peer_id().to_string();
        let self_response_topic = match local_peer_id.parse::<libp2p::PeerId>() {
            Ok(pid) => response_topic(ENCRYPTION_TOPIC, &pid),
            Err(_) => {
                // Non-libp2p peer id (e.g. iroh): fall back to a string-joined
                // sub-topic. Go interop only applies on libp2p, so this branch
                // exists purely to keep the type generic over transports.
                format!("{ENCRYPTION_TOPIC}/{local_peer_id}/_response")
            }
        };

        transport
            .subscribe(DefraTopic::Encryption)
            .await
            .map_err(|e| kms::Error::Internal(format!("subscribe encryption topic: {e}")))?;
        transport
            .register_pubsub_rpc_topic(ENCRYPTION_TOPIC.to_string())
            .await
            .map_err(|e| kms::Error::Internal(format!("register raw routing: {e}")))?;
        transport
            .subscribe_raw(self_response_topic.clone())
            .await
            .map_err(|e| kms::Error::Internal(format!("subscribe encryption _response: {e}")))?;
        transport
            .register_pubsub_rpc_topic(self_response_topic.clone())
            .await
            .map_err(|e| kms::Error::Internal(format!("register _response routing: {e}")))?;

        Ok(Arc::new(Self {
            transport,
            handler: RwLock::new(None),
            correlator: Correlator::new(),
            local_peer_id,
            self_response_topic,
        }))
    }

    /// The `encryption/<self>/_response` sub-topic this transport owns. The
    /// dispatcher routes inbound messages on this topic here.
    pub fn self_response_topic(&self) -> &str {
        &self.self_response_topic
    }

    /// Called by the sync coordinator when a `GossipRawMessage` arrives on
    /// the encryption topic or its `_response` sub-topic.
    ///
    /// - `topic == encryption`: treat as an inbound request — run the handler
    ///   and publish the reply envelope on the caller's `_response` sub-topic.
    /// - `topic == encryption/<self>/_response`: decode the `internalResponse`
    ///   envelope and route it to the correlator for the waiting `send_request`.
    pub async fn dispatch_incoming(&self, from_peer: String, topic: String, payload: Vec<u8>) {
        // `from_peer` is the verified gossip source in transport-native form
        // (libp2p base58 over libp2p, iroh hex over iroh). It is forwarded
        // through correlation/AAD as an opaque string and never parsed, so the
        // KMS pubsub path is transport-agnostic (#976).

        // Reply path: our own `_response` sub-topic.
        if topic == self.self_response_topic {
            let envelope = match InternalResponse::from_cbor(&payload) {
                Ok(e) => e,
                Err(e) => {
                    debug!(
                        from = %from_peer,
                        error = %e,
                        payload_len = payload.len(),
                        "KMS dispatch: failed to decode response envelope; dropping"
                    );
                    return;
                }
            };
            let delivered = self.correlator.deliver(from_peer.clone(), envelope);
            debug!(
                from = %from_peer,
                payload_len = payload.len(),
                delivered,
                "KMS dispatch: response envelope routed to correlator"
            );
            return;
        }

        // Request path: bare-CBOR FetchEncryptionKeyRequest on the base topic.
        if topic == ENCRYPTION_TOPIC {
            self.handle_request(from_peer, payload).await;
            return;
        }

        // Any other `encryption/<peer>/_response` topic is addressed to a peer
        // that is not us — we only subscribe to our own — so it should never
        // reach here. Drop defensively.
        debug!(topic = %topic, "KMS dispatch: unexpected topic; dropping");
    }

    /// Handle an inbound request on the base topic: decode, dispatch to the
    /// installed handler, then publish the reply on the caller's `_response`
    /// sub-topic wrapped in an `internalResponse` envelope (Go parity).
    async fn handle_request(&self, from: String, payload: Vec<u8>) {
        // Ignore our own request echoed back by the mesh.
        if from == self.local_peer_id {
            return;
        }
        let handler = self.handler.read().ok().and_then(|g| g.clone());
        let Some(handler) = handler else {
            warn!("KMS request arrived but no handler installed; dropping");
            return;
        };
        let req: FetchEncryptionKeyRequest = match serde_cbor::from_slice(&payload) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "KMS dispatch: failed to decode request");
                return;
            }
        };
        let request_id = crate::pubsub_rpc::derive_request_id(&payload);
        let (reply_bytes, err) = match handler
            .handle(
                kms::PeerIdentity {
                    peer_id: from.clone(),
                },
                req,
            )
            .await
        {
            Ok(reply) => match serde_cbor::to_vec(&reply) {
                Ok(b) => (b, String::new()),
                Err(e) => {
                    warn!(error = %e, "KMS dispatch: failed to encode reply");
                    return;
                }
            },
            Err(e) => {
                warn!(error = %e, "KMS handler errored");
                (Vec::new(), e.to_string())
            }
        };

        // Go's serve side returns an empty reply (no blocks) when it holds or
        // is authorized for nothing; it still publishes a response envelope so
        // the requester's correlation slot resolves rather than timing out.
        let envelope = InternalResponse {
            id: request_id.to_string(),
            err,
            data: reply_bytes,
            from: Vec::new(), // filled in by the recipient from the gossip source
        };
        let bytes = match envelope.to_cbor() {
            Ok(b) => b,
            Err(e) => {
                warn!(error = %e, "KMS dispatch: failed to encode response envelope");
                return;
            }
        };
        // Build the caller's `_response` sub-topic from the transport-native
        // peer-id string. Identical to `response_topic(ENCRYPTION_TOPIC, &pid)`
        // for a libp2p base58 `from` (Go wire-compat), and correct for iroh hex
        // peer ids that do not parse as a `libp2p::PeerId`.
        let reply_topic = format!("{ENCRYPTION_TOPIC}/{from}/_response");
        if let Err(e) = self
            .publish_with_graft_retry(reply_topic.clone(), bytes)
            .await
        {
            warn!(
                topic = %reply_topic,
                error = %e,
                "KMS dispatch: failed to publish reply envelope on _response sub-topic"
            );
        } else {
            debug!(topic = %reply_topic, "KMS dispatch: reply envelope published");
        }
    }

    /// Block (bounded) until at least one peer is known to be subscribed to
    /// the encryption topic, so the subsequent `flood_publish` has a target.
    ///
    /// Returns as soon as a subscriber appears, or after
    /// [`SUBSCRIBER_WAIT_TIMEOUT`]. Timing out is not fatal: the caller still
    /// attempts the publish (it may yet succeed, or surface a clear error).
    async fn wait_for_subscriber(&self) {
        let deadline = tokio::time::Instant::now() + SUBSCRIBER_WAIT_TIMEOUT;
        loop {
            match self.transport.topic_peers(DefraTopic::Encryption).await {
                Ok(peers) if !peers.is_empty() => return,
                Ok(_) => {}
                Err(e) => {
                    debug!(error = %e, "topic_peers query failed while awaiting KMS subscriber");
                }
            }
            if tokio::time::Instant::now() >= deadline {
                warn!(
                    "no encryption-topic subscriber appeared within {:?}; publishing anyway",
                    SUBSCRIBER_WAIT_TIMEOUT
                );
                return;
            }
            tokio::time::sleep(SUBSCRIBER_POLL_INTERVAL).await;
        }
    }

    /// Publish on `topic`, retrying briefly on `InsufficientPeers`.
    ///
    /// Even after a subscriber is known, the very first publish can still race
    /// the mesh graft (gossipsub heartbeat). Retry within the same bounded
    /// window rather than failing outright.
    async fn publish_with_graft_retry(&self, topic: String, payload: Vec<u8>) -> KmsResult<()> {
        let deadline = tokio::time::Instant::now() + SUBSCRIBER_WAIT_TIMEOUT;
        loop {
            match self
                .transport
                .publish_raw(topic.clone(), payload.clone())
                .await
            {
                Ok(_) => return Ok(()),
                Err(e) if tokio::time::Instant::now() < deadline => {
                    debug!(topic = %topic, error = %e, "publish not yet ready; retrying");
                    tokio::time::sleep(SUBSCRIBER_POLL_INTERVAL).await;
                }
                Err(e) => return Err(kms::Error::Internal(format!("publish KMS message: {e}"))),
            }
        }
    }
}

#[async_trait::async_trait]
impl<T: P2PTransport> KeyTransport for PubsubKeyTransport<T> {
    fn name(&self) -> &'static str {
        "pubsub"
    }

    async fn send_request(&self, req: EncodedFetchRequest) -> KmsResult<TransportReplyStream> {
        // Register correlation by request-CID, then publish the raw request on
        // the base topic. Go peers reply on `encryption/<self>/_response`,
        // which `dispatch_incoming` routes into the correlator.
        // Single-response, matching Go's `PublishToTopic(..., withMultiResponse:
        // false)` for the KMS topic. The correlator auto-removes the entry after
        // the first reply, closing `prep.responses` so the adapter task below
        // ends and drops its `KeyResults` sender — which is how `get_keys`'s
        // `wait_all()` learns the fetch is complete (it drains until the channel
        // closes). A multi-response entry would never close and would hang
        // `wait_all` for the full timeout even after the key arrived.
        let mut prep = self
            .correlator
            .publish(req.payload, PublishOptions::default());

        self.wait_for_subscriber().await;
        if let Err(e) = self
            .publish_with_graft_retry(ENCRYPTION_TOPIC.to_string(), prep.data.clone())
            .await
        {
            // Drop the correlation slot before returning so it doesn't linger.
            self.correlator.cancel(&prep.id);
            return Err(e);
        }
        debug!(
            request_id = %prep.id,
            payload_len = prep.data.len(),
            "KMS request published on encryption topic"
        );

        // Adapt the pubsub_rpc response stream into the KMS reply stream
        // `(FetchEncryptionKeyReply, responder_peer_id)`. The responder peer id
        // is the verified gossip source of the `_response` message — exactly
        // what Go binds into the ECIES AAD via `resp.From`.
        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(async move {
            // Hold `prep` for the lifetime of the task; its Drop releases the
            // correlation slot once the spawned task ends.
            while let Some(resp) = prep.responses.recv().await {
                if let Some(err) = &resp.err {
                    debug!(from = %resp.from, error = %err, "KMS reply carried responder error");
                    continue;
                }
                let reply: FetchEncryptionKeyReply = match serde_cbor::from_slice(&resp.data) {
                    Ok(r) => r,
                    Err(e) => {
                        warn!(
                            from = %resp.from,
                            error = %e,
                            payload_len = resp.data.len(),
                            "KMS reply: failed to decode FetchEncryptionKeyReply from envelope Data"
                        );
                        continue;
                    }
                };
                if tx.send((reply, resp.from)).await.is_err() {
                    // Receiver (get_keys spawned task) gone; stop draining.
                    break;
                }
            }
        });
        Ok(rx)
    }

    fn install_handler(&self, handler: Arc<dyn IncomingHandler>) {
        if let Ok(mut slot) = self.handler.write() {
            *slot = Some(handler);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{Error, Result};
    use crate::message::{
        BranchableSyncReply, BranchableSyncRequest, DocSyncReply, DocSyncRequest, PushLogBroadcast,
        PushLogReply, PushLogRequest, PushSEArtifactsRequest,
    };
    use crate::transport::{MessageId, PeerAddr, PeerId};
    use crate::{QueryId, ReplicatorInfo};
    use async_trait::async_trait;
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    fn a_libp2p_peer() -> libp2p::PeerId {
        libp2p::PeerId::from_public_key(&libp2p::identity::Keypair::generate_ed25519().public())
    }

    /// Mock transport that records raw publishes and mimics the gossipsub
    /// subscription-propagation race for `topic_peers`/`publish_raw`.
    #[derive(Clone)]
    #[allow(clippy::type_complexity)]
    struct RacyTransport {
        local_peer_id: PeerId,
        peer: PeerId,
        topic_peers_calls: Arc<AtomicUsize>,
        subscriber_visible_after: usize,
        publish_attempts: Arc<AtomicUsize>,
        published: Arc<Mutex<Vec<(String, Vec<u8>)>>>,
    }

    impl RacyTransport {
        fn new(subscriber_visible_after: usize) -> Self {
            let lp = a_libp2p_peer().to_string();
            Self {
                local_peer_id: PeerId::new(lp),
                peer: PeerId::new(a_libp2p_peer().to_string()),
                topic_peers_calls: Arc::new(AtomicUsize::new(0)),
                subscriber_visible_after,
                publish_attempts: Arc::new(AtomicUsize::new(0)),
                published: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn subscriber_known(&self) -> bool {
            self.topic_peers_calls.load(Ordering::SeqCst) >= self.subscriber_visible_after
        }
    }

    #[async_trait]
    impl crate::transport::P2PTransport for RacyTransport {
        type ResponseToken = ();

        fn local_peer_id(&self) -> &PeerId {
            &self.local_peer_id
        }
        fn local_public_key_proto(&self) -> &[u8] {
            &[]
        }
        fn sign(&self, _data: &[u8]) -> Result<Vec<u8>> {
            Ok(Vec::new())
        }
        async fn dial(&self, _p: &PeerId, _a: Vec<PeerAddr>) -> Result<()> {
            Ok(())
        }
        async fn disconnect(&self, _p: &PeerId) -> Result<()> {
            Ok(())
        }
        async fn listen(&self, _a: PeerAddr) -> Result<()> {
            Ok(())
        }
        async fn connected_peers(&self) -> Result<Vec<PeerId>> {
            Ok(vec![self.peer.clone()])
        }
        async fn listen_addresses(&self) -> Result<Vec<PeerAddr>> {
            Ok(Vec::new())
        }
        async fn poll_until_connected(&self, _p: &PeerId, _t: Duration) -> Result<()> {
            Ok(())
        }
        async fn peer_addresses(&self) -> Result<Vec<String>> {
            Ok(Vec::new())
        }
        async fn topic_peers(&self, _topic: DefraTopic) -> Result<Vec<PeerId>> {
            let n = self.topic_peers_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if n >= self.subscriber_visible_after {
                Ok(vec![self.peer.clone()])
            } else {
                Ok(Vec::new())
            }
        }
        async fn subscribe(&self, _t: DefraTopic) -> Result<bool> {
            Ok(true)
        }
        async fn unsubscribe(&self, _t: DefraTopic) -> Result<bool> {
            Ok(true)
        }
        async fn publish(&self, _t: DefraTopic, _m: PushLogBroadcast) -> Result<MessageId> {
            Ok(MessageId::new("noop".to_string()))
        }
        async fn publish_raw(&self, t: String, d: Vec<u8>) -> Result<MessageId> {
            self.publish_attempts.fetch_add(1, Ordering::SeqCst);
            if self.subscriber_known() {
                self.published.lock().push((t, d));
                Ok(MessageId::new("ok".to_string()))
            } else {
                Err(Error::GossipSubPublish("InsufficientPeers".to_string()))
            }
        }
        async fn subscribe_raw(&self, _t: String) -> Result<bool> {
            Ok(true)
        }
        async fn register_pubsub_rpc_topic(&self, _t: String) -> Result<()> {
            Ok(())
        }
        async fn send_pushlog_response(&self, _t: (), _r: PushLogReply) -> Result<()> {
            Ok(())
        }
        async fn send_two_stream_request(
            &self,
            _p: &PeerId,
            _r: PushLogRequest,
        ) -> Result<PushLogReply> {
            Err(Error::Transport("n/a".to_string()))
        }
        async fn send_two_stream_response(&self, _p: &PeerId, _r: PushLogReply) -> Result<()> {
            Ok(())
        }
        async fn send_doc_sync_request(&self, _p: &PeerId, _r: DocSyncRequest) -> Result<()> {
            Ok(())
        }
        async fn send_doc_sync_response(&self, _p: &PeerId, _r: DocSyncReply) -> Result<()> {
            Ok(())
        }
        async fn send_branchable_sync_request(
            &self,
            _p: &PeerId,
            _r: BranchableSyncRequest,
        ) -> Result<()> {
            Ok(())
        }
        async fn send_branchable_sync_response(
            &self,
            _p: &PeerId,
            _r: BranchableSyncReply,
        ) -> Result<()> {
            Ok(())
        }
        async fn send_car_request(&self, _p: &PeerId, _c: cid::Cid) -> Result<()> {
            Ok(())
        }
        async fn send_car_response(&self, _p: &PeerId, _c: Vec<u8>) -> Result<()> {
            Ok(())
        }
        async fn send_car_response_token(&self, _t: (), _c: Vec<u8>) -> Result<()> {
            Ok(())
        }
        async fn send_doc_sync_response_token(&self, _t: (), _r: DocSyncReply) -> Result<()> {
            Ok(())
        }
        async fn send_branchable_sync_response_token(
            &self,
            _t: (),
            _r: BranchableSyncReply,
        ) -> Result<()> {
            Ok(())
        }
        async fn send_se_artifacts(&self, _p: &PeerId, _r: PushSEArtifactsRequest) -> Result<()> {
            Ok(())
        }
        async fn sync_blocks(
            &self,
            _r: cid::Cid,
            _p: Vec<PeerId>,
            _m: Vec<cid::Cid>,
        ) -> Result<QueryId> {
            Ok(QueryId(0))
        }
        async fn cancel_sync(&self, _q: QueryId) -> Result<bool> {
            Ok(true)
        }
        async fn create_replicator(&self, _p: &PeerId, _c: Vec<String>) -> Result<()> {
            Ok(())
        }
        async fn delete_replicator(&self, _p: &PeerId) -> Result<()> {
            Ok(())
        }
        async fn list_replicators(&self) -> Result<Vec<ReplicatorInfo>> {
            Ok(Vec::new())
        }
        async fn get_replicator(&self, _p: &PeerId) -> Result<Option<ReplicatorInfo>> {
            Ok(None)
        }
        async fn remove_replicator_collections(
            &self,
            _p: &PeerId,
            _c: Vec<String>,
        ) -> Result<bool> {
            Ok(false)
        }
        async fn shutdown(&self) -> Result<()> {
            Ok(())
        }
    }

    /// Regression test for #976: `send_request` must wait for the encryption
    /// topic to have a known subscriber before publishing, so a fetch issued
    /// immediately on a key-miss does not fail with `InsufficientPeers`. The
    /// request must be published on the base `encryption` topic.
    #[tokio::test(start_paused = true)]
    async fn send_request_waits_for_subscriber_then_publishes() {
        let transport = RacyTransport::new(3);
        let publish_attempts = transport.publish_attempts.clone();
        let published = transport.published.clone();
        let kt = PubsubKeyTransport::new(transport).await.unwrap();

        let req = EncodedFetchRequest {
            payload: b"fetch".to_vec(),
            request_id: "r1".to_string(),
        };
        let _rx = kt
            .send_request(req)
            .await
            .expect("send_request must succeed");

        assert_eq!(
            publish_attempts.load(Ordering::SeqCst),
            1,
            "publish should fire exactly once, after the subscriber is known"
        );
        let pubs = published.lock();
        assert_eq!(pubs.len(), 1);
        assert_eq!(pubs[0].0, ENCRYPTION_TOPIC, "request must go on base topic");
        assert_eq!(pubs[0].1, b"fetch");
    }

    /// A response envelope arriving on the local `_response` sub-topic must be
    /// decoded, correlated to the outstanding request, and surfaced on the
    /// reply stream as `(FetchEncryptionKeyReply, responder_peer_id)`.
    #[tokio::test]
    async fn response_envelope_routes_to_waiting_request() {
        let transport = RacyTransport::new(1);
        let kt = PubsubKeyTransport::new(transport).await.unwrap();

        let payload = b"the-request-bytes".to_vec();
        let req = EncodedFetchRequest {
            payload: payload.clone(),
            request_id: "r1".to_string(),
        };
        let mut rx = kt.send_request(req).await.expect("send_request");

        // Build the reply Go would send: bare-CBOR FetchEncryptionKeyReply
        // wrapped in an internalResponse envelope, ID = CID of the request.
        let reply = FetchEncryptionKeyReply {
            links: vec![vec![1, 2, 3]],
            blocks: vec![vec![4, 5, 6]],
            ephemeral_public_key: vec![7; 32],
        };
        let data = serde_cbor::to_vec(&reply).unwrap();
        let request_id = crate::pubsub_rpc::derive_request_id(&payload);
        let envelope = InternalResponse {
            id: request_id.to_string(),
            err: String::new(),
            data,
            from: Vec::new(),
        };
        let responder = a_libp2p_peer();
        kt.dispatch_incoming(
            responder.to_string(),
            kt.self_response_topic().to_string(),
            envelope.to_cbor().unwrap(),
        )
        .await;

        let (got_reply, responder_id) = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("reply must arrive within timeout")
            .expect("reply present");
        assert_eq!(got_reply, reply);
        assert_eq!(
            responder_id,
            responder.to_string(),
            "responder peer id must be the verified gossip source"
        );
    }

    /// An inbound request on the base topic must be answered by publishing an
    /// internalResponse envelope on the caller's `_response` sub-topic.
    #[tokio::test]
    async fn inbound_request_publishes_reply_on_caller_response_topic() {
        struct EchoHandler;
        #[async_trait]
        impl IncomingHandler for EchoHandler {
            async fn handle(
                &self,
                _from: kms::PeerIdentity,
                _req: FetchEncryptionKeyRequest,
            ) -> KmsResult<FetchEncryptionKeyReply> {
                Ok(FetchEncryptionKeyReply {
                    links: vec![vec![9]],
                    blocks: vec![vec![8]],
                    ephemeral_public_key: vec![1; 32],
                })
            }
        }

        // subscriber_visible_after = 0 ⇒ publish_raw is always "ready" without a
        // prior topic_peers poll (the serve/reply path doesn't call
        // wait_for_subscriber).
        let transport = RacyTransport::new(0);
        let published = transport.published.clone();
        let kt = PubsubKeyTransport::new(transport).await.unwrap();
        kt.install_handler(Arc::new(EchoHandler));

        let caller = a_libp2p_peer();
        let req = FetchEncryptionKeyRequest {
            identity: b"did:key:zalice".to_vec(),
            links: vec![vec![1]],
            ephemeral_public_key: vec![2; 32],
        };
        let req_bytes = serde_cbor::to_vec(&req).unwrap();

        kt.dispatch_incoming(caller.to_string(), ENCRYPTION_TOPIC.to_string(), req_bytes)
            .await;

        let pubs = published.lock();
        let expected_topic = response_topic(ENCRYPTION_TOPIC, &caller);
        let reply_pub = pubs
            .iter()
            .find(|(t, _)| *t == expected_topic)
            .expect("reply must be published on caller's _response sub-topic");
        let env = InternalResponse::from_cbor(&reply_pub.1).expect("decode envelope");
        let reply: FetchEncryptionKeyReply =
            serde_cbor::from_slice(&env.data).expect("decode reply");
        assert_eq!(reply.blocks, vec![vec![8]]);
    }
}
