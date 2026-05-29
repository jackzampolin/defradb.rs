//! Pubsub-gossip transport for KMS request/reply, generic over any
//! [`P2PTransport`] that supports raw gossip (`publish_raw` +
//! `register_pubsub_rpc_topic`).
//!
//! Wire-compatible at the topic level with Go's `internal/kms/pubsub.go`:
//! bare CBOR on topic `"encryption"`, ECIES-encrypted reply blocks. No
//! request-id envelope — requests and replies are matched cryptographically.

use kms::{
    EncodedFetchRequest, FetchEncryptionKeyReply, FetchEncryptionKeyRequest, IncomingHandler,
    KeyTransport, PeerIdentity, Result as KmsResult, TransportReplyStream,
};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, warn};

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
/// **M1 limitation:** single in-flight reply slot. Each `get_keys` call
/// produces one transport iteration, and `DefraKms` does not issue
/// concurrent `get_keys` against the same transport. A later milestone
/// broadens this to N slots via request-id correlation.
pub struct PubsubKeyTransport<T: P2PTransport> {
    transport: T,
    handler: RwLock<Option<Arc<dyn IncomingHandler>>>,
    in_flight: RwLock<Option<mpsc::Sender<(FetchEncryptionKeyReply, String)>>>,
}

impl<T: P2PTransport> PubsubKeyTransport<T> {
    /// Construct, subscribe to ENCRYPTION_TOPIC, register raw routing.
    pub async fn new(transport: T) -> KmsResult<Arc<Self>> {
        transport
            .subscribe(DefraTopic::Encryption)
            .await
            .map_err(|e| kms::Error::Internal(format!("subscribe encryption topic: {e}")))?;
        transport
            .register_pubsub_rpc_topic(ENCRYPTION_TOPIC.to_string())
            .await
            .map_err(|e| kms::Error::Internal(format!("register raw routing: {e}")))?;
        Ok(Arc::new(Self {
            transport,
            handler: RwLock::new(None),
            in_flight: RwLock::new(None),
        }))
    }

    /// Called by the sync coordinator when a `GossipRawMessage` arrives on
    /// ENCRYPTION_TOPIC. Reply-decode-first; else dispatch as a request.
    pub async fn dispatch_incoming(&self, from: PeerIdentity, payload: Vec<u8>) {
        if let Some(tx) = self.in_flight.read().ok().and_then(|g| g.clone()) {
            if let Ok(reply) = serde_cbor::from_slice::<FetchEncryptionKeyReply>(&payload) {
                // `from` is the gossip source = the responder; its peer id is
                // needed to rebuild the ECIES AAD on the requester side.
                let _ = tx.send((reply, from.peer_id.clone())).await;
                return;
            }
        }
        let handler = self.handler.read().ok().and_then(|g| g.clone());
        let Some(handler) = handler else {
            warn!("KMS request arrived but no handler installed; dropping");
            return;
        };
        let req: FetchEncryptionKeyRequest = match serde_cbor::from_slice(&payload) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "failed to decode KMS request");
                return;
            }
        };
        let reply = match handler.handle(from, req).await {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "KMS handler errored");
                return;
            }
        };
        if reply.blocks.is_empty() {
            return;
        }
        let bytes = match serde_cbor::to_vec(&reply) {
            Ok(b) => b,
            Err(e) => {
                warn!(error = %e, "failed to encode KMS reply");
                return;
            }
        };
        if let Err(e) = self.publish_with_graft_retry(bytes).await {
            warn!(error = %e, "failed to publish KMS reply on encryption topic");
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

    /// Publish on the encryption topic, retrying briefly on `InsufficientPeers`.
    ///
    /// Even after a subscriber is known, the very first publish can still race
    /// the mesh graft (gossipsub heartbeat). Retry within the same bounded
    /// window rather than failing the fetch outright.
    async fn publish_with_graft_retry(&self, payload: Vec<u8>) -> KmsResult<()> {
        let deadline = tokio::time::Instant::now() + SUBSCRIBER_WAIT_TIMEOUT;
        loop {
            match self
                .transport
                .publish_raw(ENCRYPTION_TOPIC.to_string(), payload.clone())
                .await
            {
                Ok(_) => return Ok(()),
                Err(e) if tokio::time::Instant::now() < deadline => {
                    debug!(error = %e, "encryption-topic publish not yet ready; retrying");
                    tokio::time::sleep(SUBSCRIBER_POLL_INTERVAL).await;
                }
                Err(e) => return Err(kms::Error::Internal(format!("publish KMS request: {e}"))),
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
        let (tx, rx) = mpsc::channel(16);
        if let Ok(mut slot) = self.in_flight.write() {
            *slot = Some(tx);
        }
        self.wait_for_subscriber().await;
        self.publish_with_graft_retry(req.payload).await?;
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// Mock transport that mimics the gossipsub subscription-propagation race:
    /// `topic_peers` reports an empty set until `subscribe_after` polls have
    /// elapsed, and `publish_raw` fails with `InsufficientPeers` until a
    /// subscriber is visible.
    #[derive(Clone)]
    struct RacyTransport {
        local_peer_id: PeerId,
        peer: PeerId,
        topic_peers_calls: Arc<AtomicUsize>,
        subscriber_visible_after: usize,
        publish_attempts: Arc<AtomicUsize>,
    }

    impl RacyTransport {
        fn new(subscriber_visible_after: usize) -> Self {
            let id = |s: &str| PeerId::new(s.to_string());
            Self {
                local_peer_id: id("local"),
                peer: id("remote"),
                topic_peers_calls: Arc::new(AtomicUsize::new(0)),
                subscriber_visible_after,
                publish_attempts: Arc::new(AtomicUsize::new(0)),
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
        async fn publish_raw(&self, _t: String, _d: Vec<u8>) -> Result<MessageId> {
            self.publish_attempts.fetch_add(1, Ordering::SeqCst);
            if self.subscriber_known() {
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
    /// immediately on a key-miss does not fail with `InsufficientPeers`.
    #[tokio::test(start_paused = true)]
    async fn send_request_waits_for_subscriber_then_publishes() {
        // Subscriber becomes visible only on the 3rd topic_peers poll, i.e.
        // after ~2 poll intervals — modelling gossipsub propagation delay.
        let transport = RacyTransport::new(3);
        let publish_attempts = transport.publish_attempts.clone();
        let kt = PubsubKeyTransport::new(transport).await.unwrap();

        let req = EncodedFetchRequest {
            payload: b"fetch".to_vec(),
            request_id: "r1".to_string(),
        };
        // Should succeed (not error) because it waits for the subscriber.
        let _rx = kt
            .send_request(req)
            .await
            .expect("send_request must succeed");

        // The publish only fired after the subscriber was known, so the single
        // publish attempt succeeded (no InsufficientPeers surfaced).
        assert_eq!(
            publish_attempts.load(Ordering::SeqCst),
            1,
            "publish should fire exactly once, after the subscriber is known"
        );
    }
}
