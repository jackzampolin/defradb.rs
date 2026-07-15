//! Transport event handling for the sync coordinator.

mod bitswap;
mod branchable_sync;
pub(crate) mod car;
mod doc_sync;
mod gossip;
mod pubsub_raw;
mod pushlog;

use blockstore::Blockstore;
use std::time::Duration;

use super::SyncCoordinator;
use crate::error::{Error, Result, RATE_LIMITED_MESSAGE};
use crate::message::{BranchableSyncReply, DocSyncReply, PushLogReply};
use crate::signing::sign_with_transport;
use crate::sync::rate_limiter::RateLimitDecision;
use crate::transport::{P2PTransport, PeerId, TransportEvent};

const MAX_RETRIABLE_EVENT_ATTEMPTS: usize = 4;

fn retriable_event_delay(attempt: usize) -> Duration {
    match attempt {
        1 => Duration::from_millis(10),
        2 => Duration::from_millis(25),
        _ => Duration::from_millis(50),
    }
}

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    async fn retry_retriable_event<F, Fut>(&self, event_kind: &'static str, mut op: F) -> Result<()>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let mut attempt = 1;
        loop {
            match op().await {
                Ok(()) => return Ok(()),
                Err(error) if error.is_retriable() && attempt < MAX_RETRIABLE_EVENT_ATTEMPTS => {
                    tracing::debug!(
                        event_kind,
                        attempt,
                        max_attempts = MAX_RETRIABLE_EVENT_ATTEMPTS,
                        error = %error,
                        "Retryable transport event failed; backing off and retrying"
                    );
                    tokio::time::sleep(retriable_event_delay(attempt)).await;
                    attempt += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn handle_peer_connected(&self, peer_id: PeerId) {
        tracing::debug!(peer_id = %peer_id, "Peer connected");
        self.access.peer_state.peer_connected(peer_id.as_str());
        self.redrive_pending_dags_for_peer(&peer_id);
        // Durable registrations whose in-memory entries were TTL-evicted can
        // only complete once a provider is reachable again: reconcile them
        // now (single-flight; a no-op while another sweep runs).
        self.manager.resync_persisted_pending_dags().await;
    }

    /// A newly connected peer may be able to complete pending DAGs it
    /// provided (or ones whose fetches exhausted providers): make them
    /// promptly due and let the retry clock dispatch, so simultaneous
    /// connects cannot re-drive the same root more than once per tick.
    fn redrive_pending_dags_for_peer(&self, peer_id: &PeerId) {
        let peer_key = peer_id.to_string();
        for (root_cid, _dag) in self.manager.pending_dags_needing_redrive(&peer_key) {
            self.manager.expedite_pending_dag_retry(&root_cid);
        }
    }

    fn handle_peer_disconnected(&self, peer_id: PeerId) {
        tracing::debug!(peer_id = %peer_id, "Peer disconnected");
        self.access.peer_state.peer_disconnected(peer_id.as_str());
        self.runtime.rate_limiter.remove_peer(&peer_id);
        self.runtime.request_rate_limiter.remove_peer(&peer_id);
    }

    fn handle_peer_subscribed(&self, peer_id: PeerId, topic: String) {
        tracing::debug!(peer_id = %peer_id, topic = %topic, "Peer subscribed to topic");
        self.access
            .peer_state
            .peer_subscribed(peer_id.as_str(), topic);
    }

    fn handle_peer_unsubscribed(&self, peer_id: PeerId, topic: String) {
        tracing::debug!(peer_id = %peer_id, topic = %topic, "Peer unsubscribed from topic");
        self.access
            .peer_state
            .peer_unsubscribed(peer_id.as_str(), &topic);
    }

    fn rate_limited_error(peer_id: &PeerId) -> Error {
        Error::AccessDenied {
            peer_id: peer_id.to_string(),
            collection_id: "rate-limited".into(),
        }
    }

    /// Consume one token from `limiter` for `peer_id`, returning the
    /// synthetic rate-limit error when the peer is over budget.
    ///
    /// Two limiters exist on purpose and must stay separate: gossip has no
    /// reply channel, so refusals are silent drops governed by the long abuse
    /// ladder; request events are nacked with `RATE_LIMITED_MESSAGE` (via the
    /// `reject_rate_limited_*` helpers) and use the paced limiter, whose
    /// retry horizon is ~one token refill — a long lockout here wedges any
    /// full-DAG push deeper than the burst (see `p2p_deep_catchup`).
    /// Nack-on-overload is the Go-aligned behavior: Go's direct replicator
    /// channel drives its retry ladder off error replies; overload replies
    /// are orthogonal to its trust/ACP bypasses.
    fn check_rate_limit(
        &self,
        limiter: &crate::sync::rate_limiter::PeerRateLimiter,
        peer_id: &PeerId,
        event_kind: &'static str,
    ) -> Result<()> {
        match limiter.check(peer_id) {
            RateLimitDecision::Allowed => Ok(()),
            RateLimitDecision::Limited {
                retry_after,
                consecutive_failures,
            } => {
                tracing::debug!(
                    peer_id = %peer_id,
                    event_kind,
                    retry_after_ms = retry_after.as_millis(),
                    consecutive_failures,
                    "Rate limit exceeded, rejecting event"
                );
                Err(Self::rate_limited_error(peer_id))
            }
        }
    }

    async fn reject_rate_limited_pushlog(
        &self,
        message_id: &str,
        token: T::ResponseToken,
        error: Error,
    ) -> Error {
        let reply = PushLogReply::error(message_id, RATE_LIMITED_MESSAGE);
        // Best-effort: if the nack cannot be sent, the pusher times out and
        // lands in the same retry path; no state was discarded.
        if let Err(send_err) = self
            .runtime
            .transport
            .send_pushlog_response(token, reply)
            .await
        {
            tracing::debug!(error = %send_err, "Failed to send PushLog backpressure nack");
        }
        error
    }

    async fn reject_rate_limited_two_stream(
        &self,
        peer_id: &PeerId,
        message_id: &str,
        token: Option<T::ResponseToken>,
        supports_same_stream_reply: bool,
        error: Error,
    ) -> Error {
        let mut reply = PushLogReply::error(message_id, RATE_LIMITED_MESSAGE);
        // Best-effort: send_two_stream_reply logs its own failures; an unsent
        // nack degrades to a pusher-side timeout on the same retry path.
        if let Err(sign_err) = sign_with_transport(&self.runtime.transport, &mut reply) {
            tracing::debug!(error = %sign_err, "Failed to sign two-stream backpressure nack");
        }
        self.send_two_stream_reply(peer_id, reply, token, supports_same_stream_reply)
            .await;
        error
    }

    async fn reject_rate_limited_doc_sync(
        &self,
        peer_id: &PeerId,
        message_id: &str,
        token: Option<T::ResponseToken>,
        error: Error,
    ) -> Error {
        let mut reply = DocSyncReply::error(message_id, RATE_LIMITED_MESSAGE);
        if let Err(sign_err) = sign_with_transport(&self.runtime.transport, &mut reply) {
            tracing::debug!(error = %sign_err, "Failed to sign DocSync backpressure nack");
        }
        let send_result = if let Some(token) = token {
            self.runtime
                .transport
                .send_doc_sync_response_token(token, reply)
                .await
        } else {
            self.runtime
                .transport
                .send_doc_sync_response(peer_id, reply)
                .await
        };
        if let Err(send_err) = send_result {
            tracing::debug!(error = %send_err, "Failed to send DocSync backpressure nack");
        }
        error
    }

    async fn reject_rate_limited_branchable_sync(
        &self,
        peer_id: &PeerId,
        message_id: &str,
        collection_id: &str,
        token: Option<T::ResponseToken>,
        error: Error,
    ) -> Error {
        let mut reply = BranchableSyncReply::error(message_id, collection_id, RATE_LIMITED_MESSAGE);
        if let Err(sign_err) = sign_with_transport(&self.runtime.transport, &mut reply) {
            tracing::debug!(error = %sign_err, "Failed to sign BranchableSync backpressure nack");
        }
        let send_result = if let Some(token) = token {
            self.runtime
                .transport
                .send_branchable_sync_response_token(token, reply)
                .await
        } else {
            self.runtime
                .transport
                .send_branchable_sync_response(peer_id, reply)
                .await
        };
        if let Err(send_err) = send_result {
            tracing::debug!(error = %send_err, "Failed to send BranchableSync backpressure nack");
        }
        error
    }

    async fn reject_rate_limited_car_fetch(
        &self,
        peer_id: &PeerId,
        token: Option<T::ResponseToken>,
        error: Error,
    ) -> Error {
        // CAR has no error reply type — send an empty response so the sender
        // sees an explicit (parseable) rejection rather than a hung stream.
        let send_result = if let Some(token) = token {
            self.runtime
                .transport
                .send_car_response_token(token, Vec::new())
                .await
        } else {
            self.runtime
                .transport
                .send_car_response(peer_id, Vec::new())
                .await
        };
        if let Err(send_err) = send_result {
            tracing::debug!(error = %send_err, "Failed to send CAR backpressure rejection");
        }
        error
    }

    /// Handle an event from the transport layer.
    ///
    /// This should be called from the event loop that processes TransportEvents.
    pub async fn handle_transport_event(
        &self,
        event: TransportEvent<T::ResponseToken>,
    ) -> Result<()> {
        if self.runtime.shutdown.is_shutting_down() {
            tracing::trace!("Ignoring transport event because coordinator is shutting down");
            return Ok(());
        }

        match event {
            TransportEvent::PeerConnected(peer_id) => {
                self.handle_peer_connected(peer_id).await;
            }
            TransportEvent::PeerDisconnected(peer_id) => {
                self.handle_peer_disconnected(peer_id);
            }
            TransportEvent::PeerSubscribed { peer_id, topic } => {
                self.handle_peer_subscribed(peer_id, topic);
            }
            TransportEvent::PeerUnsubscribed { peer_id, topic } => {
                self.handle_peer_unsubscribed(peer_id, topic);
            }
            TransportEvent::GossipMessage {
                propagation_source,
                message,
                topic,
                ..
            } => {
                self.check_rate_limit(
                    &self.runtime.rate_limiter,
                    &propagation_source,
                    "GossipMessage",
                )?;
                self.handle_gossip_message(propagation_source, message, topic)
                    .await?;
            }
            TransportEvent::GossipRawMessage {
                propagation_source,
                topic,
                data,
                ..
            } => {
                self.check_rate_limit(
                    &self.runtime.rate_limiter,
                    &propagation_source,
                    "GossipRawMessage",
                )?;
                self.handle_gossip_raw_message(propagation_source, topic, data)
                    .await?;
            }
            TransportEvent::PushLogRequest {
                peer_id,
                request,
                token,
            } => {
                if let Err(error) = self.check_rate_limit(
                    &self.runtime.request_rate_limiter,
                    &peer_id,
                    "PushLogRequest",
                ) {
                    return Err(self
                        .reject_rate_limited_pushlog(&request.message_id, token, error)
                        .await);
                }
                self.handle_pushlog_request(peer_id, request, token).await?;
            }
            TransportEvent::TwoStreamRequest {
                peer_id,
                request,
                token,
                is_explicit_replicator,
                explicit_replay_authorization,
            } => {
                let supports_same_stream_reply = request.supports_same_stream_reply;
                if let Err(error) = self.check_rate_limit(
                    &self.runtime.request_rate_limiter,
                    &peer_id,
                    "TwoStreamRequest",
                ) {
                    return Err(self
                        .reject_rate_limited_two_stream(
                            &peer_id,
                            &request.message_id,
                            token,
                            supports_same_stream_reply,
                            error,
                        )
                        .await);
                }
                self.handle_two_stream_request(
                    peer_id,
                    request,
                    token,
                    is_explicit_replicator,
                    explicit_replay_authorization,
                )
                .await?;
            }
            TransportEvent::BitswapBlockReceived {
                query_id,
                cid,
                data,
            } => {
                self.retry_retriable_event("bitswap_block_received", || {
                    self.handle_bitswap_block_received(query_id, cid, data.clone())
                })
                .await?;
            }
            TransportEvent::BitswapComplete {
                query_id,
                success,
                error,
            } => {
                self.handle_bitswap_complete(query_id, success, error)
                    .await?;
            }
            TransportEvent::DocSyncRequest {
                peer_id,
                request,
                token,
            } => {
                if let Err(error) = self.check_rate_limit(
                    &self.runtime.request_rate_limiter,
                    &peer_id,
                    "DocSyncRequest",
                ) {
                    return Err(self
                        .reject_rate_limited_doc_sync(&peer_id, &request.message_id, token, error)
                        .await);
                }
                self.handle_doc_sync_request(peer_id, request, token)
                    .await?;
            }
            TransportEvent::DocSyncReply { peer_id, reply } => {
                self.handle_doc_sync_reply(peer_id, reply).await?;
            }
            TransportEvent::BranchableSyncRequest {
                peer_id,
                request,
                token,
            } => {
                if let Err(error) = self.check_rate_limit(
                    &self.runtime.request_rate_limiter,
                    &peer_id,
                    "BranchableSyncRequest",
                ) {
                    return Err(self
                        .reject_rate_limited_branchable_sync(
                            &peer_id,
                            &request.message_id,
                            &request.collection_id,
                            token,
                            error,
                        )
                        .await);
                }
                self.handle_branchable_sync_request(peer_id, request, token)
                    .await?;
            }
            TransportEvent::BranchableSyncReply { peer_id, reply } => {
                self.handle_branchable_sync_reply(peer_id, reply).await?;
            }
            TransportEvent::CarFetchRequest {
                peer_id,
                request,
                token,
            } => {
                if let Err(error) = self.check_rate_limit(
                    &self.runtime.request_rate_limiter,
                    &peer_id,
                    "CarFetchRequest",
                ) {
                    return Err(self
                        .reject_rate_limited_car_fetch(&peer_id, token, error)
                        .await);
                }
                self.handle_car_fetch_request(peer_id, request, token)
                    .await?;
            }
            TransportEvent::CarFetchResponse {
                peer_id,
                root_cid,
                car_data,
            } => {
                self.retry_retriable_event("car_fetch_response", || {
                    self.handle_car_fetch_response(peer_id.clone(), root_cid, car_data.clone())
                })
                .await?;
            }
            other => {
                let _ = other;
                tracing::trace!("Ignoring non-sync transport event");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use blockstore::DefraBlockstore;
    use bytes::Bytes;
    use cid::Cid;
    use defra_core::{Block, CompositeDeltaPayload, CrdtDelta, DAGLink, LwwDeltaPayload};
    use std::sync::Arc;
    use storage::backends::MemoryStore;

    use crate::error::Result as P2PResult;
    use crate::message::{
        BranchableSyncReply, BranchableSyncRequest, DocSyncReply, DocSyncRequest, PushLogBroadcast,
        PushLogReply, PushLogRequest, PushSEArtifactsRequest,
    };
    use crate::sync::{SyncConfig, SyncCoordinator, SyncEvent};
    use crate::topics::DefraTopic;
    use crate::transport::{MessageId, PeerAddr};
    use crate::{QueryId, ReplicatorInfo};

    #[derive(Clone)]
    struct TestTransport {
        peer_id: PeerId,
        pubkey: Vec<u8>,
    }

    impl TestTransport {
        fn new() -> Self {
            Self {
                peer_id: PeerId::new("local-peer".to_string()),
                pubkey: vec![1, 2, 3],
            }
        }
    }

    #[async_trait]
    impl P2PTransport for TestTransport {
        type ResponseToken = ();

        fn local_peer_id(&self) -> &PeerId {
            &self.peer_id
        }

        fn local_public_key_proto(&self) -> &[u8] {
            &self.pubkey
        }

        fn sign(&self, _data: &[u8]) -> P2PResult<Vec<u8>> {
            Ok(vec![0])
        }

        async fn dial(&self, _peer_id: &PeerId, _addrs: Vec<PeerAddr>) -> P2PResult<()> {
            Ok(())
        }

        async fn disconnect(&self, _peer_id: &PeerId) -> P2PResult<()> {
            Ok(())
        }

        async fn listen(&self, _addr: PeerAddr) -> P2PResult<()> {
            Ok(())
        }

        async fn connected_peers(&self) -> P2PResult<Vec<PeerId>> {
            Ok(Vec::new())
        }

        async fn listen_addresses(&self) -> P2PResult<Vec<PeerAddr>> {
            Ok(Vec::new())
        }

        async fn poll_until_connected(
            &self,
            _peer_id: &PeerId,
            _timeout: Duration,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn peer_addresses(&self) -> P2PResult<Vec<String>> {
            Ok(Vec::new())
        }

        async fn subscribe(&self, _topic: DefraTopic) -> P2PResult<bool> {
            Ok(true)
        }

        async fn unsubscribe(&self, _topic: DefraTopic) -> P2PResult<bool> {
            Ok(true)
        }

        async fn publish(
            &self,
            _topic: DefraTopic,
            _msg: PushLogBroadcast,
        ) -> P2PResult<MessageId> {
            Ok(MessageId::new("noop".to_string()))
        }

        async fn topic_peers(&self, _topic: DefraTopic) -> P2PResult<Vec<PeerId>> {
            Ok(Vec::new())
        }

        async fn send_pushlog_response(
            &self,
            _token: Self::ResponseToken,
            _reply: PushLogReply,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_two_stream_request(
            &self,
            _peer_id: &PeerId,
            _req: PushLogRequest,
        ) -> P2PResult<PushLogReply> {
            Ok(PushLogReply::success("noop"))
        }

        async fn send_two_stream_response(
            &self,
            _peer_id: &PeerId,
            _reply: PushLogReply,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_doc_sync_request(
            &self,
            _peer_id: &PeerId,
            _req: DocSyncRequest,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_doc_sync_response(
            &self,
            _peer_id: &PeerId,
            _reply: DocSyncReply,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_branchable_sync_request(
            &self,
            _peer_id: &PeerId,
            _req: BranchableSyncRequest,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_branchable_sync_response(
            &self,
            _peer_id: &PeerId,
            _reply: BranchableSyncReply,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_car_request(&self, _peer_id: &PeerId, _root_cid: Cid) -> P2PResult<()> {
            Ok(())
        }

        async fn send_car_response(&self, _peer_id: &PeerId, _car_data: Vec<u8>) -> P2PResult<()> {
            Ok(())
        }

        async fn send_car_response_token(
            &self,
            _token: Self::ResponseToken,
            _car_data: Vec<u8>,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_doc_sync_response_token(
            &self,
            _token: Self::ResponseToken,
            _reply: DocSyncReply,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_branchable_sync_response_token(
            &self,
            _token: Self::ResponseToken,
            _reply: BranchableSyncReply,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_se_artifacts(
            &self,
            _peer_id: &PeerId,
            _req: PushSEArtifactsRequest,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn sync_blocks(
            &self,
            _root: Cid,
            _providers: Vec<PeerId>,
            _missing: Vec<Cid>,
        ) -> P2PResult<QueryId> {
            Ok(QueryId(999))
        }

        async fn cancel_sync(&self, _query_id: QueryId) -> P2PResult<bool> {
            Ok(true)
        }

        async fn create_replicator(
            &self,
            _peer_id: &PeerId,
            _collections: Vec<String>,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn delete_replicator(&self, _peer_id: &PeerId) -> P2PResult<()> {
            Ok(())
        }

        async fn list_replicators(&self) -> P2PResult<Vec<ReplicatorInfo>> {
            Ok(Vec::new())
        }

        async fn get_replicator(&self, _peer_id: &PeerId) -> P2PResult<Option<ReplicatorInfo>> {
            Ok(None)
        }

        async fn remove_replicator_collections(
            &self,
            _peer_id: &PeerId,
            _collections: Vec<String>,
        ) -> P2PResult<bool> {
            Ok(false)
        }

        async fn shutdown(&self) -> P2PResult<()> {
            Ok(())
        }
    }

    fn create_lww_block(field_name: &str) -> (Cid, Vec<u8>) {
        let block = Block::new(
            CrdtDelta::Lww(LwwDeltaPayload {
                field_name: field_name.to_string(),
                priority: 1,
                schema_version_id: "schema1".to_string(),
                data: b"value".to_vec(),
            }),
            vec![],
            vec![],
        );
        let bytes = block.to_dag_cbor().expect("encode lww block");
        let cid = block.generate_cid().expect("generate lww cid");
        (cid, bytes)
    }

    fn create_composite_block(_doc_id: &str, field_name: &str, field_cid: Cid) -> (Cid, Vec<u8>) {
        let block = Block::new(
            CrdtDelta::Composite(CompositeDeltaPayload {
                schema_version_id: "schema1".to_string(),
                priority: 1,
                status: 1,
            }),
            vec![],
            vec![DAGLink::new(field_name, field_cid)],
        );
        let bytes = block.to_dag_cbor().expect("encode composite block");
        let cid = block.generate_cid().expect("generate composite cid");
        (cid, bytes)
    }

    fn make_broadcast(
        doc_id: &str,
        cid: Cid,
        block: Vec<u8>,
        collection_id: &str,
    ) -> PushLogBroadcast {
        PushLogBroadcast::new(
            doc_id.to_string(),
            Bytes::from(cid.to_bytes()),
            collection_id.to_string(),
            "creator1".to_string(),
            Bytes::from(block),
        )
    }

    /// #1116 stage 2: a peer connect must not dispatch `DagNeedsFetch`
    /// directly anymore — it only expedites the root's retry clock, and the
    /// clock (a single dispatch site) performs the actual fetch. This
    /// prevents simultaneous connects from re-driving the same root more
    /// than once per tick.
    #[tokio::test(start_paused = true)]
    async fn peer_connect_expedites_and_clock_dispatches_once() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let transport = TestTransport::new();
        let (coordinator, mut events) =
            SyncCoordinator::new(transport, blockstore, SyncConfig::default())
                .await
                .expect("coordinator");

        let (field_cid, _field_block) = create_lww_block("name");
        let (root_cid, root_block) = create_composite_block("doc123", "name", field_cid);

        coordinator
            .manager()
            .process_pushlog(
                &make_broadcast("doc123", root_cid, root_block, "collection1"),
                Some("peer-1"),
                false,
                None,
            )
            .await
            .expect("root pushlog");

        // Registration claims its own immediate dispatch (#1116 stage 2);
        // drain that event and use its dispatched count as the baseline.
        match events.try_recv().expect("initial DagNeedsFetch event") {
            SyncEvent::DagNeedsFetch {
                root_cid: event_root,
                ..
            } => assert_eq!(event_root, root_cid),
            other => panic!("expected DagNeedsFetch, got {other:?}"),
        }
        let dispatched_after_registration = coordinator
            .manager()
            .diagnostics()
            .snapshot()
            .pending_dag_retry_dispatched;

        // Exhausted providers so this root qualifies for redrive on any
        // peer connect, not just its original source peer.
        coordinator
            .manager()
            .record_pending_dag_fetch_failure(&root_cid, "synthetic exhaustion");

        // Less than the first backoff rung (4s after one claim): not due yet.
        tokio::time::advance(std::time::Duration::from_secs(2)).await;

        // Act: a peer connects. Connect must only expedite, never dispatch.
        coordinator
            .handle_transport_event(TransportEvent::PeerConnected(PeerId::new(
                "peer-2".to_string(),
            )))
            .await
            .expect("peer connected handled");
        assert!(
            events.try_recv().is_err(),
            "connect must expedite only, not dispatch directly"
        );

        // One clock tick: the expedited root is now due.
        let due = coordinator
            .manager()
            .claim_due_pending_dag_retries(tokio::time::Instant::now());
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].0, root_cid);
        for (due_root, dag) in &due {
            coordinator.dispatch_pending_dag_fetch(*due_root, dag, None);
        }
        match events.try_recv().expect("redrive DagNeedsFetch event") {
            SyncEvent::DagNeedsFetch {
                root_cid: event_root,
                ..
            } => assert_eq!(event_root, root_cid),
            other => panic!("expected DagNeedsFetch, got {other:?}"),
        }
        assert_eq!(
            coordinator
                .manager()
                .diagnostics()
                .snapshot()
                .pending_dag_retry_dispatched,
            dispatched_after_registration + 1,
            "clock tick should dispatch exactly once"
        );

        // A second tick without advancing time re-claims nothing: the claim
        // above already re-armed the root to the next backoff rung.
        let due_again = coordinator
            .manager()
            .claim_due_pending_dag_retries(tokio::time::Instant::now());
        assert!(due_again.is_empty());
        assert!(
            events.try_recv().is_err(),
            "second tick must dispatch nothing"
        );
        assert_eq!(
            coordinator
                .manager()
                .diagnostics()
                .snapshot()
                .pending_dag_retry_dispatched,
            dispatched_after_registration + 1,
            "second tick must not add another dispatch"
        );
    }

    /// #1116 stage 2: `SyncStatus` must surface the retry-clock counters and
    /// the earliest due retry time so operators can see the clock's state
    /// without instrumenting the manager directly.
    #[tokio::test(start_paused = true)]
    async fn sync_status_surfaces_pending_dag_retry_clock() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let transport = TestTransport::new();
        let (coordinator, mut events) =
            SyncCoordinator::new(transport, Arc::clone(&blockstore), SyncConfig::default())
                .await
                .expect("coordinator");

        // No pending DAGs registered yet: the clock has nothing due.
        assert_eq!(coordinator.sync_status().next_pending_retry_in_ms, None);

        let (field_cid, field_block) = create_lww_block("name");
        let (root_cid, root_block) = create_composite_block("doc123", "name", field_cid);

        coordinator
            .manager()
            .process_pushlog(
                &make_broadcast("doc123", root_cid, root_block, "collection1"),
                Some("peer-1"),
                false,
                None,
            )
            .await
            .expect("root pushlog");

        // Registration claims its own immediate dispatch (#1116 stage 2).
        match events.try_recv().expect("initial DagNeedsFetch event") {
            SyncEvent::DagNeedsFetch {
                root_cid: event_root,
                ..
            } => assert_eq!(event_root, root_cid),
            other => panic!("expected DagNeedsFetch, got {other:?}"),
        }

        let status = coordinator.sync_status();
        assert_eq!(status.pending_dag_retry_dispatched, 1);
        let next_retry_ms = status
            .next_pending_retry_in_ms
            .expect("incomplete pending DAG must report a due time");
        assert!(
            next_retry_ms <= 60_000,
            "next retry must respect the backoff cap, got {next_retry_ms}"
        );

        // A same-instant claim attempt is suppressed.
        assert!(!coordinator
            .manager()
            .try_claim_pending_dag_dispatch(&root_cid, tokio::time::Instant::now()));
        assert_eq!(coordinator.sync_status().pending_dag_retry_suppressed, 1);

        // Feed the missing field block: the DAG completes and drains from
        // the pending map, so the clock has nothing left to report.
        blockstore
            .put(&field_cid, &field_block)
            .await
            .expect("store field block");
        let completed = coordinator
            .manager()
            .retry_pending_dags_waiting_on(&field_cid)
            .await
            .expect("retry on field arrival");
        assert_eq!(completed, vec![root_cid]);
        match events.try_recv().expect("DagReady event") {
            SyncEvent::DagReady {
                root_cid: event_root,
                ..
            } => assert_eq!(event_root, root_cid),
            other => panic!("expected DagReady, got {other:?}"),
        }

        assert_eq!(coordinator.sync_status().next_pending_retry_in_ms, None);
    }
}
