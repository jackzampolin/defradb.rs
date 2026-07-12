//! Broadcasting local updates to the network.
//!
//! Replicator pushes are admitted into the bounded push backlog as compact
//! job specs; the fixed worker pool (`push_worker`) expands DAGs, signs, and
//! sends. Admission happens before any task is spawned or payload captured,
//! so outbound resident state stays bounded under sustained writes (#1099).

use std::sync::Arc;
use std::time::Duration;

use blockstore::Blockstore;
use bytes::Bytes;
use cid::Cid;
use serde_json::Value as JsonValue;

use super::push_worker::{report_observed_head, report_push_failure};
use super::SyncCoordinator;
use crate::error::Result;
use crate::message::{PushSEArtifactsRequest, SEArtifact};
use crate::sync::broadcaster::Broadcaster;
use crate::sync::push_backlog::{EnqueueOutcome, PushJobSpec};
use crate::sync::push_fanout_coalescer::PendingPush;
use crate::sync::BroadcastResult;
use crate::transport::{P2PTransport, PeerId};

pub(super) const MAX_RATE_LIMITED_PUSH_ATTEMPTS: usize = 10;

pub(super) fn rate_limited_push_delay(attempt: usize) -> Duration {
    #[cfg(test)]
    {
        match attempt {
            1 => Duration::from_millis(1),
            2 => Duration::from_millis(2),
            3 => Duration::from_millis(4),
            4 => Duration::from_millis(8),
            _ => Duration::from_millis(10),
        }
    }

    #[cfg(not(test))]
    {
        match attempt {
            1 => Duration::from_millis(25),
            2 => Duration::from_millis(50),
            3 => Duration::from_millis(100),
            4 => Duration::from_millis(200),
            5 => Duration::from_millis(400),
            _ => Duration::from_millis(500),
        }
    }
}

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    async fn list_replicators_for_push(&self) -> Option<Vec<crate::replicator::ReplicatorInfo>> {
        if self.runtime.shutdown.is_shutting_down() {
            tracing::debug!("Skipping replicator push because coordinator is shutting down");
            return None;
        }

        match self.runtime.transport.list_replicators().await {
            Ok(replicators) => Some(replicators),
            Err(e) => {
                if e.is_connection_like() {
                    tracing::debug!(
                        error = %e,
                        "Skipping replicator push because the transport is unavailable"
                    );
                } else {
                    tracing::warn!(error = %e, "Failed to get replicators for push");
                }
                None
            }
        }
    }

    /// Admit one replicator push into the bounded backlog. Overflow is an
    /// explicit outcome: it is counted, logged, and handed to the persisted
    /// retry ladder — never a silent drop and never another waiting task.
    async fn enqueue_replicator_push(&self, job: PushJobSpec) {
        let peer_id = job.peer_id.clone();
        let doc_id = job.doc_id.clone();
        let collection_id = job.collection_id.clone();
        let root_cid = job.root_cid;
        let head_priority = job.head_priority();
        let outcome = self.runtime.push_backlog.try_enqueue(job.clone());
        match outcome {
            EnqueueOutcome::Enqueued => {
                report_observed_head(&self.runtime.failure_tx, &job).await;
            }
            EnqueueOutcome::Coalesced | EnqueueOutcome::RetiredStale => {}
            EnqueueOutcome::RejectedItems | EnqueueOutcome::RejectedBytes => {
                tracing::warn!(
                    peer_id = %peer_id,
                    doc_id = %doc_id,
                    collection_id = %collection_id,
                    outcome = ?outcome,
                    "Outbound push backlog full; deferring push to persisted retry"
                );
                report_push_failure(
                    &self.runtime.failure_tx,
                    &peer_id,
                    doc_id,
                    collection_id,
                    Some(root_cid),
                    head_priority,
                )
                .await;
            }
            EnqueueOutcome::Closed => {
                tracing::debug!(
                    peer_id = %peer_id,
                    doc_id = %doc_id,
                    "Skipping replicator push because the backlog is closed"
                );
            }
        }
    }

    fn replicator_in_collection(
        rep: &crate::replicator::ReplicatorInfo,
        collection_id: &str,
    ) -> bool {
        rep.collections.is_empty() || rep.collections.iter().any(|id| id == collection_id)
    }

    fn peer_id_for_replicator(rep: &crate::replicator::ReplicatorInfo) -> Option<PeerId> {
        let peer_id_str = rep.peer_id_str();
        if peer_id_str.is_empty() {
            None
        } else {
            Some(PeerId::new(peer_id_str.to_string()))
        }
    }

    /// Broadcast a local update to the network.
    pub async fn broadcast_local_update(
        &self,
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
    ) -> Result<BroadcastResult> {
        self.broadcast_local_update_with_creator(cid, block, doc_id, collection_id, None)
            .await
    }

    /// Broadcast a local update with an optional creator override.
    ///
    /// When `creator_override` is Some, the PushLog Creator field uses the
    /// given DID instead of this node's PeerId. This enables ACP owner
    /// registration on the receiving node during merge.
    pub async fn broadcast_local_update_with_creator(
        &self,
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
        creator_override: Option<&str>,
    ) -> Result<BroadcastResult> {
        let creator = creator_override.unwrap_or(&self.access.local_peer_id);
        let broadcast =
            Broadcaster::<T>::create_broadcast(cid, block, doc_id, collection_id, creator);
        if doc_id.is_empty() {
            return self.runtime.broadcaster.broadcast_update(&broadcast).await;
        }
        let broadcaster = self.runtime.broadcaster.clone();
        self.runtime
            .broadcast_coalescer
            .run(broadcast, move |latest| async move {
                broadcaster
                    .broadcast_update(&latest)
                    .await
                    .map_err(|error| error.to_string())
            })
            .await
            .map_err(crate::error::Error::GossipSubPublish)
    }

    /// Push a full document DAG to replicator peers.
    pub async fn push_dag_to_replicators(
        &self,
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
    ) {
        self.push_dag_to_replicators_with_creator(cid, block, doc_id, collection_id, None)
            .await
    }

    /// Push a full document DAG to replicators with optional creator override.
    pub async fn push_dag_to_replicators_with_creator(
        &self,
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
        creator_override: Option<&str>,
    ) {
        let creator = creator_override.unwrap_or(&self.access.local_peer_id);
        self.coalesce_replicator_push(PendingPush {
            cid: *cid,
            block: Bytes::copy_from_slice(block),
            doc_id: doc_id.to_string(),
            collection_id: collection_id.to_string(),
            creator: creator.to_string(),
            expand_unfiltered_dag: true,
            document: None,
        })
        .await;
    }

    /// Push a single block to replicator peers (no DAG expansion).
    pub async fn push_to_replicators(
        &self,
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
    ) {
        self.push_to_replicators_with_creator(cid, block, doc_id, collection_id, None)
            .await
    }

    /// Push a single block to replicators with optional creator override.
    pub async fn push_to_replicators_with_creator(
        &self,
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
        creator_override: Option<&str>,
    ) {
        let creator = creator_override.unwrap_or(&self.access.local_peer_id);
        self.coalesce_replicator_push(PendingPush {
            cid: *cid,
            block: Bytes::copy_from_slice(block),
            doc_id: doc_id.to_string(),
            collection_id: collection_id.to_string(),
            creator: creator.to_string(),
            expand_unfiltered_dag: false,
            document: None,
        })
        .await;
    }

    /// Push a committed document update to replicators using document JSON to
    /// evaluate filtered peers. Filtered peers receive the full document DAG so
    /// merge completeness does not depend on generic Bitswap access.
    pub async fn push_document_to_replicators_with_creator(
        &self,
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
        document: &JsonValue,
        creator_override: Option<&str>,
    ) {
        let creator = creator_override.unwrap_or(&self.access.local_peer_id);
        self.coalesce_replicator_push(PendingPush {
            cid: *cid,
            block: Bytes::copy_from_slice(block),
            doc_id: doc_id.to_string(),
            collection_id: collection_id.to_string(),
            creator: creator.to_string(),
            expand_unfiltered_dag: false,
            document: Some(document.clone()),
        })
        .await;
    }

    async fn coalesce_replicator_push(&self, push: PendingPush) {
        let coalescer = Arc::clone(&self.runtime.push_fanout_coalescer);
        coalescer
            .run(push, |latest| async move {
                self.dispatch_replicator_push(latest).await;
            })
            .await;
    }

    async fn dispatch_replicator_push(&self, push: PendingPush) {
        let Some(replicators) = self.list_replicators_for_push().await else {
            return;
        };
        if replicators.is_empty() {
            return;
        }
        tracing::debug!(
            cid = %push.cid,
            doc_id = %push.doc_id,
            collection_id = %push.collection_id,
            replicator_count = replicators.len(),
            "Queueing coalesced push to replicators"
        );
        let mut payload_guard = None;
        for rep in &replicators {
            if !Self::replicator_in_collection(rep, &push.collection_id) {
                continue;
            }
            let Some(peer_id) = Self::peer_id_for_replicator(rep) else {
                continue;
            };
            let expand_dag = if rep.is_filtered_for_collection(&push.collection_id) {
                let Some(document) = push.document.as_ref() else {
                    continue;
                };
                if !rep.matches_filter(
                    self.runtime.filter_matcher.as_ref(),
                    &push.collection_id,
                    document,
                ) {
                    continue;
                }
                true
            } else {
                push.expand_unfiltered_dag
            };
            let mut job = PushJobSpec::new(
                peer_id,
                push.doc_id.clone(),
                push.collection_id.clone(),
                push.creator.clone(),
                push.cid,
                push.block.clone(),
                expand_dag,
            );
            let payload = self.runtime.push_encode_cache.acquire(&job);
            payload_guard.get_or_insert_with(|| Arc::clone(&payload));
            job.encoded_payload = Some(payload);
            self.enqueue_replicator_push(job).await;
        }
    }

    /// Push searchable-encryption artifacts for a committed document to
    /// replicators of the collection. This mirrors Go's SE coordinator, which
    /// listens to committed update events independently of document access.
    pub async fn push_se_artifacts_to_replicators(
        &self,
        collection_id: &str,
        artifacts: Vec<SEArtifact>,
    ) {
        if artifacts.is_empty() {
            return;
        }

        let Some(replicators) = self.list_replicators_for_push().await else {
            return;
        };

        for rep in replicators {
            if !rep.collections.iter().any(|id| id == collection_id) {
                continue;
            }
            if rep.is_filtered_for_collection(collection_id) {
                continue;
            }

            let peer_id = PeerId::new(rep.id.clone());
            let request = PushSEArtifactsRequest::new(collection_id.to_string(), artifacts.clone());
            if let Err(error) = self
                .runtime
                .transport
                .send_se_artifacts(&peer_id, request)
                .await
            {
                tracing::warn!(
                    peer_id = %peer_id,
                    collection_id,
                    error = %error,
                    "Failed to push SE artifacts to replicator"
                );
                // Record a retry entry per (peer, doc) so the replicator retry
                // pass regenerates and re-pushes the SE artifacts once the peer
                // reconnects. Mirrors Go's independent `seRetryInfo`; the doc
                // block push failure is racy and may not fire when the SE push
                // does, so SE pushes must record their own retries.
                for doc_id in artifacts
                    .iter()
                    .map(|artifact| artifact.doc_id.clone())
                    .collect::<std::collections::HashSet<_>>()
                {
                    report_push_failure(
                        &self.runtime.failure_tx,
                        &peer_id,
                        doc_id,
                        collection_id.to_string(),
                        None,
                        0,
                    )
                    .await;
                }
            }
        }
    }

    /// Push SE artifacts with document-filter evaluation for filtered peers.
    pub async fn push_se_artifacts_to_replicators_for_document(
        &self,
        collection_id: &str,
        artifacts: Vec<SEArtifact>,
        document: &JsonValue,
    ) {
        if artifacts.is_empty() {
            return;
        }

        let Some(replicators) = self.list_replicators_for_push().await else {
            return;
        };

        for rep in replicators {
            if !rep.collections.iter().any(|id| id == collection_id) {
                continue;
            }
            if !rep.matches_filter(
                self.runtime.filter_matcher.as_ref(),
                collection_id,
                document,
            ) {
                continue;
            }

            let Some(peer_id) = Self::peer_id_for_replicator(&rep) else {
                continue;
            };
            let request = PushSEArtifactsRequest::new(collection_id.to_string(), artifacts.clone());
            if let Err(error) = self
                .runtime
                .transport
                .send_se_artifacts(&peer_id, request)
                .await
            {
                tracing::warn!(
                    peer_id = %peer_id,
                    collection_id,
                    error = %error,
                    "Failed to push SE artifacts to replicator"
                );
                for doc_id in artifacts
                    .iter()
                    .map(|artifact| artifact.doc_id.clone())
                    .collect::<std::collections::HashSet<_>>()
                {
                    report_push_failure(
                        &self.runtime.failure_tx,
                        &peer_id,
                        doc_id,
                        collection_id.to_string(),
                        None,
                        0,
                    )
                    .await;
                }
            }
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use multihash_codetable::{Code, MultihashDigest};

    use crate::error::{Result as P2PResult, RATE_LIMITED_MESSAGE};
    use crate::message::{
        BranchableSyncReply, BranchableSyncRequest, DocSyncReply, DocSyncRequest, PushLogBroadcast,
        PushLogReply, PushLogRequest, PushSEArtifactsRequest,
    };
    use crate::topics::DefraTopic;
    use crate::transport::{MessageId, P2PTransport, PeerAddr, PeerId};
    use crate::{QueryId, ReplicatorInfo};

    use super::super::push_worker::send_ordered_pushlogs_via_transport;
    use super::*;

    type SentLog = Vec<(String, Vec<u8>)>;

    #[derive(Clone)]
    pub(in crate::sync::coordinator) struct TestTransport {
        peer_id: PeerId,
        pubkey: Vec<u8>,
        replies: Arc<Mutex<VecDeque<PushLogReply>>>,
        sent: Arc<Mutex<SentLog>>,
        stalled_peers: Arc<Mutex<std::collections::HashSet<String>>>,
        send_delay: Duration,
        signs: Arc<AtomicUsize>,
        sign_failures_remaining: Arc<AtomicUsize>,
    }

    impl TestTransport {
        pub(in crate::sync::coordinator) fn new(replies: Vec<PushLogReply>) -> Self {
            Self {
                peer_id: PeerId::new("local-peer".to_string()),
                pubkey: vec![1, 2, 3],
                replies: Arc::new(Mutex::new(VecDeque::from(replies))),
                sent: Arc::new(Mutex::new(Vec::new())),
                stalled_peers: Arc::new(Mutex::new(std::collections::HashSet::new())),
                send_delay: Duration::ZERO,
                signs: Arc::new(AtomicUsize::new(0)),
                sign_failures_remaining: Arc::new(AtomicUsize::new(0)),
            }
        }

        pub(in crate::sync::coordinator) fn with_send_delay(
            mut self,
            send_delay: Duration,
        ) -> Self {
            self.send_delay = send_delay;
            self
        }

        /// Sends to this peer never complete: a deterministic nonresponsive
        /// peer for worker fault-injection tests.
        pub(in crate::sync::coordinator) fn with_stalled_peer(self, peer: &str) -> Self {
            self.stalled_peers.lock().unwrap().insert(peer.to_string());
            self
        }

        pub(in crate::sync::coordinator) fn with_sign_failures(self, count: usize) -> Self {
            self.sign_failures_remaining.store(count, Ordering::Relaxed);
            self
        }

        fn sent_cids(&self) -> Vec<Vec<u8>> {
            self.sent
                .lock()
                .unwrap()
                .iter()
                .map(|(_, cid)| cid.clone())
                .collect()
        }

        pub(in crate::sync::coordinator) fn sent(&self) -> SentLog {
            self.sent.lock().unwrap().clone()
        }

        pub(in crate::sync::coordinator) fn sign_count(&self) -> usize {
            self.signs.load(Ordering::Relaxed)
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
            self.signs.fetch_add(1, Ordering::Relaxed);
            if self
                .sign_failures_remaining
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(crate::Error::SigningFailed("injected failure".to_string()));
            }
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
            peer_id: &PeerId,
            req: PushLogRequest,
        ) -> P2PResult<PushLogReply> {
            if self
                .stalled_peers
                .lock()
                .unwrap()
                .contains(&peer_id.to_string())
            {
                std::future::pending::<()>().await;
            }
            self.sent
                .lock()
                .unwrap()
                .push((peer_id.to_string(), req.cid.to_vec()));
            if !self.send_delay.is_zero() {
                tokio::time::sleep(self.send_delay).await;
            }
            Ok(self
                .replies
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| PushLogReply::success("ok")))
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
            Ok(QueryId(1))
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

    #[tokio::test]
    async fn ordered_pushlogs_retry_rate_limited_request_before_advancing() {
        let transport = TestTransport::new(vec![
            PushLogReply::error("first", RATE_LIMITED_MESSAGE),
            PushLogReply::success("first"),
            PushLogReply::success("second"),
        ]);
        let peer_id = PeerId::new("remote-peer".to_string());
        let cid1 = Cid::new_v1(0x55, Code::Sha2_256.digest(b"cid-1"));
        let cid2 = Cid::new_v1(0x55, Code::Sha2_256.digest(b"cid-2"));
        let requests = vec![
            (
                cid1,
                PushLogRequest::new(
                    "doc-1".to_string(),
                    Bytes::from(cid1.to_bytes()),
                    "collection".to_string(),
                    "creator".to_string(),
                    Bytes::from_static(b"block-1"),
                ),
            ),
            (
                cid2,
                PushLogRequest::new(
                    "doc-1".to_string(),
                    Bytes::from(cid2.to_bytes()),
                    "collection".to_string(),
                    "creator".to_string(),
                    Bytes::from_static(b"block-2"),
                ),
            ),
        ];

        let any_failed = send_ordered_pushlogs_via_transport(
            &transport,
            &peer_id,
            requests,
            Duration::from_secs(1),
        )
        .await;

        assert!(!any_failed.failed);
        assert_eq!(
            transport.sent_cids(),
            vec![cid1.to_bytes(), cid1.to_bytes(), cid2.to_bytes()]
        );
    }

    #[tokio::test]
    async fn ordered_pushlogs_stop_immediately_on_capacity_nack_and_park_the_peer() {
        // defradb#1112: a saturated receiver is a PEER-WIDE, structural condition —
        // it cannot accept any new root until it drains. Answering it with the
        // rate-limit pacing ladder meant one logical push became 11 resends in
        // ~3.3s, each costing the receiver a block write plus a full DAG
        // traversal, all guaranteed to fail. The sender must stop at the first
        // capacity nack, report it, and let the persisted retry ledger
        // (exponential + jittered) own the replay.
        let capacity_nack = crate::error::Error::PendingDagCapacity { max: 1 }
            .backpressure_reply_message()
            .expect("capacity error maps to the capacity sentinel");
        let transport = TestTransport::new(vec![
            PushLogReply::error("first", capacity_nack),
            PushLogReply::success("first"),
            PushLogReply::success("second"),
        ]);
        let peer_id = PeerId::new("remote-peer".to_string());
        let cid1 = Cid::new_v1(0x55, Code::Sha2_256.digest(b"cid-1"));
        let cid2 = Cid::new_v1(0x55, Code::Sha2_256.digest(b"cid-2"));
        let requests = vec![
            (
                cid1,
                PushLogRequest::new(
                    "doc-1".to_string(),
                    Bytes::from(cid1.to_bytes()),
                    "collection".to_string(),
                    "creator".to_string(),
                    Bytes::from_static(b"block-1"),
                ),
            ),
            (
                cid2,
                PushLogRequest::new(
                    "doc-1".to_string(),
                    Bytes::from(cid2.to_bytes()),
                    "collection".to_string(),
                    "creator".to_string(),
                    Bytes::from_static(b"block-2"),
                ),
            ),
        ];

        let outcome = send_ordered_pushlogs_via_transport(
            &transport,
            &peer_id,
            requests,
            Duration::from_secs(1),
        )
        .await;

        assert!(outcome.failed, "a capacity nack is a failed push");
        assert!(
            outcome.at_capacity,
            "the caller must learn the receiver is saturated so it can park the peer"
        );
        assert_eq!(
            transport.sent_cids(),
            vec![cid1.to_bytes()],
            "the sender must NOT resend the rejected block, and must not push the \
             next CID at a saturated peer"
        );
    }

    #[tokio::test]
    async fn ordered_pushlogs_stop_after_bounded_rate_limit_retries() {
        let transport = TestTransport::new(
            std::iter::repeat_with(|| PushLogReply::error("first", RATE_LIMITED_MESSAGE))
                .take(MAX_RATE_LIMITED_PUSH_ATTEMPTS + 2)
                .collect(),
        );
        let peer_id = PeerId::new("remote-peer".to_string());
        let cid1 = Cid::new_v1(0x55, Code::Sha2_256.digest(b"cid-1"));
        let cid2 = Cid::new_v1(0x55, Code::Sha2_256.digest(b"cid-2"));
        let requests = vec![
            (
                cid1,
                PushLogRequest::new(
                    "doc-1".to_string(),
                    Bytes::from(cid1.to_bytes()),
                    "collection".to_string(),
                    "creator".to_string(),
                    Bytes::from_static(b"block-1"),
                ),
            ),
            (
                cid2,
                PushLogRequest::new(
                    "doc-1".to_string(),
                    Bytes::from(cid2.to_bytes()),
                    "collection".to_string(),
                    "creator".to_string(),
                    Bytes::from_static(b"block-2"),
                ),
            ),
        ];

        let any_failed = send_ordered_pushlogs_via_transport(
            &transport,
            &peer_id,
            requests,
            Duration::from_secs(1),
        )
        .await;

        assert!(any_failed.failed);
        assert_eq!(
            transport.sent_cids(),
            vec![cid1.to_bytes(); MAX_RATE_LIMITED_PUSH_ATTEMPTS + 1]
        );
    }

    #[tokio::test]
    async fn ordered_pushlogs_timeout_stops_peer_push() {
        let transport = TestTransport::new(vec![PushLogReply::success("first")])
            .with_send_delay(Duration::from_millis(25));
        let peer_id = PeerId::new("remote-peer".to_string());
        let cid1 = Cid::new_v1(0x55, Code::Sha2_256.digest(b"cid-1"));
        let cid2 = Cid::new_v1(0x55, Code::Sha2_256.digest(b"cid-2"));
        let requests = vec![
            (
                cid1,
                PushLogRequest::new(
                    "doc-1".to_string(),
                    Bytes::from(cid1.to_bytes()),
                    "collection".to_string(),
                    "creator".to_string(),
                    Bytes::from_static(b"block-1"),
                ),
            ),
            (
                cid2,
                PushLogRequest::new(
                    "doc-1".to_string(),
                    Bytes::from(cid2.to_bytes()),
                    "collection".to_string(),
                    "creator".to_string(),
                    Bytes::from_static(b"block-2"),
                ),
            ),
        ];

        let any_failed = send_ordered_pushlogs_via_transport(
            &transport,
            &peer_id,
            requests,
            Duration::from_millis(1),
        )
        .await;

        assert!(any_failed.failed);
        assert_eq!(transport.sent_cids(), vec![cid1.to_bytes()]);
    }
}
