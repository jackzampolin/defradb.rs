//! Broadcasting local updates to the network.

use std::collections::HashSet;
use std::time::Duration;

use acp::ReplicatedDocActorRelationships;
use blockstore::Blockstore;
use bytes::Bytes;
use cid::Cid;

use super::SyncCoordinator;
use crate::error::{is_rate_limited_message, Result};
use crate::message::PushLogRequest;
use crate::signing::sign_with_transport;
use crate::sync::broadcaster::Broadcaster;
use crate::sync::BroadcastResult;
use crate::transport::{P2PTransport, PeerId};

const MAX_RATE_LIMITED_PUSH_ATTEMPTS: usize = 10;

fn rate_limited_push_delay(attempt: usize) -> Duration {
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

    fn report_push_failure(
        failure_tx: &Option<tokio::sync::mpsc::Sender<super::PushFailure>>,
        peer_id: &PeerId,
        doc_id: String,
        collection_id: String,
    ) {
        if let Some(tx) = failure_tx {
            let _ = tx.try_send(super::PushFailure {
                peer_id: peer_id.to_string(),
                doc_id,
                collection_id,
            });
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
        self.broadcast_local_update_with_creator_and_relationships(
            cid,
            block,
            doc_id,
            collection_id,
            None,
            None,
        )
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
        self.broadcast_local_update_with_creator_and_relationships(
            cid,
            block,
            doc_id,
            collection_id,
            creator_override,
            None,
        )
        .await
    }

    /// Broadcast a local update with optional creator and ACP relationship snapshot.
    pub async fn broadcast_local_update_with_creator_and_relationships(
        &self,
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
        creator_override: Option<&str>,
        acp_actor_relationships: Option<ReplicatedDocActorRelationships>,
    ) -> Result<BroadcastResult> {
        let creator = creator_override.unwrap_or(&self.access.local_peer_id);
        let broadcast = Broadcaster::<T>::create_broadcast(
            cid,
            block,
            doc_id,
            collection_id,
            creator,
            acp_actor_relationships,
        );
        self.runtime.broadcaster.broadcast_update(&broadcast).await
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
        let Some(replicators) = self.list_replicators_for_push().await else {
            return;
        };

        if replicators.is_empty() {
            return;
        }

        let dag_blocks = self
            .load_dag_blocks(*cid, Bytes::copy_from_slice(block))
            .await;

        tracing::debug!(
            cid = %cid,
            doc_id = %doc_id,
            collection_id = %collection_id,
            replicator_count = replicators.len(),
            dag_block_count = dag_blocks.len(),
            "Pushing DAG to replicators"
        );

        for rep in &replicators {
            if !rep.collections.is_empty() && !rep.collections.contains(&collection_id.to_string())
            {
                continue;
            }

            let peer_id_str = rep.peer_id_str().to_string();
            if peer_id_str.is_empty() {
                continue;
            }
            let peer_id = PeerId::new(peer_id_str);

            let mut requests: Vec<(Cid, PushLogRequest)> = Vec::new();

            for (block_cid, block_data) in &dag_blocks {
                let mut req = PushLogRequest::new(
                    doc_id.to_string(),
                    Bytes::from(block_cid.to_bytes()),
                    collection_id.to_string(),
                    creator.to_string(),
                    block_data.clone(),
                );
                if sign_with_transport(&self.runtime.transport, &mut req).is_ok() {
                    requests.push((*block_cid, req));
                }
            }

            // Spawn a task per peer, bounded by push_semaphore to prevent
            // resource exhaustion during document creation bursts.
            let transport = self.runtime.transport.clone();
            let failure_tx = self.runtime.failure_tx.clone();
            let doc_id_owned = doc_id.to_string();
            let collection_id_owned = collection_id.to_string();
            let semaphore = self.runtime.push_semaphore.clone();
            let send_timeout = self.runtime.push_send_timeout;
            self.spawn_background_task("push_dag_to_replicators", async move {
                let Ok(_permit) = semaphore.acquire().await else {
                    return;
                };
                let any_failed = Self::send_ordered_pushlogs_via_transport(
                    &transport,
                    &peer_id,
                    requests,
                    send_timeout,
                )
                .await;
                if any_failed {
                    Self::report_push_failure(
                        &failure_tx,
                        &peer_id,
                        doc_id_owned,
                        collection_id_owned,
                    );
                }
            });
        }
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
        let Some(replicators) = self.list_replicators_for_push().await else {
            return;
        };

        for rep in &replicators {
            if !rep.collections.is_empty() && !rep.collections.contains(&collection_id.to_string())
            {
                continue;
            }

            let peer_id_str = rep.peer_id_str().to_string();
            if peer_id_str.is_empty() {
                continue;
            }
            let peer_id = PeerId::new(peer_id_str);

            let mut request = PushLogRequest::new(
                doc_id.to_string(),
                Bytes::from(cid.to_bytes()),
                collection_id.to_string(),
                creator.to_string(),
                Bytes::copy_from_slice(block),
            );

            if let Err(e) = sign_with_transport(&self.runtime.transport, &mut request) {
                tracing::debug!(error = %e, "Failed to sign PushLog request");
                continue;
            }

            let transport = self.runtime.transport.clone();
            let cid_clone = *cid;
            let failure_tx = self.runtime.failure_tx.clone();
            let doc_id_owned = doc_id.to_string();
            let collection_id_owned = collection_id.to_string();
            let semaphore = self.runtime.push_semaphore.clone();
            let peer_id_clone = peer_id.clone();
            let send_timeout = self.runtime.push_send_timeout;
            self.spawn_background_task("push_to_replicators", async move {
                let Ok(_permit) = semaphore.acquire().await else {
                    return;
                };
                let any_failed = Self::send_ordered_pushlogs_via_transport(
                    &transport,
                    &peer_id_clone,
                    vec![(cid_clone, request)],
                    send_timeout,
                )
                .await;
                if any_failed {
                    Self::report_push_failure(
                        &failure_tx,
                        &peer_id_clone,
                        doc_id_owned,
                        collection_id_owned,
                    );
                }
            });
        }
    }

    /// Load every transitive block in a document DAG, with dependencies first.
    async fn load_dag_blocks(&self, root_cid: Cid, root_bytes: Bytes) -> Vec<(Cid, Bytes)> {
        let mut ordered = Vec::new();
        let mut visited = HashSet::new();
        let mut stack = vec![(root_cid, root_bytes, false)];

        while let Some((cid, data, expanded)) = stack.pop() {
            if expanded {
                ordered.push((cid, data));
                continue;
            }

            if !visited.insert(cid) {
                continue;
            }

            let linked_cids = defra_core::Block::from_dag_cbor(&data)
                .ok()
                .and_then(|block| defra_core::collect_block_links(&block).ok())
                .unwrap_or_default();

            stack.push((cid, data, true));

            for linked_cid in linked_cids.into_iter().rev() {
                match self.blockstore().get(&linked_cid).await {
                    Ok(Some(linked_data)) => stack.push((linked_cid, linked_data, false)),
                    Ok(None) => {
                        tracing::debug!(
                            root_cid = %root_cid,
                            linked_cid = %linked_cid,
                            "Linked DAG block not found in blockstore"
                        );
                    }
                    Err(error) => {
                        tracing::debug!(
                            root_cid = %root_cid,
                            linked_cid = %linked_cid,
                            error = %error,
                            "Failed to load linked DAG block"
                        );
                    }
                }
            }
        }

        ordered
    }

    /// Send PushLog requests to a peer in order via the transport, waiting for each to complete.
    async fn send_ordered_pushlogs_via_transport(
        transport: &T,
        peer_id: &PeerId,
        requests: Vec<(Cid, PushLogRequest)>,
        send_timeout: Duration,
    ) -> bool {
        let mut any_failed = false;
        'requests: for (cid, request) in requests {
            let mut rate_limited_attempts = 0;
            loop {
                match tokio::time::timeout(
                    send_timeout,
                    transport.send_two_stream_request(peer_id, request.clone()),
                )
                .await
                {
                    Err(_) => {
                        tracing::warn!(
                            peer_id = %peer_id,
                            cid = %cid,
                            timeout_ms = send_timeout.as_millis(),
                            "PushLog to replicator timed out"
                        );
                        any_failed = true;
                        break 'requests;
                    }
                    Ok(Err(e)) => {
                        if e.is_connection_like() {
                            tracing::debug!(
                                peer_id = %peer_id,
                                cid = %cid,
                                error = %e,
                                "PushLog to replicator failed because the connection became unavailable; stopping replay for this peer"
                            );
                            any_failed = true;
                            break 'requests;
                        }

                        tracing::debug!(
                            peer_id = %peer_id,
                            cid = %cid,
                            error = %e,
                            "PushLog to replicator failed"
                        );
                        any_failed = true;
                        break;
                    }
                    Ok(Ok(reply)) => {
                        let Some(error_message) = reply.err_message.as_deref() else {
                            break;
                        };

                        if is_rate_limited_message(error_message) {
                            rate_limited_attempts += 1;
                            if rate_limited_attempts > MAX_RATE_LIMITED_PUSH_ATTEMPTS {
                                tracing::warn!(
                                    peer_id = %peer_id,
                                    cid = %cid,
                                    attempts = rate_limited_attempts,
                                    "PushLog to replicator remained rate-limited; stopping ordered push"
                                );
                                any_failed = true;
                                break 'requests;
                            }

                            let delay = rate_limited_push_delay(rate_limited_attempts);
                            tracing::debug!(
                                peer_id = %peer_id,
                                cid = %cid,
                                attempt = rate_limited_attempts,
                                delay_ms = delay.as_millis(),
                                "PushLog to replicator was rate-limited; backing off before retry"
                            );
                            tokio::time::sleep(delay).await;
                            continue;
                        }

                        tracing::warn!(
                            peer_id = %peer_id,
                            cid = %cid,
                            error = %error_message,
                            "PushLog to replicator was rejected"
                        );
                        any_failed = true;
                        break;
                    }
                }
            }
        }
        any_failed
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use cid::multihash::{Code, MultihashDigest};

    use crate::error::{Result as P2PResult, RATE_LIMITED_MESSAGE};
    use crate::message::{
        BranchableSyncReply, BranchableSyncRequest, DocSyncReply, DocSyncRequest, PushLogBroadcast,
        PushLogReply, PushSEArtifactsRequest,
    };
    use crate::topics::DefraTopic;
    use crate::transport::{MessageId, P2PTransport, PeerAddr, PeerId};
    use crate::{QueryId, ReplicatorInfo};

    use super::*;

    #[derive(Clone)]
    struct TestTransport {
        peer_id: PeerId,
        pubkey: Vec<u8>,
        replies: Arc<Mutex<VecDeque<PushLogReply>>>,
        sent_cids: Arc<Mutex<Vec<Vec<u8>>>>,
        send_delay: Duration,
    }

    impl TestTransport {
        fn new(replies: Vec<PushLogReply>) -> Self {
            Self {
                peer_id: PeerId::new("local-peer".to_string()),
                pubkey: vec![1, 2, 3],
                replies: Arc::new(Mutex::new(VecDeque::from(replies))),
                sent_cids: Arc::new(Mutex::new(Vec::new())),
                send_delay: Duration::ZERO,
            }
        }

        fn with_send_delay(mut self, send_delay: Duration) -> Self {
            self.send_delay = send_delay;
            self
        }

        fn sent_cids(&self) -> Vec<Vec<u8>> {
            self.sent_cids.lock().unwrap().clone()
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
            req: PushLogRequest,
        ) -> P2PResult<PushLogReply> {
            self.sent_cids.lock().unwrap().push(req.cid.to_vec());
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

        let any_failed = SyncCoordinator::<
            blockstore::DefraBlockstore<storage::backends::MemoryStore>,
            TestTransport,
        >::send_ordered_pushlogs_via_transport(
            &transport,
            &peer_id,
            requests,
            Duration::from_secs(1),
        )
        .await;

        assert!(!any_failed);
        assert_eq!(
            transport.sent_cids(),
            vec![cid1.to_bytes(), cid1.to_bytes(), cid2.to_bytes()]
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

        let any_failed = SyncCoordinator::<
            blockstore::DefraBlockstore<storage::backends::MemoryStore>,
            TestTransport,
        >::send_ordered_pushlogs_via_transport(
            &transport,
            &peer_id,
            requests,
            Duration::from_secs(1),
        )
        .await;

        assert!(any_failed);
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

        let any_failed = SyncCoordinator::<
            blockstore::DefraBlockstore<storage::backends::MemoryStore>,
            TestTransport,
        >::send_ordered_pushlogs_via_transport(
            &transport,
            &peer_id,
            requests,
            Duration::from_millis(1),
        )
        .await;

        assert!(any_failed);
        assert_eq!(transport.sent_cids(), vec![cid1.to_bytes()]);
    }
}
