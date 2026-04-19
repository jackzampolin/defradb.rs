//! Poll-based DAG fetcher for DocSync and BranchableSync.
//!
//! Tries CAR fetch first (single round-trip for entire DAG), then falls back
//! to batched selective block fetch + blockstore polling for any remaining blocks.

use std::sync::Arc;
use std::time::Duration;

use blockstore::Blockstore;
use cid::Cid;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::sync::manager::links::find_all_missing_links;
use crate::sync::manager::SyncEvent;
use crate::transport::{P2PTransport, PeerId};

const SELECTIVE_FETCH_BATCH_SIZE: usize = 2048;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FetchBatchOutcome {
    Complete,
    Partial,
    NoProgress,
}

/// Fetch an entire DAG rooted at `root_cid`.
///
/// Strategy: try CAR fetch first (one round-trip), then selective block fetch
/// for any missing blocks.
#[allow(clippy::too_many_arguments)]
pub async fn poll_fetch_dag<B: Blockstore + 'static, T: P2PTransport>(
    transport: T,
    blockstore: Arc<B>,
    event_tx: mpsc::Sender<SyncEvent>,
    root_cid: Cid,
    doc_id: String,
    collection_id: String,
    schema_version_id: String,
    source_peer: PeerId,
) {
    debug!(
        root_cid = %root_cid,
        doc_id = %doc_id,
        source_peer = %source_peer,
        "Starting DAG fetch (CAR-first, selective block fallback)"
    );

    let car_missing_watch = match blockstore.get(&root_cid).await {
        Ok(Some(root_data)) => find_all_missing_links(blockstore.as_ref(), &root_data)
            .await
            .ok()
            .filter(|missing| !missing.is_empty()),
        _ => None,
    };

    // Try CAR fetch first
    if try_car_fetch(
        &transport,
        &blockstore,
        &root_cid,
        &source_peer,
        car_missing_watch.as_deref(),
    )
    .await
    {
        if let Ok(Some(root_data)) = blockstore.get(&root_cid).await {
            let missing = find_all_missing_links(blockstore.as_ref(), &root_data)
                .await
                .unwrap_or_default();
            if missing.is_empty() {
                info!(root_cid = %root_cid, doc_id = %doc_id, "DAG fetch complete via CAR");
                let _ = event_tx
                    .send(SyncEvent::DagReady {
                        root_cid,
                        doc_id,
                        collection_id,
                        creator: schema_version_id,
                        sender_peer: Some(source_peer.to_string()),
                        is_explicit_replicator: false,
                        explicit_replay_authorization: None,
                        acp_actor_relationships: None,
                    })
                    .await;
                return;
            }
            debug!(
                root_cid = %root_cid,
                missing_count = missing.len(),
                "CAR fetch was partial, falling through to selective block fetch"
            );
        }
    }

    // Fallback fetch: fetch the root block first so we can enumerate missing links.
    if !matches!(
        poll_fetch_blocks(
        &root_cid,
        std::slice::from_ref(&root_cid),
        &transport,
        &blockstore,
        &source_peer,
    )
    .await,
        FetchBatchOutcome::Complete
    ) {
        warn!(root_cid = %root_cid, "Failed to fetch root block");
        return;
    }

    // Walk DAG, fetching missing blocks level by level
    for iteration in 0..20 {
        let root_data = match blockstore.get(&root_cid).await {
            Ok(Some(data)) => data,
            _ => {
                warn!(root_cid = %root_cid, "Root block disappeared from blockstore");
                return;
            }
        };

        let missing = match find_all_missing_links(blockstore.as_ref(), &root_data).await {
            Ok(m) => m,
            Err(e) => {
                warn!(root_cid = %root_cid, error = %e, "find_all_missing_links failed");
                return;
            }
        };

        if missing.is_empty() {
            break;
        }

        debug!(
            root_cid = %root_cid,
            iteration = iteration,
            missing_count = missing.len(),
            "Fetching missing DAG blocks via selective block fetch"
        );

        let mut made_progress = false;
        for batch in missing.chunks(SELECTIVE_FETCH_BATCH_SIZE) {
            match poll_fetch_blocks(&root_cid, batch, &transport, &blockstore, &source_peer).await
            {
                FetchBatchOutcome::Complete => {
                    made_progress = true;
                }
                FetchBatchOutcome::Partial => {
                    made_progress = true;
                    debug!(
                        root_cid = %root_cid,
                        requested_count = batch.len(),
                        "Selective block batch made partial progress; continuing DAG walk"
                    );
                }
                FetchBatchOutcome::NoProgress => {
                    warn!(
                        root_cid = %root_cid,
                        requested_count = batch.len(),
                        "Timeout fetching selective block batch (30s)"
                    );
                }
            }
        }
        if !made_progress {
            break;
        }
    }

    // Verify DAG is complete
    let root_data = match blockstore.get(&root_cid).await {
        Ok(Some(data)) => data,
        _ => return,
    };
    let remaining = find_all_missing_links(blockstore.as_ref(), &root_data)
        .await
        .unwrap_or_default();

    if remaining.is_empty() {
        info!(root_cid = %root_cid, doc_id = %doc_id, "DAG fetch complete");
        let _ = event_tx
            .send(SyncEvent::DagReady {
                root_cid,
                doc_id,
                collection_id,
                creator: schema_version_id,
                sender_peer: Some(source_peer.to_string()),
                is_explicit_replicator: false,
                explicit_replay_authorization: None,
                acp_actor_relationships: None,
            })
            .await;
    } else {
        warn!(
            root_cid = %root_cid,
            doc_id = %doc_id,
            remaining_count = remaining.len(),
            "DAG fetch incomplete"
        );
    }
}

/// Try to fetch an entire DAG via a single CAR request.
async fn try_car_fetch<B: Blockstore, T: P2PTransport>(
    transport: &T,
    blockstore: &Arc<B>,
    root_cid: &Cid,
    source_peer: &PeerId,
    watch_missing: Option<&[Cid]>,
) -> bool {
    if let Err(e) = transport.send_car_request(source_peer, *root_cid).await {
        debug!(root_cid = %root_cid, error = %e, "CAR request failed, will use selective block fetch");
        return false;
    }

    let timeout = Duration::from_secs(10);
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if let Some(missing) = watch_missing {
            let mut remaining = 0usize;
            for cid in missing {
                if !matches!(blockstore.has(cid).await, Ok(true)) {
                    remaining += 1;
                }
            }
            if remaining < missing.len() {
                return true;
            }
        } else if let Ok(true) = blockstore.has(root_cid).await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    debug!(root_cid = %root_cid, "CAR fetch timed out (10s), falling back to selective block fetch");
    false
}

/// Fetch one batch of exact blocks via the transport's block-sync path.
async fn poll_fetch_blocks<B: Blockstore, T: P2PTransport>(
    root_cid: &Cid,
    cids: &[Cid],
    transport: &T,
    blockstore: &Arc<B>,
    source_peer: &PeerId,
) -> FetchBatchOutcome {
    let mut missing = Vec::new();
    for cid in cids {
        if matches!(blockstore.has(cid).await, Ok(true)) {
            continue;
        }
        missing.push(*cid);
    }

    if missing.is_empty() {
        return FetchBatchOutcome::Complete;
    }

    if let Err(e) = transport
        .sync_blocks(*root_cid, vec![source_peer.clone()], missing.clone())
        .await
    {
        warn!(
            root_cid = %root_cid,
            requested_count = missing.len(),
            error = %e,
            "selective block fetch failed"
        );
        return FetchBatchOutcome::NoProgress;
    }

    let timeout = Duration::from_secs(30);
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        let mut remaining = 0usize;
        for cid in &missing {
            if !matches!(blockstore.has(cid).await, Ok(true)) {
                remaining += 1;
            }
        }
        if remaining == 0 {
            return FetchBatchOutcome::Complete;
        }
        if remaining < missing.len() {
            return FetchBatchOutcome::Partial;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    FetchBatchOutcome::NoProgress
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result as P2PResult;
    use crate::message::{
        BranchableSyncReply, BranchableSyncRequest, DocSyncReply, DocSyncRequest, PushLogBroadcast,
        PushLogReply, PushLogRequest, PushSEArtifactsRequest,
    };
    use crate::sync::manager::SyncEvent;
    use crate::topics::DefraTopic;
    use crate::transport::{MessageId, P2PTransport, PeerAddr, PeerId};
    use crate::{QueryId, ReplicatorInfo};
    use async_trait::async_trait;
    use blockstore::{Blockstore, DefraBlockstore};
    use libipld::multihash::{Code, MultihashDigest};
    use libipld::{cbor::DagCborCodec, codec::Codec, ipld};
    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use storage::backends::MemoryStore;
    use tokio::sync::mpsc;

    fn make_cid(data: &[u8]) -> Cid {
        let hash = Code::Sha2_256.digest(data);
        Cid::new_v1(0x71, hash)
    }

    fn encode_ipld(ipld: libipld::Ipld) -> Vec<u8> {
        DagCborCodec.encode(&ipld).unwrap()
    }

    #[derive(Clone)]
    struct TestTransport {
        peer_id: PeerId,
        pubkey: Vec<u8>,
        blockstore: Arc<DefraBlockstore<MemoryStore>>,
        root_cid: Cid,
        root_data: Vec<u8>,
        car_blocks: Arc<HashMap<Cid, Vec<u8>>>,
        selective_blocks: Arc<HashMap<Cid, Vec<u8>>>,
        car_requests: Arc<AtomicUsize>,
        sync_batches: Arc<Mutex<Vec<Vec<Cid>>>>,
    }

    impl TestTransport {
        fn new(
            blockstore: Arc<DefraBlockstore<MemoryStore>>,
            root_cid: Cid,
            root_data: Vec<u8>,
            car_blocks: HashMap<Cid, Vec<u8>>,
            selective_blocks: HashMap<Cid, Vec<u8>>,
        ) -> Self {
            Self {
                peer_id: PeerId::new("local-peer".to_string()),
                pubkey: vec![1, 2, 3],
                blockstore,
                root_cid,
                root_data,
                car_blocks: Arc::new(car_blocks),
                selective_blocks: Arc::new(selective_blocks),
                car_requests: Arc::new(AtomicUsize::new(0)),
                sync_batches: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn car_request_count(&self) -> usize {
            self.car_requests.load(Ordering::SeqCst)
        }

        fn sync_batches(&self) -> Vec<Vec<Cid>> {
            self.sync_batches.lock().unwrap().clone()
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

        async fn topic_peers(&self, _topic: DefraTopic) -> P2PResult<Vec<PeerId>> {
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

        async fn send_car_request(&self, _peer_id: &PeerId, root_cid: Cid) -> P2PResult<()> {
            assert_eq!(root_cid, self.root_cid);
            self.car_requests.fetch_add(1, Ordering::SeqCst);
            self.blockstore
                .put(&self.root_cid, &self.root_data)
                .await
                .map_err(|e| crate::error::Error::BlockstoreError(e.to_string()))?;
            for (cid, data) in self.car_blocks.iter() {
                self.blockstore
                    .put(cid, data)
                    .await
                    .map_err(|e| crate::error::Error::BlockstoreError(e.to_string()))?;
            }
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
            missing: Vec<Cid>,
        ) -> P2PResult<QueryId> {
            self.sync_batches.lock().unwrap().push(missing.clone());
            for cid in missing {
                if let Some(data) = self.selective_blocks.get(&cid) {
                    self.blockstore
                        .put(&cid, data)
                        .await
                        .map_err(|e| crate::error::Error::BlockstoreError(e.to_string()))?;
                }
            }
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
    async fn poll_fetch_dag_recovers_partial_car_with_batched_selective_fetch() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));

        let child_one_data = encode_ipld(ipld!({ "value": 1 }));
        let child_one_cid = make_cid(&child_one_data);
        let child_two_data = encode_ipld(ipld!({ "value": 2 }));
        let child_two_cid = make_cid(&child_two_data);
        let root_data = encode_ipld(ipld!({ "children": [child_one_cid, child_two_cid] }));
        let root_cid = make_cid(&root_data);

        let selective_blocks = HashMap::from([
            (child_one_cid, child_one_data.clone()),
            (child_two_cid, child_two_data.clone()),
        ]);
        let transport = TestTransport::new(
            blockstore.clone(),
            root_cid,
            root_data,
            HashMap::new(),
            selective_blocks,
        );

        let (event_tx, mut event_rx) = mpsc::channel(1);
        let source_peer = PeerId::new("remote-peer".to_string());

        poll_fetch_dag(
            transport.clone(),
            blockstore.clone(),
            event_tx,
            root_cid,
            "doc-id".to_string(),
            "collection-id".to_string(),
            "schema-version-id".to_string(),
            source_peer,
        )
        .await;

        assert!(matches!(
            event_rx.recv().await,
            Some(SyncEvent::DagReady { root_cid: ready_cid, .. }) if ready_cid == root_cid
        ));
        assert!(matches!(blockstore.has(&child_one_cid).await, Ok(true)));
        assert!(matches!(blockstore.has(&child_two_cid).await, Ok(true)));
        assert_eq!(transport.car_request_count(), 1);

        let batches = transport.sync_batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 2);

        let requested: HashSet<_> = batches[0].iter().copied().collect();
        assert_eq!(requested, HashSet::from([child_one_cid, child_two_cid]));
    }

    #[tokio::test]
    async fn poll_fetch_dag_does_not_treat_preexisting_root_as_car_success() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));

        let child_data = encode_ipld(ipld!({ "value": 1 }));
        let child_cid = make_cid(&child_data);
        let root_data = encode_ipld(ipld!({ "child": child_cid }));
        let root_cid = make_cid(&root_data);

        blockstore.put(&root_cid, &root_data).await.unwrap();

        let transport = TestTransport::new(
            blockstore.clone(),
            root_cid,
            root_data,
            HashMap::from([(child_cid, child_data.clone())]),
            HashMap::new(),
        );

        let (event_tx, mut event_rx) = mpsc::channel(1);
        let source_peer = PeerId::new("remote-peer".to_string());

        poll_fetch_dag(
            transport.clone(),
            blockstore.clone(),
            event_tx,
            root_cid,
            "doc-id".to_string(),
            "collection-id".to_string(),
            "schema-version-id".to_string(),
            source_peer,
        )
        .await;

        assert!(matches!(
            event_rx.recv().await,
            Some(SyncEvent::DagReady { root_cid: ready_cid, .. }) if ready_cid == root_cid
        ));
        assert!(matches!(blockstore.has(&child_cid).await, Ok(true)));
        assert_eq!(transport.car_request_count(), 1);
        assert!(transport.sync_batches().is_empty());
    }

    #[tokio::test]
    async fn poll_fetch_dag_continues_after_partial_selective_batch_progress() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));

        let leaf_one_data = encode_ipld(ipld!({ "value": 1 }));
        let leaf_one_cid = make_cid(&leaf_one_data);
        let leaf_two_data = encode_ipld(ipld!({ "value": 2 }));
        let leaf_two_cid = make_cid(&leaf_two_data);

        let mid_one_data = encode_ipld(ipld!({ "child": leaf_one_cid }));
        let mid_one_cid = make_cid(&mid_one_data);
        let mid_two_data = encode_ipld(ipld!({ "child": leaf_two_cid }));
        let mid_two_cid = make_cid(&mid_two_data);

        let root_data = encode_ipld(ipld!({ "children": [mid_one_cid, mid_two_cid] }));
        let root_cid = make_cid(&root_data);

        let selective_blocks = HashMap::from([
            (mid_one_cid, mid_one_data.clone()),
            (mid_two_cid, mid_two_data.clone()),
            (leaf_one_cid, leaf_one_data.clone()),
            (leaf_two_cid, leaf_two_data.clone()),
        ]);
        let transport = TestTransport::new(
            blockstore.clone(),
            root_cid,
            root_data,
            HashMap::new(),
            selective_blocks,
        );

        let (event_tx, mut event_rx) = mpsc::channel(1);
        let source_peer = PeerId::new("remote-peer".to_string());

        poll_fetch_dag(
            transport.clone(),
            blockstore.clone(),
            event_tx,
            root_cid,
            "doc-id".to_string(),
            "collection-id".to_string(),
            "schema-version-id".to_string(),
            source_peer,
        )
        .await;

        assert!(matches!(
            event_rx.recv().await,
            Some(SyncEvent::DagReady { root_cid: ready_cid, .. }) if ready_cid == root_cid
        ));

        let batches = transport.sync_batches();
        assert_eq!(batches.len(), 2);
        assert_eq!(
            batches[0].iter().copied().collect::<HashSet<_>>(),
            HashSet::from([mid_one_cid, mid_two_cid])
        );
        assert_eq!(
            batches[1].iter().copied().collect::<HashSet<_>>(),
            HashSet::from([leaf_one_cid, leaf_two_cid])
        );
    }
}
