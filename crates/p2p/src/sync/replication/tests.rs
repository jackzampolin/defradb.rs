use super::handlers::{handle_block_received, process_event, process_merge_batch};
use super::*;
use crate::bitswap::AccessMode;
use crate::error::Result as P2PResult;
use crate::message::{
    BranchableSyncReply, BranchableSyncRequest, DocSyncReply, DocSyncRequest, PushLogBroadcast,
    PushLogReply, PushLogRequest, PushSEArtifactsRequest,
};
use crate::replicator::EqOnlyFilterMatcher;
use crate::sync::manager::SyncEvent;
use crate::sync::merge::{
    BlockMetadata, MergeBlock, MergeErrorDisposition, MergeHandler, MergeOutcome,
    RecoveredBlockMetadata,
};
use crate::topics::DefraTopic;
use crate::transport::{MessageId, P2PTransport, PeerAddr, PeerId, TransportEvent};
use crate::ExplicitReplayAuthorization;
use crate::QueryId;
use crate::ReplicatorInfo;
use async_trait::async_trait;
use blockstore::{Blockstore, DefraBlockstore};
use cid::Cid;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use storage::RegolithStore;
use tokio::sync::mpsc;

fn test_cid() -> Cid {
    Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap()
}

/// Generate distinct test CIDs by hashing different data.
fn make_cid(data: &[u8]) -> Cid {
    use multihash_codetable::{Code, MultihashDigest};
    let hash = Code::Sha2_256.digest(data);
    Cid::new_v1(0x55, hash) // 0x55 = raw codec
}

/// Test merge handler that tracks calls
struct TestMergeHandler {
    call_count: AtomicUsize,
    should_succeed: bool,
    should_skip: bool,
}

impl TestMergeHandler {
    fn new(should_succeed: bool, should_skip: bool) -> Self {
        Self {
            call_count: AtomicUsize::new(0),
            should_succeed,
            should_skip,
        }
    }

    fn calls(&self) -> usize {
        self.call_count.load(Ordering::SeqCst)
    }
}

struct SlowMergeHandler {
    call_count: AtomicUsize,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
}

impl SlowMergeHandler {
    fn new() -> Self {
        Self {
            call_count: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.call_count.load(Ordering::SeqCst)
    }

    fn max_in_flight(&self) -> usize {
        self.max_in_flight.load(Ordering::SeqCst)
    }

    fn record_in_flight(&self, current: usize) {
        let mut observed = self.max_in_flight.load(Ordering::SeqCst);
        while current > observed {
            match self.max_in_flight.compare_exchange(
                observed,
                current,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(next) => observed = next,
            }
        }
    }
}

#[derive(Debug)]
struct TestError(String);

impl std::fmt::Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TestError: {}", self.0)
    }
}

impl std::error::Error for TestError {}

#[derive(Clone)]
struct NoopTransport {
    peer_id: PeerId,
    pubkey: Vec<u8>,
    publish_calls: Arc<AtomicUsize>,
    replicators: Arc<parking_lot::Mutex<Vec<ReplicatorInfo>>>,
    pushlog_requests: Arc<parking_lot::Mutex<Vec<(PeerId, PushLogRequest)>>>,
}

impl NoopTransport {
    fn new() -> Self {
        Self {
            peer_id: PeerId::new("local-peer".to_string()),
            pubkey: vec![1, 2, 3],
            publish_calls: Arc::new(AtomicUsize::new(0)),
            replicators: Arc::new(parking_lot::Mutex::new(Vec::new())),
            pushlog_requests: Arc::new(parking_lot::Mutex::new(Vec::new())),
        }
    }

    fn publish_calls(&self) -> usize {
        self.publish_calls.load(Ordering::SeqCst)
    }

    fn set_replicators(&self, replicators: Vec<ReplicatorInfo>) {
        *self.replicators.lock() = replicators;
    }

    fn pushlog_requests(&self) -> Vec<(PeerId, PushLogRequest)> {
        self.pushlog_requests.lock().clone()
    }
}

#[derive(Clone)]
struct PollFetchTransport {
    peer_id: PeerId,
    pubkey: Vec<u8>,
    blockstore: Arc<DefraBlockstore<RegolithStore>>,
    child_cid: Cid,
    child_data: Vec<u8>,
    source_blockstore: Option<Arc<DefraBlockstore<RegolithStore>>>,
    car_request_calls: Arc<AtomicUsize>,
    car_requested_cids: Arc<AtomicUsize>,
    car_present_blocks: Arc<AtomicUsize>,
    car_served_blocks: Arc<AtomicUsize>,
    car_filtered_blocks: Arc<AtomicUsize>,
    car_served_bytes: Arc<AtomicUsize>,
    sync_blocks_calls: Arc<AtomicUsize>,
    sync_requested_cids: Arc<AtomicUsize>,
    sync_present_blocks: Arc<AtomicUsize>,
    sync_served_blocks: Arc<AtomicUsize>,
    sync_served_bytes: Arc<AtomicUsize>,
    sync_completion:
        Arc<parking_lot::Mutex<Option<crate::sync::manager::BlockSyncCompletionTracker>>>,
}

impl PollFetchTransport {
    fn new(
        blockstore: Arc<DefraBlockstore<RegolithStore>>,
        child_cid: Cid,
        child_data: Vec<u8>,
    ) -> Self {
        Self {
            peer_id: PeerId::new("local-peer".to_string()),
            pubkey: vec![1, 2, 3],
            blockstore,
            child_cid,
            child_data,
            source_blockstore: None,
            car_request_calls: Arc::new(AtomicUsize::new(0)),
            car_requested_cids: Arc::new(AtomicUsize::new(0)),
            car_present_blocks: Arc::new(AtomicUsize::new(0)),
            car_served_blocks: Arc::new(AtomicUsize::new(0)),
            car_filtered_blocks: Arc::new(AtomicUsize::new(0)),
            car_served_bytes: Arc::new(AtomicUsize::new(0)),
            sync_blocks_calls: Arc::new(AtomicUsize::new(0)),
            sync_requested_cids: Arc::new(AtomicUsize::new(0)),
            sync_present_blocks: Arc::new(AtomicUsize::new(0)),
            sync_served_blocks: Arc::new(AtomicUsize::new(0)),
            sync_served_bytes: Arc::new(AtomicUsize::new(0)),
            sync_completion: Arc::new(parking_lot::Mutex::new(None)),
        }
    }

    fn with_car_source(
        blockstore: Arc<DefraBlockstore<RegolithStore>>,
        source_blockstore: Arc<DefraBlockstore<RegolithStore>>,
    ) -> Self {
        Self {
            peer_id: PeerId::new("local-peer".to_string()),
            pubkey: vec![1, 2, 3],
            blockstore,
            child_cid: test_cid(),
            child_data: Vec::new(),
            source_blockstore: Some(source_blockstore),
            car_request_calls: Arc::new(AtomicUsize::new(0)),
            car_requested_cids: Arc::new(AtomicUsize::new(0)),
            car_present_blocks: Arc::new(AtomicUsize::new(0)),
            car_served_blocks: Arc::new(AtomicUsize::new(0)),
            car_filtered_blocks: Arc::new(AtomicUsize::new(0)),
            car_served_bytes: Arc::new(AtomicUsize::new(0)),
            sync_blocks_calls: Arc::new(AtomicUsize::new(0)),
            sync_requested_cids: Arc::new(AtomicUsize::new(0)),
            sync_present_blocks: Arc::new(AtomicUsize::new(0)),
            sync_served_blocks: Arc::new(AtomicUsize::new(0)),
            sync_served_bytes: Arc::new(AtomicUsize::new(0)),
            sync_completion: Arc::new(parking_lot::Mutex::new(None)),
        }
    }

    fn car_request_calls(&self) -> usize {
        self.car_request_calls.load(Ordering::SeqCst)
    }

    fn car_served_blocks(&self) -> usize {
        self.car_served_blocks.load(Ordering::SeqCst)
    }

    fn car_requested_cids(&self) -> usize {
        self.car_requested_cids.load(Ordering::SeqCst)
    }

    fn car_present_blocks(&self) -> usize {
        self.car_present_blocks.load(Ordering::SeqCst)
    }

    fn car_filtered_blocks(&self) -> usize {
        self.car_filtered_blocks.load(Ordering::SeqCst)
    }

    fn car_served_bytes(&self) -> usize {
        self.car_served_bytes.load(Ordering::SeqCst)
    }

    fn sync_blocks_calls(&self) -> usize {
        self.sync_blocks_calls.load(Ordering::SeqCst)
    }

    fn sync_requested_cids(&self) -> usize {
        self.sync_requested_cids.load(Ordering::SeqCst)
    }

    fn sync_present_blocks(&self) -> usize {
        self.sync_present_blocks.load(Ordering::SeqCst)
    }

    fn sync_served_blocks(&self) -> usize {
        self.sync_served_blocks.load(Ordering::SeqCst)
    }

    fn sync_served_bytes(&self) -> usize {
        self.sync_served_bytes.load(Ordering::SeqCst)
    }

    fn set_sync_completion(&self, completion: crate::sync::manager::BlockSyncCompletionTracker) {
        *self.sync_completion.lock() = Some(completion);
    }

    fn signal_sync_complete(&self, query_id: QueryId) {
        let completion = self.sync_completion.lock().clone();
        tokio::spawn(async move {
            // The real transport emits completion after `sync_blocks` returns
            // and the poll owner registers its waiter.
            tokio::task::yield_now().await;
            if let Some(completion) = completion {
                completion.complete(query_id, true);
            }
        });
    }
}

#[async_trait]
impl P2PTransport for PollFetchTransport {
    type ResponseToken = ();

    fn supports_cancellable_rooted_sync(&self) -> bool {
        true
    }

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
        // This transport's fetch methods synchronously serve requests from the
        // fixed source used by the tests, so model that source as connected.
        Ok(vec![PeerId::new("source-peer".to_string())])
    }

    async fn listen_addresses(&self) -> P2PResult<Vec<PeerAddr>> {
        Ok(Vec::new())
    }

    async fn poll_until_connected(&self, _peer_id: &PeerId, _timeout: Duration) -> P2PResult<()> {
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

    async fn publish(&self, _topic: DefraTopic, _msg: PushLogBroadcast) -> P2PResult<MessageId> {
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
        self.car_request_calls.fetch_add(1, Ordering::SeqCst);
        self.car_requested_cids.fetch_add(1, Ordering::SeqCst);
        if let Some(source) = &self.source_blockstore {
            let collected =
                crate::sync::car::collect_dag_blocks_from_roots(source.as_ref(), &[root_cid])
                    .await?;
            let block_refs: Vec<_> = collected
                .blocks
                .iter()
                .map(|(cid, data)| (cid, data.as_ref()))
                .collect();
            let car = crate::sync::car::encode_car(&[root_cid], &block_refs)?;
            let (_roots, blocks) = crate::sync::car::decode_car(&car)?;
            self.car_served_blocks
                .fetch_add(blocks.len(), Ordering::SeqCst);
            self.car_present_blocks
                .fetch_add(blocks.len(), Ordering::SeqCst);
            self.car_served_bytes.fetch_add(car.len(), Ordering::SeqCst);
            for (cid, data) in blocks {
                self.blockstore
                    .put(&cid, &data)
                    .await
                    .map_err(|e| crate::error::Error::BlockstoreError(e.to_string()))?;
            }
            return Ok(());
        }
        self.blockstore
            .put(&self.child_cid, &self.child_data)
            .await
            .map_err(|e| crate::error::Error::BlockstoreError(e.to_string()))?;
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
        root: Cid,
        _providers: Vec<PeerId>,
        missing: Vec<Cid>,
    ) -> P2PResult<QueryId> {
        self.sync_blocks_calls.fetch_add(1, Ordering::SeqCst);
        self.sync_requested_cids
            .fetch_add(missing.len(), Ordering::SeqCst);
        if missing.is_empty() {
            if let Some(source) = &self.source_blockstore {
                let collected =
                    crate::sync::car::collect_dag_blocks_from_roots(source.as_ref(), &[root])
                        .await?;
                for (cid, data) in collected.blocks {
                    self.sync_present_blocks.fetch_add(1, Ordering::SeqCst);
                    self.blockstore
                        .put(&cid, &data)
                        .await
                        .map_err(|e| crate::error::Error::BlockstoreError(e.to_string()))?;
                    self.sync_served_blocks.fetch_add(1, Ordering::SeqCst);
                    self.sync_served_bytes
                        .fetch_add(data.len(), Ordering::SeqCst);
                }
            } else {
                self.sync_present_blocks.fetch_add(1, Ordering::SeqCst);
                self.blockstore
                    .put(&self.child_cid, &self.child_data)
                    .await
                    .map_err(|e| crate::error::Error::BlockstoreError(e.to_string()))?;
                self.sync_served_blocks.fetch_add(1, Ordering::SeqCst);
                self.sync_served_bytes
                    .fetch_add(self.child_data.len(), Ordering::SeqCst);
            }
            let query_id = QueryId(1);
            self.signal_sync_complete(query_id);
            return Ok(query_id);
        }
        for cid in missing {
            let data = if let Some(source) = &self.source_blockstore {
                source
                    .get(&cid)
                    .await
                    .map_err(|e| crate::error::Error::BlockstoreError(e.to_string()))?
            } else if cid == self.child_cid {
                Some(self.child_data.clone().into())
            } else {
                None
            };
            let Some(data) = data else {
                continue;
            };
            self.sync_present_blocks.fetch_add(1, Ordering::SeqCst);
            self.blockstore
                .put(&cid, &data)
                .await
                .map_err(|e| crate::error::Error::BlockstoreError(e.to_string()))?;
            self.sync_served_blocks.fetch_add(1, Ordering::SeqCst);
            self.sync_served_bytes
                .fetch_add(data.len(), Ordering::SeqCst);
        }
        let query_id = QueryId(1);
        self.signal_sync_complete(query_id);
        Ok(query_id)
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

#[async_trait]
impl P2PTransport for NoopTransport {
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

    async fn poll_until_connected(&self, _peer_id: &PeerId, _timeout: Duration) -> P2PResult<()> {
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

    async fn publish(&self, _topic: DefraTopic, _msg: PushLogBroadcast) -> P2PResult<MessageId> {
        self.publish_calls.fetch_add(1, Ordering::SeqCst);
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
        peer_id: &PeerId,
        req: PushLogRequest,
    ) -> P2PResult<PushLogReply> {
        self.pushlog_requests.lock().push((peer_id.clone(), req));
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
        Ok(QueryId(0))
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
        Ok(self.replicators.lock().clone())
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

struct RetryThenMergeHandler {
    call_count: AtomicUsize,
}

struct ErrorThenMergeHandler {
    call_count: AtomicUsize,
    disposition: MergeErrorDisposition,
}

impl ErrorThenMergeHandler {
    fn new(disposition: MergeErrorDisposition) -> Self {
        Self {
            call_count: AtomicUsize::new(0),
            disposition,
        }
    }
}

impl RetryThenMergeHandler {
    fn new() -> Self {
        Self {
            call_count: AtomicUsize::new(0),
        }
    }
}

/// Merge handler that always returns a deterministic content rejection.
struct RejectingMergeHandler {
    reason: String,
}

impl RejectingMergeHandler {
    fn new(reason: &str) -> Self {
        Self {
            reason: reason.to_string(),
        }
    }
}

#[async_trait]
impl MergeHandler for RejectingMergeHandler {
    type Error = TestError;

    async fn handle_block(
        &self,
        _cid: &Cid,
        _block_data: &[u8],
        _metadata: BlockMetadata<'_>,
    ) -> Result<MergeOutcome, Self::Error> {
        Ok(MergeOutcome::rejected(self.reason.clone()))
    }
}

#[async_trait]
impl MergeHandler for RetryThenMergeHandler {
    type Error = TestError;

    async fn handle_block(
        &self,
        _cid: &Cid,
        _block_data: &[u8],
        _metadata: BlockMetadata<'_>,
    ) -> Result<MergeOutcome, Self::Error> {
        let attempt = self.call_count.fetch_add(1, Ordering::SeqCst);
        if attempt == 0 {
            Ok(MergeOutcome::retryable_skip("pending ACP"))
        } else {
            Ok(MergeOutcome::Merged)
        }
    }
}

#[async_trait]
impl MergeHandler for ErrorThenMergeHandler {
    type Error = TestError;

    fn error_disposition(&self, _error: &Self::Error) -> MergeErrorDisposition {
        self.disposition
    }

    async fn handle_block(
        &self,
        _cid: &Cid,
        _block_data: &[u8],
        _metadata: BlockMetadata<'_>,
    ) -> Result<MergeOutcome, Self::Error> {
        if self.call_count.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(TestError("classified merge failure".to_string()))
        } else {
            Ok(MergeOutcome::Merged)
        }
    }
}

#[async_trait]
impl MergeHandler for TestMergeHandler {
    type Error = TestError;

    async fn handle_block(
        &self,
        _cid: &Cid,
        _block_data: &[u8],
        _metadata: BlockMetadata<'_>,
    ) -> Result<MergeOutcome, Self::Error> {
        self.call_count.fetch_add(1, Ordering::SeqCst);

        if !self.should_succeed {
            return Err(TestError("merge failed".to_string()));
        }

        if self.should_skip {
            Ok(MergeOutcome::terminal_skip("test skip reason"))
        } else {
            Ok(MergeOutcome::Merged)
        }
    }
}

#[async_trait]
impl MergeHandler for SlowMergeHandler {
    type Error = TestError;

    async fn handle_block(
        &self,
        _cid: &Cid,
        _block_data: &[u8],
        _metadata: BlockMetadata<'_>,
    ) -> Result<MergeOutcome, Self::Error> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        let current = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.record_in_flight(current);
        tokio::time::sleep(Duration::from_millis(50)).await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        Ok(MergeOutcome::Merged)
    }
}

/// Batch-aware merge handler that tracks per-block and batch calls separately.
struct BatchTestHandler {
    per_block_calls: AtomicUsize,
    batch_calls: AtomicUsize,
    batch_block_count: AtomicUsize,
    fail_at_index: Option<usize>,
    reject_at_index: Option<(usize, String)>,
}

impl BatchTestHandler {
    fn new() -> Self {
        Self {
            per_block_calls: AtomicUsize::new(0),
            batch_calls: AtomicUsize::new(0),
            batch_block_count: AtomicUsize::new(0),
            fail_at_index: None,
            reject_at_index: None,
        }
    }

    fn with_failure_at(index: usize) -> Self {
        Self {
            per_block_calls: AtomicUsize::new(0),
            batch_calls: AtomicUsize::new(0),
            batch_block_count: AtomicUsize::new(0),
            fail_at_index: Some(index),
            reject_at_index: None,
        }
    }

    /// Returns `MergeOutcome::Rejected` for the block at `index`, leaving
    /// every other block in the batch a normal merge — exercises that a
    /// batch-mixed Rejected outcome does not stop siblings from merging.
    fn with_rejection_at(index: usize, reason: &str) -> Self {
        Self {
            per_block_calls: AtomicUsize::new(0),
            batch_calls: AtomicUsize::new(0),
            batch_block_count: AtomicUsize::new(0),
            fail_at_index: None,
            reject_at_index: Some((index, reason.to_string())),
        }
    }

    fn per_block_calls(&self) -> usize {
        self.per_block_calls.load(Ordering::SeqCst)
    }

    fn batch_calls(&self) -> usize {
        self.batch_calls.load(Ordering::SeqCst)
    }

    fn batch_block_count(&self) -> usize {
        self.batch_block_count.load(Ordering::SeqCst)
    }
}

struct RejectingAuthorizationHandler {
    validation_calls: AtomicUsize,
    batch_calls: AtomicUsize,
}

impl RejectingAuthorizationHandler {
    fn new() -> Self {
        Self {
            validation_calls: AtomicUsize::new(0),
            batch_calls: AtomicUsize::new(0),
        }
    }

    fn validation_calls(&self) -> usize {
        self.validation_calls.load(Ordering::SeqCst)
    }

    fn batch_calls(&self) -> usize {
        self.batch_calls.load(Ordering::SeqCst)
    }
}

struct RecoveryMetadataHandler {
    recover_calls: AtomicUsize,
    handle_calls: AtomicUsize,
}

impl RecoveryMetadataHandler {
    fn new() -> Self {
        Self {
            recover_calls: AtomicUsize::new(0),
            handle_calls: AtomicUsize::new(0),
        }
    }

    fn recover_calls(&self) -> usize {
        self.recover_calls.load(Ordering::SeqCst)
    }

    fn handle_calls(&self) -> usize {
        self.handle_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl MergeHandler for BatchTestHandler {
    type Error = TestError;

    async fn handle_block(
        &self,
        _cid: &Cid,
        _block_data: &[u8],
        _metadata: BlockMetadata<'_>,
    ) -> Result<MergeOutcome, Self::Error> {
        self.per_block_calls.fetch_add(1, Ordering::SeqCst);
        Ok(MergeOutcome::Merged)
    }

    async fn handle_block_batch(
        &self,
        blocks: &[MergeBlock],
    ) -> Vec<Result<MergeOutcome, Self::Error>> {
        self.batch_calls.fetch_add(1, Ordering::SeqCst);
        self.batch_block_count
            .fetch_add(blocks.len(), Ordering::SeqCst);

        blocks
            .iter()
            .enumerate()
            .map(|(i, _block)| {
                if self.fail_at_index == Some(i) {
                    Err(TestError("batch block failed".to_string()))
                } else if let Some((reject_index, reason)) = &self.reject_at_index {
                    if *reject_index == i {
                        Ok(MergeOutcome::rejected(reason.clone()))
                    } else {
                        Ok(MergeOutcome::Merged)
                    }
                } else {
                    Ok(MergeOutcome::Merged)
                }
            })
            .collect()
    }
}

#[async_trait]
impl MergeHandler for RejectingAuthorizationHandler {
    type Error = TestError;

    async fn validate_authorization(
        &self,
        authorization: Option<&ExplicitReplayAuthorization>,
        _block: &MergeBlock,
    ) -> Result<(), Self::Error> {
        self.validation_calls.fetch_add(1, Ordering::SeqCst);
        if authorization.is_some() {
            Err(TestError("authorization rejected".to_string()))
        } else {
            Ok(())
        }
    }

    async fn handle_block(
        &self,
        _cid: &Cid,
        _block_data: &[u8],
        _metadata: BlockMetadata<'_>,
    ) -> Result<MergeOutcome, Self::Error> {
        Ok(MergeOutcome::Merged)
    }

    async fn handle_block_batch(
        &self,
        blocks: &[MergeBlock],
    ) -> Vec<Result<MergeOutcome, Self::Error>> {
        self.batch_calls.fetch_add(1, Ordering::SeqCst);
        blocks.iter().map(|_| Ok(MergeOutcome::Merged)).collect()
    }
}

#[async_trait]
impl MergeHandler for RecoveryMetadataHandler {
    type Error = TestError;

    async fn recover_block_metadata(
        &self,
        _cid: &Cid,
        _block_data: &[u8],
    ) -> Result<Option<RecoveredBlockMetadata>, Self::Error> {
        self.recover_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Some(RecoveredBlockMetadata::new(
            "doc-recovered",
            "col-recovered",
            "did:key:creator",
        )))
    }

    async fn handle_block(
        &self,
        _cid: &Cid,
        _block_data: &[u8],
        metadata: BlockMetadata<'_>,
    ) -> Result<MergeOutcome, Self::Error> {
        self.handle_calls.fetch_add(1, Ordering::SeqCst);
        if !metadata.is_recovery {
            return Err(TestError(
                "metadata should remain in recovery mode".to_string(),
            ));
        }
        if metadata.doc_id != Some("doc-recovered")
            || metadata.collection_id != Some("col-recovered")
            || metadata.creator != Some("did:key:creator")
        {
            return Err(TestError(
                "recovered metadata was not forwarded".to_string(),
            ));
        }
        Ok(MergeOutcome::Merged)
    }
}

#[tokio::test]
async fn test_process_block_received_success() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));

    // Store a block
    let cid = test_cid();
    blockstore.put(&cid, b"test data").await.unwrap();

    let handler = Arc::new(TestMergeHandler::new(true, false));

    // Create a simple event
    let (tx, _rx) = mpsc::channel(1);
    tx.send(SyncEvent::BlockReceived {
        cid,
        doc_id: "doc1".to_string(),
        collection_id: "col1".to_string(),
        creator: "peer1".to_string(),
        sender_peer: None,
        is_explicit_replicator: false,
        explicit_replay_authorization: None,
    })
    .await
    .unwrap();
    drop(tx); // Close channel

    // We can't easily test the full loop without a coordinator
    // but we can verify the handler trait works
    let result = handler
        .handle_block(
            &cid,
            b"test data",
            BlockMetadata::normal("doc1", "col1", "peer1", None, false),
        )
        .await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_merged());
    assert_eq!(handler.calls(), 1);
}

#[tokio::test]
async fn test_handler_skip() {
    let cid = test_cid();
    let handler = TestMergeHandler::new(true, true); // succeed but skip

    let result = handler
        .handle_block(
            &cid,
            b"test",
            BlockMetadata::normal("doc", "col", "peer", None, false),
        )
        .await;
    assert!(result.is_ok());
    let outcome = result.unwrap();
    assert!(outcome.is_skipped());
    match outcome {
        MergeOutcome::Skipped { reason, terminal } => {
            assert_eq!(reason, "test skip reason");
            assert!(terminal);
        }
        _ => panic!("Expected Skipped outcome"),
    }
}

#[tokio::test]
async fn test_handler_error() {
    let cid = test_cid();
    let handler = TestMergeHandler::new(false, false); // fail

    let result = handler
        .handle_block(
            &cid,
            b"test",
            BlockMetadata::normal("doc", "col", "peer", None, false),
        )
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_handler_recovery_mode() {
    let cid = test_cid();
    let handler = TestMergeHandler::new(true, false);

    // Recovery mode - metadata is None
    let metadata = BlockMetadata::recovery();
    assert!(metadata.is_recovery);
    assert!(metadata.is_incomplete());
    assert!(metadata.doc_id.is_none());

    let result = handler.handle_block(&cid, b"test", metadata).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_retryable_skip_remains_unmerged_until_replayed() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let cid = test_cid();
    blockstore.put(&cid, b"test data").await.unwrap();

    let (coordinator, _events) = crate::sync::coordinator::SyncCoordinator::with_access_control(
        NoopTransport::new(),
        blockstore.clone(),
        crate::sync::SyncConfig::default(),
        AccessMode::Open,
        Arc::new(crate::ReplicatorRegistry::new()),
        Arc::new(crate::sync::collection_store::NoOpCollectionStorage),
        Arc::new(EqOnlyFilterMatcher),
    )
    .await
    .unwrap();

    let config = ReplicationConfig {
        continue_on_error: true,
        rebroadcast_on_merge: false,
        batch_size: 1,
    };
    let handler = RetryThenMergeHandler::new();

    let first = handle_block_received(
        &coordinator,
        &handler,
        &config,
        cid,
        BlockMetadata::normal("doc1", "col1", "peer1", Some("sender1"), true),
    )
    .await;

    match first {
        ReplicationResult::Skipped {
            terminal: false,
            ref reason,
            ..
        } => assert_eq!(reason, "pending ACP"),
        other => panic!("expected retryable skip, got {:?}", other),
    }
    assert!(
        !blockstore.is_merged(&cid).await.unwrap(),
        "retryable skip must leave the CID unmerged"
    );

    let second = handle_block_received(
        &coordinator,
        &handler,
        &config,
        cid,
        BlockMetadata::normal("doc1", "col1", "peer1", Some("sender1"), true),
    )
    .await;

    match second {
        ReplicationResult::Merged { .. } => {}
        other => panic!("expected replay merge, got {:?}", other),
    }
    assert!(
        blockstore.is_merged(&cid).await.unwrap(),
        "successful replay should mark the CID as merged"
    );
}

/// Seed a coordinator whose manager has a durable pending-DAG store
/// installed and a live registration for `cid`, mirroring a push-driven
/// registration awaiting merge.
async fn coordinator_with_live_pending_dag(
    blockstore: Arc<DefraBlockstore<RegolithStore>>,
    cid: Cid,
) -> (
    crate::sync::coordinator::SyncCoordinator<DefraBlockstore<RegolithStore>, NoopTransport>,
    Arc<crate::sync::pending_store::PendingDagStore<RegolithStore>>,
) {
    use crate::sync::pending_store::PendingDagStorage;

    let (coordinator, _events) = crate::sync::coordinator::SyncCoordinator::with_access_control(
        NoopTransport::new(),
        blockstore,
        crate::sync::SyncConfig::default(),
        AccessMode::Open,
        Arc::new(crate::ReplicatorRegistry::new()),
        Arc::new(crate::sync::collection_store::NoOpCollectionStorage),
        Arc::new(EqOnlyFilterMatcher),
    )
    .await
    .unwrap();

    let pending_store = Arc::new(crate::sync::pending_store::PendingDagStore::new(Arc::new(
        RegolithStore::in_memory().unwrap(),
    )));
    pending_store
        .put(
            &cid,
            &crate::sync::pending_store::PersistedPendingDag {
                doc_id: "doc1".to_string(),
                collection_id: "col1".to_string(),
                head_priority: None,
                creator: "peer1".to_string(),
                source_peer: Some("sender1".to_string()),
                alternate_providers: Vec::new(),
                is_explicit_replicator: true,
                explicit_replay_authorization: None,
            },
        )
        .await
        .expect("persist live pending dag record");

    // install_pending_dag_store hydrates persisted_roots from the store
    // at install time, so the record must already be `put` above.
    coordinator
        .manager()
        .install_pending_dag_store(pending_store.clone())
        .await;

    (coordinator, pending_store)
}

#[tokio::test]
async fn test_rejected_merge_quarantines_and_leaves_block_unmerged() {
    use crate::sync::pending_store::PendingDagStorage;

    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let cid = test_cid();
    blockstore.put(&cid, b"test data").await.unwrap();

    let (coordinator, pending_store) =
        coordinator_with_live_pending_dag(blockstore.clone(), cid).await;
    assert_eq!(
        coordinator.manager().resync_persisted_pending_dags().await,
        1
    );
    assert_eq!(coordinator.pending_dag_count(), 1);

    let handler = RejectingMergeHandler::new("unique constraint violation");
    let result = handle_block_received(
        &coordinator,
        &handler,
        &ReplicationConfig::default(),
        cid,
        BlockMetadata::normal("doc1", "col1", "peer1", Some("sender1"), true),
    )
    .await;

    match result {
        ReplicationResult::Quarantined {
            cid: result_cid,
            reason,
            ..
        } => {
            assert_eq!(result_cid, cid);
            assert_eq!(reason, "unique constraint violation");
        }
        other => panic!("expected Quarantined, got {:?}", other),
    }

    assert!(
        !blockstore.is_merged(&cid).await.unwrap(),
        "quarantine must not mark the block merged (mark_as_merged must not run)"
    );
    assert!(
        pending_store.is_quarantined(&cid).await.unwrap(),
        "quarantine store must be populated"
    );
    assert!(
        pending_store.load_all().await.unwrap().is_empty(),
        "live durable record must be removed after quarantine"
    );
}

#[tokio::test]
async fn terminal_merge_error_quarantines_and_releases_pending_slot() {
    use crate::sync::pending_store::PendingDagStorage;

    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let cid = test_cid();
    blockstore.put(&cid, b"test data").await.unwrap();
    let (coordinator, pending_store) =
        coordinator_with_live_pending_dag(blockstore.clone(), cid).await;
    assert_eq!(
        coordinator.manager().resync_persisted_pending_dags().await,
        1
    );
    assert_eq!(coordinator.pending_dag_count(), 1);

    let result = handle_block_received(
        &coordinator,
        &ErrorThenMergeHandler::new(MergeErrorDisposition::Terminal),
        &ReplicationConfig::default(),
        cid,
        BlockMetadata::normal("doc1", "col1", "peer1", Some("sender1"), true),
    )
    .await;

    assert!(
        matches!(result, ReplicationResult::Quarantined { cid: result_cid, .. } if result_cid == cid)
    );
    assert!(pending_store.load_all().await.unwrap().is_empty());
    assert!(pending_store.is_quarantined(&cid).await.unwrap());
    let status = coordinator.sync_status();
    assert_eq!(status.pending_dags, 0);
    assert_eq!(status.persisted_pending_dags, 0);
    assert_eq!(status.pending_dag_terminal_quarantined, 1);
    assert_eq!(status.quarantined_pending_dags, 1);
}

#[tokio::test]
async fn batched_terminal_merge_error_quarantines_and_releases_pending_slot() {
    use crate::sync::pending_store::PendingDagStorage;

    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let cid = test_cid();
    blockstore.put(&cid, b"test data").await.unwrap();
    let (coordinator, pending_store) =
        coordinator_with_live_pending_dag(blockstore.clone(), cid).await;
    assert_eq!(
        coordinator.manager().resync_persisted_pending_dags().await,
        1
    );

    let results = process_merge_batch(
        &coordinator,
        vec![dag_ready_event(cid)],
        &ErrorThenMergeHandler::new(MergeErrorDisposition::Terminal),
        &ReplicationConfig::default(),
    )
    .await;

    assert!(matches!(
        results.as_slice(),
        [ReplicationResult::Quarantined { cid: result_cid, .. }] if *result_cid == cid
    ));
    assert_eq!(coordinator.pending_dag_count(), 0);
    assert!(pending_store.load_all().await.unwrap().is_empty());
    assert!(pending_store.is_quarantined(&cid).await.unwrap());
}

#[tokio::test]
async fn retryable_merge_error_retains_pending_slot_and_can_converge() {
    use crate::sync::pending_store::PendingDagStorage;

    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let cid = test_cid();
    blockstore.put(&cid, b"test data").await.unwrap();
    let (coordinator, pending_store) =
        coordinator_with_live_pending_dag(blockstore.clone(), cid).await;
    assert_eq!(
        coordinator.manager().resync_persisted_pending_dags().await,
        1
    );
    let handler = ErrorThenMergeHandler::new(MergeErrorDisposition::Retryable);
    let metadata = || BlockMetadata::normal("doc1", "col1", "peer1", Some("sender1"), true);

    let first = handle_block_received(
        &coordinator,
        &handler,
        &ReplicationConfig::default(),
        cid,
        metadata(),
    )
    .await;
    assert!(matches!(first, ReplicationResult::Failed { .. }));
    assert_eq!(pending_store.load_all().await.unwrap().len(), 1);
    assert!(!pending_store.is_quarantined(&cid).await.unwrap());

    let second = handle_block_received(
        &coordinator,
        &handler,
        &ReplicationConfig::default(),
        cid,
        metadata(),
    )
    .await;
    assert!(matches!(second, ReplicationResult::Merged { .. }));
    assert!(pending_store.load_all().await.unwrap().is_empty());
}

#[tokio::test]
async fn test_transient_merge_error_stays_failed_and_does_not_quarantine() {
    use crate::sync::pending_store::PendingDagStorage;

    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let cid = test_cid();
    blockstore.put(&cid, b"test data").await.unwrap();

    let (coordinator, pending_store) =
        coordinator_with_live_pending_dag(blockstore.clone(), cid).await;

    let handler = TestMergeHandler::new(false, false); // handle_block returns Err
    let result = handle_block_received(
        &coordinator,
        &handler,
        &ReplicationConfig::default(),
        cid,
        BlockMetadata::normal("doc1", "col1", "peer1", Some("sender1"), true),
    )
    .await;

    match result {
        ReplicationResult::Failed {
            cid: result_cid, ..
        } => assert_eq!(result_cid, cid),
        other => panic!("expected Failed, got {:?}", other),
    }

    assert!(!blockstore.is_merged(&cid).await.unwrap());
    assert!(
        !pending_store.is_quarantined(&cid).await.unwrap(),
        "a transient failure must not quarantine the root"
    );
    assert_eq!(
        pending_store.load_all().await.unwrap().len(),
        1,
        "durable record must remain live after a transient failure"
    );
}

fn dag_ready_event(cid: Cid) -> SyncEvent {
    SyncEvent::DagReady {
        root_cid: cid,
        doc_id: "doc1".to_string(),
        collection_id: "col1".to_string(),
        creator: "peer1".to_string(),
        sender_peer: Some("sender1".to_string()),
        is_explicit_replicator: true,
        explicit_replay_authorization: None,
    }
}

#[tokio::test]
async fn dag_ready_merge_failure_retains_receiver_obligation() {
    use crate::sync::pending_store::PendingDagStorage;

    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let cid = test_cid();
    blockstore.put(&cid, b"test data").await.unwrap();
    let (coordinator, pending_store) =
        coordinator_with_live_pending_dag(blockstore.clone(), cid).await;

    assert_eq!(
        coordinator.manager().resync_persisted_pending_dags().await,
        1
    );
    assert_eq!(coordinator.pending_dag_count(), 1);
    let result = process_event(
        &coordinator,
        dag_ready_event(cid),
        &TestMergeHandler::new(false, false),
        &ReplicationConfig::default(),
    )
    .await;

    assert!(matches!(result, ReplicationResult::Failed { .. }));
    assert_eq!(
        coordinator.pending_dag_count(),
        1,
        "a transient merge failure must remain owned by the receiver clock"
    );
    assert_eq!(pending_store.load_all().await.unwrap().len(), 1);
    assert!(!blockstore.is_merged(&cid).await.unwrap());
    assert!(
        coordinator
            .manager()
            .claim_due_pending_dag_retries(tokio::time::Instant::now())
            .iter()
            .any(|(due_cid, _)| *due_cid == cid),
        "the receiver clock must own merge re-drive after a transient failure"
    );
}

#[tokio::test]
async fn batched_dag_ready_merge_failure_retains_receiver_obligation() {
    use crate::sync::pending_store::PendingDagStorage;

    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let cid = test_cid();
    blockstore.put(&cid, b"test data").await.unwrap();
    let (coordinator, pending_store) =
        coordinator_with_live_pending_dag(blockstore.clone(), cid).await;

    assert_eq!(
        coordinator.manager().resync_persisted_pending_dags().await,
        1
    );
    assert_eq!(coordinator.pending_dag_count(), 1);
    let results = process_merge_batch(
        &coordinator,
        vec![dag_ready_event(cid)],
        &BatchTestHandler::with_failure_at(0),
        &ReplicationConfig::default(),
    )
    .await;

    assert!(matches!(
        results.as_slice(),
        [ReplicationResult::Failed { .. }]
    ));
    assert_eq!(
        coordinator.pending_dag_count(),
        1,
        "batching must not create a second terminal-cleanup owner"
    );
    assert_eq!(pending_store.load_all().await.unwrap().len(), 1);
    assert!(!blockstore.is_merged(&cid).await.unwrap());
}

#[tokio::test]
async fn test_batch_rejected_block_not_marked_merged_while_sibling_merges() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let rejected_cid = make_cid(b"batch-reject");
    let merged_cid = make_cid(b"batch-merge");
    blockstore
        .put(&rejected_cid, b"batch-reject")
        .await
        .unwrap();
    blockstore.put(&merged_cid, b"batch-merge").await.unwrap();

    let (coordinator, _events) = crate::sync::coordinator::SyncCoordinator::with_access_control(
        NoopTransport::new(),
        blockstore.clone(),
        crate::sync::SyncConfig::default(),
        AccessMode::Open,
        Arc::new(crate::ReplicatorRegistry::new()),
        Arc::new(crate::sync::collection_store::NoOpCollectionStorage),
        Arc::new(EqOnlyFilterMatcher),
    )
    .await
    .unwrap();

    let events = vec![
        SyncEvent::BlockReceived {
            cid: rejected_cid,
            doc_id: "doc-reject".to_string(),
            collection_id: "col1".to_string(),
            creator: "peer1".to_string(),
            sender_peer: None,
            is_explicit_replicator: false,
            explicit_replay_authorization: None,
        },
        SyncEvent::BlockReceived {
            cid: merged_cid,
            doc_id: "doc-merge".to_string(),
            collection_id: "col1".to_string(),
            creator: "peer1".to_string(),
            sender_peer: None,
            is_explicit_replicator: false,
            explicit_replay_authorization: None,
        },
    ];
    let handler = BatchTestHandler::with_rejection_at(0, "unique constraint violation");

    let results = process_merge_batch(
        &coordinator,
        events,
        &handler,
        &ReplicationConfig::default(),
    )
    .await;

    assert_eq!(results.len(), 2);
    match &results[0] {
        ReplicationResult::Quarantined {
            cid: result_cid,
            reason,
            ..
        } => {
            assert_eq!(*result_cid, rejected_cid);
            assert_eq!(reason, "unique constraint violation");
        }
        other => panic!(
            "expected Quarantined for the rejected block, got {:?}",
            other
        ),
    }
    assert!(matches!(
        &results[1],
        ReplicationResult::Merged { cid, .. } if *cid == merged_cid
    ));

    assert!(
            !blockstore.is_merged(&rejected_cid).await.unwrap(),
            "a Rejected block in a batch must not be marked merged, even though a sibling merged in the same call"
        );
    assert!(
        blockstore.is_merged(&merged_cid).await.unwrap(),
        "the sibling block must still merge normally"
    );
}

#[tokio::test]
async fn test_recovery_refuses_blocks_without_recovered_metadata() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let cid = test_cid();
    blockstore.put(&cid, b"test data").await.unwrap();

    let (coordinator, _events) = crate::sync::coordinator::SyncCoordinator::with_access_control(
        NoopTransport::new(),
        blockstore,
        crate::sync::SyncConfig::default(),
        AccessMode::Open,
        Arc::new(crate::ReplicatorRegistry::new()),
        Arc::new(crate::sync::collection_store::NoOpCollectionStorage),
        Arc::new(EqOnlyFilterMatcher),
    )
    .await
    .unwrap();

    let handler = TestMergeHandler::new(true, false);
    let result = handle_block_received(
        &coordinator,
        &handler,
        &ReplicationConfig::default(),
        cid,
        BlockMetadata::recovery(),
    )
    .await;

    match result {
        ReplicationResult::Failed { error, .. } => {
            assert!(error.contains("Recovery metadata incomplete"));
        }
        other => panic!("expected recovery metadata failure, got {:?}", other),
    }
    assert_eq!(
        handler.calls(),
        0,
        "recovery must fail before merge when metadata cannot be recovered"
    );
}

#[tokio::test]
async fn two_stream_pushlog_merge_denial_leaves_block_unmerged() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let cid = make_cid(b"two-stream-acp-denied");
    blockstore
        .put(&cid, b"two-stream-acp-denied")
        .await
        .unwrap();

    let (coordinator, _events) = crate::sync::coordinator::SyncCoordinator::with_access_control(
        NoopTransport::new(),
        blockstore.clone(),
        crate::sync::SyncConfig::default(),
        AccessMode::Controlled,
        Arc::new(crate::ReplicatorRegistry::new()),
        Arc::new(crate::sync::collection_store::NoOpCollectionStorage),
        Arc::new(EqOnlyFilterMatcher),
    )
    .await
    .unwrap();

    // Two-stream PushLog ingress intentionally trusts the transport enough
    // to accept the block, but document ACP is still enforced by the merge
    // handler. A merge-time denial must leave the block unmerged so it can
    // be retried only if policy state changes.
    let handler = TestMergeHandler::new(false, false);
    let result = handle_block_received(
        &coordinator,
        &handler,
        &ReplicationConfig::default(),
        cid,
        BlockMetadata::normal(
            "doc1",
            "collection1",
            "did:key:z6MkrUnauthorizedPush",
            Some("unauthorized-peer"),
            false,
        ),
    )
    .await;

    assert!(matches!(result, ReplicationResult::Failed { .. }));
    assert_eq!(handler.calls(), 1);
    assert!(
        !blockstore.is_merged(&cid).await.unwrap(),
        "merge-time ACP denial must not mark a two-stream PushLog block as merged"
    );
}

#[tokio::test]
async fn test_recovery_forwards_handler_recovered_metadata() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let cid = test_cid();
    blockstore.put(&cid, b"test data").await.unwrap();

    let (coordinator, _events) = crate::sync::coordinator::SyncCoordinator::with_access_control(
        NoopTransport::new(),
        blockstore,
        crate::sync::SyncConfig::default(),
        AccessMode::Open,
        Arc::new(crate::ReplicatorRegistry::new()),
        Arc::new(crate::sync::collection_store::NoOpCollectionStorage),
        Arc::new(EqOnlyFilterMatcher),
    )
    .await
    .unwrap();

    let handler = RecoveryMetadataHandler::new();
    let result = handle_block_received(
        &coordinator,
        &handler,
        &ReplicationConfig::default(),
        cid,
        BlockMetadata::recovery(),
    )
    .await;

    assert!(matches!(result, ReplicationResult::Merged { .. }));
    assert_eq!(handler.recover_calls(), 1);
    assert_eq!(handler.handle_calls(), 1);
}

#[tokio::test]
async fn test_pushlog_dag_needs_fetch_uses_poll_fetcher_when_sender_known() {
    use defra_core::{Block, CompositeDeltaPayload, CrdtDelta, DAGLink, LwwDeltaPayload};

    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));

    let child_block = Block::new(
        CrdtDelta::Lww(LwwDeltaPayload {
            field_name: "name".to_string(),
            priority: 1,
            schema_version_id: "schema1".to_string(),
            data: b"value".to_vec(),
        }),
        vec![],
        vec![],
    );
    let child_data = child_block.to_dag_cbor().unwrap();
    let child_cid = child_block.generate_cid().unwrap();

    let root_block = Block::new(
        CrdtDelta::Composite(CompositeDeltaPayload {
            schema_version_id: "schema1".to_string(),
            priority: 1,
            status: 1,
        }),
        vec![],
        vec![DAGLink::new("name", child_cid)],
    );
    let root_data = root_block.to_dag_cbor().unwrap();
    let root_cid = root_block.generate_cid().unwrap();

    blockstore.put(&root_cid, &root_data).await.unwrap();

    let transport = PollFetchTransport::new(blockstore.clone(), child_cid, child_data);
    let transport_handle = transport.clone();
    let (coordinator, mut events) = crate::sync::coordinator::SyncCoordinator::with_access_control(
        transport,
        blockstore.clone(),
        crate::sync::SyncConfig::default(),
        AccessMode::Open,
        Arc::new(crate::ReplicatorRegistry::new()),
        Arc::new(crate::sync::collection_store::NoOpCollectionStorage),
        Arc::new(EqOnlyFilterMatcher),
    )
    .await
    .unwrap();
    transport_handle.set_sync_completion(coordinator.manager().block_sync_completion_tracker());

    coordinator
        .handle_transport_event(TransportEvent::TwoStreamRequest {
            peer_id: PeerId::new("source-peer".to_string()),
            request: PushLogRequest::new(
                "doc1".to_string(),
                bytes::Bytes::from(root_cid.to_bytes()),
                "col1".to_string(),
                "creator-1".to_string(),
                bytes::Bytes::from(root_data),
            ),
            token: None,
            is_explicit_replicator: true,
            explicit_replay_authorization: None,
        })
        .await
        .unwrap();

    assert_eq!(
        coordinator.dispatch_due_pending_dag_fetches_for_test(tokio::time::Instant::now()),
        1
    );

    let dag_needs_fetch = tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .expect("DagNeedsFetch should arrive")
        .expect("event should be present");

    assert_eq!(coordinator.pending_dag_count(), 1);

    let result = process_event(
        &coordinator,
        dag_needs_fetch,
        &TestMergeHandler::new(true, false),
        &ReplicationConfig::default(),
    )
    .await;

    assert!(matches!(
        result,
        ReplicationResult::DagFetchStarted { root_cid: cid } if cid == root_cid
    ));

    let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .expect("DagReady should arrive")
        .expect("event should be present");

    match &event {
        SyncEvent::DagReady {
            root_cid: cid,
            doc_id,
            collection_id,
            creator,
            sender_peer,
            is_explicit_replicator,
            ..
        } => {
            assert_eq!(*cid, root_cid);
            assert_eq!(doc_id, "doc1");
            assert_eq!(collection_id, "col1");
            assert_eq!(creator, "creator-1");
            assert_eq!(sender_peer.as_deref(), Some("source-peer"));
            assert!(
                *is_explicit_replicator,
                "poll fetcher should preserve push-driven explicit replicator trust"
            );
        }
        other => panic!("expected DagReady, got {:?}", other),
    }
    assert_eq!(transport_handle.sync_blocks_calls(), 1);
    assert!(blockstore.has(&child_cid).await.unwrap());

    let merge_result = process_event(
        &coordinator,
        event,
        &TestMergeHandler::new(true, false),
        &ReplicationConfig::default(),
    )
    .await;

    assert!(matches!(
        merge_result,
        ReplicationResult::Merged { cid, .. } if cid == root_cid
    ));
    assert_eq!(coordinator.pending_dag_count(), 0);
}

#[derive(Debug)]
struct ReceiverOwnershipArm {
    pushlogs_scheduled: usize,
    pushlogs_transmitted: usize,
    announced_bytes: usize,
    pending_high_water: usize,
    persisted_pending_high_water: usize,
    car_requests: usize,
    car_requested_cids: usize,
    car_present_blocks: usize,
    car_served_blocks: usize,
    car_filtered_blocks: usize,
    car_served_bytes: usize,
    selective_requests: usize,
    selective_requested_cids: usize,
    selective_present_blocks: usize,
    selective_served_blocks: usize,
    selective_served_bytes: usize,
    provider_rotations: u64,
    sender_retry_dispatches: u64,
    retry_dispatches: u64,
    retry_suppressions: u64,
    exhausted_roots: u64,
    capacity_nacks: u64,
    registered_terminal: u64,
    merged_terminal: u64,
    quarantined_terminal: u64,
    pending_at_quiescence: usize,
    persisted_pending_at_quiescence: usize,
    retained_handles_at_quiescence: usize,
    source_has_current_head: bool,
    receiver_had_current_head_after_first_wave: bool,
    receiver_has_current_head: bool,
}

/// Run one frozen sender-ownership arm through the real receiving coordinator.
///
/// The full-DAG arm presents a legacy field descendant and then the composite
/// document head before the collection head. The receiver stores the field
/// without treating it as a head, but the valid composite dependency still
/// saturates a one-root admission bound before the collection hint arrives.
/// The head-hint arm presents only the collection root and lets the CAR fetcher
/// acquire the entire DAG under that one durable obligation.
async fn run_receiver_ownership_arm(expand_dag: bool) -> ReceiverOwnershipArm {
    use crate::sync::pending_store::{PendingDagStorage, PendingDagStore};
    use defra_core::{
        Block, CollectionDeltaPayload, CompositeDeltaPayload, CrdtDelta, DAGLink, LwwDeltaPayload,
    };

    let signature_data = defra_core::cbor::to_vec(&"signature-metadata").unwrap();
    let signature_cid = defra_core::block::generate_cid_from_bytes(&signature_data).unwrap();
    let field = Block::new_with_options(
        CrdtDelta::Lww(LwwDeltaPayload {
            field_name: "value".to_string(),
            priority: 1,
            schema_version_id: "schema".to_string(),
            data: b"current".to_vec(),
        }),
        vec![],
        vec![],
        None,
        Some(signature_cid),
    );
    let field_data = field.to_dag_cbor().unwrap();
    let field_cid = field.generate_cid().unwrap();
    let composite = Block::new(
        CrdtDelta::Composite(CompositeDeltaPayload {
            schema_version_id: "schema".to_string(),
            priority: 1,
            status: 1,
        }),
        vec![],
        vec![DAGLink::new("value", field_cid)],
    );
    let composite_data = composite.to_dag_cbor().unwrap();
    let composite_cid = composite.generate_cid().unwrap();
    let root = Block::new(
        CrdtDelta::Collection(CollectionDeltaPayload {
            schema_version_id: "schema".to_string(),
            priority: 1,
        }),
        vec![],
        vec![DAGLink::new("doc", composite_cid)],
    );
    let root_data = root.to_dag_cbor().unwrap();
    let root_cid = root.generate_cid().unwrap();

    let source_store = Arc::new(RegolithStore::in_memory().unwrap());
    let source = Arc::new(DefraBlockstore::new(source_store, true));
    source.put(&signature_cid, &signature_data).await.unwrap();
    source.put(&field_cid, &field_data).await.unwrap();
    source.put(&composite_cid, &composite_data).await.unwrap();
    source.put(&root_cid, &root_data).await.unwrap();
    source.mark_as_merged(&root_cid).await.unwrap();

    let receiver_store = Arc::new(RegolithStore::in_memory().unwrap());
    let receiver = Arc::new(DefraBlockstore::new(receiver_store, true));
    let transport = PollFetchTransport::with_car_source(receiver.clone(), source.clone());
    let transport_handle = transport.clone();
    let config = crate::sync::SyncConfig {
        max_pending_dags: 1,
        ..Default::default()
    };
    let (coordinator, mut events) = crate::sync::coordinator::SyncCoordinator::with_access_control(
        transport,
        receiver.clone(),
        config,
        AccessMode::Open,
        Arc::new(crate::ReplicatorRegistry::new()),
        Arc::new(crate::sync::collection_store::NoOpCollectionStorage),
        Arc::new(EqOnlyFilterMatcher),
    )
    .await
    .unwrap();
    transport_handle.set_sync_completion(coordinator.manager().block_sync_completion_tracker());
    let pending_store = Arc::new(PendingDagStore::new(Arc::new(
        RegolithStore::in_memory().unwrap(),
    )));
    coordinator
        .install_pending_dag_store(pending_store.clone())
        .await;

    let peer_id = PeerId::new("source-peer".to_string());
    let request = |doc_id: &str, cid: Cid, data: Vec<u8>| TransportEvent::TwoStreamRequest {
        peer_id: peer_id.clone(),
        request: PushLogRequest::new(
            doc_id.to_string(),
            bytes::Bytes::from(cid.to_bytes()),
            "collection".to_string(),
            "creator".to_string(),
            bytes::Bytes::from(data),
        ),
        token: None,
        is_explicit_replicator: true,
        explicit_replay_authorization: None,
    };

    let mut announced_bytes = 0usize;
    let mut pushlogs_transmitted = 0usize;
    let first_event = if expand_dag {
        announced_bytes += field_data.len();
        pushlogs_transmitted += 1;
        coordinator
            .handle_transport_event(request("doc", field_cid, field_data.clone()))
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(25), events.recv())
                .await
                .is_err(),
            "legacy field dependency must not become a pending head"
        );
        announced_bytes += composite_data.len();
        pushlogs_transmitted += 1;
        coordinator
            .handle_transport_event(request("doc", composite_cid, composite_data.clone()))
            .await
            .unwrap();
        assert_eq!(
            coordinator.dispatch_due_pending_dag_fetches_for_test(tokio::time::Instant::now()),
            1
        );
        tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .expect("composite pending event should arrive")
            .expect("composite pending event should be present")
    } else {
        announced_bytes += root_data.len();
        pushlogs_transmitted += 1;
        coordinator
            .handle_transport_event(request("", root_cid, root_data.clone()))
            .await
            .unwrap();
        assert_eq!(
            coordinator.dispatch_due_pending_dag_fetches_for_test(tokio::time::Instant::now()),
            1
        );
        tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .expect("root pending event should arrive")
            .expect("root pending event should be present")
    };

    assert_eq!(coordinator.pending_dag_count(), 1);
    assert_eq!(coordinator.manager().persisted_pending_count(), 1);

    if expand_dag {
        announced_bytes += root_data.len();
        pushlogs_transmitted += 1;
        let error = coordinator
            .handle_transport_event(request("", root_cid, root_data.clone()))
            .await
            .expect_err("full-DAG feedback must hit the fixed admission bound");
        assert!(matches!(
            error,
            crate::error::Error::PendingDagCapacity { max: 1 }
        ));
    }

    let handler = TestMergeHandler::new(true, false);
    let started = process_event(
        &coordinator,
        first_event,
        &handler,
        &ReplicationConfig::default(),
    )
    .await;
    assert!(matches!(started, ReplicationResult::DagFetchStarted { .. }));
    let ready = tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .expect("CAR completion should emit DagReady")
        .expect("DagReady should be present");
    let merged = process_event(&coordinator, ready, &handler, &ReplicationConfig::default()).await;
    assert!(matches!(merged, ReplicationResult::Merged { .. }));

    let receiver_had_current_head_after_first_wave = receiver.is_merged(&root_cid).await.unwrap();
    let mut sender_retry_dispatches = 0;

    // The frozen full-DAG sender retains its logical-head marker after the
    // actionable capacity nack. Once the composite obligation drains, exercise
    // that marker's retry/re-offer path and require the same final state as the
    // head-hint arm. This establishes amplification and an avoidable durable
    // retry cycle without claiming that a fair old sender can never recover.
    if expand_dag {
        sender_retry_dispatches += 1;
        announced_bytes += root_data.len();
        pushlogs_transmitted += 1;
        coordinator
            .handle_transport_event(request("", root_cid, root_data.clone()))
            .await
            .expect("sender retry should re-offer the nacked logical head");
        let _ = coordinator.dispatch_due_pending_dag_fetches_for_test(tokio::time::Instant::now());
        let retry_pending = tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .expect("retried root pending event should arrive")
            .expect("retried root pending event should be present");
        let retry_started = process_event(
            &coordinator,
            retry_pending,
            &handler,
            &ReplicationConfig::default(),
        )
        .await;
        match retry_started {
            // The first composite-root recovery already acquired the shared
            // dependency frontier, so the collection re-offer normally merges
            // without another CAR owner.
            ReplicationResult::Merged { .. } => {}
            ReplicationResult::DagFetchStarted { .. } => {
                let retry_ready = tokio::time::timeout(Duration::from_secs(1), events.recv())
                    .await
                    .expect("retried CAR completion should emit DagReady")
                    .expect("retried DagReady should be present");
                let retry_merged = process_event(
                    &coordinator,
                    retry_ready,
                    &handler,
                    &ReplicationConfig::default(),
                )
                .await;
                assert!(matches!(retry_merged, ReplicationResult::Merged { .. }));
            }
            other => panic!("retried logical head did not converge: {other:?}"),
        }
    }

    let persisted = pending_store.load_all().await.unwrap();
    coordinator.shutdown().await;
    let status = coordinator.sync_status();
    ReceiverOwnershipArm {
        pushlogs_scheduled: if expand_dag { 4 } else { 1 },
        pushlogs_transmitted,
        announced_bytes,
        pending_high_water: status.pending_dag_high_water as usize,
        persisted_pending_high_water: status.persisted_pending_dag_high_water as usize,
        car_requests: transport_handle.car_request_calls(),
        car_requested_cids: transport_handle.car_requested_cids(),
        car_present_blocks: transport_handle.car_present_blocks(),
        car_served_blocks: transport_handle.car_served_blocks(),
        car_filtered_blocks: transport_handle.car_filtered_blocks(),
        car_served_bytes: transport_handle.car_served_bytes(),
        selective_requests: transport_handle.sync_blocks_calls(),
        selective_requested_cids: transport_handle.sync_requested_cids(),
        selective_present_blocks: transport_handle.sync_present_blocks(),
        selective_served_blocks: transport_handle.sync_served_blocks(),
        selective_served_bytes: transport_handle.sync_served_bytes(),
        provider_rotations: status.provider_rotations,
        sender_retry_dispatches,
        retry_dispatches: status.pending_dag_retry_dispatched,
        retry_suppressions: status.pending_dag_retry_suppressed,
        exhausted_roots: status.pending_dag_fetch_exhausted,
        capacity_nacks: status.pending_dag_capacity_shed,
        registered_terminal: status.pending_dag_registered,
        merged_terminal: status.pending_dag_terminal_merged,
        quarantined_terminal: status.pending_dag_terminal_quarantined,
        pending_at_quiescence: status.pending_dags,
        persisted_pending_at_quiescence: persisted.len(),
        retained_handles_at_quiescence: status.retained_background_tasks,
        source_has_current_head: source.is_merged(&root_cid).await.unwrap(),
        receiver_had_current_head_after_first_wave,
        receiver_has_current_head: receiver.is_merged(&root_cid).await.unwrap(),
    }
}

/// Deterministic ownership A/B with the same two-peer topology, logical DAG,
/// admission bound, and CAR transport. The frozen full-DAG sender produces a
/// capacity nack and misses the logical head on the first wave. Its retained
/// sender marker then retries the head and eventually drains. One head hint
/// completes on the first wave with no sender retry cycle.
#[tokio::test]
async fn ownership_ab_full_dag_amplifies_admission_and_requires_sender_retry() {
    let full_dag = run_receiver_ownership_arm(true).await;
    let head_hint = run_receiver_ownership_arm(false).await;

    assert_eq!(full_dag.pushlogs_scheduled, 4);
    assert_eq!(head_hint.pushlogs_scheduled, 1);
    assert_eq!(full_dag.pushlogs_transmitted, 4);
    assert_eq!(head_hint.pushlogs_transmitted, 1);
    assert!(full_dag.announced_bytes > head_hint.announced_bytes);
    assert_eq!(full_dag.pending_high_water, 1);
    assert_eq!(head_hint.pending_high_water, 1);
    assert_eq!(full_dag.persisted_pending_high_water, 1);
    assert_eq!(head_hint.persisted_pending_high_water, 1);
    assert_eq!(full_dag.car_requests, 0);
    assert_eq!(head_hint.car_requests, 0);
    assert_eq!(full_dag.car_requested_cids, 0);
    assert_eq!(head_hint.car_requested_cids, 0);
    assert_eq!(full_dag.car_present_blocks, 0);
    assert_eq!(head_hint.car_present_blocks, 0);
    assert_eq!(full_dag.car_served_blocks, 0);
    assert_eq!(head_hint.car_served_blocks, 0);
    assert_eq!(full_dag.car_filtered_blocks, 0);
    assert_eq!(head_hint.car_filtered_blocks, 0);
    assert_eq!(full_dag.car_served_bytes, 0);
    assert_eq!(head_hint.car_served_bytes, 0);
    assert_eq!(full_dag.selective_requests, 1);
    assert_eq!(head_hint.selective_requests, 3);
    assert_eq!(full_dag.selective_requested_cids, 1);
    assert_eq!(head_hint.selective_requested_cids, 3);
    assert_eq!(full_dag.selective_present_blocks, 1);
    assert_eq!(head_hint.selective_present_blocks, 3);
    assert_eq!(full_dag.selective_served_blocks, 1);
    assert_eq!(head_hint.selective_served_blocks, 3);
    assert!(full_dag.selective_served_bytes > 0);
    assert!(head_hint.selective_served_bytes > full_dag.selective_served_bytes);
    assert_eq!(full_dag.provider_rotations, 0);
    assert_eq!(head_hint.provider_rotations, 0);
    assert_eq!(full_dag.sender_retry_dispatches, 1);
    assert_eq!(head_hint.sender_retry_dispatches, 0);
    assert_eq!(full_dag.retry_dispatches, 2);
    assert_eq!(head_hint.retry_dispatches, 1);
    assert_eq!(full_dag.retry_suppressions, 0);
    assert_eq!(head_hint.retry_suppressions, 0);
    assert_eq!(full_dag.exhausted_roots, 0);
    assert_eq!(head_hint.exhausted_roots, 0);
    assert_eq!(full_dag.capacity_nacks, 1);
    assert_eq!(head_hint.capacity_nacks, 0);
    assert_eq!(full_dag.registered_terminal, 2);
    assert_eq!(head_hint.registered_terminal, 1);
    assert_eq!(full_dag.merged_terminal, 2);
    assert_eq!(head_hint.merged_terminal, 1);
    assert_eq!(full_dag.quarantined_terminal, 0);
    assert_eq!(head_hint.quarantined_terminal, 0);
    assert_eq!(full_dag.pending_at_quiescence, 0);
    assert_eq!(head_hint.pending_at_quiescence, 0);
    assert_eq!(full_dag.persisted_pending_at_quiescence, 0);
    assert_eq!(head_hint.persisted_pending_at_quiescence, 0);
    assert_eq!(full_dag.retained_handles_at_quiescence, 0);
    assert_eq!(head_hint.retained_handles_at_quiescence, 0);
    assert!(full_dag.source_has_current_head);
    assert!(head_hint.source_has_current_head);
    assert!(!full_dag.receiver_had_current_head_after_first_wave);
    assert!(head_hint.receiver_had_current_head_after_first_wave);
    assert!(full_dag.receiver_has_current_head);
    assert!(head_hint.receiver_has_current_head);
}

#[tokio::test]
async fn test_replication_result_merged_but_not_marked() {
    // Test that MergedButNotMarked is a distinct result type
    let cid = test_cid();
    let result = ReplicationResult::MergedButNotMarked {
        cid,
        error: "mark_as_merged failed".to_string(),
    };

    // Verify the result contains the expected data
    match result {
        ReplicationResult::MergedButNotMarked { cid: c, error } => {
            assert_eq!(c, cid);
            assert!(error.contains("mark_as_merged"));
        }
        _ => panic!("Expected MergedButNotMarked"),
    }
}

#[tokio::test]
async fn test_replication_result_merged_but_broadcast_failed() {
    // Test that MergedButBroadcastFailed is a distinct result type
    let cid = test_cid();
    let result = ReplicationResult::MergedButBroadcastFailed {
        cid,
        doc_id: "doc123".to_string(),
        collection_id: "col1".to_string(),
        broadcast_error: "no peers connected".to_string(),
    };

    // Verify the result contains the expected data
    match result {
        ReplicationResult::MergedButBroadcastFailed {
            cid: c,
            doc_id,
            collection_id,
            broadcast_error,
        } => {
            assert_eq!(c, cid);
            assert_eq!(doc_id, "doc123");
            assert_eq!(collection_id, "col1");
            assert!(broadcast_error.contains("no peers"));
        }
        _ => panic!("Expected MergedButBroadcastFailed"),
    }
}

#[tokio::test]
async fn test_run_serializes_duplicate_cids() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let cid = test_cid();
    blockstore.put(&cid, b"test data").await.unwrap();

    let (coordinator, _events) = crate::sync::coordinator::SyncCoordinator::with_access_control(
        NoopTransport::new(),
        blockstore,
        crate::sync::SyncConfig::default(),
        AccessMode::Open,
        Arc::new(crate::ReplicatorRegistry::new()),
        Arc::new(crate::sync::collection_store::NoOpCollectionStorage),
        Arc::new(EqOnlyFilterMatcher),
    )
    .await
    .unwrap();

    let (tx, rx) = mpsc::channel(2);
    for _ in 0..2 {
        tx.send(SyncEvent::BlockReceived {
            cid,
            doc_id: "doc1".to_string(),
            collection_id: "col1".to_string(),
            creator: "peer1".to_string(),
            sender_peer: Some("sender1".to_string()),
            is_explicit_replicator: true,
            explicit_replay_authorization: None,
        })
        .await
        .unwrap();
    }
    drop(tx);

    let handler = Arc::new(SlowMergeHandler::new());
    let (result_tx, mut result_rx) = mpsc::unbounded_channel();

    ReplicationLoop::run(
        Arc::new(coordinator),
        rx,
        handler.clone(),
        ReplicationConfig::default(),
        move |result| {
            let _ = result_tx.send(result.clone());
        },
    )
    .await;

    let mut results = Vec::new();
    for _ in 0..2 {
        results.push(
            tokio::time::timeout(Duration::from_secs(1), result_rx.recv())
                .await
                .expect("result should arrive")
                .expect("result channel should remain open"),
        );
    }

    assert_eq!(handler.calls(), 1, "duplicate CID should merge once");
    assert_eq!(
        handler.max_in_flight(),
        1,
        "duplicate CID merges must not overlap"
    );
    assert!(results
        .iter()
        .any(|result| matches!(result, ReplicationResult::Merged { cid: c, .. } if *c == cid)));
    assert!(results.iter().any(|result| {
        matches!(
            result,
            ReplicationResult::Skipped {
                cid: c,
                terminal: true,
                reason,
                ..
            } if *c == cid && reason == "already merged"
        )
    }));
}

// =========================================================================
// Batch merge tests
// =========================================================================

#[tokio::test]
async fn test_run_uses_one_observable_batch_merge_pipeline() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let (coordinator, _events) = crate::sync::coordinator::SyncCoordinator::with_access_control(
        NoopTransport::new(),
        blockstore.clone(),
        crate::sync::SyncConfig::default(),
        AccessMode::Open,
        Arc::new(crate::ReplicatorRegistry::new()),
        Arc::new(crate::sync::collection_store::NoOpCollectionStorage),
        Arc::new(EqOnlyFilterMatcher),
    )
    .await
    .unwrap();

    let (tx, rx) = mpsc::channel(2);
    for i in 0..2 {
        let data = format!("canonical-batch-{i}");
        let cid = make_cid(data.as_bytes());
        blockstore.put(&cid, data.as_bytes()).await.unwrap();
        tx.send(SyncEvent::BlockReceived {
            cid,
            doc_id: format!("doc{i}"),
            collection_id: "col1".to_string(),
            creator: "peer1".to_string(),
            sender_peer: None,
            is_explicit_replicator: false,
            explicit_replay_authorization: None,
        })
        .await
        .unwrap();
    }
    drop(tx);

    let handler = Arc::new(BatchTestHandler::new());
    let observed_merges = Arc::new(AtomicUsize::new(0));
    let observed_merges_for_loop = observed_merges.clone();
    ReplicationLoop::run(
        Arc::new(coordinator),
        rx,
        handler.clone(),
        ReplicationConfig::default(),
        move |result| {
            if matches!(result, ReplicationResult::Merged { .. }) {
                observed_merges_for_loop.fetch_add(1, Ordering::SeqCst);
            }
        },
    )
    .await;

    assert_eq!(handler.batch_calls(), 1);
    assert_eq!(handler.batch_block_count(), 2);
    assert_eq!(handler.per_block_calls(), 0);
    assert_eq!(observed_merges.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_process_next_batch_caps_drain_at_config_batch_size() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let (coordinator, _events) = crate::sync::coordinator::SyncCoordinator::with_access_control(
        NoopTransport::new(),
        blockstore.clone(),
        crate::sync::SyncConfig::default(),
        AccessMode::Open,
        Arc::new(crate::ReplicatorRegistry::new()),
        Arc::new(crate::sync::collection_store::NoOpCollectionStorage),
        Arc::new(EqOnlyFilterMatcher),
    )
    .await
    .unwrap();

    let (tx, mut rx) = mpsc::channel(16);
    for i in 0..10 {
        let data = format!("block{}", i);
        let cid = make_cid(data.as_bytes());
        blockstore.put(&cid, data.as_bytes()).await.unwrap();
        tx.send(SyncEvent::BlockReceived {
            cid,
            doc_id: format!("doc{}", i),
            collection_id: "col1".to_string(),
            creator: "peer1".to_string(),
            sender_peer: None,
            is_explicit_replicator: false,
            explicit_replay_authorization: None,
        })
        .await
        .unwrap();
    }

    let config = ReplicationConfig {
        continue_on_error: true,
        rebroadcast_on_merge: false,
        batch_size: 3,
    };
    let handler = BatchTestHandler::new();

    let results =
        ReplicationLoop::process_next_batch(&coordinator, &mut rx, &handler, &config).await;

    assert_eq!(results.len(), 3);
    assert_eq!(handler.batch_calls(), 1);
    assert_eq!(handler.batch_block_count(), 3);
    assert_eq!(rx.len(), 7, "remaining backlog must stay queued");
}

#[tokio::test]
async fn test_process_merge_batch_rebroadcasts_when_config_enabled() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let cid = make_cid(b"rebroadcast-batch");
    blockstore.put(&cid, b"rebroadcast-batch").await.unwrap();

    let transport = NoopTransport::new();
    let transport_handle = transport.clone();
    let (coordinator, _events) = crate::sync::coordinator::SyncCoordinator::with_access_control(
        transport,
        blockstore.clone(),
        crate::sync::SyncConfig::default(),
        AccessMode::Open,
        Arc::new(crate::ReplicatorRegistry::new()),
        Arc::new(crate::sync::collection_store::NoOpCollectionStorage),
        Arc::new(EqOnlyFilterMatcher),
    )
    .await
    .unwrap();

    let events = vec![SyncEvent::BlockReceived {
        cid,
        doc_id: "doc1".to_string(),
        collection_id: "col1".to_string(),
        creator: "peer1".to_string(),
        sender_peer: None,
        is_explicit_replicator: false,
        explicit_replay_authorization: None,
    }];
    let config = ReplicationConfig {
        continue_on_error: true,
        rebroadcast_on_merge: true,
        batch_size: 50,
    };
    let handler = BatchTestHandler::new();

    let results = process_merge_batch(&coordinator, events, &handler, &config).await;

    assert!(matches!(
        results.as_slice(),
        [ReplicationResult::Merged { cid: merged_cid, .. }] if *merged_cid == cid
    ));
    assert_eq!(
        transport_handle.publish_calls(),
        2,
        "document and collection topics should be rebroadcast"
    );
}

#[tokio::test]
async fn merged_head_forwards_to_configured_replicator_without_gossip_rebroadcast() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let cid = make_cid(b"forward-merged-head");
    let block = b"forward-merged-head";
    blockstore.put(&cid, block).await.unwrap();

    let transport = NoopTransport::new();
    let transport_handle = transport.clone();
    let downstream = PeerId::new("downstream-peer".to_string());
    transport_handle.set_replicators(vec![ReplicatorInfo::from_raw(
        downstream.to_string(),
        vec!["col1".to_string()],
        Vec::new(),
    )]);
    let (mut coordinator, _events) =
        crate::sync::coordinator::SyncCoordinator::with_access_control(
            transport,
            blockstore,
            crate::sync::SyncConfig::default(),
            AccessMode::Open,
            Arc::new(crate::ReplicatorRegistry::new()),
            Arc::new(crate::sync::collection_store::NoOpCollectionStorage),
            Arc::new(EqOnlyFilterMatcher),
        )
        .await
        .unwrap();
    let (failure_tx, mut failure_rx) = tokio::sync::mpsc::channel(8);
    coordinator.set_failure_channel(failure_tx);
    tokio::spawn(async move {
        while let Some(mut event) = failure_rx.recv().await {
            if let Some(durable_tx) = event.durable_tx.take() {
                let _ = durable_tx.send(true);
            }
        }
    });

    let events = vec![SyncEvent::BlockReceived {
        cid,
        doc_id: "doc1".to_string(),
        collection_id: "col1".to_string(),
        creator: "did:key:creator".to_string(),
        sender_peer: Some("upstream-peer".to_string()),
        is_explicit_replicator: true,
        explicit_replay_authorization: None,
    }];
    let results = process_merge_batch(
        &coordinator,
        events,
        &BatchTestHandler::new(),
        &ReplicationConfig::default(),
    )
    .await;

    assert!(matches!(
        results.as_slice(),
        [ReplicationResult::Merged { cid: merged_cid, .. }] if *merged_cid == cid
    ));
    tokio::time::timeout(Duration::from_secs(1), async {
        while transport_handle.pushlog_requests().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("merged head should be forwarded to the configured downstream replicator");

    let requests = transport_handle.pushlog_requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].0, downstream);
    assert_eq!(requests[0].1.cid.as_ref(), cid.to_bytes());
    assert_eq!(requests[0].1.block.as_ref(), block);
    assert_eq!(requests[0].1.creator, "did:key:creator");
    assert_eq!(transport_handle.publish_calls(), 0);
}

#[tokio::test]
async fn rapid_collection_commits_publish_only_the_current_head() {
    use defra_core::{Block, CompositeDeltaPayload, CrdtDelta};

    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let transport = NoopTransport::new();
    let transport_handle = transport.clone();
    let (coordinator, _events) = crate::sync::coordinator::SyncCoordinator::with_access_control(
        transport,
        blockstore,
        crate::sync::SyncConfig::default(),
        AccessMode::Open,
        Arc::new(crate::ReplicatorRegistry::new()),
        Arc::new(crate::sync::collection_store::NoOpCollectionStorage),
        Arc::new(EqOnlyFilterMatcher),
    )
    .await
    .unwrap();

    let head = |priority| {
        let block = Block::new(
            CrdtDelta::Composite(CompositeDeltaPayload {
                schema_version_id: "schema".to_string(),
                priority,
                status: 1,
            }),
            vec![],
            vec![],
        );
        let data = block.to_dag_cbor().unwrap();
        let cid = block.generate_cid().unwrap();
        (cid, data)
    };
    let first = head(1);
    let second = head(2);
    let current = head(3);

    let (first_result, second_result, current_result) = tokio::join!(
        coordinator.broadcast_local_update(&first.0, &first.1, "", "collection"),
        coordinator.broadcast_local_update(&second.0, &second.1, "", "collection"),
        coordinator.broadcast_local_update(&current.0, &current.1, "", "collection"),
    );
    assert!(first_result.is_ok());
    assert!(second_result.is_ok());
    assert!(current_result.is_ok());
    assert_eq!(
        transport_handle.publish_calls(),
        2,
        "one current collection head must be published once to its document and collection topics"
    );
    assert_eq!(coordinator.sync_status().broadcast_coalesced_total, 2);
}

#[tokio::test]
async fn test_batch_validates_explicit_replay_before_merge() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let cid = make_cid(b"auth-rejected");
    blockstore.put(&cid, b"auth-rejected").await.unwrap();
    let (coordinator, _events) = crate::sync::coordinator::SyncCoordinator::with_access_control(
        NoopTransport::new(),
        blockstore,
        crate::sync::SyncConfig::default(),
        AccessMode::Open,
        Arc::new(crate::ReplicatorRegistry::new()),
        Arc::new(crate::sync::collection_store::NoOpCollectionStorage),
        Arc::new(EqOnlyFilterMatcher),
    )
    .await
    .unwrap();

    let handler = RejectingAuthorizationHandler::new();
    let events = vec![SyncEvent::BlockReceived {
        cid,
        doc_id: "doc1".to_string(),
        collection_id: "col1".to_string(),
        creator: "did:key:authorizer".to_string(),
        sender_peer: Some("source-peer".to_string()),
        is_explicit_replicator: true,
        explicit_replay_authorization: Some(ExplicitReplayAuthorization {
            source_peer_id: "source-peer".to_string(),
            target_peer_id: "target-peer".to_string(),
            collection_id: "col1".to_string(),
            authorizer_did: "did:key:authorizer".to_string(),
            expires_at: u64::MAX,
            capability: None,
        }),
    }];

    let results = process_merge_batch(
        &coordinator,
        events,
        &handler,
        &ReplicationConfig::default(),
    )
    .await;

    assert!(matches!(
        results.as_slice(),
        [ReplicationResult::Failed { error, .. }] if error.contains("authorization rejected")
    ));
    assert_eq!(handler.validation_calls(), 1);
    assert_eq!(handler.batch_calls(), 0);
}

#[tokio::test]
async fn test_default_handle_block_batch_calls_per_block() {
    // The default handle_block_batch delegates to handle_block per block.
    let handler = TestMergeHandler::new(true, false);
    let blocks: Vec<MergeBlock> = (0..5)
        .map(|i| {
            let data = format!("block{}", i);
            MergeBlock {
                cid: make_cid(data.as_bytes()),
                block_data: bytes::Bytes::from(data.into_bytes()),
                doc_id: format!("doc{}", i),
                collection_id: "col1".to_string(),
                creator: "peer1".to_string(),
                sender_peer: None,
                is_explicit_replicator: false,
                explicit_replay_authorization: None,
                verified_creator: None,
            }
        })
        .collect();

    let results = handler.handle_block_batch(&blocks).await;

    assert_eq!(results.len(), 5);
    assert!(results.iter().all(|r| r.as_ref().unwrap().is_merged()));
    assert_eq!(
        handler.calls(),
        5,
        "default impl should call handle_block per block"
    );
}

#[tokio::test]
async fn test_batch_handler_receives_all_blocks_at_once() {
    // A custom handle_block_batch override gets all blocks in one call.
    let handler = BatchTestHandler::new();
    let blocks: Vec<MergeBlock> = (0..10)
        .map(|i| {
            let data = format!("block{}", i);
            MergeBlock {
                cid: make_cid(data.as_bytes()),
                block_data: bytes::Bytes::from(data.into_bytes()),
                doc_id: format!("doc{}", i),
                collection_id: "col1".to_string(),
                creator: "peer1".to_string(),
                sender_peer: None,
                is_explicit_replicator: false,
                explicit_replay_authorization: None,
                verified_creator: None,
            }
        })
        .collect();

    let results = handler.handle_block_batch(&blocks).await;

    assert_eq!(results.len(), 10);
    assert!(results.iter().all(|r| r.as_ref().unwrap().is_merged()));
    assert_eq!(handler.batch_calls(), 1, "batch should be called once");
    assert_eq!(
        handler.batch_block_count(),
        10,
        "batch should receive all 10 blocks"
    );
    assert_eq!(
        handler.per_block_calls(),
        0,
        "handle_block should NOT be called"
    );
}

#[tokio::test]
async fn test_batch_handler_partial_failure() {
    // When one block in a batch fails, the rest still get results.
    let handler = BatchTestHandler::with_failure_at(2);
    let blocks: Vec<MergeBlock> = (0..5)
        .map(|i| {
            let data = format!("block{}", i);
            MergeBlock {
                cid: make_cid(data.as_bytes()),
                block_data: bytes::Bytes::from(data.into_bytes()),
                doc_id: format!("doc{}", i),
                collection_id: "col1".to_string(),
                creator: "peer1".to_string(),
                sender_peer: None,
                is_explicit_replicator: false,
                explicit_replay_authorization: None,
                verified_creator: None,
            }
        })
        .collect();

    let results = handler.handle_block_batch(&blocks).await;

    assert_eq!(results.len(), 5);
    assert!(results[0].is_ok());
    assert!(results[1].is_ok());
    assert!(results[2].is_err());
    assert!(results[3].is_ok());
    assert!(results[4].is_ok());
}

#[tokio::test]
async fn test_batch_handler_empty_batch() {
    let handler = BatchTestHandler::new();
    let results = handler.handle_block_batch(&[]).await;

    assert!(results.is_empty());
    assert_eq!(handler.batch_calls(), 1, "batch called even when empty");
    assert_eq!(handler.batch_block_count(), 0);
}

#[tokio::test]
async fn test_batch_handler_single_block() {
    // Single-block batch should still go through handle_block_batch.
    let handler = BatchTestHandler::new();
    let blocks = vec![MergeBlock {
        cid: make_cid(b"single"),
        block_data: bytes::Bytes::from_static(b"single"),
        doc_id: "doc0".to_string(),
        collection_id: "col1".to_string(),
        creator: "peer1".to_string(),
        sender_peer: None,
        is_explicit_replicator: false,
        explicit_replay_authorization: None,
        verified_creator: None,
    }];

    let results = handler.handle_block_batch(&blocks).await;

    assert_eq!(results.len(), 1);
    assert!(results[0].as_ref().unwrap().is_merged());
    assert_eq!(handler.batch_calls(), 1);
    assert_eq!(handler.batch_block_count(), 1);
}

#[tokio::test]
async fn test_merge_block_metadata_preserved() {
    // Verify that MergeBlock fields are accessible to the batch handler.
    let handler = TestMergeHandler::new(true, false);
    let cid = make_cid(b"metadata-test");
    let blocks = vec![MergeBlock {
        cid,
        block_data: bytes::Bytes::from_static(b"test data"),
        doc_id: "my-doc".to_string(),
        collection_id: "my-collection".to_string(),
        creator: "my-peer".to_string(),
        sender_peer: None,
        is_explicit_replicator: false,
        explicit_replay_authorization: None,
        verified_creator: None,
    }];

    // The default impl passes metadata through to handle_block.
    // We can't inspect the metadata inside TestMergeHandler, but we can
    // verify the call succeeds and the block round-trips correctly.
    let results = handler.handle_block_batch(&blocks).await;
    assert_eq!(results.len(), 1);
    assert!(results[0].as_ref().unwrap().is_merged());
    assert_eq!(blocks[0].doc_id, "my-doc");
    assert_eq!(blocks[0].collection_id, "my-collection");
    assert_eq!(blocks[0].creator, "my-peer");
}

#[tokio::test]
async fn test_default_batch_propagates_errors() {
    // The default handle_block_batch should propagate per-block errors.
    let handler = TestMergeHandler::new(false, false); // all blocks fail
    let blocks: Vec<MergeBlock> = (0..3)
        .map(|i| {
            let data = format!("block{}", i);
            MergeBlock {
                cid: make_cid(data.as_bytes()),
                block_data: bytes::Bytes::from(data.into_bytes()),
                doc_id: format!("doc{}", i),
                collection_id: "col1".to_string(),
                creator: "peer1".to_string(),
                sender_peer: None,
                is_explicit_replicator: false,
                explicit_replay_authorization: None,
                verified_creator: None,
            }
        })
        .collect();

    let results = handler.handle_block_batch(&blocks).await;

    assert_eq!(results.len(), 3);
    assert!(results.iter().all(|r| r.is_err()));
    assert_eq!(
        handler.calls(),
        3,
        "default impl calls handle_block for each"
    );
}

#[tokio::test]
async fn test_default_batch_mixed_skip_and_merge() {
    // Test that skip outcomes are preserved through the batch.
    let handler = TestMergeHandler::new(true, true); // all blocks skip
    let blocks: Vec<MergeBlock> = (0..3)
        .map(|i| {
            let data = format!("block{}", i);
            MergeBlock {
                cid: make_cid(data.as_bytes()),
                block_data: bytes::Bytes::from(data.into_bytes()),
                doc_id: format!("doc{}", i),
                collection_id: "col1".to_string(),
                creator: "peer1".to_string(),
                sender_peer: None,
                is_explicit_replicator: false,
                explicit_replay_authorization: None,
                verified_creator: None,
            }
        })
        .collect();

    let results = handler.handle_block_batch(&blocks).await;

    assert_eq!(results.len(), 3);
    for result in &results {
        assert!(result.as_ref().unwrap().is_skipped());
    }
}
