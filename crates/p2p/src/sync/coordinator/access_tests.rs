//! Tests for coordinator access control (findings 03-21).
//!
//! Verifies that DocSync and BranchableSync handlers enforce access checks
//! in Controlled mode before processing requests.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::{future::Future, pin::Pin};

use blockstore::{Blockstore, DefraBlockstore, Error as BlockstoreError};
use cid::Cid;
use multihash_codetable::{Code, MultihashDigest};
use storage::backends::MemoryStore;
use tokio::time::timeout;

use crate::bitswap::{
    AccessMode, BlockAcpMeta, BlockClass, BlockClassifier, LateBoundServeAcp, ReplicatorRegistry,
};
use crate::error::Error;
use crate::message::{
    BranchableSyncReply, BranchableSyncRequest, CarFetchRequest, DocSyncItem, DocSyncReply,
    DocSyncRequest, PushLogBroadcast, PushLogReply, PushLogRequest,
};
use crate::sync::broadcaster::Broadcaster;
use crate::sync::collection_store::NoOpCollectionStorage;
use crate::sync::head_provider::{DocumentHeadProvider, NoOpHeadProvider};
use crate::sync::manager::{SyncConfig, SyncEvent, SyncManager};
use crate::sync::peer_state::PeerStateTracker;
use crate::sync::rate_limiter::PeerRateLimiter;
use crate::sync::SyncShutdownHandle;
use crate::topics::{DefraTopic, DOC_SYNC_TOPIC};
use crate::transport::{MessageId, P2PTransport, PeerAddr, PeerId, TransportEvent};
use crate::QueryId;
use crate::{ReplicationFilter, ReplicationFilters, ReplicatorInfo};
use async_trait::async_trait;
use parking_lot::RwLock;

use super::authorizer::RuntimeAuthorizer;
use super::{
    DagFetchLimiter, SyncAccessState, SyncCoordinator, SyncRuntime, SyncSubscriptionState,
    DEFAULT_MAX_CONCURRENT_DAG_FETCHES, DEFAULT_MAX_CONCURRENT_PUSH_TASKS,
    DEFAULT_MAX_DOC_SYNC_REQUEST_DOC_IDS,
};

type TestBlockstore = DefraBlockstore<MemoryStore>;
type TwoStreamHandler = Arc<
    dyn Fn(
            PeerId,
            PushLogRequest,
        ) -> Pin<Box<dyn Future<Output = crate::Result<PushLogReply>> + Send>>
        + Send
        + Sync,
>;
const BLOCK_DATA: &[u8] = b"block data";

struct StaticDataClassifier {
    collection_id: String,
}

#[async_trait]
impl BlockClassifier for StaticDataClassifier {
    async fn classify(&self, _cid: &Cid, _data: &[u8]) -> BlockClass {
        BlockClass::Data(BlockAcpMeta {
            collection_id: self.collection_id.clone(),
            is_branchable: false,
            policy: None,
            doc_ids: vec!["doc1".to_string()],
        })
    }
}

struct CollectionHeadClassifier {
    collection_id: String,
}

#[async_trait]
impl BlockClassifier for CollectionHeadClassifier {
    async fn classify(&self, _cid: &Cid, _data: &[u8]) -> BlockClass {
        BlockClass::Data(BlockAcpMeta {
            collection_id: self.collection_id.clone(),
            is_branchable: false,
            policy: None,
            // Collection commits are intentionally not mapped to one document.
            doc_ids: Vec::new(),
        })
    }
}

fn filtered_replicator_registry(peer: &PeerId, collection_id: &str) -> Arc<ReplicatorRegistry> {
    let mut filters = ReplicationFilters::new();
    filters.insert(
        collection_id.to_string(),
        ReplicationFilter::new("status", serde_json::json!("ready")),
    );
    let registry = Arc::new(ReplicatorRegistry::new());
    registry.set_replicator_info(ReplicatorInfo::from_raw_with_filters(
        peer.to_string(),
        vec![collection_id.to_string()],
        Vec::new(),
        filters,
    ));
    registry
}

fn create_test_coordinator(
    access_mode: AccessMode,
    replicators: Arc<ReplicatorRegistry>,
    peer_state: Arc<PeerStateTracker>,
) -> (
    SyncCoordinator<TestBlockstore, NoopTransport>,
    tokio::sync::mpsc::Receiver<crate::sync::manager::SyncEvent>,
) {
    create_test_coordinator_with_rate_limiter(
        access_mode,
        replicators,
        peer_state,
        Arc::new(PeerRateLimiter::default()),
    )
}

fn create_test_coordinator_with_rate_limiter(
    access_mode: AccessMode,
    replicators: Arc<ReplicatorRegistry>,
    peer_state: Arc<PeerStateTracker>,
    rate_limiter: Arc<PeerRateLimiter>,
) -> (
    SyncCoordinator<TestBlockstore, NoopTransport>,
    tokio::sync::mpsc::Receiver<crate::sync::manager::SyncEvent>,
) {
    create_test_coordinator_with_options(
        access_mode,
        replicators,
        peer_state,
        rate_limiter,
        SyncConfig::default(),
    )
}

fn create_test_coordinator_with_sync_config(
    access_mode: AccessMode,
    replicators: Arc<ReplicatorRegistry>,
    peer_state: Arc<PeerStateTracker>,
    sync_config: SyncConfig,
) -> (
    SyncCoordinator<TestBlockstore, NoopTransport>,
    tokio::sync::mpsc::Receiver<crate::sync::manager::SyncEvent>,
) {
    create_test_coordinator_with_options(
        access_mode,
        replicators,
        peer_state,
        Arc::new(PeerRateLimiter::default()),
        sync_config,
    )
}

fn create_test_coordinator_with_options(
    access_mode: AccessMode,
    replicators: Arc<ReplicatorRegistry>,
    peer_state: Arc<PeerStateTracker>,
    rate_limiter: Arc<PeerRateLimiter>,
    sync_config: SyncConfig,
) -> (
    SyncCoordinator<TestBlockstore, NoopTransport>,
    tokio::sync::mpsc::Receiver<crate::sync::manager::SyncEvent>,
) {
    // Most limiter tests exercise one path at a time; share the injected
    // limiter across gossip and request intake so either path sees it.
    let request_rate_limiter = rate_limiter.clone();
    create_test_coordinator_full(
        access_mode,
        replicators,
        peer_state,
        rate_limiter,
        request_rate_limiter,
        sync_config,
    )
}

fn create_test_coordinator_with_split_limiters(
    access_mode: AccessMode,
    replicators: Arc<ReplicatorRegistry>,
    peer_state: Arc<PeerStateTracker>,
    rate_limiter: Arc<PeerRateLimiter>,
    request_rate_limiter: Arc<PeerRateLimiter>,
) -> (
    SyncCoordinator<TestBlockstore, NoopTransport>,
    tokio::sync::mpsc::Receiver<crate::sync::manager::SyncEvent>,
) {
    create_test_coordinator_full(
        access_mode,
        replicators,
        peer_state,
        rate_limiter,
        request_rate_limiter,
        SyncConfig::default(),
    )
}

fn create_test_coordinator_full(
    access_mode: AccessMode,
    replicators: Arc<ReplicatorRegistry>,
    peer_state: Arc<PeerStateTracker>,
    rate_limiter: Arc<PeerRateLimiter>,
    request_rate_limiter: Arc<PeerRateLimiter>,
    sync_config: SyncConfig,
) -> (
    SyncCoordinator<TestBlockstore, NoopTransport>,
    tokio::sync::mpsc::Receiver<crate::sync::manager::SyncEvent>,
) {
    let transport = NoopTransport::new();
    let local_peer_id = transport.local_peer_id().to_string();
    let broadcaster = Broadcaster::new(transport.clone());
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    create_test_coordinator_with_blockstore(TestCoordinatorParams {
        sync_config,
        request_rate_limiter,
        access_mode,
        replicators,
        peer_state,
        transport,
        local_peer_id,
        broadcaster,
        blockstore,
        rate_limiter,
    })
}

/// Bundled constructor arguments for [`create_test_coordinator_with_blockstore`].
/// Grouped so the test helper stays under clippy's `too_many_arguments` budget
/// without hiding the list of inputs behind a builder pattern.
struct TestCoordinatorParams<B: Blockstore + 'static> {
    sync_config: SyncConfig,
    request_rate_limiter: Arc<PeerRateLimiter>,
    access_mode: AccessMode,
    replicators: Arc<ReplicatorRegistry>,
    peer_state: Arc<PeerStateTracker>,
    transport: NoopTransport,
    local_peer_id: String,
    broadcaster: Broadcaster<NoopTransport>,
    blockstore: Arc<B>,
    rate_limiter: Arc<PeerRateLimiter>,
}

fn create_test_coordinator_with_blockstore<B: Blockstore + 'static>(
    params: TestCoordinatorParams<B>,
) -> (
    SyncCoordinator<B, NoopTransport>,
    tokio::sync::mpsc::Receiver<crate::sync::manager::SyncEvent>,
) {
    create_test_coordinator_with_blockstore_and_head_provider(params, Arc::new(NoOpHeadProvider))
}

fn create_test_coordinator_with_blockstore_and_head_provider<B: Blockstore + 'static>(
    params: TestCoordinatorParams<B>,
    head_provider: Arc<dyn DocumentHeadProvider>,
) -> (
    SyncCoordinator<B, NoopTransport>,
    tokio::sync::mpsc::Receiver<crate::sync::manager::SyncEvent>,
) {
    let TestCoordinatorParams {
        sync_config,
        request_rate_limiter,
        access_mode,
        replicators,
        peer_state,
        transport,
        local_peer_id,
        broadcaster,
        blockstore,
        rate_limiter,
    } = params;

    let (manager, events) = SyncManager::new(blockstore, peer_state.clone(), sync_config);

    let authorizer = Arc::new(RuntimeAuthorizer::new(
        transport.clone(),
        Arc::clone(&peer_state),
        Arc::clone(&replicators),
        access_mode,
    ));

    let coordinator = SyncCoordinator {
        runtime: SyncRuntime {
            transport,
            broadcaster,
            failure_tx: Arc::new(parking_lot::Mutex::new(None)),
            dag_fetch_limiter: DagFetchLimiter::new(DEFAULT_MAX_CONCURRENT_DAG_FETCHES),
            push_backlog: crate::sync::push_backlog::PushBacklog::new(
                crate::sync::DEFAULT_PUSH_QUEUE_CAPACITY,
                crate::sync::DEFAULT_PUSH_QUEUE_BYTE_CAPACITY,
                crate::sync::DEFAULT_MAX_ACTIVE_PUSHES_PER_PEER,
                DEFAULT_MAX_CONCURRENT_PUSH_TASKS,
            ),
            broadcast_coalescer: Arc::new(
                crate::sync::broadcast_coalescer::BroadcastCoalescer::default(),
            ),
            push_fanout_coalescer: Arc::new(
                crate::sync::push_fanout_coalescer::PushFanoutCoalescer::default(),
            ),
            selective_car_access: Arc::new(
                super::selective_car_access::SelectiveCarAccess::default(),
            ),
            rate_limiter,
            request_rate_limiter,
            max_doc_sync_request_doc_ids: DEFAULT_MAX_DOC_SYNC_REQUEST_DOC_IDS,
            shutdown: SyncShutdownHandle::new(),
            filter_matcher: Arc::new(crate::replicator::EqOnlyFilterMatcher),
        },
        manager,
        access: SyncAccessState {
            peer_state,
            local_peer_id,
            access_mode,
            replicators,
            gossip_direction_filtered: std::sync::atomic::AtomicU64::new(0),
        },
        subscriptions: SyncSubscriptionState {
            subscribed_collections: Arc::new(tokio::sync::RwLock::new(
                std::collections::HashSet::new(),
            )),
            collection_store: Arc::new(NoOpCollectionStorage),
            head_provider,
        },
        authorizer,
        classifier: Arc::new(crate::bitswap::DefaultBlockClassifier),
        serve_acp: Arc::new(crate::bitswap::LateBoundServeAcp::new()),
        document_acp: std::sync::OnceLock::new(),
        #[cfg(feature = "libp2p-transport")]
        kms_transport: std::sync::OnceLock::new(),
        #[cfg(feature = "libp2p-transport")]
        pubsub_services: None,
    };

    (coordinator, events)
}

struct ConflictOnceBlockstore {
    inner: TestBlockstore,
    remaining_put_conflicts: AtomicUsize,
    put_attempts: AtomicUsize,
}

struct ObservedWriteGuard<'a> {
    active: &'a AtomicUsize,
}

impl Drop for ObservedWriteGuard<'_> {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Test blockstore that holds the first inbound write so a second transport
/// path has a deterministic chance to contend for the same CID.
struct SingleOwnerBlockstore {
    inner: TestBlockstore,
    write_calls: AtomicUsize,
    active_writes: AtomicUsize,
    max_active_writes: AtomicUsize,
    first_write_entered: tokio::sync::Semaphore,
    concurrent_write_entered: tokio::sync::Semaphore,
    release_first_write: tokio::sync::Semaphore,
}

impl SingleOwnerBlockstore {
    fn new() -> Self {
        Self {
            inner: DefraBlockstore::new(Arc::new(MemoryStore::new()), true),
            write_calls: AtomicUsize::new(0),
            active_writes: AtomicUsize::new(0),
            max_active_writes: AtomicUsize::new(0),
            first_write_entered: tokio::sync::Semaphore::new(0),
            concurrent_write_entered: tokio::sync::Semaphore::new(0),
            release_first_write: tokio::sync::Semaphore::new(0),
        }
    }

    async fn enter_write(&self) -> ObservedWriteGuard<'_> {
        let call = self.write_calls.fetch_add(1, Ordering::SeqCst);
        let active = self.active_writes.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active_writes.fetch_max(active, Ordering::SeqCst);
        if active > 1 {
            self.concurrent_write_entered.add_permits(1);
        }
        if call == 0 {
            self.first_write_entered.add_permits(1);
            self.release_first_write
                .acquire()
                .await
                .expect("test release semaphore remains open")
                .forget();
        }
        ObservedWriteGuard {
            active: &self.active_writes,
        }
    }

    fn max_active_writes(&self) -> usize {
        self.max_active_writes.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Blockstore for SingleOwnerBlockstore {
    async fn get(&self, cid: &Cid) -> blockstore::Result<Option<bytes::Bytes>> {
        self.inner.get(cid).await
    }

    async fn put(&self, cid: &Cid, data: &[u8]) -> blockstore::Result<()> {
        let _write = self.enter_write().await;
        self.inner.put(cid, data).await
    }

    async fn put_many(&self, blocks: &[(&Cid, &[u8])]) -> blockstore::Result<()> {
        let _write = self.enter_write().await;
        self.inner.put_many(blocks).await
    }

    async fn has(&self, cid: &Cid) -> blockstore::Result<bool> {
        self.inner.has(cid).await
    }

    async fn delete(&self, cid: &Cid) -> blockstore::Result<()> {
        self.inner.delete(cid).await
    }

    async fn get_size(&self, cid: &Cid) -> blockstore::Result<Option<usize>> {
        self.inner.get_size(cid).await
    }

    async fn all_cids(&self) -> blockstore::Result<Vec<Cid>> {
        self.inner.all_cids().await
    }

    fn hash_on_read(&self, enabled: bool) {
        self.inner.hash_on_read(enabled);
    }

    async fn is_merged(&self, cid: &Cid) -> blockstore::Result<bool> {
        self.inner.is_merged(cid).await
    }

    async fn mark_as_merged(&self, cid: &Cid) -> blockstore::Result<()> {
        self.inner.mark_as_merged(cid).await
    }

    async fn mark_batch_as_merged(&self, cids: &[Cid]) -> blockstore::Result<()> {
        self.inner.mark_batch_as_merged(cids).await
    }

    async fn get_unmerged(&self) -> blockstore::Result<Vec<Cid>> {
        self.inner.get_unmerged().await
    }
}

impl ConflictOnceBlockstore {
    fn new() -> Self {
        let store = Arc::new(MemoryStore::new());
        Self {
            inner: DefraBlockstore::new(store, true),
            remaining_put_conflicts: AtomicUsize::new(1),
            put_attempts: AtomicUsize::new(0),
        }
    }

    fn put_attempts(&self) -> usize {
        self.put_attempts.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Blockstore for ConflictOnceBlockstore {
    async fn get(&self, cid: &Cid) -> blockstore::Result<Option<bytes::Bytes>> {
        self.inner.get(cid).await
    }

    async fn put(&self, cid: &Cid, data: &[u8]) -> blockstore::Result<()> {
        self.put_attempts.fetch_add(1, Ordering::SeqCst);
        if self
            .remaining_put_conflicts
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                if remaining > 0 {
                    Some(remaining - 1)
                } else {
                    None
                }
            })
            .is_ok()
        {
            return Err(BlockstoreError::Storage(
                storage::corekv::Error::TxnConflict,
            ));
        }

        self.inner.put(cid, data).await
    }

    async fn put_many(&self, blocks: &[(&Cid, &[u8])]) -> blockstore::Result<()> {
        self.inner.put_many(blocks).await
    }

    async fn has(&self, cid: &Cid) -> blockstore::Result<bool> {
        self.inner.has(cid).await
    }

    async fn delete(&self, cid: &Cid) -> blockstore::Result<()> {
        self.inner.delete(cid).await
    }

    async fn get_size(&self, cid: &Cid) -> blockstore::Result<Option<usize>> {
        self.inner.get_size(cid).await
    }

    async fn all_cids(&self) -> blockstore::Result<Vec<Cid>> {
        self.inner.all_cids().await
    }

    fn hash_on_read(&self, enabled: bool) {
        self.inner.hash_on_read(enabled);
    }

    async fn is_merged(&self, cid: &Cid) -> blockstore::Result<bool> {
        self.inner.is_merged(cid).await
    }

    async fn mark_as_merged(&self, cid: &Cid) -> blockstore::Result<()> {
        self.inner.mark_as_merged(cid).await
    }

    async fn mark_batch_as_merged(&self, cids: &[Cid]) -> blockstore::Result<()> {
        self.inner.mark_batch_as_merged(cids).await
    }

    async fn get_unmerged(&self) -> blockstore::Result<Vec<Cid>> {
        self.inner.get_unmerged().await
    }
}

#[derive(Clone)]
struct NoopTransport {
    peer_id: PeerId,
    pubkey: Vec<u8>,
    replicators: Arc<RwLock<std::collections::HashMap<String, Vec<String>>>>,
    connected_peers: Arc<RwLock<Vec<PeerId>>>,
    doc_sync_replies: Arc<RwLock<Vec<DocSyncReply>>>,
    car_responses: Arc<RwLock<Vec<Vec<u8>>>>,
    pushlog_replies: Arc<RwLock<Vec<crate::message::PushLogReply>>>,
    pushlog_response_tokens: Arc<RwLock<Vec<usize>>>,
    two_stream_replies: Arc<RwLock<Vec<crate::message::PushLogReply>>>,
    two_stream_handler: Arc<RwLock<Option<TwoStreamHandler>>>,
    branchable_replies: Arc<RwLock<Vec<BranchableSyncReply>>>,
    car_requests: Arc<RwLock<Vec<Cid>>>,
}

impl NoopTransport {
    fn new() -> Self {
        Self {
            peer_id: PeerId::new("local-peer".to_string()),
            pubkey: vec![1, 2, 3],
            replicators: Arc::new(RwLock::new(std::collections::HashMap::new())),
            connected_peers: Arc::new(RwLock::new(Vec::new())),
            doc_sync_replies: Arc::new(RwLock::new(Vec::new())),
            car_responses: Arc::new(RwLock::new(Vec::new())),
            pushlog_replies: Arc::new(RwLock::new(Vec::new())),
            pushlog_response_tokens: Arc::new(RwLock::new(Vec::new())),
            two_stream_replies: Arc::new(RwLock::new(Vec::new())),
            two_stream_handler: Arc::new(RwLock::new(None)),
            branchable_replies: Arc::new(RwLock::new(Vec::new())),
            car_requests: Arc::new(RwLock::new(Vec::new())),
        }
    }

    fn set_connected_peers(&self, peers: Vec<PeerId>) {
        *self.connected_peers.write() = peers;
    }

    fn doc_sync_replies(&self) -> Vec<DocSyncReply> {
        self.doc_sync_replies.read().clone()
    }

    fn pushlog_replies(&self) -> Vec<crate::message::PushLogReply> {
        self.pushlog_replies.read().clone()
    }

    fn pushlog_response_tokens(&self) -> Vec<usize> {
        self.pushlog_response_tokens.read().clone()
    }

    fn two_stream_replies(&self) -> Vec<crate::message::PushLogReply> {
        self.two_stream_replies.read().clone()
    }

    fn set_two_stream_handler(&self, handler: TwoStreamHandler) {
        *self.two_stream_handler.write() = Some(handler);
    }

    fn branchable_replies(&self) -> Vec<BranchableSyncReply> {
        self.branchable_replies.read().clone()
    }

    /// CARv1 payloads the coordinator sent, in order (used to assert which
    /// blocks were actually served after per-block serve filtering).
    fn car_responses(&self) -> Vec<Vec<u8>> {
        self.car_responses.read().clone()
    }

    fn car_requests(&self) -> Vec<Cid> {
        self.car_requests.read().clone()
    }
}

#[async_trait]
impl P2PTransport for NoopTransport {
    type ResponseToken = usize;

    fn local_peer_id(&self) -> &PeerId {
        &self.peer_id
    }

    fn local_public_key_proto(&self) -> &[u8] {
        &self.pubkey
    }

    fn sign(&self, _data: &[u8]) -> crate::Result<Vec<u8>> {
        Ok(vec![0])
    }

    async fn dial(&self, _peer_id: &PeerId, _addrs: Vec<PeerAddr>) -> crate::Result<()> {
        Ok(())
    }

    async fn disconnect(&self, _peer_id: &PeerId) -> crate::Result<()> {
        Ok(())
    }

    async fn listen(&self, _addr: PeerAddr) -> crate::Result<()> {
        Ok(())
    }

    async fn connected_peers(&self) -> crate::Result<Vec<PeerId>> {
        Ok(self.connected_peers.read().clone())
    }

    async fn listen_addresses(&self) -> crate::Result<Vec<PeerAddr>> {
        Ok(Vec::new())
    }

    async fn poll_until_connected(
        &self,
        _peer_id: &PeerId,
        _timeout: Duration,
    ) -> crate::Result<()> {
        Ok(())
    }

    async fn peer_addresses(&self) -> crate::Result<Vec<String>> {
        Ok(Vec::new())
    }

    async fn subscribe(&self, _topic: DefraTopic) -> crate::Result<bool> {
        Ok(true)
    }

    async fn unsubscribe(&self, _topic: DefraTopic) -> crate::Result<bool> {
        Ok(true)
    }

    async fn publish(
        &self,
        _topic: DefraTopic,
        _msg: crate::message::PushLogBroadcast,
    ) -> crate::Result<MessageId> {
        Ok(MessageId::new("noop".to_string()))
    }

    async fn topic_peers(&self, _topic: DefraTopic) -> crate::Result<Vec<PeerId>> {
        Ok(Vec::new())
    }

    async fn send_pushlog_response(
        &self,
        token: Self::ResponseToken,
        reply: crate::message::PushLogReply,
    ) -> crate::Result<()> {
        self.pushlog_response_tokens.write().push(token);
        self.pushlog_replies.write().push(reply);
        Ok(())
    }

    async fn send_two_stream_request(
        &self,
        peer_id: &PeerId,
        req: PushLogRequest,
    ) -> crate::Result<crate::message::PushLogReply> {
        let handler = self.two_stream_handler.read().clone();
        if let Some(handler) = handler {
            return handler(peer_id.clone(), req).await;
        }
        Ok(crate::message::PushLogReply::success("noop"))
    }

    async fn send_two_stream_response(
        &self,
        _peer_id: &PeerId,
        reply: crate::message::PushLogReply,
    ) -> crate::Result<()> {
        self.two_stream_replies.write().push(reply);
        Ok(())
    }

    async fn send_doc_sync_request(
        &self,
        _peer_id: &PeerId,
        _req: DocSyncRequest,
    ) -> crate::Result<()> {
        Ok(())
    }

    async fn send_doc_sync_response(
        &self,
        _peer_id: &PeerId,
        reply: crate::message::DocSyncReply,
    ) -> crate::Result<()> {
        self.doc_sync_replies.write().push(reply);
        Ok(())
    }

    async fn send_branchable_sync_request(
        &self,
        _peer_id: &PeerId,
        _req: BranchableSyncRequest,
    ) -> crate::Result<()> {
        Ok(())
    }

    async fn send_branchable_sync_response(
        &self,
        _peer_id: &PeerId,
        reply: crate::message::BranchableSyncReply,
    ) -> crate::Result<()> {
        self.branchable_replies.write().push(reply);
        Ok(())
    }

    async fn send_car_request(&self, _peer_id: &PeerId, root_cid: Cid) -> crate::Result<()> {
        self.car_requests.write().push(root_cid);
        Ok(())
    }

    async fn send_car_response(&self, _peer_id: &PeerId, car_data: Vec<u8>) -> crate::Result<()> {
        self.car_responses.write().push(car_data);
        Ok(())
    }

    async fn send_car_response_token(
        &self,
        _token: Self::ResponseToken,
        car_data: Vec<u8>,
    ) -> crate::Result<()> {
        self.car_responses.write().push(car_data);
        Ok(())
    }

    async fn send_doc_sync_response_token(
        &self,
        _token: Self::ResponseToken,
        reply: crate::message::DocSyncReply,
    ) -> crate::Result<()> {
        self.doc_sync_replies.write().push(reply);
        Ok(())
    }

    async fn send_branchable_sync_response_token(
        &self,
        _token: Self::ResponseToken,
        reply: crate::message::BranchableSyncReply,
    ) -> crate::Result<()> {
        self.branchable_replies.write().push(reply);
        Ok(())
    }

    async fn send_se_artifacts(
        &self,
        _peer_id: &PeerId,
        _req: crate::message::PushSEArtifactsRequest,
    ) -> crate::Result<()> {
        Ok(())
    }

    async fn sync_blocks(
        &self,
        _root: Cid,
        _providers: Vec<PeerId>,
        _missing: Vec<Cid>,
    ) -> crate::Result<QueryId> {
        Ok(QueryId(1))
    }

    async fn cancel_sync(&self, _query_id: QueryId) -> crate::Result<bool> {
        Ok(true)
    }

    async fn create_replicator(
        &self,
        peer_id: &PeerId,
        collections: Vec<String>,
    ) -> crate::Result<()> {
        self.replicators
            .write()
            .insert(peer_id.to_string(), collections);
        Ok(())
    }

    async fn delete_replicator(&self, peer_id: &PeerId) -> crate::Result<()> {
        self.replicators.write().remove(peer_id.as_str());
        Ok(())
    }

    async fn list_replicators(&self) -> crate::Result<Vec<ReplicatorInfo>> {
        Ok(self
            .replicators
            .read()
            .iter()
            .map(|(peer_id, collections)| {
                ReplicatorInfo::from_raw(peer_id.clone(), collections.clone(), Vec::new())
            })
            .collect())
    }

    async fn get_replicator(&self, peer_id: &PeerId) -> crate::Result<Option<ReplicatorInfo>> {
        Ok(self
            .replicators
            .read()
            .get(peer_id.as_str())
            .cloned()
            .map(|collections| {
                ReplicatorInfo::from_raw(peer_id.to_string(), collections, Vec::new())
            }))
    }

    async fn remove_replicator_collections(
        &self,
        peer_id: &PeerId,
        collections: Vec<String>,
    ) -> crate::Result<bool> {
        let mut replicators = self.replicators.write();
        let Some(existing) = replicators.get_mut(peer_id.as_str()) else {
            return Ok(false);
        };

        existing.retain(|collection| !collections.contains(collection));
        let fully_deleted = existing.is_empty();
        if fully_deleted {
            replicators.remove(peer_id.as_str());
        }

        Ok(fully_deleted)
    }

    async fn shutdown(&self) -> crate::Result<()> {
        Ok(())
    }
}

fn doc_sync_event(peer_id: PeerId) -> TransportEvent<usize> {
    doc_sync_event_with_ids(peer_id, vec!["doc1".to_string()])
}

fn doc_sync_event_with_ids(peer_id: PeerId, doc_ids: Vec<String>) -> TransportEvent<usize> {
    TransportEvent::DocSyncRequest {
        peer_id,
        request: DocSyncRequest::new(doc_ids),
        token: None,
    }
}

fn branchable_sync_event(peer_id: PeerId, collection_id: &str) -> TransportEvent<usize> {
    TransportEvent::BranchableSyncRequest {
        peer_id,
        request: BranchableSyncRequest::new(collection_id.to_string()),
        token: None,
    }
}

fn branchable_sync_reply_event(
    peer_id: PeerId,
    collection_id: &str,
    heads: Vec<Cid>,
) -> TransportEvent<usize> {
    TransportEvent::BranchableSyncReply {
        peer_id,
        reply: BranchableSyncReply::success(
            "branchable-sync-request",
            collection_id,
            heads.iter().map(|cid| cid.to_bytes()).collect(),
        ),
    }
}

fn random_peer_id() -> PeerId {
    static NEXT_PEER_ID: AtomicUsize = AtomicUsize::new(0);
    PeerId::new(format!(
        "test-peer-{}",
        NEXT_PEER_ID.fetch_add(1, Ordering::Relaxed)
    ))
}

fn cid_for(data: &[u8]) -> Cid {
    let hash = Code::Sha2_256.digest(data);
    Cid::new_v1(0x71, hash)
}

fn pushlog_request(collection_id: &str) -> PushLogRequest {
    let block = defra_core::Block::new(
        defra_core::CrdtDelta::Composite(defra_core::CompositeDeltaPayload {
            schema_version_id: "schema1".to_string(),
            priority: 1,
            status: 1,
        }),
        vec![],
        vec![],
    );
    let block_data = block.to_dag_cbor().expect("encode composite head");
    let cid = block.generate_cid().expect("composite head cid");
    PushLogRequest::new(
        "doc1".to_string(),
        bytes::Bytes::from(cid.to_bytes()),
        collection_id.to_string(),
        "creator1".to_string(),
        bytes::Bytes::from(block_data),
    )
}

/// A collection commit: doc-less, so it has no document topic to fall back to.
fn collection_commit_request(collection_id: &str) -> PushLogRequest {
    let block = defra_core::Block::new(
        defra_core::CrdtDelta::Collection(defra_core::CollectionDeltaPayload {
            schema_version_id: "schema1".to_string(),
            priority: 1,
        }),
        vec![],
        vec![],
    );
    let block_data = block.to_dag_cbor().expect("encode collection head");
    let cid = block.generate_cid().expect("collection head cid");
    PushLogRequest::new(
        String::new(),
        bytes::Bytes::from(cid.to_bytes()),
        collection_id.to_string(),
        "creator1".to_string(),
        bytes::Bytes::from(block_data),
    )
}

fn collection_commit_gossip_event(peer_id: PeerId, collection_id: &str) -> TransportEvent<usize> {
    let mut message = PushLogBroadcast::from_request(&collection_commit_request(collection_id));
    message.authenticate_source_peer(peer_id.to_string());
    TransportEvent::GossipMessage {
        propagation_source: peer_id,
        message_id: MessageId::new("gossip".to_string()),
        topic: collection_id.to_string(),
        message,
    }
}

fn pushlog_event(peer_id: PeerId, collection_id: &str) -> TransportEvent<usize> {
    TransportEvent::PushLogRequest {
        peer_id,
        request: pushlog_request(collection_id),
        token: 0,
    }
}

fn gossip_event(peer_id: PeerId, collection_id: &str) -> TransportEvent<usize> {
    gossip_event_on_topic(peer_id, collection_id, collection_id)
}

fn gossip_event_on_topic(
    peer_id: PeerId,
    topic: &str,
    collection_id: &str,
) -> TransportEvent<usize> {
    let mut message = PushLogBroadcast::from_request(&pushlog_request(collection_id));
    message.authenticate_source_peer(peer_id.to_string());
    TransportEvent::GossipMessage {
        propagation_source: peer_id,
        message_id: MessageId::new("gossip".to_string()),
        topic: topic.to_string(),
        message,
    }
}

fn car_fetch_event(peer_id: PeerId, root_cid: Cid) -> TransportEvent<usize> {
    TransportEvent::CarFetchRequest {
        peer_id,
        request: CarFetchRequest::full_dag(root_cid),
        token: None,
    }
}

fn selective_car_fetch_event(
    peer_id: PeerId,
    root_cid: Cid,
    wanted_cids: Vec<Cid>,
) -> TransportEvent<usize> {
    TransportEvent::CarFetchRequest {
        peer_id,
        request: CarFetchRequest::selective_blocks(root_cid, wanted_cids),
        token: None,
    }
}

struct StaticHeadProvider {
    heads: Vec<Cid>,
}

#[async_trait]
impl DocumentHeadProvider for StaticHeadProvider {
    async fn get_document_heads(&self, _doc_id: &str) -> crate::Result<Vec<Cid>> {
        Ok(self.heads.clone())
    }

    async fn get_collection_heads(&self, _collection_id: &str) -> crate::Result<Vec<Cid>> {
        Ok(Vec::new())
    }
}

fn two_stream_event(
    peer_id: PeerId,
    collection_id: &str,
    is_explicit_replicator: bool,
) -> TransportEvent<usize> {
    TransportEvent::TwoStreamRequest {
        peer_id,
        request: pushlog_request(collection_id),
        token: None,
        is_explicit_replicator,
        explicit_replay_authorization: None,
    }
}

async fn recv_block_received(
    events: &mut tokio::sync::mpsc::Receiver<SyncEvent>,
) -> crate::sync::manager::SyncEvent {
    timeout(Duration::from_secs(1), events.recv())
        .await
        .expect("expected sync event")
        .expect("event channel closed")
}

// --- DocSync access check tests ---

#[tokio::test]
async fn doc_sync_controlled_mode_rejects_unknown_peer() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let (coordinator, _events) =
        create_test_coordinator(AccessMode::Controlled, replicators, peer_state);

    let unknown_peer = random_peer_id();
    let result = coordinator
        .handle_transport_event(doc_sync_event(unknown_peer))
        .await;

    assert!(result.is_err());
    assert!(
        matches!(&result, Err(Error::AccessDenied { .. })),
        "Expected AccessDenied, got {:?}",
        result
    );
}

#[tokio::test]
async fn doc_sync_controlled_mode_allows_replicator() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());

    let authorized_peer = random_peer_id();
    replicators.add_replicator("collection1", authorized_peer.as_str());

    let (coordinator, _events) =
        create_test_coordinator(AccessMode::Controlled, replicators, peer_state);

    // The handler should pass the access check. It may fail later when
    // trying to sign/send the response, but should NOT fail with AccessDenied.
    let result = coordinator
        .handle_transport_event(doc_sync_event(authorized_peer))
        .await;

    assert!(
        !matches!(&result, Err(Error::AccessDenied { .. })),
        "Replicator should not get AccessDenied, got {:?}",
        result
    );
}

#[tokio::test]
async fn doc_sync_controlled_mode_allows_connected_peer() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());

    let connected_peer = random_peer_id();
    peer_state.peer_connected(connected_peer.as_str());

    let (coordinator, _events) =
        create_test_coordinator(AccessMode::Controlled, replicators, peer_state);

    let result = coordinator
        .handle_transport_event(doc_sync_event(connected_peer))
        .await;

    assert!(
        !matches!(&result, Err(Error::AccessDenied { .. })),
        "Connected peer should be allowed for Go-compatible DocSync, got {:?}",
        result
    );
}

#[tokio::test]
async fn doc_sync_controlled_mode_allows_data_topic_subscriber() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());

    let peer = random_peer_id();
    peer_state.peer_subscribed(peer.as_str(), "collection1".to_string());

    let (coordinator, _events) =
        create_test_coordinator(AccessMode::Controlled, replicators, peer_state);

    let result = coordinator
        .handle_transport_event(doc_sync_event(peer))
        .await;

    assert!(
        !matches!(&result, Err(Error::AccessDenied { .. })),
        "Data-topic subscriber should not get AccessDenied, got {:?}",
        result
    );
}

#[tokio::test]
async fn doc_sync_controlled_mode_rejects_system_topic_only_subscriber() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());

    let peer = random_peer_id();
    peer_state.peer_subscribed(peer.as_str(), DOC_SYNC_TOPIC.to_string());

    let (coordinator, _events) =
        create_test_coordinator(AccessMode::Controlled, replicators, peer_state);

    let result = coordinator
        .handle_transport_event(doc_sync_event(peer))
        .await;

    assert!(
        matches!(&result, Err(Error::AccessDenied { .. })),
        "System RPC topic subscription must not authorize DocSync, got {:?}",
        result
    );
}

#[tokio::test]
async fn doc_sync_controlled_mode_allows_any_replicator() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());

    let peer = random_peer_id();
    replicators.add_replicator("collection1", peer.as_str());

    let (coordinator, _events) =
        create_test_coordinator(AccessMode::Controlled, replicators, peer_state);

    let result = coordinator
        .handle_transport_event(doc_sync_event(peer))
        .await;

    assert!(
        !matches!(&result, Err(Error::AccessDenied { .. })),
        "Registered replicator (any collection) should be allowed, got {:?}",
        result
    );
}

#[tokio::test]
async fn doc_sync_filters_heads_outside_replicator_collection() {
    use defra_core::{Block, CompositeDeltaPayload, CrdtDelta};

    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());

    let peer = random_peer_id();
    replicators.add_replicator("collection_a", peer.as_str());

    let transport = NoopTransport::new();
    let transport_handle = transport.clone();
    let local_peer_id = transport.local_peer_id().to_string();
    let broadcaster = Broadcaster::new(transport.clone());
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));

    let block = Block::new(
        CrdtDelta::Composite(CompositeDeltaPayload {
            schema_version_id: "collection_b".to_string(),
            priority: 1,
            status: 1,
        }),
        vec![],
        vec![],
    );
    let block_data = block.to_dag_cbor().unwrap();
    let cid = block.generate_cid().unwrap();
    blockstore.put(&cid, &block_data).await.unwrap();

    let (coordinator, _events) = create_test_coordinator_with_blockstore_and_head_provider(
        TestCoordinatorParams {
            sync_config: SyncConfig::default(),
            request_rate_limiter: Arc::new(PeerRateLimiter::default()),
            access_mode: AccessMode::Controlled,
            replicators,
            peer_state,
            transport,
            local_peer_id,
            broadcaster,
            blockstore,
            rate_limiter: Arc::new(PeerRateLimiter::default()),
        },
        Arc::new(StaticHeadProvider { heads: vec![cid] }),
    );

    coordinator
        .handle_transport_event(doc_sync_event(peer))
        .await
        .unwrap();

    let replies = transport_handle.doc_sync_replies();
    assert_eq!(replies.len(), 1);
    assert!(
        replies[0].results.is_empty(),
        "DocSync must not return heads from collections the peer cannot replicate"
    );
}

// Go parity: the CAR serve path no longer returns a coarse AccessDenied for a
// wrong-collection root (that Rust-only gate also waved permissioned blocks
// through to any connected peer / subscriber with no ACP check). It now filters
// PER BLOCK, matching Go's `hasAccess`: a replicator-for-the-block's-collection
// or an ACP-authorized identity is served; everyone else is dropped and a
// well-formed (here header-only) CAR is returned. The test coordinator's
// DefaultBlockClassifier classifies a Composite/data block as Deny, so the block
// is filtered out and no data is served.
#[tokio::test]
async fn car_fetch_controlled_mode_filters_unauthorized_data_block() {
    use defra_core::{Block, CompositeDeltaPayload, CrdtDelta};

    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());

    let peer = random_peer_id();
    replicators.add_replicator("collection_a", peer.as_str());

    let transport = NoopTransport::new();
    let transport_handle = transport.clone();
    let local_peer_id = transport.local_peer_id().to_string();
    let broadcaster = Broadcaster::new(transport.clone());
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));

    let block = Block::new(
        CrdtDelta::Composite(CompositeDeltaPayload {
            schema_version_id: "collection_b".to_string(),
            priority: 1,
            status: 1,
        }),
        vec![],
        vec![],
    );
    let block_data = block.to_dag_cbor().unwrap();
    let cid = block.generate_cid().unwrap();
    blockstore.put(&cid, &block_data).await.unwrap();

    let (coordinator, _events) = create_test_coordinator_with_blockstore(TestCoordinatorParams {
        sync_config: SyncConfig::default(),
        request_rate_limiter: Arc::new(PeerRateLimiter::default()),
        access_mode: AccessMode::Controlled,
        replicators,
        peer_state,
        transport,
        local_peer_id,
        broadcaster,
        blockstore,
        rate_limiter: Arc::new(PeerRateLimiter::default()),
    });

    let result = coordinator
        .handle_transport_event(car_fetch_event(peer, cid))
        .await;

    assert!(result.is_ok(), "CAR serve must not error, got {:?}", result);

    let cars = transport_handle.car_responses();
    assert_eq!(cars.len(), 1, "handler must send exactly one CAR response");
    let (_roots, served) = crate::sync::car::decode_car(&cars[0]).unwrap();
    assert!(
        served.is_empty(),
        "no data blocks may be served for a classifier-denied block, got {}",
        served.len()
    );
}

#[tokio::test]
async fn derived_selective_car_authority_is_peer_and_root_scoped() {
    use defra_core::{Block, CompositeDeltaPayload, CrdtDelta, DAGLink, LwwDeltaPayload};

    let peer = random_peer_id();
    let transport = NoopTransport::new();
    let transport_handle = transport.clone();
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));

    let field_block = Block::new(
        CrdtDelta::Lww(LwwDeltaPayload {
            field_name: "status".to_string(),
            priority: 14,
            schema_version_id: "version1".to_string(),
            data: b"ready".to_vec(),
        }),
        vec![],
        vec![],
    );
    let field_data = field_block.to_dag_cbor().unwrap();
    let field_cid = field_block.generate_cid().unwrap();
    blockstore.put(&field_cid, &field_data).await.unwrap();

    let root_block = Block::new(
        CrdtDelta::Composite(CompositeDeltaPayload {
            schema_version_id: "version1".to_string(),
            priority: 17,
            status: 1,
        }),
        vec![],
        vec![DAGLink::new("status".to_string(), field_cid)],
    );
    let root_data = root_block.to_dag_cbor().unwrap();
    let root_cid = root_block.generate_cid().unwrap();
    blockstore.put(&root_cid, &root_data).await.unwrap();
    let unrelated_block = Block::new(
        CrdtDelta::Lww(LwwDeltaPayload {
            field_name: "other".to_string(),
            priority: 1,
            schema_version_id: "version1".to_string(),
            data: b"unrelated".to_vec(),
        }),
        vec![],
        vec![],
    );
    let unrelated_data = unrelated_block.to_dag_cbor().unwrap();
    let unrelated_cid = unrelated_block.generate_cid().unwrap();
    blockstore
        .put(&unrelated_cid, &unrelated_data)
        .await
        .unwrap();

    let replicators = filtered_replicator_registry(&peer, "collection1");
    let (coordinator, _events) = SyncCoordinator::with_access_control_and_serve_gate(
        transport,
        blockstore,
        SyncConfig::default(),
        AccessMode::Controlled,
        replicators,
        Arc::new(NoOpCollectionStorage),
        Arc::new(crate::replicator::EqOnlyFilterMatcher),
        Arc::new(StaticDataClassifier {
            collection_id: "collection1".to_string(),
        }),
        Arc::new(LateBoundServeAcp::new()),
    )
    .await
    .unwrap();

    coordinator
        .handle_transport_event(selective_car_fetch_event(
            peer.clone(),
            root_cid,
            vec![field_cid],
        ))
        .await
        .unwrap();
    let responses = transport_handle.car_responses();
    let (_roots, derived_blocks) = crate::sync::car::decode_car(&responses[0]).unwrap();
    assert_eq!(
        derived_blocks,
        vec![(field_cid, field_data.clone())],
        "durable replicator configuration plus the exact root must re-derive CAR authority"
    );

    coordinator
        .handle_transport_event(selective_car_fetch_event(
            random_peer_id(),
            root_cid,
            vec![field_cid],
        ))
        .await
        .unwrap();
    let responses = transport_handle.car_responses();
    let (_roots, unrelated_peer_blocks) = crate::sync::car::decode_car(&responses[1]).unwrap();
    assert!(
        unrelated_peer_blocks.is_empty(),
        "derived root authority must not authorize another peer"
    );

    coordinator
        .handle_transport_event(selective_car_fetch_event(
            peer.clone(),
            root_cid,
            vec![unrelated_cid],
        ))
        .await
        .unwrap();
    let responses = transport_handle.car_responses();
    let (_roots, unrelated_root_blocks) = crate::sync::car::decode_car(&responses[2]).unwrap();
    assert!(
        unrelated_root_blocks.is_empty(),
        "derived root authority must not authorize an unrelated CID"
    );

    coordinator
        .handle_transport_event(selective_car_fetch_event(peer, root_cid, vec![field_cid]))
        .await
        .unwrap();

    let responses = transport_handle.car_responses();
    let (_roots, blocks) = crate::sync::car::decode_car(&responses[3]).unwrap();
    assert_eq!(blocks, vec![(field_cid, field_data)]);
}

#[tokio::test]
async fn filtered_car_authority_is_rederived_after_sender_restart() {
    use defra_core::{Block, CompositeDeltaPayload, CrdtDelta, DAGLink, LwwDeltaPayload};

    let receiver = random_peer_id();
    let transport = NoopTransport::new();
    transport
        .create_replicator(&receiver, vec!["collection1".to_string()])
        .await
        .unwrap();

    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let field_block = Block::new(
        CrdtDelta::Lww(LwwDeltaPayload {
            field_name: "status".to_string(),
            priority: 14,
            schema_version_id: "version1".to_string(),
            data: b"ready".to_vec(),
        }),
        vec![],
        vec![],
    );
    let field_data = field_block.to_dag_cbor().unwrap();
    let field_cid = field_block.generate_cid().unwrap();
    blockstore.put(&field_cid, &field_data).await.unwrap();

    let root_block = Block::new(
        CrdtDelta::Composite(CompositeDeltaPayload {
            schema_version_id: "version1".to_string(),
            priority: 17,
            status: 1,
        }),
        vec![],
        vec![DAGLink::new("status".to_string(), field_cid)],
    );
    let root_data = root_block.to_dag_cbor().unwrap();
    let root_cid = root_block.generate_cid().unwrap();
    blockstore.put(&root_cid, &root_data).await.unwrap();

    let replicators = filtered_replicator_registry(&receiver, "collection1");
    let (mut coordinator, _events) = SyncCoordinator::with_access_control_and_serve_gate(
        transport.clone(),
        Arc::clone(&blockstore),
        SyncConfig::default(),
        AccessMode::Controlled,
        replicators,
        Arc::new(NoOpCollectionStorage),
        Arc::new(crate::replicator::EqOnlyFilterMatcher),
        Arc::new(StaticDataClassifier {
            collection_id: "collection1".to_string(),
        }),
        Arc::new(LateBoundServeAcp::new()),
    )
    .await
    .unwrap();
    let (failure_tx, mut failure_rx) = tokio::sync::mpsc::channel(16);
    coordinator.set_failure_channel(failure_tx);
    tokio::spawn(async move {
        while let Some(mut event) = failure_rx.recv().await {
            if let Some(durable_tx) = event.durable_tx.take() {
                let _ = durable_tx.send(true);
            }
        }
    });
    let coordinator = Arc::new(coordinator);

    let receiver_blocks = Arc::new(RwLock::new(std::collections::HashSet::new()));
    let root_acked = Arc::new(AtomicBool::new(false));
    transport.set_two_stream_handler(Arc::new({
        let receiver_blocks = Arc::clone(&receiver_blocks);
        let root_acked = Arc::clone(&root_acked);
        move |_peer_id, request| {
            let receiver_blocks = Arc::clone(&receiver_blocks);
            let root_acked = Arc::clone(&root_acked);
            Box::pin(async move {
                let pushed_cid = Cid::try_from(request.cid.as_ref()).unwrap();
                if pushed_cid == field_cid {
                    // Simulate a receiver hole: the dependency PushLog was acked
                    // but its block is absent when the later root arrives.
                    return Ok(PushLogReply::success("field-ack"));
                }

                receiver_blocks.write().insert(pushed_cid);
                assert_eq!(pushed_cid, root_cid);
                assert!(!receiver_blocks.read().contains(&field_cid));
                root_acked.store(true, Ordering::SeqCst);
                Ok(PushLogReply::success("root-ack"))
            })
        }
    }));

    coordinator
        .push_dag_to_replicators(&root_cid, &root_data, "doc1", "collection1")
        .await;

    let snapshot = timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = coordinator.sync_status().push_backlog;
            if root_acked.load(Ordering::SeqCst) && snapshot.completed_total == 1 {
                break snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("sender push did not complete after the receiver acked the root");

    assert_eq!(snapshot.failed_total, 0);
    assert!(receiver_blocks.read().contains(&root_cid));
    assert!(!receiver_blocks.read().contains(&field_cid));
    assert!(coordinator
        .runtime
        .selective_car_access
        .allows_root(&receiver, &root_cid));

    // A fresh coordinator has no process-local grant from the acknowledged
    // push. It must reconstruct the exact-root authority from the persisted
    // replicator configuration and the DB-classified requested root.
    // Production restart begins with a cold coordinator cache. The transport
    // owns the persisted replicator record and must repopulate the cache on
    // demand at the CAR serve boundary.
    let restarted_replicators = Arc::new(ReplicatorRegistry::new());
    let (restarted, _restart_events) = SyncCoordinator::with_access_control_and_serve_gate(
        transport.clone(),
        blockstore,
        SyncConfig::default(),
        AccessMode::Controlled,
        restarted_replicators,
        Arc::new(NoOpCollectionStorage),
        Arc::new(crate::replicator::EqOnlyFilterMatcher),
        Arc::new(StaticDataClassifier {
            collection_id: "collection1".to_string(),
        }),
        Arc::new(LateBoundServeAcp::new()),
    )
    .await
    .unwrap();
    assert!(!restarted
        .runtime
        .selective_car_access
        .allows_root(&receiver, &root_cid));

    restarted
        .handle_transport_event(selective_car_fetch_event(
            receiver,
            root_cid,
            vec![field_cid],
        ))
        .await
        .unwrap();

    let response = transport.car_responses().last().cloned().unwrap();
    let (_roots, blocks) = crate::sync::car::decode_car(&response).unwrap();
    for (cid, _data) in blocks {
        receiver_blocks.write().insert(cid);
    }
    assert!(receiver_blocks.read().contains(&field_cid));
}

#[tokio::test]
async fn collection_head_car_authority_is_rederived_from_transport_after_restart() {
    use defra_core::{Block, CompositeDeltaPayload, CrdtDelta, DAGLink, LwwDeltaPayload};

    let receiver = random_peer_id();
    let transport = NoopTransport::new();
    transport
        .create_replicator(&receiver, vec!["collection1".to_string()])
        .await
        .unwrap();

    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let child = Block::new(
        CrdtDelta::Lww(LwwDeltaPayload {
            field_name: "status".to_string(),
            priority: 1,
            schema_version_id: "version1".to_string(),
            data: b"ready".to_vec(),
        }),
        vec![],
        vec![],
    );
    let child_data = child.to_dag_cbor().unwrap();
    let child_cid = child.generate_cid().unwrap();
    blockstore.put(&child_cid, &child_data).await.unwrap();

    let collection_head = Block::new(
        CrdtDelta::Composite(CompositeDeltaPayload {
            schema_version_id: "version1".to_string(),
            priority: 2,
            status: 1,
        }),
        vec![],
        vec![DAGLink::new("member".to_string(), child_cid)],
    );
    let root_data = collection_head.to_dag_cbor().unwrap();
    let root_cid = collection_head.generate_cid().unwrap();
    blockstore.put(&root_cid, &root_data).await.unwrap();

    let (restarted, _events) = SyncCoordinator::with_access_control_and_serve_gate(
        transport.clone(),
        blockstore,
        SyncConfig::default(),
        AccessMode::Controlled,
        Arc::new(ReplicatorRegistry::new()),
        Arc::new(NoOpCollectionStorage),
        Arc::new(crate::replicator::EqOnlyFilterMatcher),
        Arc::new(CollectionHeadClassifier {
            collection_id: "collection1".to_string(),
        }),
        Arc::new(LateBoundServeAcp::new()),
    )
    .await
    .unwrap();

    assert!(!restarted
        .runtime
        .selective_car_access
        .allows_root(&receiver, &root_cid));
    restarted
        .handle_transport_event(selective_car_fetch_event(
            receiver,
            root_cid,
            vec![child_cid],
        ))
        .await
        .unwrap();

    let response = transport.car_responses().last().cloned().unwrap();
    let (_roots, blocks) = crate::sync::car::decode_car(&response).unwrap();
    assert_eq!(blocks, vec![(child_cid, child_data)]);
}

#[tokio::test]
async fn gossip_car_authority_is_rederived_from_readvertised_subscription_after_restart() {
    use defra_core::{Block, CompositeDeltaPayload, CrdtDelta, DAGLink, LwwDeltaPayload};

    let receiver = random_peer_id();
    let transport = NoopTransport::new();
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let child = Block::new(
        CrdtDelta::Lww(LwwDeltaPayload {
            field_name: "status".to_string(),
            priority: 1,
            schema_version_id: "version1".to_string(),
            data: b"ready".to_vec(),
        }),
        vec![],
        vec![],
    );
    let child_data = child.to_dag_cbor().unwrap();
    let child_cid = child.generate_cid().unwrap();
    blockstore.put(&child_cid, &child_data).await.unwrap();

    let root = Block::new(
        CrdtDelta::Composite(CompositeDeltaPayload {
            schema_version_id: "version1".to_string(),
            priority: 2,
            status: 1,
        }),
        vec![],
        vec![DAGLink::new("status".to_string(), child_cid)],
    );
    let root_data = root.to_dag_cbor().unwrap();
    let root_cid = root.generate_cid().unwrap();
    blockstore.put(&root_cid, &root_data).await.unwrap();

    // This is a cold sender coordinator: no volatile head-hint grant and no
    // outbound replicator record. The receiver's configured collection
    // subscription has been re-advertised after reconnect.
    let (restarted, _events) = SyncCoordinator::with_access_control_and_serve_gate(
        transport.clone(),
        blockstore,
        SyncConfig::default(),
        AccessMode::Controlled,
        Arc::new(ReplicatorRegistry::new()),
        Arc::new(NoOpCollectionStorage),
        Arc::new(crate::replicator::EqOnlyFilterMatcher),
        Arc::new(StaticDataClassifier {
            collection_id: "collection1".to_string(),
        }),
        Arc::new(LateBoundServeAcp::new()),
    )
    .await
    .unwrap();
    restarted
        .access
        .peer_state
        .peer_subscribed(receiver.as_str(), "collection1".to_string());
    assert!(!restarted
        .runtime
        .selective_car_access
        .allows_root(&receiver, &root_cid));

    restarted
        .handle_transport_event(selective_car_fetch_event(
            receiver,
            root_cid,
            vec![child_cid],
        ))
        .await
        .unwrap();

    let response = transport.car_responses().last().cloned().unwrap();
    let (_roots, blocks) = crate::sync::car::decode_car(&response).unwrap();
    assert_eq!(blocks, vec![(child_cid, child_data)]);
}

#[tokio::test]
async fn doc_sync_open_mode_allows_any_peer() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let (coordinator, _events) = create_test_coordinator(AccessMode::Open, replicators, peer_state);

    let random_peer = random_peer_id();
    let result = coordinator
        .handle_transport_event(doc_sync_event(random_peer))
        .await;

    assert!(
        !matches!(&result, Err(Error::AccessDenied { .. })),
        "Open mode should not deny access, got {:?}",
        result
    );
}

#[tokio::test]
async fn doc_sync_rejects_requests_above_configured_doc_id_limit() {
    let transport = NoopTransport::new();
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let config = SyncConfig {
        max_doc_sync_request_doc_ids: 2,
        ..Default::default()
    };
    let (coordinator, _events) = SyncCoordinator::new(transport, blockstore, config)
        .await
        .unwrap();

    let result = coordinator
        .handle_transport_event(doc_sync_event_with_ids(
            random_peer_id(),
            vec!["doc1".into(), "doc2".into(), "doc3".into()],
        ))
        .await;

    let Err(Error::InvalidConfig(message)) = result else {
        panic!("expected InvalidConfig, got {:?}", result);
    };
    assert!(message.contains("3 doc IDs"));
    assert!(message.contains("limit of 2"));
}

#[tokio::test]
async fn doc_sync_accepts_requests_at_exactly_configured_doc_id_limit() {
    let transport = NoopTransport::new();
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let config = SyncConfig {
        max_doc_sync_request_doc_ids: 2,
        ..Default::default()
    };
    let (coordinator, _events) = SyncCoordinator::new(transport, blockstore, config)
        .await
        .unwrap();

    let result = coordinator
        .handle_transport_event(doc_sync_event_with_ids(
            random_peer_id(),
            vec!["doc1".into(), "doc2".into()],
        ))
        .await;

    assert!(
        !matches!(&result, Err(Error::InvalidConfig(_))),
        "a request with exactly the configured limit of doc IDs must be accepted, got {:?}",
        result
    );
}

#[tokio::test]
async fn doc_sync_zero_config_resolves_to_default_limit() {
    let transport = NoopTransport::new();
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let config = SyncConfig {
        max_doc_sync_request_doc_ids: 0,
        ..Default::default()
    };
    let (coordinator, _events) = SyncCoordinator::new(transport, blockstore, config)
        .await
        .unwrap();

    let at_default = (0..DEFAULT_MAX_DOC_SYNC_REQUEST_DOC_IDS)
        .map(|i| format!("doc{i}"))
        .collect::<Vec<_>>();
    let result = coordinator
        .handle_transport_event(doc_sync_event_with_ids(random_peer_id(), at_default))
        .await;
    assert!(
        !matches!(&result, Err(Error::InvalidConfig(_))),
        "config 0 should resolve to the default limit, accepting a default-sized request, got {:?}",
        result
    );

    let over_default = (0..=DEFAULT_MAX_DOC_SYNC_REQUEST_DOC_IDS)
        .map(|i| format!("doc{i}"))
        .collect::<Vec<_>>();
    let result = coordinator
        .handle_transport_event(doc_sync_event_with_ids(random_peer_id(), over_default))
        .await;
    assert!(
        matches!(&result, Err(Error::InvalidConfig(_))),
        "config 0 should resolve to the default limit, rejecting an over-default request, got {:?}",
        result
    );
}

// --- BranchableSync access check tests ---

#[tokio::test]
async fn branchable_sync_controlled_mode_rejects_unknown_peer() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let (coordinator, _events) =
        create_test_coordinator(AccessMode::Controlled, replicators, peer_state);

    let unknown_peer = random_peer_id();
    let result = coordinator
        .handle_transport_event(branchable_sync_event(unknown_peer, "collection1"))
        .await;

    assert!(result.is_err());
    assert!(
        matches!(&result, Err(Error::AccessDenied { .. })),
        "Expected AccessDenied, got {:?}",
        result
    );
}

#[tokio::test]
async fn branchable_sync_controlled_mode_rejects_wrong_collection() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());

    let peer = random_peer_id();
    replicators.add_replicator("collection_A", peer.as_str());

    let (coordinator, _events) =
        create_test_coordinator(AccessMode::Controlled, replicators, peer_state);

    // Request for collection_B, but peer is only registered for collection_A
    let result = coordinator
        .handle_transport_event(branchable_sync_event(peer, "collection_B"))
        .await;

    assert!(result.is_err());
    assert!(
        matches!(&result, Err(Error::AccessDenied { .. })),
        "Expected AccessDenied for wrong collection, got {:?}",
        result
    );
}

#[tokio::test]
async fn branchable_sync_controlled_mode_allows_replicator() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());

    let authorized_peer = random_peer_id();
    replicators.add_replicator("collection1", authorized_peer.as_str());

    let (coordinator, _events) =
        create_test_coordinator(AccessMode::Controlled, replicators, peer_state);

    let result = coordinator
        .handle_transport_event(branchable_sync_event(authorized_peer, "collection1"))
        .await;

    assert!(
        !matches!(&result, Err(Error::AccessDenied { .. })),
        "Replicator should not get AccessDenied, got {:?}",
        result
    );
}

#[tokio::test]
async fn branchable_sync_controlled_mode_allows_connected_peer() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());

    let connected_peer = random_peer_id();
    peer_state.peer_connected(connected_peer.as_str());

    let (coordinator, _events) =
        create_test_coordinator(AccessMode::Controlled, replicators, peer_state);

    let result = coordinator
        .handle_transport_event(branchable_sync_event(connected_peer, "collection1"))
        .await;

    assert!(
        !matches!(&result, Err(Error::AccessDenied { .. })),
        "Connected peer should be allowed for Go-compatible BranchableSync, got {:?}",
        result
    );
}

#[tokio::test]
async fn branchable_sync_reply_remerges_locally_complete_unmerged_head() {
    use defra_core::{Block, CompositeDeltaPayload, CrdtDelta};

    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let peer = random_peer_id();

    let transport = NoopTransport::new();
    let local_peer_id = transport.local_peer_id().to_string();
    let broadcaster = Broadcaster::new(transport.clone());
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));

    let block = Block::new(
        CrdtDelta::Composite(CompositeDeltaPayload {
            schema_version_id: "collection1".to_string(),
            priority: 1,
            status: 1,
        }),
        vec![],
        vec![],
    );
    let block_data = block.to_dag_cbor().unwrap();
    let cid = block.generate_cid().unwrap();
    blockstore.put(&cid, &block_data).await.unwrap();
    assert!(
        !blockstore.is_merged(&cid).await.unwrap(),
        "test setup requires an unmerged local root"
    );

    let (coordinator, mut events) =
        create_test_coordinator_with_blockstore(TestCoordinatorParams {
            sync_config: SyncConfig::default(),
            request_rate_limiter: Arc::new(PeerRateLimiter::default()),
            access_mode: AccessMode::Open,
            replicators,
            peer_state,
            transport,
            local_peer_id,
            broadcaster,
            blockstore,
            rate_limiter: Arc::new(PeerRateLimiter::default()),
        });

    coordinator
        .handle_transport_event(branchable_sync_reply_event(
            peer.clone(),
            "collection1",
            vec![cid],
        ))
        .await
        .unwrap();

    match recv_block_received(&mut events).await {
        SyncEvent::BlockReceived {
            cid: event_cid,
            doc_id,
            collection_id,
            sender_peer,
            ..
        } => {
            assert_eq!(event_cid, cid);
            assert_eq!(doc_id, document::DocID::new_v0(cid).to_string());
            assert_eq!(collection_id, "collection1");
            assert_eq!(sender_peer.as_deref(), Some(peer.as_str()));
        }
        other => panic!("expected BlockReceived for unmerged BranchableSync head, got {other:?}"),
    }
}

#[tokio::test]
async fn gossip_subscribed_collection_accepts_without_peer_connection_cache() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let peer = random_peer_id();
    let (coordinator, _events) =
        create_test_coordinator(AccessMode::Controlled, replicators, peer_state.clone());

    assert!(
        !peer_state.is_connected(peer.as_str()),
        "test setup requires the coordinator peer_state cache to start cold"
    );

    coordinator
        .subscriptions
        .subscribed_collections
        .write()
        .await
        .insert("collection1".to_string());

    let result = coordinator
        .handle_transport_event(gossip_event(peer.clone(), "collection1"))
        .await;

    assert!(
        !matches!(&result, Err(Error::AccessDenied { .. })),
        "delivered gossip on a subscribed collection should not require a separate connection-cache hit, got {:?}",
        result
    );
}

#[tokio::test]
async fn branchable_sync_open_mode_allows_any_peer() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let (coordinator, _events) = create_test_coordinator(AccessMode::Open, replicators, peer_state);

    let random_peer = random_peer_id();
    let result = coordinator
        .handle_transport_event(branchable_sync_event(random_peer, "any_collection"))
        .await;

    assert!(
        !matches!(&result, Err(Error::AccessDenied { .. })),
        "Open mode should not deny access, got {:?}",
        result
    );
}

// #838: PushLog in Controlled mode must reject a merely-connected peer
// that isn't registered as a replicator for the target collection.
#[tokio::test]
async fn pushlog_controlled_mode_rejects_non_replicator_connected_peer() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let peer = random_peer_id();
    peer_state.peer_connected(peer.as_str());

    let (coordinator, _events) =
        create_test_coordinator(AccessMode::Controlled, replicators, peer_state);

    let result = coordinator
        .handle_transport_event(pushlog_event(peer, "collection1"))
        .await;

    assert!(
        matches!(&result, Err(Error::AccessDenied { .. })),
        "Connected-but-not-registered peer must be denied in Controlled mode, got {:?}",
        result
    );
}

#[tokio::test]
async fn pushlog_controlled_mode_allows_locally_subscribed_collection() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let peer = random_peer_id();
    let (coordinator, mut events) =
        create_test_coordinator(AccessMode::Controlled, replicators, peer_state);

    coordinator
        .subscriptions
        .subscribed_collections
        .write()
        .await
        .insert("collection1".to_string());

    coordinator
        .handle_transport_event(pushlog_event(peer.clone(), "collection1"))
        .await
        .unwrap();

    match recv_block_received(&mut events).await {
        SyncEvent::BlockReceived {
            sender_peer,
            is_explicit_replicator,
            ..
        } => {
            assert_eq!(sender_peer.as_deref(), Some(peer.as_str()));
            assert!(
                !is_explicit_replicator,
                "collection subscription should not mark the source as an explicit replicator"
            );
        }
        other => panic!("expected BlockReceived, got {:?}", other),
    }
}

#[tokio::test]
async fn pushlog_registered_replicator_is_marked_explicit_replicator() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let peer = random_peer_id();
    replicators.add_replicator("collection1", peer.as_str());

    let (coordinator, mut events) =
        create_test_coordinator(AccessMode::Controlled, replicators, peer_state);

    coordinator
        .handle_transport_event(pushlog_event(peer.clone(), "collection1"))
        .await
        .unwrap();

    match recv_block_received(&mut events).await {
        SyncEvent::BlockReceived {
            sender_peer,
            is_explicit_replicator,
            ..
        } => {
            assert_eq!(sender_peer.as_deref(), Some(peer.as_str()));
            assert!(
                is_explicit_replicator,
                "registered replicator should preserve explicit replicator trust"
            );
        }
        other => panic!("expected BlockReceived, got {:?}", other),
    }
}

// Go parity: the direct replicator protocol skips the receiver-side
// collection access gate. The sending node's replicator registry selects the
// target; merge-time ACP still applies downstream.
#[tokio::test]
async fn two_stream_controlled_mode_accepts_replicator_protocol_without_local_registration() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let peer = random_peer_id();
    peer_state.peer_connected(peer.as_str());

    let (coordinator, mut events) =
        create_test_coordinator(AccessMode::Controlled, replicators, peer_state);

    coordinator
        .handle_transport_event(two_stream_event(peer.clone(), "collection1", false))
        .await
        .unwrap();

    match recv_block_received(&mut events).await {
        SyncEvent::BlockReceived {
            sender_peer,
            is_explicit_replicator,
            ..
        } => {
            assert_eq!(sender_peer.as_deref(), Some(peer.as_str()));
            assert!(
                !is_explicit_replicator,
                "ordinary direct replicator pushes should not imply explicit replay trust"
            );
        }
        other => panic!("expected BlockReceived, got {:?}", other),
    }
}

#[tokio::test]
async fn two_stream_controlled_mode_accepts_unknown_peer() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let peer = random_peer_id();

    let (coordinator, mut events) =
        create_test_coordinator(AccessMode::Controlled, replicators, peer_state);

    coordinator
        .handle_transport_event(two_stream_event(peer.clone(), "collection1", false))
        .await
        .unwrap();

    match recv_block_received(&mut events).await {
        SyncEvent::BlockReceived {
            sender_peer,
            is_explicit_replicator,
            ..
        } => {
            assert_eq!(sender_peer.as_deref(), Some(peer.as_str()));
            assert!(
                !is_explicit_replicator,
                "two-stream transport auth and explicit replay trust remain separate"
            );
        }
        other => panic!("expected BlockReceived, got {:?}", other),
    }
}

#[tokio::test]
async fn two_stream_controlled_mode_allows_locally_subscribed_collection() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let peer = random_peer_id();
    let (coordinator, mut events) =
        create_test_coordinator(AccessMode::Controlled, replicators, peer_state);

    coordinator
        .subscriptions
        .subscribed_collections
        .write()
        .await
        .insert("collection1".to_string());

    coordinator
        .handle_transport_event(two_stream_event(peer.clone(), "collection1", false))
        .await
        .unwrap();

    match recv_block_received(&mut events).await {
        SyncEvent::BlockReceived {
            sender_peer,
            is_explicit_replicator,
            ..
        } => {
            assert_eq!(sender_peer.as_deref(), Some(peer.as_str()));
            assert!(
                !is_explicit_replicator,
                "collection subscription should not mark two-stream senders as explicit replicators"
            );
        }
        other => panic!("expected BlockReceived, got {:?}", other),
    }
}

// --- GossipSub access check tests ---

#[tokio::test]
async fn gossip_controlled_mode_rejects_unregistered_unsubscribed_peer() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let peer = random_peer_id();
    peer_state.peer_connected(peer.as_str());
    let (coordinator, _events) =
        create_test_coordinator(AccessMode::Controlled, replicators, peer_state);

    let result = coordinator
        .handle_transport_event(gossip_event(peer, "collection1"))
        .await;

    assert!(
        matches!(&result, Err(Error::AccessDenied { .. })),
        "connected peer that is neither registered nor on a subscribed collection must be denied, got {:?}",
        result
    );
}

#[tokio::test]
async fn gossip_controlled_mode_allows_subscribed_collection() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let peer = random_peer_id();
    let (coordinator, _events) =
        create_test_coordinator(AccessMode::Controlled, replicators, peer_state);

    coordinator
        .subscriptions
        .subscribed_collections
        .write()
        .await
        .insert("collection1".to_string());

    let result = coordinator
        .handle_transport_event(gossip_event(peer, "collection1"))
        .await;

    assert!(
        !matches!(&result, Err(Error::AccessDenied { .. })),
        "gossip on a subscribed collection must be accepted, got {:?}",
        result
    );
}

#[tokio::test]
async fn gossip_controlled_mode_allows_document_topic() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let peer = random_peer_id();
    let (coordinator, _events) =
        create_test_coordinator(AccessMode::Controlled, replicators, peer_state);

    let result = coordinator
        .handle_transport_event(gossip_event_on_topic(peer, "doc1", "collection1"))
        .await;

    assert!(
        !matches!(&result, Err(Error::AccessDenied { .. })),
        "gossip on a subscribed document topic must be accepted, got {:?}",
        result
    );
}

#[tokio::test]
async fn gossip_controlled_mode_rejects_mismatched_topic_and_payload_collection() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let peer = random_peer_id();
    let (coordinator, _events) =
        create_test_coordinator(AccessMode::Controlled, replicators, peer_state);

    coordinator
        .subscriptions
        .subscribed_collections
        .write()
        .await
        .insert("collection1".to_string());

    let result = coordinator
        .handle_transport_event(gossip_event_on_topic(peer, "collection2", "collection1"))
        .await;

    assert!(
        matches!(&result, Err(Error::AccessDenied { .. })),
        "gossip payload collection must match the received topic, got {:?}",
        result
    );
}

#[tokio::test]
async fn gossip_controlled_mode_rejects_mismatched_document_topic() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let peer = random_peer_id();
    let (coordinator, _events) =
        create_test_coordinator(AccessMode::Controlled, replicators, peer_state);

    let result = coordinator
        .handle_transport_event(gossip_event_on_topic(peer, "doc2", "collection1"))
        .await;

    assert!(
        matches!(&result, Err(Error::AccessDenied { .. })),
        "gossip document topic must match the payload document id, got {:?}",
        result
    );
}

#[tokio::test]
async fn gossip_controlled_mode_allows_subscribed_outbound_replicator_target() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let peer = random_peer_id();
    peer_state.peer_connected(peer.as_str());
    let (coordinator, _events) =
        create_test_coordinator(AccessMode::Controlled, replicators, peer_state);

    coordinator
        .create_replicator(&peer, vec!["collection1".to_string()], false)
        .await
        .unwrap();

    coordinator
        .subscriptions
        .subscribed_collections
        .write()
        .await
        .insert("collection1".to_string());

    let result = coordinator
        .handle_transport_event(gossip_event(peer, "collection1"))
        .await;

    assert!(
        !matches!(&result, Err(Error::AccessDenied { .. })),
        "subscribed collections must accept gossip from outbound replicator targets, got {:?}",
        result
    );
    assert_eq!(coordinator.sync_status().gossip_direction_filtered_total, 0);
}

/// Collection commits carry an EMPTY `doc_id`, so unlike document updates they
/// have no document-topic fallback: the collection topic is their only delivery
/// path. A symmetric mesh makes both peers each other's outbound replicator
/// target, so if that closed the collection topic, collection commits could
/// never be delivered at all (source-inc/gents#696).
#[tokio::test]
async fn gossip_allows_doc_less_collection_commit_from_outbound_replicator_target() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let peer = random_peer_id();
    peer_state.peer_connected(peer.as_str());
    let (coordinator, _events) =
        create_test_coordinator(AccessMode::Controlled, replicators, peer_state);

    coordinator
        .create_replicator(&peer, vec!["collection1".to_string()], false)
        .await
        .unwrap();

    coordinator
        .subscriptions
        .subscribed_collections
        .write()
        .await
        .insert("collection1".to_string());

    let result = coordinator
        .handle_transport_event(collection_commit_gossip_event(peer, "collection1"))
        .await;

    assert!(
        !matches!(&result, Err(Error::AccessDenied { .. })),
        "a doc-less collection commit has no document-topic fallback; the subscribed \
         collection topic must accept it from a peer we also replicate to, got {:?}",
        result
    );
    assert_eq!(coordinator.sync_status().gossip_direction_filtered_total, 0);
}

#[tokio::test]
async fn gossip_document_topic_allows_outbound_replicator_target() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let peer = random_peer_id();
    peer_state.peer_connected(peer.as_str());
    let (coordinator, _events) =
        create_test_coordinator(AccessMode::Controlled, replicators, peer_state);

    coordinator
        .create_replicator(&peer, vec!["collection1".to_string()], false)
        .await
        .unwrap();

    let result = coordinator
        .handle_transport_event(gossip_event_on_topic(peer, "doc1", "collection1"))
        .await;

    assert!(
        !matches!(&result, Err(Error::AccessDenied { .. })),
        "document-topic subscriptions must accept updates from outbound replicator targets, got {:?}",
        result
    );
}

#[tokio::test]
async fn gossip_open_mode_rejects_unsubscribed_outbound_replicator_target() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let peer = random_peer_id();
    replicators.add_replicator("collection1", peer.as_str());

    let (coordinator, _events) = create_test_coordinator(AccessMode::Open, replicators, peer_state);

    let result = coordinator
        .handle_transport_event(gossip_event(peer.clone(), "collection1"))
        .await;

    assert!(
        matches!(&result, Err(Error::AccessDenied { .. })),
        "unsubscribed outbound replicator targets must not become gossip sources, got {:?}",
        result
    );
    assert_eq!(coordinator.sync_status().gossip_direction_filtered_total, 1);
}

#[tokio::test]
async fn gossip_ignores_transport_replicator_state_for_directionality() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let transport = NoopTransport::new();
    let local_peer_id = transport.local_peer_id().to_string();
    let broadcaster = Broadcaster::new(transport.clone());
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));

    let peer = random_peer_id();
    transport
        .create_replicator(&peer, vec!["collection1".to_string()])
        .await
        .unwrap();

    let (coordinator, _events) = create_test_coordinator_with_blockstore(TestCoordinatorParams {
        sync_config: SyncConfig::default(),
        request_rate_limiter: Arc::new(PeerRateLimiter::default()),
        access_mode: AccessMode::Controlled,
        replicators: replicators.clone(),
        peer_state,
        transport,
        local_peer_id,
        broadcaster,
        blockstore,
        rate_limiter: Arc::new(PeerRateLimiter::default()),
    });

    assert!(
        !replicators.is_replicator("collection1", peer.as_str()),
        "test setup requires the coordinator registry to start empty"
    );

    let result = coordinator
        .handle_transport_event(gossip_event(peer.clone(), "collection1"))
        .await;

    assert!(
        matches!(&result, Err(Error::AccessDenied { .. })),
        "transport outbound replicator state must not authorize inbound gossip, got {:?}",
        result
    );
    assert!(
        !replicators.is_replicator("collection1", peer.as_str()),
        "gossip access checks must not backfill outbound replicator state as inbound gossip trust"
    );
}

#[tokio::test]
async fn delete_replicator_removes_gossip_access_without_subscription() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let transport = NoopTransport::new();
    let local_peer_id = transport.local_peer_id().to_string();
    let broadcaster = Broadcaster::new(transport.clone());
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));

    let peer = random_peer_id();
    transport.set_connected_peers(vec![peer.clone()]);

    let (coordinator, _events) = create_test_coordinator_with_blockstore(TestCoordinatorParams {
        sync_config: SyncConfig::default(),
        request_rate_limiter: Arc::new(PeerRateLimiter::default()),
        access_mode: AccessMode::Controlled,
        replicators,
        peer_state,
        transport,
        local_peer_id,
        broadcaster,
        blockstore,
        rate_limiter: Arc::new(PeerRateLimiter::default()),
    });

    coordinator
        .create_replicator(&peer, vec!["collection1".to_string()], false)
        .await
        .unwrap();
    coordinator.delete_replicator(&peer).await.unwrap();

    let result = coordinator
        .handle_transport_event(gossip_event(peer, "collection1"))
        .await;

    assert!(
        matches!(&result, Err(Error::AccessDenied { .. })),
        "connected peers without registry or subscription access must be denied after delete, got {:?}",
        result
    );
}

#[tokio::test]
async fn create_replicator_update_keeps_outbound_targets_from_gossip_sources() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let transport = NoopTransport::new();
    let local_peer_id = transport.local_peer_id().to_string();
    let broadcaster = Broadcaster::new(transport.clone());
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));

    let peer = random_peer_id();
    transport.set_connected_peers(vec![peer.clone()]);

    let (coordinator, _events) = create_test_coordinator_with_blockstore(TestCoordinatorParams {
        sync_config: SyncConfig::default(),
        request_rate_limiter: Arc::new(PeerRateLimiter::default()),
        access_mode: AccessMode::Controlled,
        replicators,
        peer_state,
        transport,
        local_peer_id,
        broadcaster,
        blockstore,
        rate_limiter: Arc::new(PeerRateLimiter::default()),
    });

    coordinator
        .create_replicator(&peer, vec!["collection_a".to_string()], false)
        .await
        .unwrap();
    coordinator
        .create_replicator(&peer, vec!["collection_b".to_string()], false)
        .await
        .unwrap();

    let old_collection_result = coordinator
        .handle_transport_event(gossip_event(peer.clone(), "collection_a"))
        .await;
    let new_collection_result = coordinator
        .handle_transport_event(gossip_event(peer, "collection_b"))
        .await;

    assert!(
        matches!(&old_collection_result, Err(Error::AccessDenied { .. })),
        "updating a replicator must remove old collection access without subscription, got {:?}",
        old_collection_result
    );
    assert!(
        matches!(&new_collection_result, Err(Error::AccessDenied { .. })),
        "updating a replicator must not turn the outbound target into a gossip source, got {:?}",
        new_collection_result
    );
}

#[tokio::test]
async fn two_stream_authenticated_explicit_replicator_is_marked_explicit_replicator() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let peer = random_peer_id();
    peer_state.peer_connected(peer.as_str());

    let (coordinator, mut events) =
        create_test_coordinator(AccessMode::Controlled, replicators, peer_state);

    coordinator
        .handle_transport_event(two_stream_event(peer.clone(), "collection1", true))
        .await
        .unwrap();

    match recv_block_received(&mut events).await {
        SyncEvent::BlockReceived {
            sender_peer,
            is_explicit_replicator,
            ..
        } => {
            assert_eq!(sender_peer.as_deref(), Some(peer.as_str()));
            assert!(
                is_explicit_replicator,
                "authenticated two-stream explicit replicator push should preserve explicit trust"
            );
        }
        other => panic!("expected BlockReceived, got {:?}", other),
    }
}

// fa4a84f7's `two_stream_bypasses_gossip_rate_limiter_for_authenticated_sync`
// asserted that authenticated two-stream pushes skip the rate limiter entirely.
// #1088 W4 re-lands #592's intake admission: the concern that test protected —
// authenticated sync must never be SILENTLY dropped like gossip — is preserved
// and strengthened by `two_stream_rate_limited_replies_backpressure_nack` below:
// over-budget pushes now get an explicit RATE_LIMITED_MESSAGE nack that the
// pusher's backoff consumer (#843) turns into a retry, never a lost document.

#[tokio::test]
async fn gossip_remains_rate_limited_when_bucket_is_empty() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let (coordinator, _events) = create_test_coordinator_with_rate_limiter(
        AccessMode::Open,
        replicators,
        peer_state,
        Arc::new(PeerRateLimiter::new(0, 0.0)),
    );

    let peer = random_peer_id();
    let result = coordinator
        .handle_transport_event(gossip_event(peer, "collection1"))
        .await;

    assert!(
        matches!(&result, Err(Error::AccessDenied { .. })),
        "gossip should still be rate limited, got {:?}",
        result
    );
}

#[tokio::test]
async fn pushlog_retries_transient_transaction_conflicts_without_sync_error() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let transport = NoopTransport::new();
    let local_peer_id = transport.local_peer_id().to_string();
    let broadcaster = Broadcaster::new(transport.clone());
    let blockstore = Arc::new(ConflictOnceBlockstore::new());

    let (coordinator, mut events) =
        create_test_coordinator_with_blockstore(TestCoordinatorParams {
            sync_config: SyncConfig::default(),
            request_rate_limiter: Arc::new(PeerRateLimiter::default()),
            access_mode: AccessMode::Open,
            replicators,
            peer_state,
            transport,
            local_peer_id,
            broadcaster,
            blockstore: blockstore.clone(),
            rate_limiter: Arc::new(PeerRateLimiter::default()),
        });

    let peer = random_peer_id();
    coordinator
        .handle_transport_event(pushlog_event(peer.clone(), "collection1"))
        .await
        .unwrap();

    assert_eq!(blockstore.put_attempts(), 2);
    match recv_block_received(&mut events).await {
        SyncEvent::BlockReceived { sender_peer, .. } => {
            assert_eq!(sender_peer.as_deref(), Some(peer.as_str()));
        }
        other => panic!("expected BlockReceived after retry, got {:?}", other),
    }
}

#[tokio::test]
async fn gossip_retries_transient_transaction_conflicts_without_sync_error() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let transport = NoopTransport::new();
    let local_peer_id = transport.local_peer_id().to_string();
    let broadcaster = Broadcaster::new(transport.clone());
    let blockstore = Arc::new(ConflictOnceBlockstore::new());

    let (coordinator, mut events) =
        create_test_coordinator_with_blockstore(TestCoordinatorParams {
            sync_config: SyncConfig::default(),
            request_rate_limiter: Arc::new(PeerRateLimiter::default()),
            access_mode: AccessMode::Open,
            replicators,
            peer_state,
            transport,
            local_peer_id,
            broadcaster,
            blockstore: blockstore.clone(),
            rate_limiter: Arc::new(PeerRateLimiter::default()),
        });

    let peer = random_peer_id();
    coordinator
        .handle_transport_event(gossip_event(peer.clone(), "collection1"))
        .await
        .unwrap();

    assert_eq!(blockstore.put_attempts(), 2);
    match recv_block_received(&mut events).await {
        SyncEvent::BlockReceived { sender_peer, .. } => {
            assert_eq!(sender_peer.as_deref(), Some(peer.as_str()));
        }
        other => panic!("expected BlockReceived after gossip retry, got {:?}", other),
    }
}

#[tokio::test]
async fn concurrent_same_cid_pushlog_and_car_have_one_storage_owner() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let transport = NoopTransport::new();
    let local_peer_id = transport.local_peer_id().to_string();
    let broadcaster = Broadcaster::new(transport.clone());
    let blockstore = Arc::new(SingleOwnerBlockstore::new());

    let (coordinator, mut events) =
        create_test_coordinator_with_blockstore(TestCoordinatorParams {
            sync_config: SyncConfig::default(),
            request_rate_limiter: Arc::new(PeerRateLimiter::default()),
            access_mode: AccessMode::Open,
            replicators,
            peer_state,
            transport,
            local_peer_id,
            broadcaster,
            blockstore: blockstore.clone(),
            rate_limiter: Arc::new(PeerRateLimiter::default()),
        });
    let coordinator = Arc::new(coordinator);
    let peer = random_peer_id();
    let request = pushlog_request("collection1");
    let root_cid = Cid::try_from(request.cid.as_ref()).expect("request CID");
    let root_data = request.block.clone();
    let car_data = crate::sync::car::encode_car(&[root_cid], &[(&root_cid, root_data.as_ref())])
        .expect("encode test CAR");

    let push = {
        let coordinator = Arc::clone(&coordinator);
        let peer = peer.clone();
        tokio::spawn(async move {
            coordinator
                .handle_transport_event(TransportEvent::PushLogRequest {
                    peer_id: peer,
                    request,
                    token: 0,
                })
                .await
        })
    };
    blockstore
        .first_write_entered
        .acquire()
        .await
        .expect("first write observation semaphore remains open")
        .forget();

    let car = {
        let coordinator = Arc::clone(&coordinator);
        let peer = peer.clone();
        tokio::spawn(async move {
            coordinator
                .handle_transport_event(TransportEvent::CarFetchResponse {
                    peer_id: peer,
                    root_cid,
                    car_data,
                })
                .await
        })
    };

    assert!(
        timeout(
            Duration::from_millis(25),
            blockstore.concurrent_write_entered.acquire()
        )
        .await
        .is_err(),
        "CAR storage must wait behind the PushLog owner for the same root"
    );
    blockstore.release_first_write.add_permits(1);

    push.await.unwrap().unwrap();
    car.await.unwrap().unwrap();
    assert_eq!(blockstore.max_active_writes(), 1);
    assert!(matches!(
        events.try_recv().expect("PushLog emits merge event"),
        SyncEvent::BlockReceived { cid, .. } if cid == root_cid
    ));
}

// --- #1088 W1/W4: intake backpressure nacks (re-land #592, regressed by fa4a84f7) ---
//
// The M1 invariant: a success PushLogReply implies the pushed block is either
// merged or registered as pending on the hub. Both rejection classes must reply
// a byte-exact sentinel so the sender retains its durable scope marker instead
// of laundering the failure as success. They remain distinct sentinels for
// observability; neither starts an in-process resend loop.

/// A PushLog request whose composite block links to a field block that is never
/// stored, so `process_pushlog` must register a pending DAG to track it.
fn pushlog_request_with_missing_link(collection_id: &str, doc_id: &str) -> PushLogRequest {
    // Deltas no longer carry a docID, so distinct documents must differ in
    // content to produce distinct genesis CIDs (as unsigned creates do).
    let field_block = defra_core::Block::new(
        defra_core::CrdtDelta::Lww(defra_core::LwwDeltaPayload {
            field_name: "field".to_string(),
            priority: 1,
            schema_version_id: "schema1".to_string(),
            data: doc_id.as_bytes().to_vec(),
        }),
        vec![],
        vec![],
    );
    let field_cid = field_block.generate_cid().expect("field cid");

    let composite = defra_core::Block::new(
        defra_core::CrdtDelta::Composite(defra_core::CompositeDeltaPayload {
            schema_version_id: "schema1".to_string(),
            priority: 1,
            status: 1,
        }),
        vec![],
        vec![defra_core::DAGLink::new("field", field_cid)],
    );
    let composite_bytes = composite.to_dag_cbor().expect("encode composite");
    let composite_cid = composite.generate_cid().expect("composite cid");

    PushLogRequest::new(
        doc_id.to_string(),
        bytes::Bytes::from(composite_cid.to_bytes()),
        collection_id.to_string(),
        "creator1".to_string(),
        bytes::Bytes::from(composite_bytes),
    )
}

fn always_limited_rate_limiter() -> Arc<PeerRateLimiter> {
    Arc::new(PeerRateLimiter::with_backoff_steps(
        0,
        0.0,
        vec![Duration::from_secs(60)],
    ))
}

#[tokio::test]
async fn pushlog_request_at_pending_capacity_replies_at_capacity_nack() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let (coordinator, _events) = create_test_coordinator_with_sync_config(
        AccessMode::Open,
        replicators,
        peer_state,
        SyncConfig {
            max_pending_dags: 1,
            ..SyncConfig::default()
        },
    );
    let transport = coordinator.runtime.transport.clone();
    let peer = random_peer_id();

    coordinator
        .handle_transport_event(TransportEvent::PushLogRequest {
            peer_id: peer.clone(),
            request: pushlog_request_with_missing_link("collection1", "docA"),
            token: 0,
        })
        .await
        .unwrap();
    let first = transport.pushlog_replies().pop().expect("first reply");
    assert_eq!(
        first.err_message, None,
        "registered pending DAG must ack success"
    );

    let result = coordinator
        .handle_transport_event(TransportEvent::PushLogRequest {
            peer_id: peer.clone(),
            request: pushlog_request_with_missing_link("collection1", "docB"),
            token: 0,
        })
        .await;
    assert!(
        result.is_err(),
        "capacity drop must surface as an error, got {:?}",
        result
    );

    let reply = transport.pushlog_replies().pop().expect("overflow reply");
    assert_eq!(
        reply.err_message.as_deref(),
        Some(crate::error::AT_CAPACITY_MESSAGE),
        "capacity overflow must nack with the byte-exact capacity sentinel, never success"
    );
}

#[tokio::test]
async fn two_stream_at_pending_capacity_replies_at_capacity_nack() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let (coordinator, _events) = create_test_coordinator_with_sync_config(
        AccessMode::Open,
        replicators,
        peer_state,
        SyncConfig {
            max_pending_dags: 1,
            ..SyncConfig::default()
        },
    );
    let transport = coordinator.runtime.transport.clone();
    let peer = random_peer_id();

    coordinator
        .handle_transport_event(TransportEvent::TwoStreamRequest {
            peer_id: peer.clone(),
            request: pushlog_request_with_missing_link("collection1", "docA"),
            token: None,
            is_explicit_replicator: false,
            explicit_replay_authorization: None,
        })
        .await
        .unwrap();
    let first = transport.two_stream_replies().pop().expect("first reply");
    assert_eq!(
        first.err_message, None,
        "registered pending DAG must ack success"
    );

    let result = coordinator
        .handle_transport_event(TransportEvent::TwoStreamRequest {
            peer_id: peer.clone(),
            request: pushlog_request_with_missing_link("collection1", "docB"),
            token: None,
            is_explicit_replicator: false,
            explicit_replay_authorization: None,
        })
        .await;
    assert!(
        result.is_err(),
        "capacity drop must surface as an error, got {:?}",
        result
    );

    let reply = transport
        .two_stream_replies()
        .pop()
        .expect("overflow reply");
    assert_eq!(
        reply.err_message.as_deref(),
        Some(crate::error::AT_CAPACITY_MESSAGE),
        "capacity overflow must nack with the byte-exact capacity sentinel, never success"
    );
}

#[tokio::test]
async fn two_stream_reply_with_response_token_does_not_reverse_dial() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let (coordinator, _events) = create_test_coordinator(AccessMode::Open, replicators, peer_state);
    let transport = coordinator.runtime.transport.clone();
    let peer = random_peer_id();

    coordinator
        .send_two_stream_reply(&peer, PushLogReply::success("message-1"), Some(7), true)
        .await;

    assert_eq!(transport.pushlog_response_tokens(), vec![7]);
    assert_eq!(transport.pushlog_replies().len(), 1);
    assert!(
        transport.two_stream_replies().is_empty(),
        "a response token must avoid a fresh reverse-dial"
    );
}

#[tokio::test]
async fn two_stream_reply_without_response_token_falls_back_to_reverse_dial() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let (coordinator, _events) = create_test_coordinator(AccessMode::Open, replicators, peer_state);
    let transport = coordinator.runtime.transport.clone();
    let peer = random_peer_id();

    coordinator
        .send_two_stream_reply(&peer, PushLogReply::success("message-1"), None, true)
        .await;

    assert!(transport.pushlog_response_tokens().is_empty());
    assert!(transport.pushlog_replies().is_empty());
    assert_eq!(transport.two_stream_replies().len(), 1);
}

#[tokio::test]
async fn two_stream_reply_for_legacy_sender_falls_back_to_reverse_dial() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let (coordinator, _events) = create_test_coordinator(AccessMode::Open, replicators, peer_state);
    let transport = coordinator.runtime.transport.clone();
    let peer = random_peer_id();

    coordinator
        .send_two_stream_reply(&peer, PushLogReply::success("message-1"), Some(7), false)
        .await;

    assert!(transport.pushlog_response_tokens().is_empty());
    assert!(transport.pushlog_replies().is_empty());
    assert_eq!(transport.two_stream_replies().len(), 1);
}

#[tokio::test]
async fn pushlog_request_rate_limited_replies_backpressure_nack() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let (coordinator, _events) = create_test_coordinator_with_rate_limiter(
        AccessMode::Open,
        replicators,
        peer_state,
        always_limited_rate_limiter(),
    );
    let transport = coordinator.runtime.transport.clone();
    let peer = random_peer_id();

    let result = coordinator
        .handle_transport_event(pushlog_event(peer.clone(), "collection1"))
        .await;
    assert!(
        matches!(&result, Err(e) if e.is_rate_limited()),
        "expected rate-limited rejection, got {:?}",
        result
    );

    let reply = transport.pushlog_replies().pop().expect("nack reply sent");
    assert_eq!(
        reply.err_message.as_deref(),
        Some(crate::error::RATE_LIMITED_MESSAGE)
    );
}

#[tokio::test]
async fn two_stream_rate_limited_replies_backpressure_nack() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let (coordinator, _events) = create_test_coordinator_with_rate_limiter(
        AccessMode::Open,
        replicators,
        peer_state,
        always_limited_rate_limiter(),
    );
    let transport = coordinator.runtime.transport.clone();
    let peer = random_peer_id();

    let result = coordinator
        .handle_transport_event(two_stream_event(peer.clone(), "collection1", false))
        .await;
    assert!(
        matches!(&result, Err(e) if e.is_rate_limited()),
        "expected rate-limited rejection, got {:?}",
        result
    );

    let reply = transport
        .two_stream_replies()
        .pop()
        .expect("nack reply sent");
    assert_eq!(
        reply.err_message.as_deref(),
        Some(crate::error::RATE_LIMITED_MESSAGE)
    );
}

#[tokio::test]
async fn doc_sync_rate_limited_replies_backpressure_nack() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let (coordinator, _events) = create_test_coordinator_with_rate_limiter(
        AccessMode::Open,
        replicators,
        peer_state,
        always_limited_rate_limiter(),
    );
    let transport = coordinator.runtime.transport.clone();
    let peer = random_peer_id();

    let result = coordinator
        .handle_transport_event(doc_sync_event(peer.clone()))
        .await;
    assert!(
        matches!(&result, Err(e) if e.is_rate_limited()),
        "expected rate-limited rejection, got {:?}",
        result
    );

    let reply = transport.doc_sync_replies().pop().expect("nack reply sent");
    assert_eq!(
        reply.err_message.as_deref(),
        Some(crate::error::RATE_LIMITED_MESSAGE)
    );
}

#[tokio::test]
async fn branchable_sync_rate_limited_replies_backpressure_nack() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let (coordinator, _events) = create_test_coordinator_with_rate_limiter(
        AccessMode::Open,
        replicators,
        peer_state,
        always_limited_rate_limiter(),
    );
    let transport = coordinator.runtime.transport.clone();
    let peer = random_peer_id();

    let result = coordinator
        .handle_transport_event(branchable_sync_event(peer.clone(), "collection1"))
        .await;
    assert!(
        matches!(&result, Err(e) if e.is_rate_limited()),
        "expected rate-limited rejection, got {:?}",
        result
    );

    let reply = transport
        .branchable_replies()
        .pop()
        .expect("nack reply sent");
    assert_eq!(
        reply.err_message.as_deref(),
        Some(crate::error::RATE_LIMITED_MESSAGE)
    );
}

#[tokio::test]
async fn car_fetch_rate_limited_replies_empty_response() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let (coordinator, _events) = create_test_coordinator_with_rate_limiter(
        AccessMode::Open,
        replicators,
        peer_state,
        always_limited_rate_limiter(),
    );
    let transport = coordinator.runtime.transport.clone();
    let peer = random_peer_id();

    let result = coordinator
        .handle_transport_event(car_fetch_event(peer.clone(), cid_for(BLOCK_DATA)))
        .await;
    assert!(
        matches!(&result, Err(e) if e.is_rate_limited()),
        "expected rate-limited rejection, got {:?}",
        result
    );

    // CAR has no error reply type - an explicit empty response beats a hung stream.
    let responses = transport.car_responses();
    assert_eq!(responses.last().map(Vec::len), Some(0));
}

/// #1088 W5 (in-process half): 8 pushers × 6 docs fan into a hub whose pending
/// map holds 2 slots. Nothing drains (no Bitswap in NoopTransport), so exactly
/// `cap` pushes can be admitted; every other push must be nacked — never
/// success-acked (M1) — and the pending depth must never exceed the cap.
#[tokio::test]
async fn fan_in_pushes_keep_pending_depth_bounded_and_account_every_reply() {
    const CAP: usize = 2;
    const FAN_IN_PUSHERS: usize = 8;
    const DOCS_PER_PUSHER: usize = 6;

    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let (coordinator, _events) = create_test_coordinator_with_sync_config(
        AccessMode::Open,
        replicators,
        peer_state,
        SyncConfig {
            max_pending_dags: CAP,
            ..SyncConfig::default()
        },
    );
    let transport = coordinator.runtime.transport.clone();

    for pusher in 0..FAN_IN_PUSHERS {
        let peer = random_peer_id();
        for doc in 0..DOCS_PER_PUSHER {
            let _ = coordinator
                .handle_transport_event(TransportEvent::TwoStreamRequest {
                    peer_id: peer.clone(),
                    request: pushlog_request_with_missing_link(
                        "collection1",
                        &format!("p{pusher}-d{doc}"),
                    ),
                    token: None,
                    is_explicit_replicator: false,
                    explicit_replay_authorization: None,
                })
                .await;
            assert!(
                coordinator.manager.pending_dag_count() <= CAP,
                "pending depth exceeded the configured cap"
            );
        }
    }

    let replies = transport.two_stream_replies();
    let total = FAN_IN_PUSHERS * DOCS_PER_PUSHER;
    assert_eq!(replies.len(), total, "every push must receive a reply");

    let successes = replies.iter().filter(|r| r.err_message.is_none()).count();
    let nacks = replies
        .iter()
        .filter(|r| r.err_message.as_deref() == Some(crate::error::AT_CAPACITY_MESSAGE))
        .count();
    assert_eq!(
        successes, CAP,
        "only registered pushes may be success-acked (M1: success => registered-or-merged)"
    );
    assert_eq!(
        nacks,
        total - CAP,
        "every dropped registration must be nacked with the capacity sentinel"
    );

    // No completed DAGs and no arriving link blocks here, so admission overflow
    // must not trigger any pending-DAG re-walks (the M4 CPU burn shape).
    let diagnostics = coordinator.manager.diagnostics().snapshot();
    assert_eq!(
        diagnostics.missing_link_retries, 0,
        "capacity overflow must not cause retry walks"
    );
}

/// An error DocSyncReply (e.g. a RATE_LIMITED_MESSAGE nack from #1088 W4)
/// carries no results; it must surface as an error, not be consumed as an
/// empty successful sync.
#[tokio::test]
async fn doc_sync_error_reply_is_not_consumed_as_empty_success() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let (coordinator, _events) = create_test_coordinator(AccessMode::Open, replicators, peer_state);
    let peer = random_peer_id();

    let result = coordinator
        .handle_transport_event(TransportEvent::DocSyncReply {
            peer_id: peer.clone(),
            reply: DocSyncReply::error("doc-sync-1", crate::error::RATE_LIMITED_MESSAGE),
        })
        .await;

    assert!(
        result.is_err(),
        "an error reply must not be treated as an empty successful sync, got {:?}",
        result
    );
}

#[tokio::test]
async fn doc_sync_reply_starts_independent_dag_roots_concurrently() {
    let transport = NoopTransport::new();
    let transport_handle = transport.clone();
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let (coordinator, _events) = SyncCoordinator::new(transport, blockstore, SyncConfig::default())
        .await
        .unwrap();
    let first_root = cid_for(b"first root");
    let second_root = cid_for(b"second root");

    coordinator
        .handle_transport_event(TransportEvent::DocSyncReply {
            peer_id: random_peer_id(),
            reply: DocSyncReply::success(
                "doc-sync-1",
                vec![
                    DocSyncItem {
                        doc_id: "doc1".to_string(),
                        heads: vec![first_root.to_bytes()],
                    },
                    DocSyncItem {
                        doc_id: "doc2".to_string(),
                        heads: vec![second_root.to_bytes()],
                    },
                ],
            ),
        })
        .await
        .unwrap();

    timeout(Duration::from_secs(1), async {
        while transport_handle.car_requests().len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("a stalled first root must not block the second root");

    let requested: std::collections::HashSet<_> =
        transport_handle.car_requests().into_iter().collect();
    assert_eq!(
        requested,
        std::collections::HashSet::from([first_root, second_root])
    );
}

/// Same for BranchableSync: an error reply has empty heads and must not be
/// mistaken for "peer has no heads for collection".
#[tokio::test]
async fn branchable_sync_error_reply_is_not_consumed_as_no_heads() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let (coordinator, _events) = create_test_coordinator(AccessMode::Open, replicators, peer_state);
    let peer = random_peer_id();

    let result = coordinator
        .handle_transport_event(TransportEvent::BranchableSyncReply {
            peer_id: peer.clone(),
            reply: BranchableSyncReply::error(
                "branchable-1",
                "collection1",
                crate::error::RATE_LIMITED_MESSAGE,
            ),
        })
        .await;

    assert!(
        result.is_err(),
        "an error reply must not be treated as \"peer has no heads\", got {:?}",
        result
    );
}

/// The gossip limiter (abuse ladder) and the request-intake limiter (pacing)
/// are separate: an exhausted gossip bucket must not block replicator pushes,
/// which have their own paced bucket.
#[tokio::test]
async fn request_intake_uses_paced_limiter_separate_from_gossip_ladder() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let (coordinator, _events) = create_test_coordinator_with_split_limiters(
        AccessMode::Open,
        replicators,
        peer_state,
        always_limited_rate_limiter(),
        Arc::new(PeerRateLimiter::default()),
    );
    let peer = random_peer_id();

    let gossip_result = coordinator
        .handle_transport_event(gossip_event(peer.clone(), "collection1"))
        .await;
    assert!(
        matches!(&gossip_result, Err(e) if e.is_rate_limited()),
        "gossip must still be governed by the ladder limiter, got {:?}",
        gossip_result
    );

    let push_result = coordinator
        .handle_transport_event(two_stream_event(peer.clone(), "collection1", false))
        .await;
    assert!(
        push_result.is_ok(),
        "request intake must use its own paced limiter, got {:?}",
        push_result
    );
}

#[tokio::test]
async fn sync_status_surfaces_quarantine_counters() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let (coordinator, _events) = create_test_coordinator(AccessMode::Open, replicators, peer_state);

    let before = coordinator.sync_status();
    assert_eq!(before.pending_dag_terminal_quarantined, 0);
    assert_eq!(before.quarantined_pending_dags, 0);

    let root = cid_for(b"sync-status-quarantine-root");
    coordinator
        .manager
        .quarantine_pending_dag(&root, "unique constraint violation")
        .await;

    let after = coordinator.sync_status();
    assert_eq!(after.pending_dag_terminal_quarantined, 1);
    assert_eq!(after.quarantined_pending_dags, 1);
}
