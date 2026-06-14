//! Tests for coordinator access control (findings 03-21).
//!
//! Verifies that DocSync and BranchableSync handlers enforce access checks
//! in Controlled mode before processing requests.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use blockstore::{Blockstore, DefraBlockstore, Error as BlockstoreError};
use cid::Cid;
use multihash_codetable::{Code, MultihashDigest};
use storage::backends::MemoryStore;
use tokio::time::timeout;

use crate::bitswap::{AccessMode, ReplicatorRegistry};
use crate::error::Error;
use crate::message::{
    BranchableSyncReply, BranchableSyncRequest, CarFetchRequest, DocSyncReply, DocSyncRequest,
    PushLogBroadcast, PushLogRequest,
};
use crate::sync::broadcaster::Broadcaster;
use crate::sync::collection_store::NoOpCollectionStorage;
use crate::sync::head_provider::{DocumentHeadProvider, NoOpHeadProvider};
use crate::sync::manager::{SyncConfig, SyncEvent, SyncManager, DEFAULT_PUSH_SEND_TIMEOUT};
use crate::sync::peer_state::PeerStateTracker;
use crate::sync::rate_limiter::PeerRateLimiter;
use crate::sync::SyncShutdownHandle;
use crate::topics::{DefraTopic, DOC_SYNC_TOPIC};
use crate::transport::{MessageId, P2PTransport, PeerAddr, PeerId, TransportEvent};
use crate::QueryId;
use crate::ReplicatorInfo;
use async_trait::async_trait;
use parking_lot::RwLock;

use super::authorizer::RuntimeAuthorizer;
use super::{
    DagFetchLimiter, SyncAccessState, SyncCoordinator, SyncRuntime, SyncSubscriptionState,
    DEFAULT_MAX_CONCURRENT_DAG_FETCHES, DEFAULT_MAX_CONCURRENT_PUSH_TASKS,
};

type TestBlockstore = DefraBlockstore<MemoryStore>;
const BLOCK_DATA: &[u8] = b"block data";

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
    let transport = NoopTransport::new();
    let local_peer_id = transport.local_peer_id().to_string();
    let broadcaster = Broadcaster::new(transport.clone());
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    create_test_coordinator_with_blockstore(TestCoordinatorParams {
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
        access_mode,
        replicators,
        peer_state,
        transport,
        local_peer_id,
        broadcaster,
        blockstore,
        rate_limiter,
    } = params;

    let (manager, events) = SyncManager::new(blockstore, peer_state.clone(), SyncConfig::default());

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
            failure_tx: None,
            dag_fetch_limiter: DagFetchLimiter::new(DEFAULT_MAX_CONCURRENT_DAG_FETCHES),
            push_semaphore: Arc::new(tokio::sync::Semaphore::new(
                DEFAULT_MAX_CONCURRENT_PUSH_TASKS,
            )),
            rate_limiter,
            push_send_timeout: DEFAULT_PUSH_SEND_TIMEOUT,
            shutdown: SyncShutdownHandle::new(),
            filter_matcher: Arc::new(crate::replicator::EqOnlyFilterMatcher),
        },
        manager,
        access: SyncAccessState {
            peer_state,
            local_peer_id,
            access_mode,
            replicators,
        },
        subscriptions: SyncSubscriptionState {
            subscribed_collections: Arc::new(tokio::sync::RwLock::new(
                std::collections::HashSet::new(),
            )),
            collection_store: Arc::new(NoOpCollectionStorage),
            head_provider,
        },
        authorizer,
        document_acp: std::sync::OnceLock::new(),
        kms_transport: std::sync::OnceLock::new(),
        pubsub_services: None,
    };

    (coordinator, events)
}

struct ConflictOnceBlockstore {
    inner: TestBlockstore,
    remaining_put_conflicts: AtomicUsize,
    put_attempts: AtomicUsize,
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
}

impl NoopTransport {
    fn new() -> Self {
        Self {
            peer_id: PeerId::new("local-peer".to_string()),
            pubkey: vec![1, 2, 3],
            replicators: Arc::new(RwLock::new(std::collections::HashMap::new())),
            connected_peers: Arc::new(RwLock::new(Vec::new())),
            doc_sync_replies: Arc::new(RwLock::new(Vec::new())),
        }
    }

    fn set_connected_peers(&self, peers: Vec<PeerId>) {
        *self.connected_peers.write() = peers;
    }

    fn doc_sync_replies(&self) -> Vec<DocSyncReply> {
        self.doc_sync_replies.read().clone()
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
        _token: Self::ResponseToken,
        _reply: crate::message::PushLogReply,
    ) -> crate::Result<()> {
        Ok(())
    }

    async fn send_two_stream_request(
        &self,
        _peer_id: &PeerId,
        _req: PushLogRequest,
    ) -> crate::Result<crate::message::PushLogReply> {
        Ok(crate::message::PushLogReply::success("noop"))
    }

    async fn send_two_stream_response(
        &self,
        _peer_id: &PeerId,
        _reply: crate::message::PushLogReply,
    ) -> crate::Result<()> {
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
        _reply: crate::message::BranchableSyncReply,
    ) -> crate::Result<()> {
        Ok(())
    }

    async fn send_car_request(&self, _peer_id: &PeerId, _root_cid: Cid) -> crate::Result<()> {
        Ok(())
    }

    async fn send_car_response(&self, _peer_id: &PeerId, _car_data: Vec<u8>) -> crate::Result<()> {
        Ok(())
    }

    async fn send_car_response_token(
        &self,
        _token: Self::ResponseToken,
        _car_data: Vec<u8>,
    ) -> crate::Result<()> {
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
        _reply: crate::message::BranchableSyncReply,
    ) -> crate::Result<()> {
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

fn doc_sync_event(peer_id: PeerId) -> TransportEvent<()> {
    TransportEvent::DocSyncRequest {
        peer_id,
        request: DocSyncRequest::new(vec!["doc1".to_string()]),
        token: None,
    }
}

fn branchable_sync_event(peer_id: PeerId, collection_id: &str) -> TransportEvent<()> {
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
) -> TransportEvent<()> {
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
    let libp2p_peer = libp2p::PeerId::random();
    PeerId::from(libp2p_peer)
}

fn cid_for(data: &[u8]) -> Cid {
    let hash = Code::Sha2_256.digest(data);
    Cid::new_v1(0x71, hash)
}

fn pushlog_request(collection_id: &str) -> PushLogRequest {
    PushLogRequest::new(
        "doc1".to_string(),
        bytes::Bytes::from(cid_for(BLOCK_DATA).to_bytes()),
        collection_id.to_string(),
        "creator1".to_string(),
        bytes::Bytes::copy_from_slice(BLOCK_DATA),
    )
}

fn pushlog_event(peer_id: PeerId, collection_id: &str) -> TransportEvent<()> {
    TransportEvent::PushLogRequest {
        peer_id,
        request: pushlog_request(collection_id),
        token: (),
    }
}

fn gossip_event(peer_id: PeerId, collection_id: &str) -> TransportEvent<()> {
    gossip_event_on_topic(peer_id, collection_id, collection_id)
}

fn gossip_event_on_topic(peer_id: PeerId, topic: &str, collection_id: &str) -> TransportEvent<()> {
    TransportEvent::GossipMessage {
        propagation_source: peer_id,
        message_id: MessageId::new("gossip".to_string()),
        topic: topic.to_string(),
        message: PushLogBroadcast::from_request(&pushlog_request(collection_id)),
    }
}

fn car_fetch_event(peer_id: PeerId, root_cid: Cid) -> TransportEvent<()> {
    TransportEvent::CarFetchRequest {
        peer_id,
        request: CarFetchRequest::full_dag(root_cid),
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
) -> TransportEvent<()> {
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
            doc_id: b"doc1".to_vec(),
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

#[tokio::test]
async fn car_fetch_controlled_mode_rejects_wrong_collection_root() {
    use defra_core::{Block, CompositeDeltaPayload, CrdtDelta};

    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());

    let peer = random_peer_id();
    replicators.add_replicator("collection_a", peer.as_str());

    let transport = NoopTransport::new();
    let local_peer_id = transport.local_peer_id().to_string();
    let broadcaster = Broadcaster::new(transport.clone());
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));

    let block = Block::new(
        CrdtDelta::Composite(CompositeDeltaPayload {
            doc_id: b"doc1".to_vec(),
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

    assert!(
        matches!(result, Err(Error::AccessDenied { .. })),
        "CAR root access must be collection-scoped in Controlled mode, got {:?}",
        result
    );
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
            doc_id: b"doc1".to_vec(),
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
            assert_eq!(doc_id, "doc1");
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
async fn gossip_controlled_mode_rejects_outbound_replicator_target() {
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
        matches!(&result, Err(Error::AccessDenied { .. })),
        "outbound replicator targets must not be accepted as gossip sources, got {:?}",
        result
    );
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
async fn gossip_open_mode_rejects_outbound_replicator_target() {
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
        "open access mode must still preserve one-way replicator gossip direction, got {:?}",
        result
    );
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

#[tokio::test]
async fn two_stream_bypasses_gossip_rate_limiter_for_authenticated_sync() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let (coordinator, mut events) = create_test_coordinator_with_rate_limiter(
        AccessMode::Open,
        replicators,
        peer_state,
        Arc::new(PeerRateLimiter::new(0, 0.0)),
    );

    let peer = random_peer_id();
    coordinator
        .handle_transport_event(two_stream_event(peer.clone(), "collection1", false))
        .await
        .unwrap();

    match recv_block_received(&mut events).await {
        SyncEvent::BlockReceived { sender_peer, .. } => {
            assert_eq!(sender_peer.as_deref(), Some(peer.as_str()));
        }
        other => panic!("expected BlockReceived, got {:?}", other),
    }
}

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
