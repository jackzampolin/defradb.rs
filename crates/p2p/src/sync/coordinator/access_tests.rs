//! Tests for coordinator access control (findings 03-21).
//!
//! Verifies that DocSync and BranchableSync handlers enforce access checks
//! in Controlled mode before processing requests.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use blockstore::{Blockstore, DefraBlockstore, Error as BlockstoreError};
use cid::multihash::{Code, MultihashDigest};
use cid::Cid;
use storage::backends::MemoryStore;
use tokio::time::timeout;

use crate::bitswap::{AccessMode, ReplicatorRegistry};
use crate::error::Error;
use crate::message::{
    BranchableSyncRequest, DocSyncRequest, MetaData, PushLogBroadcast, PushLogRequest,
};
use crate::sync::broadcaster::Broadcaster;
use crate::sync::collection_store::NoOpCollectionStorage;
use crate::sync::head_provider::NoOpHeadProvider;
use crate::sync::manager::{SyncConfig, SyncEvent, SyncManager};
use crate::sync::peer_state::PeerStateTracker;
use crate::sync::rate_limiter::PeerRateLimiter;
use crate::topics::DefraTopic;
use crate::transport::{MessageId, P2PTransport, PeerAddr, PeerId, TransportEvent};
use crate::QueryId;
use crate::ReplicatorInfo;
use async_trait::async_trait;
use parking_lot::RwLock;

use super::{
    SyncAccessState, SyncCoordinator, SyncRuntime, SyncSubscriptionState,
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
    let transport = NoopTransport::new();
    let local_peer_id = transport.local_peer_id().to_string();
    let broadcaster = Broadcaster::new(transport.clone());
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    create_test_coordinator_with_blockstore(
        access_mode,
        replicators,
        peer_state,
        transport,
        local_peer_id,
        broadcaster,
        blockstore,
    )
}

fn create_test_coordinator_with_blockstore<B: Blockstore + 'static>(
    access_mode: AccessMode,
    replicators: Arc<ReplicatorRegistry>,
    peer_state: Arc<PeerStateTracker>,
    transport: NoopTransport,
    local_peer_id: String,
    broadcaster: Broadcaster<NoopTransport>,
    blockstore: Arc<B>,
) -> (
    SyncCoordinator<B, NoopTransport>,
    tokio::sync::mpsc::Receiver<crate::sync::manager::SyncEvent>,
) {
    let (manager, events) = SyncManager::new(blockstore, peer_state.clone(), SyncConfig::default());

    let coordinator = SyncCoordinator {
        runtime: SyncRuntime {
            transport,
            broadcaster,
            failure_tx: None,
            dag_fetch_semaphore: Arc::new(tokio::sync::Semaphore::new(
                DEFAULT_MAX_CONCURRENT_DAG_FETCHES,
            )),
            push_semaphore: Arc::new(tokio::sync::Semaphore::new(
                DEFAULT_MAX_CONCURRENT_PUSH_TASKS,
            )),
            rate_limiter: Arc::new(PeerRateLimiter::default()),
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
            head_provider: Arc::new(NoOpHeadProvider),
        },
        document_acp: std::sync::OnceLock::new(),
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
}

impl NoopTransport {
    fn new() -> Self {
        Self {
            peer_id: PeerId::new("local-peer".to_string()),
            pubkey: vec![1, 2, 3],
            replicators: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
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

    async fn listen(&self, _addr: PeerAddr) -> crate::Result<()> {
        Ok(())
    }

    async fn connected_peers(&self) -> crate::Result<Vec<PeerId>> {
        Ok(Vec::new())
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
        _reply: crate::message::DocSyncReply,
    ) -> crate::Result<()> {
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
        _reply: crate::message::DocSyncReply,
    ) -> crate::Result<()> {
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
        request: DocSyncRequest {
            metadata: MetaData::new(),
            doc_ids: vec!["doc1".to_string()],
        },
        token: None,
    }
}

fn branchable_sync_event(peer_id: PeerId, collection_id: &str) -> TransportEvent<()> {
    TransportEvent::BranchableSyncRequest {
        peer_id,
        request: BranchableSyncRequest {
            metadata: MetaData::new(),
            collection_id: collection_id.to_string(),
        },
        token: None,
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
    TransportEvent::GossipMessage {
        propagation_source: peer_id,
        message_id: MessageId::new("gossip".to_string()),
        topic: collection_id.to_string(),
        message: PushLogBroadcast::from_request(&pushlog_request(collection_id)),
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
        "Connected peer should not get AccessDenied, got {:?}",
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

    // Connected peers are allowed without explicit collection subscription.
    // This matches Go DefraDB behavior where replicator targets accept
    // push-logs from any connected peer.
    let result = coordinator
        .handle_transport_event(branchable_sync_event(connected_peer, "collection1"))
        .await;

    assert!(
        !matches!(&result, Err(Error::AccessDenied { .. })),
        "Connected peer should not get AccessDenied, got {:?}",
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

#[tokio::test]
async fn pushlog_connected_peer_is_not_marked_explicit_replicator() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let peer = random_peer_id();
    peer_state.peer_connected(peer.as_str());

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
                !is_explicit_replicator,
                "connected peer must not get explicit replicator trust"
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

#[tokio::test]
async fn create_replicator_updates_access_registry_for_gossip() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let (coordinator, _events) =
        create_test_coordinator(AccessMode::Controlled, replicators, peer_state);

    let peer = random_peer_id();
    coordinator
        .create_replicator(&peer, vec!["collection1".to_string()], false)
        .await
        .unwrap();

    let result = coordinator
        .handle_transport_event(gossip_event(peer, "collection1"))
        .await;

    assert!(
        !matches!(&result, Err(Error::AccessDenied { .. })),
        "create_replicator should authorize collection access immediately, got {:?}",
        result
    );
}

#[tokio::test]
async fn delete_replicator_revokes_access_for_gossip() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let (coordinator, _events) =
        create_test_coordinator(AccessMode::Controlled, replicators, peer_state);

    let peer = random_peer_id();
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
        "delete_replicator should revoke collection access, got {:?}",
        result
    );
}

#[tokio::test]
async fn two_stream_connected_peer_is_not_marked_explicit_replicator() {
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
                "connected peer must not get explicit replicator trust on two-stream ingress"
            );
        }
        other => panic!("expected BlockReceived, got {:?}", other),
    }
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
async fn pushlog_retries_transient_transaction_conflicts_without_sync_error() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());
    let transport = NoopTransport::new();
    let local_peer_id = transport.local_peer_id().to_string();
    let broadcaster = Broadcaster::new(transport.clone());
    let blockstore = Arc::new(ConflictOnceBlockstore::new());

    let (coordinator, mut events) = create_test_coordinator_with_blockstore(
        AccessMode::Open,
        replicators,
        peer_state,
        transport,
        local_peer_id,
        broadcaster,
        blockstore.clone(),
    );

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

    let (coordinator, mut events) = create_test_coordinator_with_blockstore(
        AccessMode::Open,
        replicators,
        peer_state,
        transport,
        local_peer_id,
        broadcaster,
        blockstore.clone(),
    );

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
