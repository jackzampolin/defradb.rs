//! Tests for coordinator access control (findings 03-21).
//!
//! Verifies that DocSync and BranchableSync handlers enforce access checks
//! in Controlled mode before processing requests.

use std::sync::Arc;

use blockstore::DefraBlockstore;
use storage::backends::MemoryStore;

use crate::bitswap::{AccessMode, ReplicatorRegistry};
use crate::error::Error;
use crate::host::libp2p_transport::Libp2pTransport;
use crate::host::P2PHostHandle;
use crate::message::{BranchableSyncRequest, DocSyncRequest, MetaData};
use crate::sync::broadcaster::Broadcaster;
use crate::sync::collection_store::NoOpCollectionStorage;
use crate::sync::head_provider::NoOpHeadProvider;
use crate::sync::manager::{SyncConfig, SyncManager};
use crate::sync::peer_state::PeerStateTracker;
use crate::sync::rate_limiter::PeerRateLimiter;
use crate::transport::{PeerId, TransportEvent};

use super::{SyncCoordinator, MAX_CONCURRENT_DAG_FETCHES, MAX_CONCURRENT_PUSH_TASKS};

type TestBlockstore = DefraBlockstore<MemoryStore>;

fn create_test_coordinator(
    access_mode: AccessMode,
    replicators: Arc<ReplicatorRegistry>,
    peer_state: Arc<PeerStateTracker>,
) -> (
    SyncCoordinator<TestBlockstore, Libp2pTransport>,
    tokio::sync::mpsc::Receiver<crate::sync::manager::SyncEvent>,
) {
    let host = P2PHostHandle::test_handle();
    let local_peer_id = host.local_peer_id_cached().to_string();
    let transport = Libp2pTransport::new(host);
    let broadcaster = Broadcaster::new(transport.clone());
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let (manager, events) = SyncManager::new(blockstore, peer_state.clone(), SyncConfig::default());

    let coordinator = SyncCoordinator {
        transport,
        broadcaster,
        manager,
        peer_state,
        local_peer_id,
        access_mode,
        replicators,
        subscribed_collections: Arc::new(
            tokio::sync::RwLock::new(std::collections::HashSet::new()),
        ),
        collection_store: Arc::new(NoOpCollectionStorage),
        head_provider: Arc::new(NoOpHeadProvider),
        failure_tx: None,
        dag_fetch_semaphore: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_DAG_FETCHES)),
        push_semaphore: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_PUSH_TASKS)),
        rate_limiter: Arc::new(PeerRateLimiter::default()),
    };

    (coordinator, events)
}

fn doc_sync_event(peer_id: PeerId) -> TransportEvent {
    TransportEvent::DocSyncRequest {
        peer_id,
        request: DocSyncRequest {
            metadata: MetaData::new(),
            doc_ids: vec!["doc1".to_string()],
        },
        token: None,
    }
}

fn branchable_sync_event(peer_id: PeerId, collection_id: &str) -> TransportEvent {
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
async fn branchable_sync_controlled_mode_allows_subscribed_connected_peer() {
    let replicators = Arc::new(ReplicatorRegistry::new());
    let peer_state = Arc::new(PeerStateTracker::new());

    let connected_peer = random_peer_id();
    peer_state.peer_connected(connected_peer.as_str());

    let (coordinator, _events) =
        create_test_coordinator(AccessMode::Controlled, replicators, peer_state);

    // Add collection to subscribed set
    coordinator
        .subscribed_collections
        .write()
        .await
        .insert("collection1".to_string());

    let result = coordinator
        .handle_transport_event(branchable_sync_event(connected_peer, "collection1"))
        .await;

    assert!(
        !matches!(&result, Err(Error::AccessDenied { .. })),
        "Connected peer on subscribed collection should not get AccessDenied, got {:?}",
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
