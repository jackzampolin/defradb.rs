//! Tests for the sync manager module.

use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;

use blockstore::{Blockstore, DefraBlockstore};
use cid::Cid;
use defra_core::{Block, CompositeDeltaPayload, CrdtDelta, DAGLink, LwwDeltaPayload};
use multihash_codetable::{Code, MultihashDigest};
use storage::backends::MemoryStore;

use p2p::error::Error;
use p2p::message::PushLogBroadcast;
use p2p::sync::{PeerStateTracker, SyncConfig, SyncEvent, SyncManager};

/// Compute a valid SHA2-256 CIDv1 for `data`.
fn cid_for(data: &[u8]) -> Cid {
    let hash = Code::Sha2_256.digest(data);
    Cid::new_v1(0x71, hash)
}

/// Block data used in most tests.
const BLOCK_DATA: &[u8] = b"block data";

fn test_cid() -> Cid {
    cid_for(BLOCK_DATA)
}

fn test_peer_state() -> Arc<PeerStateTracker> {
    Arc::new(PeerStateTracker::new())
}

fn create_test_broadcast(cid: &Cid) -> PushLogBroadcast {
    PushLogBroadcast::new(
        "doc123".to_string(),
        Bytes::from(cid.to_bytes()),
        "collection1".to_string(),
        "creator1".to_string(),
        Bytes::from(BLOCK_DATA.to_vec()),
    )
}

fn create_lww_block(field_name: &str) -> (Cid, Vec<u8>) {
    let block = Block::new(
        CrdtDelta::Lww(LwwDeltaPayload {
            doc_id: b"doc123".to_vec(),
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

fn create_composite_block(links: Vec<DAGLink>) -> (Cid, Vec<u8>) {
    let block = Block::new(
        CrdtDelta::Composite(CompositeDeltaPayload {
            doc_id: b"doc123".to_vec(),
            schema_version_id: "schema1".to_string(),
            priority: 1,
            status: 1,
        }),
        vec![],
        links,
    );
    let bytes = block.to_dag_cbor().expect("encode composite block");
    let cid = block.generate_cid().expect("generate composite cid");
    (cid, bytes)
}

#[tokio::test]
async fn test_process_pushlog_stores_block() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let (manager, mut events) =
        SyncManager::new(blockstore.clone(), test_peer_state(), SyncConfig::default());

    let cid = test_cid();
    let msg = create_test_broadcast(&cid);

    // Process the pushlog
    manager
        .process_pushlog(&msg, None, false, None)
        .await
        .unwrap();

    // Block should be stored
    assert!(blockstore.has(&cid).await.unwrap());

    // Should not be merged yet
    assert!(!blockstore.is_merged(&cid).await.unwrap());

    // Should receive BlockReceived event
    let event = events.try_recv().unwrap();
    match event {
        SyncEvent::BlockReceived {
            cid: event_cid,
            doc_id,
            ..
        } => {
            assert_eq!(event_cid, cid);
            assert_eq!(doc_id, "doc123");
        }
        _ => panic!("Expected BlockReceived event"),
    }
}

#[tokio::test]
async fn test_process_pushlog_already_merged() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let (manager, mut events) =
        SyncManager::new(blockstore.clone(), test_peer_state(), SyncConfig::default());

    let cid = test_cid();
    let msg = create_test_broadcast(&cid);

    // Pre-store and merge the block directly in the blockstore.
    blockstore.put(&cid, BLOCK_DATA).await.unwrap();
    blockstore.mark_as_merged(&cid).await.unwrap();

    // Process the pushlog
    manager
        .process_pushlog(&msg, None, false, None)
        .await
        .unwrap();

    // Should receive BlockAlreadyMerged event
    let event = events.try_recv().unwrap();
    match event {
        SyncEvent::BlockAlreadyMerged { cid: event_cid, .. } => {
            assert_eq!(event_cid, cid);
        }
        _ => panic!("Expected BlockAlreadyMerged event"),
    }
}

#[tokio::test]
async fn test_mark_as_merged() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let (manager, _events) =
        SyncManager::new(blockstore.clone(), test_peer_state(), SyncConfig::default());

    let cid = test_cid();
    let msg = create_test_broadcast(&cid);

    // Process the pushlog
    manager
        .process_pushlog(&msg, None, false, None)
        .await
        .unwrap();

    // Not merged initially
    assert!(!manager.is_merged(&cid).await.unwrap());

    // Mark as merged
    manager.mark_as_merged(&cid).await.unwrap();

    // Now merged
    assert!(manager.is_merged(&cid).await.unwrap());
}

#[tokio::test]
async fn test_get_unmerged() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let (manager, _events) =
        SyncManager::new(blockstore.clone(), test_peer_state(), SyncConfig::default());

    let cid = test_cid();
    let msg = create_test_broadcast(&cid);

    // Initially no unmerged
    let unmerged = manager.get_unmerged().await.unwrap();
    assert!(unmerged.is_empty());

    // Process pushlog
    manager
        .process_pushlog(&msg, None, false, None)
        .await
        .unwrap();

    // Now one unmerged
    let unmerged = manager.get_unmerged().await.unwrap();
    assert_eq!(unmerged.len(), 1);
    assert!(unmerged.contains(&cid));

    // Mark as merged
    manager.mark_as_merged(&cid).await.unwrap();

    // Now none unmerged
    let unmerged = manager.get_unmerged().await.unwrap();
    assert!(unmerged.is_empty());
}

#[tokio::test]
async fn test_process_pushlog_invalid_cid_returns_error() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let (manager, _events) = SyncManager::new(blockstore, test_peer_state(), SyncConfig::default());

    // Create a broadcast with invalid CID bytes
    let msg = PushLogBroadcast::new(
        "doc123".to_string(),
        Bytes::from(vec![0xFF, 0xFF, 0xFF]), // Invalid CID bytes
        "collection1".to_string(),
        "creator1".to_string(),
        Bytes::from(b"block data".to_vec()),
    );

    // Processing should fail with InvalidCid error
    let result = manager.process_pushlog(&msg, None, false, None).await;
    assert!(result.is_err());
    match result {
        Err(Error::InvalidCid(msg)) => {
            assert!(msg.contains("Failed to parse CID"));
        }
        other => panic!("Expected InvalidCid error, got {:?}", other),
    }
}

#[tokio::test]
async fn test_process_pushlog_cid_mismatch_returns_error() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let (manager, _events) = SyncManager::new(blockstore, test_peer_state(), SyncConfig::default());

    // CID is for "block data" but we send different block content.
    let cid = test_cid();
    let msg = PushLogBroadcast::new(
        "doc123".to_string(),
        Bytes::from(cid.to_bytes()),
        "collection1".to_string(),
        "creator1".to_string(),
        Bytes::from(b"tampered content".to_vec()),
    );

    let result = manager.process_pushlog(&msg, None, false, None).await;
    assert!(result.is_err());
    assert!(matches!(result, Err(Error::BlockCidMismatch { .. })));
}

#[tokio::test]
async fn test_concurrent_processing_second_waiter_processes_on_first_not_merged() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let (manager, mut events) =
        SyncManager::new(blockstore.clone(), test_peer_state(), SyncConfig::default());
    let manager = Arc::new(manager);

    let cid = test_cid();
    let msg = create_test_broadcast(&cid);

    // Flag to track if first processor completed
    let first_done = Arc::new(AtomicBool::new(false));

    // First task: acquire lock, store block, but DON'T mark as merged
    let manager1 = manager.clone();
    let msg1 = msg.clone();
    let first_done1 = first_done.clone();
    let first_task = tokio::spawn(async move {
        manager1
            .process_pushlog(&msg1, None, false, None)
            .await
            .unwrap();
        first_done1.store(true, Ordering::SeqCst);
    });

    // Give first task time to acquire the lock
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Second task: should wait for first, then also process (since not merged)
    let manager2 = manager.clone();
    let msg2 = msg.clone();
    let second_task = tokio::spawn(async move {
        manager2
            .process_pushlog(&msg2, None, false, None)
            .await
            .unwrap();
    });

    // Wait for both tasks
    first_task.await.unwrap();
    second_task.await.unwrap();

    // Block should be stored
    assert!(blockstore.has(&cid).await.unwrap());

    // We should get at least one BlockReceived event
    // (could get two if second waiter also processes before checking merge status)
    let mut received_count = 0;
    while let Ok(event) = events.try_recv() {
        match event {
            SyncEvent::BlockReceived { .. } => received_count += 1,
            SyncEvent::BlockAlreadyMerged { .. } => {} // Also valid
            _ => {}
        }
    }
    assert!(
        received_count >= 1,
        "Should have at least one BlockReceived event"
    );
}

#[tokio::test]
async fn test_process_pushlog_returns_error_when_receiver_dropped() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let (manager, events) =
        SyncManager::new(blockstore.clone(), test_peer_state(), SyncConfig::default());

    // Drop the event receiver immediately
    drop(events);

    let cid = test_cid();
    let msg = create_test_broadcast(&cid);

    // Processing should fail with ChannelSend error since receiver is dropped
    let result = manager.process_pushlog(&msg, None, false, None).await;
    assert!(result.is_err());
    match result {
        Err(Error::ChannelSend) => {
            // Expected - channel send failed because receiver was dropped
        }
        other => panic!("Expected ChannelSend error, got {:?}", other),
    }

    // Block should still be stored (we store before sending event)
    assert!(blockstore.has(&cid).await.unwrap());
}

#[tokio::test]
async fn test_already_merged_returns_error_when_receiver_dropped() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let (manager, events) =
        SyncManager::new(blockstore.clone(), test_peer_state(), SyncConfig::default());

    let cid = test_cid();
    let msg = create_test_broadcast(&cid);

    // Pre-store and merge the block directly in the blockstore.
    blockstore.put(&cid, BLOCK_DATA).await.unwrap();
    blockstore.mark_as_merged(&cid).await.unwrap();

    // Drop the event receiver
    drop(events);

    // Processing already-merged block should fail since we can't send event
    let result = manager.process_pushlog(&msg, None, false, None).await;
    assert!(result.is_err());
    match result {
        Err(Error::ChannelSend) => {
            // Expected - can't send BlockAlreadyMerged event
        }
        other => panic!("Expected ChannelSend error, got {:?}", other),
    }
}

#[tokio::test]
async fn test_pending_dag_count_initially_zero() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let (manager, _events) = SyncManager::new(blockstore, test_peer_state(), SyncConfig::default());

    assert_eq!(manager.pending_dag_count(), 0);
}

#[tokio::test]
async fn test_pending_dag_tracking() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let (manager, mut events) =
        SyncManager::new(blockstore.clone(), test_peer_state(), SyncConfig::default());

    // Create a block that has links (simulated by creating IPLD-like data)
    // For simplicity, we'll use a block that fails to parse as IPLD,
    // which will now return an error. Instead, let's test with a valid
    // scenario where the block has no links.
    let cid = test_cid();
    let msg = create_test_broadcast(&cid);

    // Process pushlog - block has no parseable links, should be complete
    manager
        .process_pushlog(&msg, None, false, None)
        .await
        .unwrap();

    // Should receive BlockReceived since no missing links
    let event = events.try_recv().unwrap();
    match event {
        SyncEvent::BlockReceived { cid: event_cid, .. } => {
            assert_eq!(event_cid, cid);
        }
        _ => panic!("Expected BlockReceived event"),
    }

    // No pending dags since block was complete
    assert_eq!(manager.pending_dag_count(), 0);
}

#[tokio::test]
async fn test_pending_dag_completes_when_missing_block_arrives_via_pushlog() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let (manager, mut events) =
        SyncManager::new(blockstore.clone(), test_peer_state(), SyncConfig::default());

    let (field_cid, field_block) = create_lww_block("name");
    let (composite_cid, composite_block) =
        create_composite_block(vec![DAGLink::new("name", field_cid)]);

    let composite_msg = PushLogBroadcast::new(
        "doc123".to_string(),
        Bytes::from(composite_cid.to_bytes()),
        "collection1".to_string(),
        "creator1".to_string(),
        Bytes::from(composite_block),
    );

    manager
        .process_pushlog(&composite_msg, Some("peer-1"), false, None)
        .await
        .unwrap();

    match events.try_recv().expect("pending DAG event") {
        SyncEvent::DagNeedsFetch {
            root_cid, missing, ..
        } => {
            assert_eq!(root_cid, composite_cid);
            assert_eq!(missing, vec![field_cid]);
        }
        other => panic!("expected DagNeedsFetch event, got {:?}", other),
    }
    assert_eq!(
        manager.pending_dag_count(),
        1,
        "composite should be pending"
    );

    let field_msg = PushLogBroadcast::new(
        "doc123".to_string(),
        Bytes::from(field_cid.to_bytes()),
        "collection1".to_string(),
        "creator1".to_string(),
        Bytes::from(field_block),
    );

    manager
        .process_pushlog(&field_msg, Some("peer-1"), false, None)
        .await
        .unwrap();

    let mut saw_field = false;
    let mut saw_root_ready = false;
    for _ in 0..2 {
        let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .expect("expected pending DAG retry event")
            .expect("event channel closed");
        match event {
            SyncEvent::BlockReceived { cid, .. } if cid == field_cid => {
                saw_field = true;
            }
            SyncEvent::DagReady { root_cid, .. } if root_cid == composite_cid => {
                saw_root_ready = true;
            }
            other => panic!("unexpected event after field arrival: {:?}", other),
        }
    }

    assert!(saw_field, "field block should still be processed normally");
    assert!(
        saw_root_ready,
        "pending composite should become ready when its missing field arrives via PushLog"
    );
    assert_eq!(
        manager.pending_dag_count(),
        0,
        "pending DAG should be cleared"
    );
    assert!(
        blockstore.has(&field_cid).await.unwrap(),
        "field block should be stored"
    );
    assert!(
        blockstore.has(&composite_cid).await.unwrap(),
        "composite block should remain stored"
    );
}

#[tokio::test]
async fn test_diagnostics_counters_track_pending_dag_lifecycle() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let (manager, mut events) =
        SyncManager::new(blockstore.clone(), test_peer_state(), SyncConfig::default());

    let diag = manager.diagnostics();
    assert_eq!(diag.snapshot().missing_link_retries, 0);
    assert_eq!(diag.snapshot().pending_dag_resolved, 0);

    let (field_cid, field_block) = create_lww_block("name");
    let (composite_cid, composite_block) =
        create_composite_block(vec![DAGLink::new("name", field_cid)]);

    manager
        .process_pushlog(
            &PushLogBroadcast::new(
                "doc123".into(),
                Bytes::from(composite_cid.to_bytes()),
                "collection1".into(),
                "creator1".into(),
                Bytes::from(composite_block),
            ),
            Some("peer-1"),
            false,
            None,
        )
        .await
        .unwrap();

    // Drain DagNeedsFetch so the channel isn't full.
    let _ = events.try_recv().expect("DagNeedsFetch event");

    // Three retry rounds while the field block is still missing.
    for _ in 0..3 {
        assert!(!manager.retry_pending_dag(&composite_cid).await.unwrap());
    }
    let snap = diag.snapshot();
    assert_eq!(snap.missing_link_retries, 3);
    assert_eq!(snap.pending_dag_resolved, 0);

    // Field arrives via PushLog. process_pushlog calls retry for the
    // composite internally, so both counters advance.
    manager
        .process_pushlog(
            &PushLogBroadcast::new(
                "doc123".into(),
                Bytes::from(field_cid.to_bytes()),
                "collection1".into(),
                "creator1".into(),
                Bytes::from(field_block),
            ),
            Some("peer-1"),
            false,
            None,
        )
        .await
        .unwrap();

    // Drain BlockReceived (field) and DagReady (composite).
    for _ in 0..2 {
        tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .expect("event")
            .expect("channel open");
    }

    let snap = diag.snapshot();
    assert_eq!(
        snap.missing_link_retries, 4,
        "one more retry on field arrival"
    );
    assert_eq!(snap.pending_dag_resolved, 1, "composite DAG resolved once");
    assert_eq!(snap.pending_dag_expired, 0, "never expired within test");
}

#[tokio::test]
async fn test_pending_dag_attempts_increment_per_retry() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let (manager, mut events) =
        SyncManager::new(blockstore.clone(), test_peer_state(), SyncConfig::default());

    let (field_cid, _field_block) = create_lww_block("name");
    let (composite_cid, composite_block) =
        create_composite_block(vec![DAGLink::new("name", field_cid)]);

    let composite_msg = PushLogBroadcast::new(
        "doc123".to_string(),
        Bytes::from(composite_cid.to_bytes()),
        "collection1".to_string(),
        "creator1".to_string(),
        Bytes::from(composite_block),
    );

    manager
        .process_pushlog(&composite_msg, Some("peer-1"), false, None)
        .await
        .unwrap();

    // drain the DagNeedsFetch event so the channel isn't blocked.
    let _ = events.try_recv().expect("pending DAG event");
    assert_eq!(manager.pending_dag_count(), 1);
    assert_eq!(manager.pending_dag_attempts(&composite_cid), 0);

    assert!(!manager.retry_pending_dag(&composite_cid).await.unwrap());
    assert_eq!(manager.pending_dag_attempts(&composite_cid), 1);

    assert!(!manager.retry_pending_dag(&composite_cid).await.unwrap());
    assert_eq!(manager.pending_dag_attempts(&composite_cid), 2);

    // Unknown CIDs report 0 attempts (not panic).
    assert_eq!(manager.pending_dag_attempts(&test_cid()), 0);
}

#[tokio::test]
async fn test_blockstore_accessor() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let (manager, _events) =
        SyncManager::new(blockstore.clone(), test_peer_state(), SyncConfig::default());

    // Verify blockstore accessor returns the same blockstore.
    // Use the computed CID so the put succeeds.
    let cid = test_cid();
    manager.blockstore().put(&cid, BLOCK_DATA).await.unwrap();
    assert!(blockstore.has(&cid).await.unwrap());
}

// --- #1088 W1: pending-DAG capacity must surface as a typed error ---

fn broadcast_for(cid: &Cid, doc_id: &str, block: Vec<u8>) -> PushLogBroadcast {
    PushLogBroadcast::new(
        doc_id.to_string(),
        Bytes::from(cid.to_bytes()),
        "collection1".to_string(),
        "creator1".to_string(),
        Bytes::from(block),
    )
}

#[tokio::test]
async fn test_process_pushlog_pending_capacity_returns_typed_error() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let config = SyncConfig {
        max_pending_dags: 1,
        ..SyncConfig::default()
    };
    let (manager, mut events) = SyncManager::new(blockstore.clone(), test_peer_state(), config);

    // Composite A links to a field block that is never stored -> registers pending.
    let (field_a_cid, _) = create_lww_block("field_a");
    let (comp_a_cid, comp_a_block) =
        create_composite_block(vec![DAGLink::new("field_a", field_a_cid)]);
    manager
        .process_pushlog(
            &broadcast_for(&comp_a_cid, "docA", comp_a_block),
            Some("peer-1"),
            false,
            None,
        )
        .await
        .unwrap();
    let _ = events.try_recv().expect("DagNeedsFetch for composite A");
    assert_eq!(manager.pending_dag_count(), 1);

    // Composite B overflows the single-slot pending map: the registration is
    // dropped, and the caller MUST see a typed capacity error so the reply
    // seams can nack instead of acking success (#1088 M1 invariant:
    // success reply => merged or registered as pending).
    let (field_b_cid, _) = create_lww_block("field_b");
    let (comp_b_cid, comp_b_block) =
        create_composite_block(vec![DAGLink::new("field_b", field_b_cid)]);
    let result = manager
        .process_pushlog(
            &broadcast_for(&comp_b_cid, "docB", comp_b_block),
            Some("peer-2"),
            false,
            None,
        )
        .await;

    assert!(
        matches!(result, Err(Error::PendingDagCapacity { .. })),
        "capacity drop must be a typed error, got {:?}",
        result
    );
    assert_eq!(manager.pending_dag_count(), 1, "registration was dropped");
    // The block itself is kept (a later retry can complete without re-sending it).
    assert!(blockstore.has(&comp_b_cid).await.unwrap());
}

#[tokio::test]
async fn test_max_pending_dags_zero_is_normalized_to_one() {
    // A zero cap would reject every missing-link push forever (permanent
    // admission outage); the manager normalizes it to a 1-slot map.
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let config = SyncConfig {
        max_pending_dags: 0,
        ..SyncConfig::default()
    };
    let (manager, mut events) = SyncManager::new(blockstore, test_peer_state(), config);

    let (field_cid, _) = create_lww_block("field_a");
    let (comp_cid, comp_block) = create_composite_block(vec![DAGLink::new("field_a", field_cid)]);
    manager
        .process_pushlog(
            &broadcast_for(&comp_cid, "docA", comp_block),
            Some("peer-1"),
            false,
            None,
        )
        .await
        .expect("a single pending registration must be admitted even with cap 0");
    let _ = events.try_recv().expect("DagNeedsFetch for composite");
    assert_eq!(manager.pending_dag_count(), 1);
}

mod pending_persistence {
    //! #1099: success-acked pending registrations must be restart-recoverable
    //! once a `PendingDagStorage` is installed (proofs/tla/PendingDagRestart.tla).

    use super::*;
    use async_trait::async_trait;
    use p2p::sync::{PendingDagStorage, PendingDagStore, PersistedPendingDag};

    fn manager_with_store(
        store: Arc<MemoryStore>,
    ) -> (
        SyncManager<DefraBlockstore<MemoryStore>>,
        tokio::sync::mpsc::Receiver<SyncEvent>,
        Arc<PendingDagStore<MemoryStore>>,
    ) {
        let blockstore = Arc::new(DefraBlockstore::new(store.clone(), true));
        let (manager, events) =
            SyncManager::new(blockstore, test_peer_state(), SyncConfig::default());
        let pending_store = Arc::new(PendingDagStore::new(store));
        manager.install_pending_dag_store(pending_store.clone());
        (manager, events, pending_store)
    }

    fn composite_with_missing_field() -> (Cid, Vec<u8>, Cid, Vec<u8>) {
        let (field_cid, field_bytes) = create_lww_block("field_a");
        let (comp_cid, comp_bytes) =
            create_composite_block(vec![DAGLink::new("field_a", field_cid)]);
        (comp_cid, comp_bytes, field_cid, field_bytes)
    }

    fn pushlog_for(cid: &Cid, bytes: &[u8]) -> PushLogBroadcast {
        PushLogBroadcast::new(
            "doc123".to_string(),
            Bytes::from(cid.to_bytes()),
            "collection1".to_string(),
            "creator1".to_string(),
            Bytes::from(bytes.to_vec()),
        )
    }

    #[tokio::test]
    async fn pending_registration_persists_and_clears_on_resolve() {
        let store = Arc::new(MemoryStore::new());
        let (manager, mut events, pending_store) = manager_with_store(store);
        let (comp_cid, comp_bytes, _field_cid, field_bytes) = composite_with_missing_field();

        manager
            .process_pushlog(
                &pushlog_for(&comp_cid, &comp_bytes),
                Some("peer-1"),
                true,
                None,
            )
            .await
            .expect("composite with missing link registers pending");
        assert_eq!(manager.pending_dag_count(), 1);

        let persisted = pending_store.load_all().await.expect("load persisted");
        assert_eq!(persisted.len(), 1);
        assert_eq!(persisted[0].0, comp_cid);
        assert_eq!(persisted[0].1.doc_id, "doc123");
        assert_eq!(persisted[0].1.source_peer.as_deref(), Some("peer-1"));
        assert!(persisted[0].1.is_explicit_replicator);

        // Drain the DagNeedsFetch event so the channel stays open.
        let _ = tokio::time::timeout(Duration::from_secs(1), events.recv()).await;

        // The missing field arrives: the DAG resolves and the durable record
        // is deleted.
        let (field_cid, _) = create_lww_block("field_a");
        manager
            .process_pushlog(
                &pushlog_for(&field_cid, &field_bytes),
                Some("peer-1"),
                true,
                None,
            )
            .await
            .expect("field block completes the pending DAG");
        assert_eq!(manager.pending_dag_count(), 0);
        assert!(pending_store
            .load_all()
            .await
            .expect("load persisted")
            .is_empty());
    }

    #[tokio::test]
    async fn restore_re_registers_and_re_drives_fetch() {
        let store = Arc::new(MemoryStore::new());
        let comp_cid = {
            let (manager, mut events, _pending_store) = manager_with_store(store.clone());
            let (comp_cid, comp_bytes, _field_cid, _field_bytes) = composite_with_missing_field();
            manager
                .process_pushlog(
                    &pushlog_for(&comp_cid, &comp_bytes),
                    Some("peer-1"),
                    true,
                    None,
                )
                .await
                .expect("composite with missing link registers pending");
            let _ = tokio::time::timeout(Duration::from_secs(1), events.recv()).await;
            comp_cid
            // manager dropped here: simulates the crash after the success ack
        };

        // "Restarted" manager over the same physical store.
        let (manager, mut events, _pending_store) = manager_with_store(store);
        assert_eq!(manager.pending_dag_count(), 0);

        let restored = manager.restore_persisted_pending_dags().await;
        assert_eq!(restored, 1);
        assert_eq!(manager.pending_dag_count(), 1);

        let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .expect("restore must emit a fetch event")
            .expect("event channel open");
        match event {
            SyncEvent::DagNeedsFetch {
                root_cid,
                missing,
                providers,
                doc_id,
                is_explicit_replicator,
                ..
            } => {
                assert_eq!(root_cid, comp_cid);
                assert!(!missing.is_empty());
                assert_eq!(doc_id, "doc123");
                assert!(is_explicit_replicator);
                assert!(
                    providers.contains(&"peer-1".to_string()),
                    "persisted source peer must be offered as a provider"
                );
            }
            other => panic!("expected DagNeedsFetch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn restore_skips_and_deletes_merged_roots() {
        let store = Arc::new(MemoryStore::new());
        let (manager, mut _events, pending_store) = manager_with_store(store.clone());
        let (comp_cid, comp_bytes, _field_cid, _field_bytes) = composite_with_missing_field();

        manager
            .process_pushlog(
                &pushlog_for(&comp_cid, &comp_bytes),
                Some("peer-1"),
                true,
                None,
            )
            .await
            .expect("composite registers pending");
        manager
            .mark_as_merged(&comp_cid)
            .await
            .expect("mark merged");

        let (manager, _events2, _) = manager_with_store(store);
        let restored = manager.restore_persisted_pending_dags().await;
        assert_eq!(restored, 0);
        assert_eq!(manager.pending_dag_count(), 0);
        assert!(pending_store
            .load_all()
            .await
            .expect("load persisted")
            .is_empty());
    }

    struct FailingStore;

    #[async_trait]
    impl PendingDagStorage for FailingStore {
        async fn put(
            &self,
            _root_cid: &Cid,
            _record: &PersistedPendingDag,
        ) -> p2p::error::Result<()> {
            Err(Error::Storage("disk full".to_string()))
        }
        async fn remove(&self, _root_cid: &Cid) -> p2p::error::Result<()> {
            Ok(())
        }
        async fn load_all(&self) -> p2p::error::Result<Vec<(Cid, PersistedPendingDag)>> {
            Ok(Vec::new())
        }
    }

    /// Fail closed: if the registration cannot be made durable the push must
    /// be nacked (pusher keeps its retry record), not success-acked.
    #[tokio::test]
    async fn persist_failure_nacks_the_push_and_keeps_nothing_registered() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let (manager, _events) =
            SyncManager::new(blockstore, test_peer_state(), SyncConfig::default());
        manager.install_pending_dag_store(Arc::new(FailingStore));

        let (comp_cid, comp_bytes, _field_cid, _field_bytes) = composite_with_missing_field();
        let result = manager
            .process_pushlog(
                &pushlog_for(&comp_cid, &comp_bytes),
                Some("peer-1"),
                true,
                None,
            )
            .await;

        assert!(result.is_err(), "unpersistable registration must nack");
        assert_eq!(manager.pending_dag_count(), 0);
    }
}
