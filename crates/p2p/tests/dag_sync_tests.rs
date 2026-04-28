//! Tests for DAG synchronization.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use cid::Cid;
use p2p::{DagSync, DagSyncConfig, DagSyncState, NeedsFetchData, PeerStateTracker, SyncPlan};

fn test_cid() -> Cid {
    Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap()
}

fn test_cid2() -> Cid {
    Cid::from_str("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy").unwrap()
}

fn test_cid3() -> Cid {
    Cid::from_str("bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku").unwrap()
}

fn random_peer_str() -> String {
    p2p::PeerId::random().to_string()
}

#[tokio::test]
async fn test_sync_state_lifecycle() {
    let state = DagSyncState::new();
    let cid = test_cid();

    // Initially not syncing or synced
    assert!(!state.is_syncing(&cid).await);
    assert!(!state.is_synced(&cid).await);

    // Start sync
    assert!(state.start_sync(cid).await);
    assert!(state.is_syncing(&cid).await);
    assert!(!state.is_synced(&cid).await);

    // Can't start again while syncing
    assert!(!state.start_sync(cid).await);

    // Complete sync
    state.complete_sync(cid).await;
    assert!(!state.is_syncing(&cid).await);
    assert!(state.is_synced(&cid).await);

    // Can't start after synced
    assert!(!state.start_sync(cid).await);
}

#[tokio::test]
async fn test_sync_state_cancel() {
    let state = DagSyncState::new();
    let cid = test_cid();

    state.start_sync(cid).await;
    assert!(state.is_syncing(&cid).await);

    state.cancel_sync(&cid).await;
    assert!(!state.is_syncing(&cid).await);
    assert!(!state.is_synced(&cid).await);

    // Can start again after cancel
    assert!(state.start_sync(cid).await);
}

#[tokio::test]
async fn test_dag_sync_no_missing_links() {
    let peer_state = Arc::new(PeerStateTracker::new());
    let dag_sync = DagSync::new(peer_state);

    let root = test_cid();
    let links = vec![test_cid2(), test_cid3()];

    // All links exist locally
    let local_has = |_: &Cid| true;

    let plan = dag_sync
        .prepare_sync(root, &links, local_has)
        .await
        .unwrap();

    assert!(matches!(plan, SyncPlan::Complete));
    assert!(dag_sync.state.is_synced(&root).await);
}

#[tokio::test]
async fn test_dag_sync_with_missing_links() {
    let peer_state = Arc::new(PeerStateTracker::new());
    let peer = random_peer_str();
    peer_state.peer_connected(&peer);

    let dag_sync = DagSync::new(peer_state);

    let root = test_cid();
    let links = vec![test_cid2(), test_cid3()];

    // Only first link exists locally
    let cid2 = test_cid2();
    let local_has = move |cid: &Cid| *cid == cid2;

    let plan = dag_sync
        .prepare_sync(root, &links, local_has)
        .await
        .unwrap();

    match plan {
        SyncPlan::NeedsFetch(data) => {
            assert_eq!(data.root(), root);
            assert_eq!(data.missing().len(), 1);
            assert_eq!(data.missing()[0], test_cid3());
            assert!(!data.providers().is_empty());
        }
        _ => panic!("Expected NeedsFetch"),
    }

    // Root should be marked as syncing
    assert!(dag_sync.state.is_syncing(&root).await);
}

#[tokio::test]
async fn test_dag_sync_already_syncing() {
    let peer_state = Arc::new(PeerStateTracker::new());
    let dag_sync = DagSync::new(peer_state);

    let root = test_cid();
    let links = vec![];

    // Start first sync
    dag_sync.state.start_sync(root).await;

    // Try to sync again
    let plan = dag_sync
        .prepare_sync(root, &links, |_| false)
        .await
        .unwrap();

    assert!(matches!(plan, SyncPlan::AlreadySyncing));
}

#[tokio::test]
async fn test_dag_sync_already_synced() {
    let peer_state = Arc::new(PeerStateTracker::new());
    let dag_sync = DagSync::new(peer_state);

    let root = test_cid();
    let links = vec![];

    // Mark as synced
    dag_sync.state.complete_sync(root).await;

    // Try to sync again
    let plan = dag_sync
        .prepare_sync(root, &links, |_| false)
        .await
        .unwrap();

    assert!(matches!(plan, SyncPlan::AlreadySynced));
}

#[tokio::test]
async fn test_dag_sync_handle_complete() {
    let peer_state = Arc::new(PeerStateTracker::new());
    let dag_sync = DagSync::new(peer_state);

    let root = test_cid();
    dag_sync.state.start_sync(root).await;

    // Success case - should return Ok
    let result = dag_sync.handle_sync_complete(root, true, None).await;
    assert!(result.is_ok());
    assert!(dag_sync.state.is_synced(&root).await);
    assert!(!dag_sync.state.is_syncing(&root).await);

    // Failure case - should return Err with reason
    let root2 = test_cid2();
    dag_sync.state.start_sync(root2).await;
    let result = dag_sync
        .handle_sync_complete(root2, false, Some("timeout"))
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("timeout"));
    assert!(!dag_sync.state.is_synced(&root2).await);
    assert!(!dag_sync.state.is_syncing(&root2).await);
}

#[tokio::test]
async fn test_sync_plan_accessors() {
    let plan = SyncPlan::needs_fetch_new(test_cid(), vec![test_cid2()], vec![random_peer_str()])
        .expect("missing is non-empty");

    assert!(plan.needs_fetch());
    assert_eq!(plan.missing().unwrap().len(), 1);
    assert_eq!(plan.providers().unwrap().len(), 1);
    assert!(plan.fetch_data().is_some());

    let plan2 = SyncPlan::Complete;
    assert!(!plan2.needs_fetch());
    assert!(plan2.missing().is_none());
    assert!(plan2.providers().is_none());
    assert!(plan2.fetch_data().is_none());
}

#[test]
fn test_needs_fetch_data_empty_missing_returns_none() {
    let result = NeedsFetchData::new(test_cid(), vec![], vec![]);
    assert!(result.is_none());
}

#[test]
fn test_needs_fetch_data_non_empty_missing_returns_some() {
    let result = NeedsFetchData::new(test_cid(), vec![test_cid2()], vec![]);
    assert!(result.is_some());
    let data = result.unwrap();
    assert_eq!(data.root(), test_cid());
    assert_eq!(data.missing().len(), 1);
    assert!(data.providers().is_empty());
}

#[tokio::test]
async fn test_dag_sync_uses_peer_state_for_providers() {
    let peer_state = Arc::new(PeerStateTracker::new());
    let peer1 = random_peer_str();
    let peer2 = random_peer_str();

    peer_state.peer_connected(&peer1);
    peer_state.peer_connected(&peer2);

    // peer1 has cid2
    let cid2 = test_cid2();
    peer_state.peer_has_cid(&peer1, cid2);

    let dag_sync = DagSync::new(peer_state);

    let root = test_cid();
    let links = vec![cid2];

    let plan = dag_sync
        .prepare_sync(root, &links, |_| false)
        .await
        .unwrap();

    match plan {
        SyncPlan::NeedsFetch(data) => {
            // Should prefer peer1 since it has the CID
            assert!(data.providers().contains(&peer1));
        }
        _ => panic!("Expected NeedsFetch"),
    }
}

#[tokio::test]
async fn test_dag_sync_state_concurrent_operations() {
    // Test that concurrent start_sync operations are handled atomically
    let state = Arc::new(DagSyncState::new());
    let cid = test_cid();

    // Spawn multiple tasks trying to start sync for the same CID
    let mut handles = Vec::new();
    for _ in 0..10 {
        let state_clone = Arc::clone(&state);
        handles.push(tokio::spawn(
            async move { state_clone.start_sync(cid).await },
        ));
    }

    // Collect results
    let mut successes = 0;
    let mut failures = 0;
    for handle in handles {
        if handle.await.unwrap() {
            successes += 1;
        } else {
            failures += 1;
        }
    }

    // Exactly one task should have succeeded
    assert_eq!(
        successes, 1,
        "Exactly one task should acquire the sync lock"
    );
    assert_eq!(failures, 9, "Other tasks should fail to acquire");

    // CID should be syncing
    assert!(state.is_syncing(&cid).await);
    assert!(!state.is_synced(&cid).await);
}

#[tokio::test]
async fn test_dag_sync_state_concurrent_complete() {
    let state = Arc::new(DagSyncState::new());
    let cid = test_cid();

    // Start sync
    assert!(state.start_sync(cid).await);

    // Spawn multiple tasks trying to complete sync
    let mut handles = Vec::new();
    for _ in 0..10 {
        let state_clone = Arc::clone(&state);
        handles.push(tokio::spawn(async move {
            state_clone.complete_sync(cid).await;
        }));
    }

    // Wait for all to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // CID should be synced (not syncing)
    assert!(!state.is_syncing(&cid).await);
    assert!(state.is_synced(&cid).await);

    // Can't start sync again
    assert!(!state.start_sync(cid).await);
}

#[test]
fn test_dag_sync_config_zero_timeout_returns_error() {
    // DagSyncConfig::new should return error if block_fetch_timeout is zero
    let result = DagSyncConfig::new(Duration::ZERO);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("block_fetch_timeout"));
}

#[test]
fn test_dag_sync_config_with_timeout_zero_returns_error() {
    // with_timeout builder method should return error on zero
    let result = DagSyncConfig::default().with_timeout(Duration::ZERO);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("timeout"));
}

#[test]
fn test_dag_sync_config_valid_timeout_succeeds() {
    // Valid timeout should succeed
    let result = DagSyncConfig::new(Duration::from_secs(10));
    assert!(result.is_ok());
    let config = result.unwrap();
    assert_eq!(config.block_fetch_timeout(), Duration::from_secs(10));
}

#[test]
fn test_dag_sync_config_default_values() {
    let config = DagSyncConfig::default();

    // Verify default values
    assert_eq!(config.block_fetch_timeout(), Duration::from_secs(30));
}

#[test]
fn test_dag_sync_config_builder() {
    let config = DagSyncConfig::default()
        .with_timeout(Duration::from_secs(60))
        .expect("valid timeout");

    assert_eq!(config.block_fetch_timeout(), Duration::from_secs(60));
}

#[tokio::test]
async fn test_sync_state_eviction() {
    // Create state with very small max to test eviction
    let state = DagSyncState::with_max_synced(3);

    // Create 5 different CIDs
    let cids: Vec<Cid> = (0..5)
        .map(|i| {
            let bytes = format!(
                "bafybeig{}yrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
                i
            );
            // Use a simple hash-based approach to generate different CIDs
            use cid::multihash::{Code, MultihashDigest};
            let hash = Code::Sha2_256.digest(bytes.as_bytes());
            Cid::new_v1(0x71, hash)
        })
        .collect();

    // Sync all 5 CIDs
    for cid in &cids {
        state.start_sync(*cid).await;
        state.complete_sync(*cid).await;
    }

    // Should only have 3 synced (the limit)
    assert_eq!(state.synced_count().await, 3);

    // First 2 CIDs should have been evicted
    assert!(!state.is_synced(&cids[0]).await);
    assert!(!state.is_synced(&cids[1]).await);

    // Last 3 CIDs should still be synced
    assert!(state.is_synced(&cids[2]).await);
    assert!(state.is_synced(&cids[3]).await);
    assert!(state.is_synced(&cids[4]).await);

    // Evicted CIDs can be synced again
    assert!(state.start_sync(cids[0]).await);
}

#[tokio::test]
async fn test_sync_state_is_synced_is_recent_memory_hint() {
    let state = DagSyncState::with_max_synced(1);
    let first = test_cid();
    let second = test_cid2();

    state.start_sync(first).await;
    state.complete_sync(first).await;
    assert!(state.is_synced(&first).await);

    state.start_sync(second).await;
    state.complete_sync(second).await;

    assert!(!state.is_synced(&first).await);
    assert!(state.start_sync(first).await);
}

#[tokio::test]
async fn test_sync_state_eviction_no_duplicates() {
    // Completing sync for the same CID twice should not cause issues
    let state = DagSyncState::with_max_synced(2);
    let cid = test_cid();

    state.start_sync(cid).await;
    state.complete_sync(cid).await;

    // Complete again - should be idempotent
    state.complete_sync(cid).await;

    assert_eq!(state.synced_count().await, 1);
    assert!(state.is_synced(&cid).await);
}

#[tokio::test]
async fn test_sync_state_synced_count() {
    let state = DagSyncState::new();
    let cid1 = test_cid();
    let cid2 = test_cid2();

    assert_eq!(state.synced_count().await, 0);

    state.start_sync(cid1).await;
    state.complete_sync(cid1).await;
    assert_eq!(state.synced_count().await, 1);

    state.start_sync(cid2).await;
    state.complete_sync(cid2).await;
    assert_eq!(state.synced_count().await, 2);
}
