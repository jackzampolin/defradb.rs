use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use super::*;
use crate::backends::MemoryStore;
use crate::corekv::Key;
use crate::keys::peerstore::ReplicatorKey;

#[tokio::test]
async fn push_transaction_conflicts_retry_to_success() {
    let attempts = AtomicUsize::new(0);

    retry_push_txn_conflicts(|| async {
        if attempts.fetch_add(1, AtomicOrdering::Relaxed) < 2 {
            Err(crate::corekv::Error::TxnConflict)
        } else {
            Ok(())
        }
    })
    .await
    .unwrap();

    assert_eq!(attempts.load(AtomicOrdering::Relaxed), 3);
}

#[tokio::test]
async fn push_transaction_conflicts_stop_at_bound() {
    let attempts = AtomicUsize::new(0);

    let error = retry_push_txn_conflicts(|| async {
        attempts.fetch_add(1, AtomicOrdering::Relaxed);
        Err::<(), _>(crate::corekv::Error::TxnConflict)
    })
    .await
    .unwrap_err();

    assert!(error.is_txn_conflict());
    assert_eq!(
        attempts.load(AtomicOrdering::Relaxed),
        PUSH_RETRY_TXN_MAX_ATTEMPTS
    );
}

#[tokio::test]
async fn test_peerstore_basic() {
    let store = Arc::new(MemoryStore::new());
    let peerstore = Peerstore::new(store);

    let key = ReplicatorKey::new("replicator_1");

    // Write
    let mut txn = peerstore.new_txn(false).await.unwrap();
    txn.set(&key.bytes(), b"replicator_config").await.unwrap();
    txn.commit().await.unwrap();

    // Read
    let txn = peerstore.new_txn(true).await.unwrap();
    let value = txn.get(&key.bytes()).await.unwrap();
    assert_eq!(value, Some(b"replicator_config".to_vec()));
}

#[tokio::test]
async fn test_set_get_replicator() {
    let store = Arc::new(MemoryStore::new());
    let peerstore = Peerstore::new(store);

    let peer_id = "QmTestPeer123";
    let data = b"replicator_config_data";

    // Set
    peerstore.create_replicator(peer_id, data).await.unwrap();

    // Get
    let result = peerstore.get_replicator(peer_id).await.unwrap();
    assert_eq!(result, Some(data.to_vec()));

    // Has
    assert!(peerstore.has_replicator(peer_id).await.unwrap());
    assert!(!peerstore.has_replicator("nonexistent").await.unwrap());
}

#[tokio::test]
async fn test_delete_replicator() {
    let store = Arc::new(MemoryStore::new());
    let peerstore = Peerstore::new(store);

    let peer_id = "QmTestPeer123";
    let data = b"replicator_config_data";

    // Set
    peerstore.create_replicator(peer_id, data).await.unwrap();
    let retry_info = super::super::RetryInfo::new_initial().to_bytes().unwrap();
    peerstore
        .record_push_failure(peer_id, "doc", "collection", "doc-cid", 1, &retry_info)
        .await
        .unwrap();
    peerstore
        .record_push_failure(peer_id, "", "collection", "commit-cid", 1, &retry_info)
        .await
        .unwrap();
    assert!(peerstore.has_replicator(peer_id).await.unwrap());

    // Delete
    peerstore.delete_replicator(peer_id).await.unwrap();
    assert!(!peerstore.has_replicator(peer_id).await.unwrap());

    // Get returns None
    let result = peerstore.get_replicator(peer_id).await.unwrap();
    assert_eq!(result, None);
    assert!(peerstore.get_all_retry_peers().await.unwrap().is_empty());
    assert!(peerstore
        .get_retry_documents(peer_id)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(peerstore.migrate_legacy_push_retries().await.unwrap(), 0);
}

#[tokio::test]
async fn forget_waits_for_selected_retry_and_blocks_future_retries() {
    let store = Arc::new(MemoryStore::new());
    let peerstore = Peerstore::new(store.clone());
    let peer_id = "coordinated-peer";
    peerstore
        .create_replicator(peer_id, b"replicator")
        .await
        .unwrap();

    let retry_guard = peerstore
        .acquire_replicator_retry_guard(peer_id)
        .await
        .unwrap()
        .expect("replicator should be eligible for retry");
    let delete_store = Peerstore::new(store);
    let mut delete_task =
        tokio::spawn(async move { delete_store.delete_replicator(peer_id).await });

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(25), &mut delete_task)
            .await
            .is_err(),
        "forget completed while a retry still held permission to replay"
    );

    drop(retry_guard);
    tokio::time::timeout(std::time::Duration::from_secs(1), delete_task)
        .await
        .expect("forget remained blocked after retry completed")
        .expect("forget task panicked")
        .expect("forget failed");

    assert!(peerstore
        .acquire_replicator_retry_guard(peer_id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn same_peer_retry_transitions_have_one_storage_owner() {
    let store = Arc::new(MemoryStore::new());
    let peerstore = Peerstore::new(store.clone());
    peerstore
        .create_replicator("peer", b"replicator")
        .await
        .unwrap();

    let first = peerstore
        .acquire_replicator_retry_guard("peer")
        .await
        .unwrap()
        .unwrap();
    let second_store = Peerstore::new(store);
    let mut second = tokio::spawn(async move {
        second_store
            .acquire_replicator_retry_guard("peer")
            .await
            .unwrap()
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(25), &mut second)
            .await
            .is_err(),
        "two same-peer marker writers acquired ownership concurrently"
    );

    drop(first);
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), second)
            .await
            .unwrap()
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn delete_replicator_clears_orphaned_retry_state() {
    let store = Arc::new(MemoryStore::new());
    let peerstore = Peerstore::new(store);
    let retry_info = super::super::RetryInfo::new_initial().to_bytes().unwrap();

    peerstore
        .record_push_failure("orphan", "doc", "collection", "cid", 1, &retry_info)
        .await
        .unwrap();
    peerstore.delete_replicator("orphan").await.unwrap();

    assert!(peerstore.get_all_retry_peers().await.unwrap().is_empty());
    assert!(peerstore
        .get_retry_documents("orphan")
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn retry_sweep_peers_require_persisted_replicators() {
    let store = Arc::new(MemoryStore::new());
    let peerstore = Peerstore::new(store);
    let retry_info = super::super::RetryInfo::new_initial().to_bytes().unwrap();

    for peer_id in ["active", "orphan"] {
        peerstore
            .record_push_failure(peer_id, "doc", "collection", "cid", 1, &retry_info)
            .await
            .unwrap();
    }
    peerstore
        .create_replicator("active", b"replicator")
        .await
        .unwrap();

    let peers = peerstore.get_replicator_retry_peers().await.unwrap();
    assert_eq!(peers.len(), 1);
    assert_eq!(peers[0].0, "active");
}

#[tokio::test]
async fn test_list_replicators() {
    let store = Arc::new(MemoryStore::new());
    let peerstore = Peerstore::new(store);

    // Add multiple replicators
    peerstore
        .create_replicator("peer1", b"config1")
        .await
        .unwrap();
    peerstore
        .create_replicator("peer2", b"config2")
        .await
        .unwrap();
    peerstore
        .create_replicator("peer3", b"config3")
        .await
        .unwrap();

    // Get all
    let all = peerstore.list_replicators().await.unwrap();
    assert_eq!(all.len(), 3);

    // Check they're all present (order may vary)
    let peer_ids: Vec<&str> = all.iter().map(|(id, _)| id.as_str()).collect();
    assert!(peer_ids.contains(&"peer1"));
    assert!(peer_ids.contains(&"peer2"));
    assert!(peer_ids.contains(&"peer3"));
}

#[tokio::test]
async fn test_list_replicators_empty() {
    let store = Arc::new(MemoryStore::new());
    let peerstore = Peerstore::new(store);

    let all = peerstore.list_replicators().await.unwrap();
    assert!(all.is_empty());
}

#[tokio::test]
async fn test_update_replicator() {
    let store = Arc::new(MemoryStore::new());
    let peerstore = Peerstore::new(store);

    let peer_id = "QmTestPeer123";

    // Set initial
    peerstore
        .create_replicator(peer_id, b"config_v1")
        .await
        .unwrap();
    let result = peerstore.get_replicator(peer_id).await.unwrap();
    assert_eq!(result, Some(b"config_v1".to_vec()));

    // Update
    peerstore
        .create_replicator(peer_id, b"config_v2")
        .await
        .unwrap();
    let result = peerstore.get_replicator(peer_id).await.unwrap();
    assert_eq!(result, Some(b"config_v2".to_vec()));

    // Still only one replicator
    let all = peerstore.list_replicators().await.unwrap();
    assert_eq!(all.len(), 1);
}

#[tokio::test]
async fn scope_markers_are_presence_only_and_share_the_peer_clock() {
    let store = Arc::new(MemoryStore::new());
    let peerstore = Peerstore::new(store);
    let initial = super::super::RetryInfo::new_initial().to_bytes().unwrap();

    peerstore
        .record_push_failure("peer", "doc", "collection", "old-cid", 1, &initial)
        .await
        .unwrap();
    peerstore
        .record_push_failure("peer", "", "collection", "commit-cid", 1, &initial)
        .await
        .unwrap();
    peerstore
        .observe_push_head("peer", "doc", "collection", "new-cid", 2)
        .await
        .unwrap();

    let txn = peerstore.store.new_txn(true).await.unwrap();
    assert_eq!(
        txn.get(&ReplicatorRetryDocIDKey::new("peer", "doc").bytes())
            .await
            .unwrap(),
        Some(Vec::new())
    );
    assert_eq!(
        txn.get(&ReplicatorRetryCollectionKey::new("peer", "collection").bytes())
            .await
            .unwrap(),
        Some(Vec::new())
    );
    drop(txn);

    let retries = peerstore.get_retry_documents("peer").await.unwrap();
    assert_eq!(retries.len(), 2);
    assert!(retries.iter().all(|retry| retry.cid.is_empty()));
    assert!(retries.iter().all(|retry| retry.priority == 0));
    assert_eq!(
        retries[0].retry_info.next_retry_unix,
        retries[1].retry_info.next_retry_unix
    );
}

#[tokio::test]
async fn completing_one_scope_preserves_other_scope_and_peer_clock() {
    let store = Arc::new(MemoryStore::new());
    let peerstore = Peerstore::new(store);
    let initial = super::super::RetryInfo::new_initial().to_bytes().unwrap();

    peerstore
        .record_push_failure("peer", "doc", "collection", "cid", 1, &initial)
        .await
        .unwrap();
    peerstore
        .record_push_failure("peer", "", "collection", "commit", 1, &initial)
        .await
        .unwrap();

    let retries = peerstore.get_retry_documents("peer").await.unwrap();
    let doc = retries
        .iter()
        .find(|retry| !retry.is_collection_commit())
        .unwrap();
    peerstore
        .complete_retry_document("peer", doc)
        .await
        .unwrap();
    peerstore.clear_retry_peer("peer").await.unwrap();

    let remaining = peerstore.get_retry_documents("peer").await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert!(remaining[0].is_collection_commit());
    assert_eq!(peerstore.get_all_retry_peers().await.unwrap().len(), 1);

    peerstore
        .complete_retry_document("peer", &remaining[0])
        .await
        .unwrap();
    peerstore.clear_retry_peer("peer").await.unwrap();
    assert!(peerstore.get_all_retry_peers().await.unwrap().is_empty());
}

#[tokio::test]
async fn collection_updates_coalesce_to_one_rederivable_scope() {
    let store = Arc::new(MemoryStore::new());
    let peerstore = Peerstore::new(store);
    let initial = super::super::RetryInfo::new_initial().to_bytes().unwrap();

    peerstore
        .record_push_failure("peer", "", "collection", "collection-cid", 1, &initial)
        .await
        .unwrap();

    peerstore
        .record_push_failure("peer", "", "collection", "newer-cid", 2, &initial)
        .await
        .unwrap();
    let retries = peerstore.get_retry_documents("peer").await.unwrap();
    assert_eq!(retries.len(), 1);
    assert!(retries[0].is_collection_commit());
    assert_eq!(retries[0].collection_id, "collection");
    assert!(retries[0].cid.is_empty());
}

#[tokio::test]
async fn retry_marker_stats_report_scope_counts_and_oldest_peer_clock() {
    let store = Arc::new(MemoryStore::new());
    let peerstore = Peerstore::new(store);
    let initial = super::super::RetryInfo::new_initial().to_bytes().unwrap();

    peerstore
        .record_push_failure("peer-a", "doc-a", "collection", "cid-a", 1, &initial)
        .await
        .unwrap();
    peerstore
        .record_push_failure("peer-a", "", "collection", "commit-a", 1, &initial)
        .await
        .unwrap();
    peerstore
        .record_push_failure("peer-b", "doc-b", "collection", "cid-b", 1, &initial)
        .await
        .unwrap();

    let stats = peerstore.push_retry_marker_stats().await.unwrap();
    assert_eq!(stats.document_markers, 2);
    assert_eq!(stats.collection_markers, 1);
    assert_eq!(stats.scheduled_peers, 2);
    assert!(stats.oldest_scheduled_retry_unix.is_some());
}

/// A versionless doc-less failure (SE artifact, no CID) still has nothing
/// to replay and must not create state.
#[tokio::test]
async fn versionless_empty_document_failure_creates_no_retry_state() {
    let store = Arc::new(MemoryStore::new());
    let peerstore = Peerstore::new(store);
    let initial = super::super::RetryInfo::new_initial().to_bytes().unwrap();

    peerstore
        .record_push_failure("peer", "", "collection", "", 1, &initial)
        .await
        .unwrap();
    peerstore
        .observe_push_head("peer", "", "collection", "", 1)
        .await
        .unwrap();

    assert!(peerstore.get_all_retry_peers().await.unwrap().is_empty());
    assert!(peerstore
        .get_retry_documents("peer")
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn dormant_legacy_payload_document_retry_migrates_and_arms_due_schedule() {
    let store = Arc::new(MemoryStore::new());
    let peerstore = Peerstore::new(store);
    let mut txn = peerstore.store.new_txn(false).await.unwrap();
    txn.set(
        &ReplicatorRetryDocIDKey::new("peer", "doc").bytes(),
        b"collection",
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();

    let legacy = peerstore.get_retry_documents("peer").await.unwrap();
    assert_eq!(legacy.len(), 1);
    assert_eq!(legacy[0].doc_id, "doc");
    assert!(legacy[0].collection_id.is_empty());
    assert!(legacy[0].cid.is_empty());
    let txn = peerstore.store.new_txn(true).await.unwrap();
    assert_eq!(
        txn.get(&ReplicatorRetryDocIDKey::new("peer", "doc").bytes())
            .await
            .unwrap(),
        Some(Vec::new())
    );
    let schedule = txn
        .get(&ReplicatorRetryIDKey::new("peer").bytes())
        .await
        .unwrap()
        .expect("migration must reactivate a dormant legacy document marker");
    let schedule = super::super::RetryInfo::from_bytes(&schedule).unwrap();
    assert!(schedule.is_due());
    assert_eq!(schedule.num_retries, 0);
}

#[tokio::test]
async fn legacy_cid_scoped_commits_collapse_to_one_collection_marker() {
    let store = Arc::new(MemoryStore::new());
    let peerstore = Peerstore::new(store);
    let mut txn = peerstore.store.new_txn(false).await.unwrap();
    for cid in ["commit-a", "commit-b"] {
        txn.set(
            &ReplicatorRetryCommitKey::new("peer", "collection", cid).bytes(),
            b"legacy-payload",
        )
        .await
        .unwrap();
    }
    txn.commit().await.unwrap();

    assert_eq!(peerstore.migrate_legacy_push_retries().await.unwrap(), 2);
    let retries = peerstore.get_retry_documents("peer").await.unwrap();
    assert_eq!(retries.len(), 1);
    assert!(retries[0].is_collection_commit());
    assert_eq!(retries[0].collection_id, "collection");
    assert!(retries[0].cid.is_empty());

    let txn = peerstore.store.new_txn(true).await.unwrap();
    for cid in ["commit-a", "commit-b"] {
        assert!(txn
            .get(&ReplicatorRetryCommitKey::new("peer", "collection", cid).bytes())
            .await
            .unwrap()
            .is_none());
    }
    assert_eq!(
        txn.get(&ReplicatorRetryCollectionKey::new("peer", "collection").bytes())
            .await
            .unwrap(),
        Some(Vec::new())
    );
}

#[tokio::test]
async fn sweep_clear_removes_preexisting_empty_document_retry() {
    let store = Arc::new(MemoryStore::new());
    let peerstore = Peerstore::new(store);
    let retry = super::super::PersistedPushRetry {
        doc_id: String::new(),
        collection_id: "collection".to_string(),
        cid: "collection-cid".to_string(),
        priority: 1,
        pending: true,
        scope: super::super::RetryScope::CollectionCommit,
        retry_info: super::super::RetryInfo::new_initial(),
    };
    let mut txn = peerstore.store.new_txn(false).await.unwrap();
    txn.set(
        &ReplicatorRetryIDKey::new("peer").bytes(),
        &super::super::RetryInfo::new_initial().to_bytes().unwrap(),
    )
    .await
    .unwrap();
    txn.set(
        &ReplicatorRetryDocIDKey::new("peer", "").bytes(),
        &retry.to_bytes().unwrap(),
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();

    peerstore.clear_retry_peer("peer").await.unwrap();

    assert!(peerstore.get_all_retry_peers().await.unwrap().is_empty());
    let txn = peerstore.store.new_txn(true).await.unwrap();
    assert!(txn
        .get(&ReplicatorRetryDocIDKey::new("peer", "").bytes())
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn peer_reconnect_activates_schedule_without_resetting_ladder() {
    let store = Arc::new(MemoryStore::new());
    let peerstore = Peerstore::new(store);
    let mut retry = super::super::RetryInfo::new_initial();
    retry.bump_for("peer");
    retry.bump_for("peer");
    let original_rung = retry.num_retries;
    peerstore
        .record_push_failure(
            "peer",
            "doc",
            "collection",
            "cid",
            1,
            &retry.to_bytes().unwrap(),
        )
        .await
        .unwrap();

    assert!(peerstore.activate_retry_peer("peer").await.unwrap());
    let bytes = peerstore.get_retry_info("peer").await.unwrap().unwrap();
    let activated = super::super::RetryInfo::from_bytes(&bytes).unwrap();
    assert_eq!(activated.num_retries, original_rung + 1);
    assert!(activated.is_due());
    assert!(!peerstore.activate_retry_peer("absent").await.unwrap());
}
