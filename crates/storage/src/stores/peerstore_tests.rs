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

#[test]
fn push_version_tie_break_uses_cid_bytes_not_base32_text() {
    let cids: Vec<_> = (0_u8..=255)
        .map(|seed| {
            let digest = [seed; 32];
            let hash = cid::multihash::Multihash::<64>::wrap(0x12, &digest).unwrap();
            Cid::new_v1(0x55, hash)
        })
        .collect();
    let (left, right) = cids
        .iter()
        .flat_map(|left| cids.iter().map(move |right| (left, right)))
        .find(|(left, right)| left.cmp(right) != left.to_string().cmp(&right.to_string()))
        .expect("test corpus must contain a base32/CID ordering disagreement");

    assert_eq!(
        compare_push_versions(1, &left.to_string(), 1, &right.to_string()),
        left.cmp(right)
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
    assert_eq!(peerstore.activate_dormant_push_retries().await.unwrap(), 0);
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
async fn retry_documents_are_ordered_by_next_attempt() {
    let store = Arc::new(MemoryStore::new());
    let peerstore = Peerstore::new(store);
    let initial = super::super::RetryInfo::new_initial().to_bytes().unwrap();

    for doc_id in ["doc-a", "doc-b", "doc-c", "doc-d"] {
        peerstore
            .record_push_failure("peer", doc_id, "collection", doc_id, 1, &initial)
            .await
            .unwrap();
    }

    for mut retry in peerstore.get_retry_documents("peer").await.unwrap() {
        retry.retry_info.next_retry_unix = match retry.doc_id.as_str() {
            "doc-a" => 40,
            "doc-b" => 10,
            "doc-c" => 30,
            "doc-d" => 20,
            _ => unreachable!(),
        };
        peerstore
            .update_retry_document("peer", &retry)
            .await
            .unwrap();
    }

    let doc_ids: Vec<_> = peerstore
        .get_retry_documents("peer")
        .await
        .unwrap()
        .into_iter()
        .map(|retry| retry.doc_id)
        .collect();
    assert_eq!(doc_ids, ["doc-b", "doc-d", "doc-c", "doc-a"]);
}

#[tokio::test]
async fn retry_record_keeps_only_newest_cid_and_its_own_backoff() {
    let store = Arc::new(MemoryStore::new());
    let peerstore = Peerstore::new(store);
    let initial = super::super::RetryInfo::new_initial().to_bytes().unwrap();

    peerstore
        .record_push_failure("peer", "doc", "collection", "cid-1", 1, &initial)
        .await
        .unwrap();
    let mut retry = peerstore
        .get_retry_documents("peer")
        .await
        .unwrap()
        .remove(0);
    retry.retry_info.bump();
    peerstore
        .update_retry_document("peer", &retry)
        .await
        .unwrap();

    peerstore
        .observe_push_head("peer", "doc", "collection", "cid-2", 2)
        .await
        .unwrap();
    assert!(peerstore
        .get_retry_documents("peer")
        .await
        .unwrap()
        .is_empty());
    peerstore
        .record_push_failure("peer", "doc", "collection", "cid-1", 1, &initial)
        .await
        .unwrap();
    assert!(peerstore
        .get_retry_documents("peer")
        .await
        .unwrap()
        .is_empty());
    peerstore
        .record_push_failure("peer", "doc", "collection", "cid-2", 2, &initial)
        .await
        .unwrap();

    let retries = peerstore.get_retry_documents("peer").await.unwrap();
    assert_eq!(retries.len(), 1);
    assert_eq!(retries[0].cid, "cid-2");
    assert_eq!(retries[0].priority, 2);
    assert_eq!(retries[0].retry_info.num_retries, 1);

    let stale_attempt = retries[0].clone();
    peerstore
        .observe_push_head("peer", "doc", "collection", "cid-3", 3)
        .await
        .unwrap();
    peerstore
        .complete_retry_document("peer", &stale_attempt)
        .await
        .unwrap();
    peerstore
        .update_retry_document("peer", &stale_attempt)
        .await
        .unwrap();
    peerstore
        .record_push_failure("peer", "doc", "collection", "cid-2", 2, &initial)
        .await
        .unwrap();
    assert!(peerstore
        .get_retry_documents("peer")
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn observing_an_equal_pending_head_does_not_deactivate_its_retry() {
    let store = Arc::new(MemoryStore::new());
    let peerstore = Peerstore::new(store);
    let initial = super::super::RetryInfo::new_initial().to_bytes().unwrap();

    peerstore
        .record_push_failure("peer", "doc", "collection", "cid", 1, &initial)
        .await
        .unwrap();
    peerstore
        .observe_push_head("peer", "doc", "collection", "cid", 1)
        .await
        .unwrap();

    let retries = peerstore.get_retry_documents("peer").await.unwrap();
    assert_eq!(retries.len(), 1);
    assert_eq!(retries[0].cid, "cid");
    assert!(retries[0].pending);
}

#[tokio::test]
async fn versionless_se_failure_activates_current_dormant_head() {
    let store = Arc::new(MemoryStore::new());
    let peerstore = Peerstore::new(store);
    let initial = super::super::RetryInfo::new_initial().to_bytes().unwrap();

    peerstore
        .record_push_failure("peer", "doc", "collection", "old", 1, &initial)
        .await
        .unwrap();
    peerstore
        .observe_push_head("peer", "doc", "collection", "new", 2)
        .await
        .unwrap();
    assert!(peerstore
        .get_retry_documents("peer")
        .await
        .unwrap()
        .is_empty());

    peerstore
        .record_push_failure("peer", "doc", "collection", "", 0, &initial)
        .await
        .unwrap();

    let retries = peerstore.get_retry_documents("peer").await.unwrap();
    assert_eq!(retries.len(), 1);
    assert_eq!(retries[0].cid, "new");
    assert_eq!(retries[0].priority, 2);
    assert!(retries[0].pending);
}

#[tokio::test]
async fn sweep_clear_preserves_dormant_watermark_for_restart_promotion() {
    let store = Arc::new(MemoryStore::new());
    let peerstore = Peerstore::new(store);
    let initial = super::super::RetryInfo::new_initial().to_bytes().unwrap();

    peerstore
        .create_replicator("peer", b"replicator")
        .await
        .unwrap();

    peerstore
        .record_push_failure("peer", "doc", "collection", "old", 1, &initial)
        .await
        .unwrap();
    peerstore
        .observe_push_head("peer", "doc", "collection", "new", 2)
        .await
        .unwrap();
    assert!(peerstore
        .get_retry_documents("peer")
        .await
        .unwrap()
        .is_empty());

    // The live sweep stops revisiting this peer but must preserve the
    // dormant crash-recovery obligation.
    peerstore.clear_retry_peer("peer").await.unwrap();
    assert!(peerstore.get_all_retry_peers().await.unwrap().is_empty());
    assert_eq!(peerstore.activate_dormant_push_retries().await.unwrap(), 1);
    let retries = peerstore.get_retry_documents("peer").await.unwrap();
    assert_eq!(retries.len(), 1);
    assert_eq!(retries[0].cid, "new");
    assert!(retries[0].retry_info.is_due());

    // A clear racing pending work must preserve it.
    peerstore.clear_retry_peer("peer").await.unwrap();
    assert_eq!(
        peerstore.get_retry_documents("peer").await.unwrap().len(),
        1
    );
    peerstore
        .complete_retry_document("peer", &retries[0])
        .await
        .unwrap();
    peerstore.clear_retry_peer("peer").await.unwrap();
    assert!(peerstore.get_all_retry_peers().await.unwrap().is_empty());
    assert_eq!(peerstore.activate_dormant_push_retries().await.unwrap(), 0);
}

/// defradb#1113: a failed COLLECTION-COMMIT push must be durably recorded
/// and replayable. It has no document id, so it is keyed by
/// (peer, collection, CID). Dropping it made the failure permanent —
/// receivers kept heads whose parents never arrived, and their pending-DAG
/// registrations could never complete (source-inc/gents#696).
#[tokio::test]
async fn collection_commit_push_failure_is_recorded_and_replayable() {
    let store = Arc::new(MemoryStore::new());
    let peerstore = Peerstore::new(store);
    let initial = super::super::RetryInfo::new_initial().to_bytes().unwrap();

    peerstore
        .record_push_failure("peer", "", "collection", "collection-cid", 1, &initial)
        .await
        .unwrap();

    let peers: Vec<String> = peerstore
        .get_all_retry_peers()
        .await
        .unwrap()
        .into_iter()
        .map(|(peer, _)| peer)
        .collect();
    assert_eq!(
        peers,
        vec!["peer".to_string()],
        "a failed collection commit must keep its peer swept"
    );
    let retries = peerstore.get_retry_documents("peer").await.unwrap();
    assert_eq!(retries.len(), 1, "the commit obligation must be replayable");
    let retry = &retries[0];
    assert!(retry.is_collection_commit());
    assert_eq!(retry.doc_id, "");
    assert_eq!(retry.collection_id, "collection");
    assert_eq!(retry.cid, "collection-cid");
    assert!(retry.pending);

    // Completing it clears the obligation and releases the peer.
    peerstore
        .complete_retry_document("peer", retry)
        .await
        .unwrap();
    assert!(peerstore
        .get_retry_documents("peer")
        .await
        .unwrap()
        .is_empty());
    peerstore.clear_retry_peer("peer").await.unwrap();
    assert!(peerstore.get_all_retry_peers().await.unwrap().is_empty());
}

/// Commit DAGs chain, so a newer commit does NOT retire an older
/// undelivered one: each CID keeps its own record.
#[tokio::test]
async fn collection_commits_are_tracked_per_cid() {
    let store = Arc::new(MemoryStore::new());
    let peerstore = Peerstore::new(store);
    let initial = super::super::RetryInfo::new_initial().to_bytes().unwrap();

    peerstore
        .record_push_failure("peer", "", "collection", "commit-1", 1, &initial)
        .await
        .unwrap();
    peerstore
        .record_push_failure("peer", "", "collection", "commit-2", 2, &initial)
        .await
        .unwrap();

    let retries = peerstore.get_retry_documents("peer").await.unwrap();
    assert_eq!(retries.len(), 2, "each commit CID keeps its own obligation");
    let mut cids: Vec<_> = retries.iter().map(|retry| retry.cid.as_str()).collect();
    cids.sort_unstable();
    assert_eq!(cids, vec!["commit-1", "commit-2"]);
}

/// A pending commit keeps the peer swept: clearing the marker would strand
/// the obligation.
#[tokio::test]
async fn sweep_clear_keeps_peer_with_pending_collection_commit() {
    let store = Arc::new(MemoryStore::new());
    let peerstore = Peerstore::new(store);
    let initial = super::super::RetryInfo::new_initial().to_bytes().unwrap();

    peerstore
        .record_push_failure("peer", "", "collection", "commit-1", 1, &initial)
        .await
        .unwrap();
    peerstore.clear_retry_peer("peer").await.unwrap();

    let peers: Vec<String> = peerstore
        .get_all_retry_peers()
        .await
        .unwrap()
        .into_iter()
        .map(|(peer, _)| peer)
        .collect();
    assert_eq!(
        peers,
        vec!["peer".to_string()],
        "a pending collection commit must keep the peer swept"
    );
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
async fn legacy_raw_collection_retry_reads_and_migrates_on_failure() {
    let store = Arc::new(MemoryStore::new());
    let peerstore = Peerstore::new(store);
    let initial = super::super::RetryInfo::new_initial().to_bytes().unwrap();
    let mut txn = peerstore.store.new_txn(false).await.unwrap();
    txn.set(&ReplicatorRetryIDKey::new("peer").bytes(), &initial)
        .await
        .unwrap();
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
    assert_eq!(legacy[0].collection_id, "collection");
    assert!(legacy[0].cid.is_empty());

    peerstore
        .record_push_failure("peer", "doc", "collection", "cid", 1, &initial)
        .await
        .unwrap();
    let migrated = peerstore.get_retry_documents("peer").await.unwrap();
    assert_eq!(migrated.len(), 1);
    assert_eq!(migrated[0].cid, "cid");
    assert_eq!(migrated[0].priority, 1);
}

#[tokio::test]
async fn sweep_clear_removes_preexisting_empty_document_retry() {
    let store = Arc::new(MemoryStore::new());
    let peerstore = Peerstore::new(store);
    let mut retry =
        super::super::PersistedPushRetry::new_observed("", "collection", "collection-cid", 1);
    retry.activate("peer:collection-cid");
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
