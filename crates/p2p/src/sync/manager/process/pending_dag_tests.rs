use super::*;
use std::sync::Arc;

use blockstore::DefraBlockstore;
use defra_core::{Block as DefraBlock, CompositeDeltaPayload, CrdtDelta, DAGLink, LwwDeltaPayload};
use multihash_codetable::{Code, MultihashDigest};
use storage::backends::MemoryStore;

use crate::sync::manager::DEFAULT_MAX_PENDING_DAGS;
use crate::sync::{PeerStateTracker, SyncConfig};

fn test_cid(label: usize) -> Cid {
    Cid::new_v1(
        0x55,
        Code::Sha2_256.digest(format!("cid-{label}").as_bytes()),
    )
}

fn test_manager_with_config(config: SyncConfig) -> SyncManager<DefraBlockstore<MemoryStore>> {
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let peer_state = Arc::new(PeerStateTracker::new());
    let (manager, _events) = SyncManager::new(blockstore, peer_state, config);
    manager
}

fn test_manager() -> SyncManager<DefraBlockstore<MemoryStore>> {
    test_manager_with_config(SyncConfig::default())
}

fn pending_dag_from(doc_id: &str, source_peer: Option<&str>, inserted_at: Instant) -> PendingDag {
    PendingDag {
        doc_id: doc_id.to_string(),
        collection_id: "collection".to_string(),
        head_priority: None,
        creator: "creator".to_string(),
        missing: HashSet::new(),
        source_peer: source_peer.map(str::to_owned),
        is_explicit_replicator: false,
        explicit_replay_authorization: None,
        is_recovery_registered: false,
        inserted_at,
        attempts: 0,
        fetch_failures: 0,
        last_fetch_error: None,
        next_retry_at: tokio::time::Instant::now(),
        dispatches: 0,
    }
}

fn pending_dag(doc_id: &str, inserted_at: Instant) -> PendingDag {
    pending_dag_from(doc_id, Some("peer"), inserted_at)
}

#[test]
fn newer_sender_scope_head_invalidates_the_old_fetch_lease() {
    let manager = test_manager();
    let old_root = test_cid(40);
    let new_root = test_cid(41);
    let mut old = pending_dag("doc", Instant::now());
    old.head_priority = Some(1);
    assert!(manager.insert_pending_dag(old_root, old));
    let old_lease = manager.pending_dag_lease(old_root);
    assert!(old_lease.is_current());

    let mut new = pending_dag("doc", Instant::now());
    new.head_priority = Some(2);
    assert!(manager.insert_pending_dag(new_root, new));

    assert!(!old_lease.is_current());
    assert_eq!(manager.pending_dag_cids(), vec![new_root]);
}

#[test]
fn insert_pending_dag_replaces_existing_entry_at_capacity() {
    let manager = test_manager();
    let root = test_cid(0);

    assert!(manager.insert_pending_dag(
        root,
        pending_dag_from("original", Some("peer-0"), Instant::now()),
    ));
    for idx in 1..DEFAULT_MAX_PENDING_DAGS {
        let source_peer = format!("peer-{}", idx % PENDING_DAG_PEER_CAPACITY_DIVISOR);
        assert!(manager.insert_pending_dag(
            test_cid(idx),
            pending_dag_from(&format!("doc-{idx}"), Some(&source_peer), Instant::now(),),
        ));
    }
    assert_eq!(manager.pending_dag_count(), DEFAULT_MAX_PENDING_DAGS);

    assert!(manager.insert_pending_dag(
        root,
        pending_dag_from("replacement", Some("peer-0"), Instant::now()),
    ));
    assert_eq!(manager.pending_dag_count(), DEFAULT_MAX_PENDING_DAGS);
    assert_eq!(
        manager
            .pending_dags
            .read()
            .get(&root)
            .map(|dag| dag.doc_id.as_str()),
        Some("replacement")
    );
}

#[test]
fn pending_dag_peer_quota_preserves_capacity_for_other_sources() {
    let manager = test_manager_with_config(SyncConfig {
        max_pending_dags: 8,
        ..Default::default()
    });
    let first = test_cid(0);
    let second = test_cid(1);
    let rejected = test_cid(2);

    for (root, doc_id) in [(first, "first"), (second, "second")] {
        assert!(manager.insert_pending_dag(
            root,
            pending_dag_from(doc_id, Some("noisy"), Instant::now()),
        ));
    }
    assert!(!manager.insert_pending_dag(
        rejected,
        pending_dag_from("rejected", Some("noisy"), Instant::now()),
    ));
    assert!(manager.insert_pending_dag(
        test_cid(3),
        pending_dag_from("healthy", Some("healthy"), Instant::now()),
    ));

    assert!(manager.clear_pending_dag(&first));
    assert!(manager.insert_pending_dag(
        rejected,
        pending_dag_from("retried", Some("noisy"), Instant::now()),
    ));
    assert!(manager.insert_pending_dag(
        second,
        pending_dag_from("replacement", Some("noisy"), Instant::now()),
    ));
    assert_eq!(manager.pending_dags.read().source_count("noisy"), 2);

    assert!(manager.insert_pending_dag(
        second,
        pending_dag_from("transferred", Some("healthy"), Instant::now()),
    ));
    assert_eq!(manager.pending_dags.read().source_count("noisy"), 1);
    assert_eq!(manager.pending_dags.read().source_count("healthy"), 2);
    assert_eq!(manager.pending_dag_count(), 3);
}

#[test]
fn pending_dag_reverse_index_tracks_frontier_lifecycle() {
    let manager = test_manager();
    let root_a = test_cid(0);
    let root_b = test_cid(1);
    let shared = test_cid(2);
    let other = test_cid(3);
    let next = test_cid(4);

    let mut dag_a = pending_dag("a", Instant::now());
    dag_a.missing.insert(shared);
    let mut dag_b = pending_dag("b", Instant::now());
    dag_b.missing.extend([shared, other]);
    assert!(manager.insert_pending_dag(root_a, dag_a));
    assert!(manager.insert_pending_dag(root_b, dag_b));

    let waiting: HashSet<_> = manager
        .pending_dags
        .read()
        .waiting_roots(&shared)
        .into_iter()
        .collect();
    assert_eq!(waiting, [root_a, root_b].into_iter().collect());

    assert!(manager
        .pending_dags
        .write()
        .advance_waiters(&shared, &[next])
        .is_empty());
    assert!(manager
        .pending_dags
        .read()
        .waiting_roots(&shared)
        .is_empty());
    assert_eq!(
        manager
            .pending_dags
            .read()
            .waiting_roots(&next)
            .into_iter()
            .collect::<HashSet<_>>(),
        [root_a, root_b].into_iter().collect()
    );

    assert!(manager.update_pending_dag_missing_if_current(
        &root_a,
        manager.pending_dag_snapshot(&root_a).unwrap().inserted_at,
        [other].into_iter().collect(),
    ));
    assert_eq!(
        manager.pending_dags.read().waiting_roots(&next).as_slice(),
        &[root_b]
    );

    assert!(manager.clear_pending_dag(&root_b));
    assert!(manager.pending_dags.read().waiting_roots(&next).is_empty());
    assert_eq!(
        manager.pending_dags.read().waiting_roots(&other).as_slice(),
        &[root_a]
    );
}

#[test]
fn stale_pending_dag_update_does_not_resurrect_old_generation() {
    let manager = test_manager();
    let root = test_cid(0);
    let current_inserted_at = Instant::now();
    let stale_inserted_at = current_inserted_at + std::time::Duration::from_secs(1);

    assert!(manager.insert_pending_dag(root, pending_dag("current", current_inserted_at)));
    assert!(!manager.update_pending_dag_missing_if_current(
        &root,
        stale_inserted_at,
        [test_cid(1)].into_iter().collect(),
    ));
    assert!(manager.pending_dag_missing(&root).is_empty());
}

#[test]
fn concurrent_pending_dag_insert_burst_stays_bounded() {
    let manager = Arc::new(test_manager());
    let mut handles = Vec::new();

    for worker in 0..8 {
        let manager = Arc::clone(&manager);
        handles.push(std::thread::spawn(move || {
            for idx in 0..200 {
                let label = worker * 1_000 + idx;
                manager.insert_pending_dag(
                    test_cid(label),
                    pending_dag(&format!("doc-{label}"), Instant::now()),
                );
            }
        }));
    }

    for handle in handles {
        handle.join().expect("insert worker should not panic");
    }

    assert!(manager.pending_dag_count() <= DEFAULT_MAX_PENDING_DAGS);
}

#[tokio::test(start_paused = true)]
async fn claim_bumps_clock_and_suppresses_duplicates() {
    let manager = test_manager();
    let root = test_cid(1);
    let mut dag = pending_dag("doc", Instant::now());
    dag.missing.insert(test_cid(2));
    assert!(manager.insert_pending_dag(root, dag));

    let now = tokio::time::Instant::now();
    // Fresh entry is due immediately (insert leaves next_retry_at = now).
    assert!(manager.try_claim_pending_dag_dispatch(&root, now));
    // Second claim in the same instant is suppressed.
    assert!(!manager.try_claim_pending_dag_dispatch(&root, now));
    // Becomes due again after the backoff rung reached by the first
    // claim (dispatches=1 -> retry_backoff(1) = 4s).
    tokio::time::advance(std::time::Duration::from_secs(4)).await;
    assert!(manager.try_claim_pending_dag_dispatch(&root, tokio::time::Instant::now()));
}

#[tokio::test(start_paused = true)]
async fn backoff_doubles_and_caps() {
    use crate::sync::manager::pending::retry_backoff;
    assert_eq!(retry_backoff(0), std::time::Duration::from_secs(2));
    assert_eq!(retry_backoff(1), std::time::Duration::from_secs(4));
    assert_eq!(retry_backoff(4), std::time::Duration::from_secs(32));
    assert_eq!(retry_backoff(5), std::time::Duration::from_secs(60));
    assert_eq!(retry_backoff(30), std::time::Duration::from_secs(60));
}

#[tokio::test(start_paused = true)]
async fn expedite_makes_entry_due_now_without_resetting_backoff() {
    let manager = test_manager();
    let root = test_cid(1);
    let mut dag = pending_dag("doc", Instant::now());
    dag.missing.insert(test_cid(2));
    assert!(manager.insert_pending_dag(root, dag));
    let now = tokio::time::Instant::now();
    assert!(manager.try_claim_pending_dag_dispatch(&root, now)); // dispatches -> 1
    manager.expedite_pending_dag_retry(&root);
    assert!(manager.try_claim_pending_dag_dispatch(&root, now)); // dispatches -> 2
                                                                 // Next due time reflects dispatches=2 rung (8s), not a reset.
    tokio::time::advance(std::time::Duration::from_secs(4)).await;
    assert!(!manager.try_claim_pending_dag_dispatch(&root, tokio::time::Instant::now()));
    tokio::time::advance(std::time::Duration::from_secs(4)).await;
    assert!(manager.try_claim_pending_dag_dispatch(&root, tokio::time::Instant::now()));
}

#[tokio::test(start_paused = true)]
async fn claim_due_returns_and_claims_only_due_entries_with_missing_blocks() {
    let manager = test_manager();
    let due = test_cid(1);
    let complete = test_cid(3);
    let mut dag = pending_dag("doc-due", Instant::now());
    dag.missing.insert(test_cid(2));
    assert!(manager.insert_pending_dag(due, dag));
    // Entry with no missing blocks must never be dispatched.
    assert!(manager.insert_pending_dag(complete, pending_dag("doc-done", Instant::now())));

    let claimed = manager.claim_due_pending_dag_retries(tokio::time::Instant::now());
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].0, due);
    // Claiming consumed due-ness.
    assert!(manager
        .claim_due_pending_dag_retries(tokio::time::Instant::now())
        .is_empty());
}

fn lww_leaf(field_name: &str) -> (Cid, Vec<u8>) {
    let block = DefraBlock::new(
        CrdtDelta::Lww(LwwDeltaPayload {
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

fn composite_node(link_name: &str, link_cid: Cid, priority: u64) -> (Cid, Vec<u8>) {
    let block = DefraBlock::new(
        CrdtDelta::Composite(CompositeDeltaPayload {
            schema_version_id: "schema1".to_string(),
            priority,
            status: 1,
        }),
        vec![],
        vec![DAGLink::new(link_name, link_cid)],
    );
    let bytes = block.to_dag_cbor().expect("encode composite block");
    let cid = block.generate_cid().expect("generate composite cid");
    (cid, bytes)
}

#[tokio::test]
async fn block_arrival_updates_missing_incrementally_without_full_walks() {
    // A dropped event receiver would fail the completing `retry_pending_dag`
    // call with `ChannelSend` before the assertions below run, so keep it
    // alive (unlike `test_manager()`, which discards it).
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let peer_state = Arc::new(PeerStateTracker::new());
    let (manager, mut events) = SyncManager::new(blockstore, peer_state, SyncConfig::default());

    // 3-level DAG: root -> child (composite) -> grandchild (lww).
    let (grandchild_cid, grandchild_bytes) = lww_leaf("name");
    let (child_cid, child_bytes) = composite_node("name", grandchild_cid, 1);
    let (root_cid, root_bytes) = composite_node("composite", child_cid, 2);

    manager
        .blockstore
        .put(&root_cid, &root_bytes)
        .await
        .expect("store root");
    manager
        .blockstore
        .put(&child_cid, &child_bytes)
        .await
        .expect("store child");

    let mut dag = pending_dag("doc1", Instant::now());
    dag.missing.insert(child_cid);
    assert!(manager.insert_pending_dag(root_cid, dag));

    // Child arrives; grandchild is still absent. This must only shrink
    // the frontier (child -> grandchild), not run the full walk.
    let completed = manager
        .retry_pending_dags_waiting_on(&child_cid)
        .await
        .expect("retry on child arrival");
    assert!(completed.is_empty(), "root must not complete yet");
    assert_eq!(manager.pending_dag_missing(&root_cid), vec![grandchild_cid]);
    assert_eq!(
        manager.diagnostics.snapshot().missing_link_retries,
        0,
        "a frontier-shrinking arrival must not trigger the full verification walk"
    );

    // Grandchild arrives; the frontier empties, so the full walk runs
    // exactly once to verify completion and the root resolves.
    manager
        .blockstore
        .put(&grandchild_cid, &grandchild_bytes)
        .await
        .expect("store grandchild");
    let completed = manager
        .retry_pending_dags_waiting_on(&grandchild_cid)
        .await
        .expect("retry on grandchild arrival");
    assert_eq!(completed, vec![root_cid]);
    assert_eq!(manager.diagnostics.snapshot().missing_link_retries, 1);
    assert_eq!(manager.pending_dag_count(), 0);

    match events.try_recv().expect("DagReady event") {
        SyncEvent::DagReady {
            root_cid: event_root,
            ..
        } => assert_eq!(event_root, root_cid),
        other => panic!("expected DagReady, got {:?}", other),
    }
}

#[tokio::test]
async fn quarantine_pending_dag_moves_live_record_and_clears_in_memory_entry() {
    use crate::sync::pending_store::{PendingDagStorage, PendingDagStore, PersistedPendingDag};

    let blockstore = Arc::new(DefraBlockstore::new(Arc::new(MemoryStore::new()), true));
    let peer_state = Arc::new(PeerStateTracker::new());
    let (manager, _events) = SyncManager::new(blockstore, peer_state, SyncConfig::default());

    let root = test_cid(1);
    let pending_store = Arc::new(PendingDagStore::new(Arc::new(MemoryStore::new())));
    pending_store
        .put(
            &root,
            &PersistedPendingDag {
                doc_id: "doc".to_string(),
                collection_id: "collection".to_string(),
                head_priority: None,
                creator: "creator".to_string(),
                source_peer: Some("peer".to_string()),
                is_explicit_replicator: false,
                explicit_replay_authorization: None,
            },
        )
        .await
        .expect("persist live pending dag record");

    // Hydrates persisted_roots from the store (put must happen first, see
    // install_pending_dag_store's hydration-at-install contract).
    manager
        .install_pending_dag_store(pending_store.clone())
        .await;

    assert!(manager.insert_pending_dag(root, pending_dag("doc", Instant::now())));
    assert_eq!(manager.pending_dag_count(), 1);

    manager
        .quarantine_pending_dag(&root, "unique constraint violation")
        .await;

    let quarantined = pending_store
        .load_quarantined()
        .await
        .expect("load quarantined records");
    assert_eq!(quarantined.len(), 1);
    assert_eq!(quarantined[0].0, root);
    assert_eq!(quarantined[0].1.reason, "unique constraint violation");
    assert_eq!(quarantined[0].1.record.doc_id, "doc");

    assert!(
        pending_store.load_all().await.unwrap().is_empty(),
        "live durable record must be removed once quarantined"
    );
    assert_eq!(
        manager.pending_dag_count(),
        0,
        "in-memory entry must be cleared on quarantine"
    );
    assert_eq!(manager.persisted_pending_count(), 0);
    assert_eq!(
        manager
            .diagnostics
            .snapshot()
            .pending_dag_terminal_quarantined,
        1
    );
    assert_eq!(manager.quarantined_pending_count(), 1);
}

#[tokio::test]
async fn quarantine_pending_dag_dedupes_gauge_on_repeat_rejection() {
    use crate::sync::pending_store::{PendingDagStorage, PendingDagStore, PersistedPendingDag};

    let blockstore = Arc::new(DefraBlockstore::new(Arc::new(MemoryStore::new()), true));
    let peer_state = Arc::new(PeerStateTracker::new());
    let (manager, _events) = SyncManager::new(blockstore, peer_state, SyncConfig::default());

    let root = test_cid(1);
    let pending_store = Arc::new(PendingDagStore::new(Arc::new(MemoryStore::new())));
    pending_store
        .put(
            &root,
            &PersistedPendingDag {
                doc_id: "doc".to_string(),
                collection_id: "collection".to_string(),
                head_priority: None,
                creator: "creator".to_string(),
                source_peer: Some("peer".to_string()),
                is_explicit_replicator: false,
                explicit_replay_authorization: None,
            },
        )
        .await
        .expect("persist live pending dag record");

    manager
        .install_pending_dag_store(pending_store.clone())
        .await;

    assert!(manager.insert_pending_dag(root, pending_dag("doc", Instant::now())));

    // First rejection: quarantines the root, gauge and counter both move
    // to 1.
    manager
        .quarantine_pending_dag(&root, "unique constraint violation")
        .await;
    assert_eq!(manager.quarantined_pending_count(), 1);
    assert_eq!(
        manager
            .diagnostics
            .snapshot()
            .pending_dag_terminal_quarantined,
        1
    );

    // Second rejection of the SAME root (e.g. a sender-paced re-push
    // after the first rejection re-hits the same deterministic
    // classification): the occurrence counter keeps counting, but the
    // gauge must NOT double-count a root that was already quarantined —
    // it tracks distinct quarantined roots, not quarantine events.
    manager
        .quarantine_pending_dag(&root, "unique constraint violation")
        .await;
    assert_eq!(
        manager
            .diagnostics
            .snapshot()
            .pending_dag_terminal_quarantined,
        2,
        "the occurrence-level diagnostic counter must count every rejection"
    );
    assert_eq!(
        manager.quarantined_pending_count(),
        1,
        "the gauge must not drift above the true number of quarantined roots on repeat rejection"
    );

    let quarantined = pending_store
        .load_quarantined()
        .await
        .expect("load quarantined records");
    assert_eq!(
        quarantined.len(),
        1,
        "the store itself must hold exactly one quarantine record for this root"
    );
}

#[tokio::test]
async fn quarantine_pending_dag_synthesizes_record_when_no_durable_record_exists() {
    let manager = test_manager();
    let root = test_cid(1);

    assert!(manager.insert_pending_dag(root, pending_dag("doc-in-memory", Instant::now())));

    // No pending store installed at all: quarantine must still succeed
    // (never fail for lack of provenance) and clear the in-memory entry.
    manager
        .quarantine_pending_dag(&root, "unique constraint violation")
        .await;

    assert_eq!(manager.pending_dag_count(), 0);
    assert_eq!(
        manager
            .diagnostics
            .snapshot()
            .pending_dag_terminal_quarantined,
        1
    );
    assert_eq!(manager.quarantined_pending_count(), 1);
}

#[tokio::test]
async fn resync_deletes_live_leftover_of_quarantined_root_without_redriving() {
    use crate::sync::pending_store::{
        PendingDagStorage, PendingDagStore, PersistedPendingDag, PersistedQuarantinedDag,
    };

    let blockstore = Arc::new(DefraBlockstore::new(Arc::new(MemoryStore::new()), true));
    let peer_state = Arc::new(PeerStateTracker::new());
    let (manager, mut events) = SyncManager::new(blockstore, peer_state, SyncConfig::default());

    let root = test_cid(1);
    let pending_store = Arc::new(PendingDagStore::new(Arc::new(MemoryStore::new())));
    let record = PersistedPendingDag {
        doc_id: "doc".to_string(),
        collection_id: "collection".to_string(),
        head_priority: None,
        creator: "creator".to_string(),
        source_peer: Some("peer".to_string()),
        is_explicit_replicator: false,
        explicit_replay_authorization: None,
    };

    // Simulate the crash window inside `quarantine_pending_dag` between
    // writing the quarantine record and deleting the live one: both
    // records exist on disk simultaneously.
    pending_store
        .put(&root, &record)
        .await
        .expect("persist live leftover record");
    pending_store
        .quarantine(
            &root,
            &PersistedQuarantinedDag {
                record: record.clone(),
                reason: "unique constraint violation".to_string(),
                quarantined_at_unix_secs: PersistedQuarantinedDag::now_unix_secs(),
            },
        )
        .await
        .expect("persist quarantine record");

    manager
        .install_pending_dag_store(pending_store.clone())
        .await;

    // Pins the restart-hydration property the e2e composition fence no
    // longer covers: the in-memory gauge is rebuilt from load_quarantined.
    assert_eq!(manager.quarantined_pending_count(), 1);

    let restored = manager.resync_persisted_pending_dags().await;

    assert_eq!(restored, 0, "a quarantined root must not be re-registered");
    assert_eq!(
        manager.pending_dag_count(),
        0,
        "in-memory pending map must stay empty for a quarantined root"
    );
    assert!(
        pending_store.load_all().await.unwrap().is_empty(),
        "the resync sweep must delete the live leftover record"
    );
    assert!(
        pending_store
            .load_quarantined()
            .await
            .unwrap()
            .iter()
            .any(|(cid, _)| *cid == root),
        "the quarantine record itself must survive the sweep"
    );
    assert!(
        events.try_recv().is_err(),
        "no DagNeedsFetch/DagReady must be emitted for a quarantined root"
    );
}

#[tokio::test(start_paused = true)]
async fn resync_restore_consumes_retry_clock_claim_before_dispatch() {
    use crate::sync::pending_store::{PendingDagStorage, PendingDagStore, PersistedPendingDag};

    let blockstore = Arc::new(DefraBlockstore::new(Arc::new(MemoryStore::new()), true));
    let peer_state = Arc::new(PeerStateTracker::new());
    let (manager, mut events) = SyncManager::new(blockstore, peer_state, SyncConfig::default());

    // The root's block is never put in the blockstore, so the resync
    // sweep falls back to treating the root itself as missing and takes
    // the DagNeedsFetch (non-empty `missing`) path.
    let root = test_cid(1);
    let pending_store = Arc::new(PendingDagStore::new(Arc::new(MemoryStore::new())));
    pending_store
        .put(
            &root,
            &PersistedPendingDag {
                doc_id: "doc".to_string(),
                collection_id: "collection".to_string(),
                head_priority: None,
                creator: "creator".to_string(),
                source_peer: Some("peer".to_string()),
                is_explicit_replicator: false,
                explicit_replay_authorization: None,
            },
        )
        .await
        .expect("persist pending dag record");

    manager.install_pending_dag_store(pending_store).await;

    let restored = manager.resync_persisted_pending_dags().await;
    assert_eq!(restored, 1);

    match events
        .try_recv()
        .expect("DagNeedsFetch event from resync restore")
    {
        SyncEvent::DagNeedsFetch { root_cid, .. } => assert_eq!(root_cid, root),
        other => panic!("expected DagNeedsFetch, got {:?}", other),
    }

    // The restore's direct DagNeedsFetch emission already consumed the
    // immediate claim (mirrors the fresh-registration path in
    // pushlog.rs) -- the retry clock must not also dispatch this root
    // before the backoff rung elapses.
    assert!(manager
        .claim_due_pending_dag_retries(tokio::time::Instant::now())
        .is_empty());

    // Becomes due again only after the backoff rung reached by the
    // restore's claim (dispatches=1 -> retry_backoff(1) = 4s).
    tokio::time::advance(std::time::Duration::from_secs(4)).await;
    let claimed = manager.claim_due_pending_dag_retries(tokio::time::Instant::now());
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].0, root);
}
