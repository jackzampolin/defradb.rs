//! The derived collection head set and the reclamation that bounds it.
//!
//! Modeled in `proofs/tla/HeadSet.tla` and `proofs/lean/HeadSet/Core.lean`.
//! The properties asserted here are the ones stated there: a head is live
//! exactly when nothing supersedes it, reclaiming a superseded head does not
//! change the answer, and two writers never write the same key.

use cid::Cid;
use datastore::{NamespaceView, SharedTxn};
use db::block::heads::{live_collection_heads, prune_superseded_heads};
use defra_core::block::generate_cid_from_bytes;
use std::sync::Arc;
use storage::corekv::{Key, Store};
use storage::keys::headstore::{HeadstoreColKey, HeadstoreColSuperseded};
use storage::namespace::Namespace;
use storage::RegolithStore;

const COLLECTION: u32 = 3;

fn cid_of(value: &[u8]) -> Cid {
    generate_cid_from_bytes(value).unwrap()
}

fn headstore(shared: &Arc<SharedTxn>) -> NamespaceView {
    NamespaceView::new(Arc::clone(shared), Namespace::Headstore)
}

async fn commit(shared: Arc<SharedTxn>) {
    Arc::try_unwrap(shared)
        .ok()
        .expect("views dropped")
        .into_txn()
        .commit()
        .await
        .unwrap();
}

/// Write head keys and markers directly, so a case can be built that the
/// writer would take many appends to reach.
async fn seed(store: &RegolithStore, heads: &[(Cid, u64)], markers: &[(Cid, Cid)]) {
    let shared = SharedTxn::new(store.new_txn(false).await.unwrap());
    let view = headstore(&shared);
    for (cid, priority) in heads {
        view.set(
            &Key::bytes(&HeadstoreColKey::new(COLLECTION, *cid)),
            &[*priority as u8],
        )
        .await
        .unwrap();
    }
    for (parent, child) in markers {
        view.set(
            &Key::bytes(&HeadstoreColSuperseded::new(COLLECTION, *parent, *child)),
            &[],
        )
        .await
        .unwrap();
    }
    drop(view);
    commit(shared).await;
}

async fn live(store: &RegolithStore) -> (Vec<Cid>, usize) {
    let shared = SharedTxn::new(store.new_txn(true).await.unwrap());
    let view = headstore(&shared);
    let found = live_collection_heads(&view, COLLECTION).await.unwrap();
    let mut cids = found.live;
    cids.sort_by_cached_key(Cid::to_string);
    (cids, found.superseded)
}

async fn stored_keys(store: &RegolithStore, prefix: Vec<u8>) -> Vec<String> {
    let shared = SharedTxn::new(store.new_txn(true).await.unwrap());
    let view = headstore(&shared);
    let mut iter = view
        .iterator(storage::corekv::IterOptions::new().with_prefix(prefix))
        .await
        .unwrap();
    let mut keys = Vec::new();
    while let Some(pair) = iter.next().await.unwrap() {
        keys.push(String::from_utf8(pair.key).unwrap());
    }
    iter.close().await.unwrap();
    keys.sort();
    keys
}

async fn prune(store: &RegolithStore, max_keys: usize) -> db::block::heads::PruneOutcome {
    let shared = SharedTxn::new(store.new_txn(false).await.unwrap());
    let view = headstore(&shared);
    let outcome = prune_superseded_heads(&view, COLLECTION, max_keys)
        .await
        .unwrap();
    drop(view);
    commit(shared).await;
    outcome
}

/// `HeadSet.derivedHeads`: a stored head key is live exactly when no marker
/// names it.
#[tokio::test]
async fn superseded_heads_are_not_live() {
    let store = RegolithStore::in_memory().unwrap();
    let (a, b, c) = (cid_of(b"a"), cid_of(b"b"), cid_of(b"c"));
    seed(&store, &[(a, 1), (b, 2), (c, 3)], &[(a, c)]).await;

    let (heads, superseded) = live(&store).await;
    let mut expected = vec![b, c];
    expected.sort_by_cached_key(Cid::to_string);
    assert_eq!(heads, expected);
    assert_eq!(superseded, 1);
}

/// The merge join has to survive every ordering of the two prefixes, not just
/// the one a small example happens to produce.
#[tokio::test]
async fn merge_join_holds_for_every_supersede_pattern() {
    let all: Vec<Cid> = (0..6u8).map(|i| cid_of(&[i])).collect();
    let superseder = cid_of(b"superseder");

    for pattern in 0u32..(1 << 6) {
        let store = RegolithStore::in_memory().unwrap();
        let heads: Vec<(Cid, u64)> = all.iter().map(|cid| (*cid, 1)).collect();
        let markers: Vec<(Cid, Cid)> = all
            .iter()
            .enumerate()
            .filter(|(index, _)| pattern & (1 << index) != 0)
            .map(|(_, cid)| (*cid, superseder))
            .collect();
        seed(&store, &heads, &markers).await;

        let mut expected: Vec<Cid> = all
            .iter()
            .enumerate()
            .filter(|(index, _)| pattern & (1 << index) == 0)
            .map(|(_, cid)| *cid)
            .collect();
        expected.sort_by_cached_key(Cid::to_string);

        let (heads, superseded) = live(&store).await;
        assert_eq!(heads, expected, "pattern {pattern:#08b}");
        assert_eq!(superseded, markers.len(), "pattern {pattern:#08b}");
    }
}

/// Several blocks superseding one head is the state two concurrent appends
/// leave behind. The head is superseded once, not once per marker.
#[tokio::test]
async fn a_head_superseded_by_siblings_is_counted_once() {
    let store = RegolithStore::in_memory().unwrap();
    let (seed_cid, first, second) = (cid_of(b"seed"), cid_of(b"first"), cid_of(b"second"));
    seed(
        &store,
        &[(seed_cid, 1), (first, 2), (second, 2)],
        &[(seed_cid, first), (seed_cid, second)],
    )
    .await;

    let (heads, superseded) = live(&store).await;
    let mut expected = vec![first, second];
    expected.sort_by_cached_key(Cid::to_string);
    assert_eq!(heads, expected);
    assert_eq!(superseded, 1);
}

/// Reclaiming is invisible: it removes keys the query already ignored, so the
/// answer before and after is the same set.
#[tokio::test]
async fn pruning_does_not_change_the_head_set() {
    let store = RegolithStore::in_memory().unwrap();
    let (seed_cid, first, second) = (cid_of(b"seed"), cid_of(b"first"), cid_of(b"second"));
    seed(
        &store,
        &[(seed_cid, 1), (first, 2), (second, 2)],
        &[(seed_cid, first), (seed_cid, second)],
    )
    .await;

    let before = live(&store).await;
    let outcome = prune(&store, 512).await;
    assert_eq!(outcome.heads_removed, 1);
    assert_eq!(outcome.markers_removed, 2);
    assert!(!outcome.more_remaining);

    let after = live(&store).await;
    assert_eq!(before.0, after.0);
    assert_eq!(after.1, 0, "nothing left to reclaim");

    // The head key and both markers left together: a head key removed while a
    // marker survived would be fine, but a marker removed while the head key
    // survived would resurrect a superseded head.
    assert!(stored_keys(
        &store,
        HeadstoreColSuperseded::collection_prefix(COLLECTION)
    )
    .await
    .is_empty());
    assert_eq!(
        stored_keys(&store, HeadstoreColKey::collection_prefix(COLLECTION))
            .await
            .len(),
        2
    );
}

/// A second pass over a clean collection finds nothing, so the caller can tell
/// a no-op from work done and skip the commit.
#[tokio::test]
async fn pruning_a_clean_collection_is_a_no_op() {
    let store = RegolithStore::in_memory().unwrap();
    seed(&store, &[(cid_of(b"only"), 1)], &[]).await;
    assert_eq!(prune(&store, 512).await, Default::default());
}

/// A marker whose parent has no head key is not garbage: a block replicated
/// ahead of its parent records the marker first, and deleting it would leave
/// the parent live forever once it arrives.
#[tokio::test]
async fn a_marker_without_its_parent_survives_pruning() {
    let store = RegolithStore::in_memory().unwrap();
    let (absent, child, live_head) = (cid_of(b"absent"), cid_of(b"child"), cid_of(b"live"));
    seed(&store, &[(live_head, 1)], &[(absent, child)]).await;

    assert_eq!(prune(&store, 512).await, Default::default());
    assert_eq!(
        stored_keys(
            &store,
            HeadstoreColSuperseded::collection_prefix(COLLECTION)
        )
        .await
        .len(),
        1
    );

    // The parent arrives late and is correctly superseded on sight.
    seed(&store, &[(absent, 2)], &[]).await;
    let (heads, superseded) = live(&store).await;
    assert_eq!(heads, vec![live_head]);
    assert_eq!(superseded, 1);
}

/// A head with more markers than the whole budget still leaves, otherwise every
/// later pass would stop in the same place and it would never be reclaimed.
#[tokio::test]
async fn a_group_wider_than_the_budget_still_leaves() {
    let store = RegolithStore::in_memory().unwrap();
    let doomed = cid_of(b"doomed");
    let children: Vec<Cid> = (0..5u8).map(|i| cid_of(&[i, b'c'])).collect();
    let mut heads = vec![(doomed, 1)];
    heads.extend(children.iter().map(|cid| (*cid, 2)));
    let markers: Vec<(Cid, Cid)> = children.iter().map(|cid| (doomed, *cid)).collect();
    seed(&store, &heads, &markers).await;

    // A budget of two cannot hold the head key plus its five markers.
    let outcome = prune(&store, 2).await;
    assert_eq!(outcome.heads_removed, 1, "the pass must make progress");
    assert_eq!(outcome.markers_removed, 5);

    let (heads, superseded) = live(&store).await;
    assert_eq!(superseded, 0);
    let mut expected = children;
    expected.sort_by_cached_key(Cid::to_string);
    assert_eq!(heads, expected);
}

/// A pass that stops on its key budget says so rather than reading as clean,
/// and repeating it converges.
#[tokio::test]
async fn a_bounded_pass_reports_what_it_left() {
    let store = RegolithStore::in_memory().unwrap();
    let superseder = cid_of(b"superseder");
    let doomed: Vec<Cid> = (0..8u8).map(|i| cid_of(&[i, b'x'])).collect();
    let heads: Vec<(Cid, u64)> = doomed
        .iter()
        .chain(std::iter::once(&superseder))
        .map(|cid| (*cid, 1))
        .collect();
    let markers: Vec<(Cid, Cid)> = doomed.iter().map(|cid| (*cid, superseder)).collect();
    seed(&store, &heads, &markers).await;

    // Four keys is two head-and-marker groups.
    let first = prune(&store, 4).await;
    assert_eq!(first.heads_removed, 2);
    assert_eq!(first.markers_removed, 2);
    assert!(
        first.more_remaining,
        "a bounded pass must not read as clean"
    );

    let mut passes = 1;
    while prune(&store, 4).await.more_remaining {
        passes += 1;
        assert!(passes < 16, "reclamation is not converging");
    }
    let (heads, superseded) = live(&store).await;
    assert_eq!(heads, vec![superseder]);
    assert_eq!(superseded, 0);
}

/// The point of the whole design: reclamation runs in its own transaction, and
/// an append that overlaps it still commits.
#[tokio::test]
async fn reclamation_does_not_abort_a_concurrent_append() {
    let store = RegolithStore::in_memory().unwrap();
    let (old, current) = (cid_of(b"old"), cid_of(b"current"));
    seed(&store, &[(old, 1), (current, 2)], &[(old, current)]).await;

    // The appending transaction opens first and reads the head set.
    let append_txn = SharedTxn::new(store.new_txn(false).await.unwrap());
    let append_view = headstore(&append_txn);
    let found = live_collection_heads(&append_view, COLLECTION)
        .await
        .unwrap();
    assert_eq!(found.live, vec![current]);

    // Reclamation commits underneath it.
    let outcome = prune(&store, 512).await;
    assert_eq!(outcome.heads_removed, 1);

    // The append writes its own keys and commits regardless.
    let appended = cid_of(b"appended");
    db::block::heads::record_supersedes(&append_view, COLLECTION, &found.live, appended)
        .await
        .unwrap();
    append_view
        .set(
            &Key::bytes(&HeadstoreColKey::new(COLLECTION, appended)),
            &[3],
        )
        .await
        .unwrap();
    drop(append_view);
    commit(append_txn).await;

    let (heads, _) = live(&store).await;
    assert_eq!(heads, vec![appended]);
}
