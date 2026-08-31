//! Reverse iteration over a transaction that has pending writes.
//!
//! This used to be refused outright: the engine had no reverse form of the
//! merge that overlays a transaction's own uncommitted writes on the snapshot
//! beneath them, so the backend returned an error rather than a wrong answer.
//! regolith 0.1.2 added it, so the refusal is gone and these pin the behaviour
//! that replaced it.

use storage::corekv::{IterOptions, Iterator, Store, Writer};
use storage::RegolithStore;

async fn seeded() -> RegolithStore {
    let store = RegolithStore::in_memory().unwrap();
    let mut txn = store.new_txn(false).await.unwrap();
    for key in ["a", "b", "c", "d", "e"] {
        txn.set(key.as_bytes(), b"committed").await.unwrap();
    }
    txn.commit().await.unwrap();
    store
}

async fn keys(txn: &dyn storage::corekv::Txn, opts: IterOptions) -> Vec<String> {
    let mut iter = txn.iterator(opts).await.unwrap();
    let mut out = Vec::new();
    while let Some(pair) = iter.next().await.unwrap() {
        out.push(String::from_utf8(pair.key).unwrap());
    }
    iter.close().await.unwrap();
    out
}

#[tokio::test]
async fn a_writing_transaction_scans_backwards() {
    let store = seeded().await;
    let txn = store.new_txn(false).await.unwrap();
    assert_eq!(
        keys(txn.as_ref(), IterOptions::new().with_reverse(true)).await,
        ["e", "d", "c", "b", "a"]
    );
}

#[tokio::test]
async fn a_backward_scan_sees_this_transaction_s_pending_insert() {
    let store = seeded().await;
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"bb", b"pending").await.unwrap();
    assert_eq!(
        keys(txn.as_ref(), IterOptions::new().with_reverse(true)).await,
        ["e", "d", "c", "bb", "b", "a"]
    );
}

#[tokio::test]
async fn a_backward_scan_hides_this_transaction_s_pending_delete() {
    let store = seeded().await;
    let mut txn = store.new_txn(false).await.unwrap();
    txn.delete(b"c").await.unwrap();
    assert_eq!(
        keys(txn.as_ref(), IterOptions::new().with_reverse(true)).await,
        ["e", "d", "b", "a"]
    );
}

#[tokio::test]
async fn a_backward_scan_honours_the_range() {
    let store = seeded().await;
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"bb", b"pending").await.unwrap();
    assert_eq!(
        keys(
            txn.as_ref(),
            IterOptions::new()
                .with_reverse(true)
                .with_start(b"b".to_vec())
                .with_end(b"d".to_vec())
        )
        .await,
        ["c", "bb", "b"]
    );
}

/// Enough entries to cross the backend's page boundary, so the resume path is
/// exercised rather than one page answering the whole scan.
#[tokio::test]
async fn a_backward_scan_pages_without_repeating_or_dropping_a_key() {
    let store = RegolithStore::in_memory().unwrap();
    let total = 2_500u32;
    let mut txn = store.new_txn(false).await.unwrap();
    for index in 0..total {
        txn.set(format!("key{index:06}").as_bytes(), b"committed")
            .await
            .unwrap();
    }
    txn.commit().await.unwrap();

    let mut txn = store.new_txn(false).await.unwrap();
    // A pending write inside the range, so both sides of the merge are live
    // across the page boundary.
    txn.set(b"key001234x", b"pending").await.unwrap();

    let walked = keys(txn.as_ref(), IterOptions::new().with_reverse(true)).await;
    assert_eq!(
        walked.len(),
        total as usize + 1,
        "dropped or repeated a key"
    );

    let mut sorted = walked.clone();
    sorted.sort();
    sorted.reverse();
    assert_eq!(walked, sorted, "reverse order broke across a page");
    sorted.dedup();
    assert_eq!(sorted.len(), walked.len(), "a key came back twice");
}

/// Forward and reverse are the same set in opposite orders, pending writes
/// included.
#[tokio::test]
async fn forward_and_backward_agree_on_the_set() {
    let store = seeded().await;
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"bb", b"pending").await.unwrap();
    txn.delete(b"d").await.unwrap();

    let forward = keys(txn.as_ref(), IterOptions::new()).await;
    let mut backward = keys(txn.as_ref(), IterOptions::new().with_reverse(true)).await;
    backward.reverse();
    assert_eq!(forward, backward);
}

/// A seek below the range's start must land on the start, on a writing
/// transaction as well as a read-only one.
///
/// The read-only path clamps through `floor`; the writing path used to pass
/// the raw seek key into the merged scan, so it returned entries outside its
/// own bounds. The backend conformance suite missed it because every seek test
/// there opens a read-only transaction.
#[tokio::test]
async fn a_seek_below_start_is_clamped_on_a_writing_transaction() {
    let store = seeded().await;
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"bb", b"pending").await.unwrap();

    let mut iter = txn
        .iterator(IterOptions::new().with_start(b"c".to_vec()))
        .await
        .unwrap();
    // "a" is below the start bound; the scan must begin at "c" regardless.
    assert!(iter.seek(b"a").await.unwrap());

    let mut seen = Vec::new();
    while let Some(pair) = iter.next().await.unwrap() {
        seen.push(String::from_utf8(pair.key).unwrap());
    }
    iter.close().await.unwrap();

    assert_eq!(seen, ["c", "d", "e"], "seek below start escaped the range");
}

/// The same clamp with a pending write sitting below the start bound, so the
/// buffered side of the merge is exercised too.
#[tokio::test]
async fn a_pending_write_below_start_stays_out_of_a_clamped_seek() {
    let store = seeded().await;
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"aa", b"pending").await.unwrap();

    let mut iter = txn
        .iterator(IterOptions::new().with_start(b"c".to_vec()))
        .await
        .unwrap();
    assert!(iter.seek(b"a").await.unwrap());

    let mut seen = Vec::new();
    while let Some(pair) = iter.next().await.unwrap() {
        seen.push(String::from_utf8(pair.key).unwrap());
    }
    iter.close().await.unwrap();

    assert_eq!(
        seen,
        ["c", "d", "e"],
        "a pending write below start leaked in"
    );
}
