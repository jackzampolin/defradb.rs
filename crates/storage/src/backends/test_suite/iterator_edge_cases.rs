use crate::corekv::{IterOptions, Store};

/// Test iterator sees pending (uncommitted) writes
pub async fn test_iterator_sees_pending_writes<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"a", b"1").await.unwrap();
    txn.set(b"b", b"2").await.unwrap();

    // Iterator BEFORE commit should see pending writes
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();

    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"a");

    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"b");

    assert!(iter.next().await.unwrap().is_none());
}

/// Test iterator sees pending deletes
pub async fn test_iterator_sees_pending_deletes<S: Store>(store: &S) {
    // Setup
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"a", b"1").await.unwrap();
    txn.set(b"b", b"2").await.unwrap();
    txn.set(b"c", b"3").await.unwrap();
    txn.commit().await.unwrap();

    // Delete b (uncommitted)
    let mut txn = store.new_txn(false).await.unwrap();
    txn.delete(b"b").await.unwrap();

    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();

    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"a");

    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"c", "Deleted 'b' should be skipped");

    assert!(iter.next().await.unwrap().is_none());
}

/// Test pending writes that land on a chunk boundary of the snapshot read.
///
/// Backends that read the committed side in bounded windows merge it against
/// the transaction's pending writes, advancing the two sides independently.
/// A pending write sitting exactly where one window ends and the next begins
/// is the case most likely to drop or duplicate a key, because it is the one
/// place a refill happens mid-merge.
///
/// Indices are derived from `DEFAULT_CHUNK_SIZE` so that retuning the window
/// moves the seam this exercises instead of silently leaving it untested.
pub async fn test_iterator_pending_writes_at_chunk_boundary<S: Store>(store: &S) {
    let chunk = crate::chunked::DEFAULT_CHUNK_SIZE;
    let last_of_window = chunk - 1;
    let committed: Vec<Vec<u8>> = (0..chunk * 2 + 88)
        .map(|i| format!("key_{:05}", i).into_bytes())
        .collect();

    let mut txn = store.new_txn(false).await.unwrap();
    for key in &committed {
        txn.set(key, b"v").await.unwrap();
    }
    txn.commit().await.unwrap();

    // Suffixed so it sorts after the last key of the first window and before
    // the first key of the second, i.e. into the seam.
    let mut seam_key = committed[last_of_window].clone();
    seam_key.push(b'x');

    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(&seam_key, b"new").await.unwrap();
    txn.delete(&committed[chunk]).await.unwrap();
    txn.set(&committed[chunk + 1], b"override").await.unwrap();

    let mut expected: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for (i, key) in committed.iter().enumerate() {
        if i == chunk {
            continue; // deleted in this transaction
        } else if i == chunk + 1 {
            expected.push((key.clone(), b"override".to_vec()));
        } else {
            expected.push((key.clone(), b"v".to_vec()));
        }
        if i == last_of_window {
            expected.push((seam_key.clone(), b"new".to_vec()));
        }
    }

    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();
    let mut seen: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    while let Some(kv) = iter.next().await.unwrap() {
        seen.push((kv.key, kv.value));
    }

    assert_eq!(
        seen.len(),
        expected.len(),
        "one insert and one delete across the seam must net out"
    );
    assert_eq!(seen, expected, "merged order or values wrong at the seam");
}

/// Seed `doc/00000…` spanning three chunks, flanked by a neighbouring
/// keyspace on each side so an over-wide scan has something to pick up.
/// Returns the number of `doc/` keys written.
async fn seed_flanked_keyspace<S: Store>(store: &S) -> usize {
    let total = crate::chunked::DEFAULT_CHUNK_SIZE * 3;

    let mut txn = store.new_txn(false).await.unwrap();
    for i in 0..total {
        txn.set(format!("doc/{:05}", i).as_bytes(), b"v")
            .await
            .unwrap();
        // Sorts after every `doc/` key.
        txn.set(format!("docz/{:05}", i).as_bytes(), b"v")
            .await
            .unwrap();
    }
    // Sorts before every `doc/` key.
    txn.set(b"dob/zzz", b"v").await.unwrap();
    txn.commit().await.unwrap();

    total
}

/// Test that `start`/`end` survive chunk refills.
///
/// Backends reading in bounded windows re-derive the lower bound on every
/// refill, so the range is re-applied per chunk rather than once for the scan.
/// A refill that drops the upper bound overruns the range; one that widens its
/// lower bound re-yields keys. Neither shows up in a scan that fits in a
/// single window, so this one spans three.
pub async fn test_iterator_bounded_scan_across_chunks<S: Store>(store: &S) {
    let chunk = crate::chunked::DEFAULT_CHUNK_SIZE;
    seed_flanked_keyspace(store).await;

    // Off a chunk multiple, so the bounds fall inside windows rather than on
    // their seams.
    let start = chunk + 7;
    let end = chunk * 2 + 133;

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new()
        .with_start(format!("doc/{:05}", start).into_bytes())
        .with_end(format!("doc/{:05}", end).into_bytes());
    let mut iter = txn.iterator(opts).await.unwrap();

    let mut seen: Vec<Vec<u8>> = Vec::new();
    while let Some(kv) = iter.next().await.unwrap() {
        seen.push(kv.key);
    }

    let expected: Vec<Vec<u8>> = (start..end)
        .map(|i| format!("doc/{:05}", i).into_bytes())
        .collect();

    assert_eq!(
        seen.len(),
        expected.len(),
        "bounded scan spanning several chunks returned the wrong count"
    );
    assert_eq!(seen, expected, "start/end not held across a refill");
}

/// Test that a prefix survives chunk refills.
///
/// Separate from the `start`/`end` case because bounds that sit inside the
/// prefix make it redundant: every key between two `doc/…` bounds already
/// carries the prefix. Only a scan with no explicit range puts the prefix
/// itself under test, and it has to span several windows for a refill that
/// forgets the prefix to have anything to run into.
pub async fn test_iterator_prefix_scan_across_chunks<S: Store>(store: &S) {
    let total = seed_flanked_keyspace(store).await;

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new().with_prefix(b"doc/".to_vec());
    let mut iter = txn.iterator(opts).await.unwrap();

    let mut seen: Vec<Vec<u8>> = Vec::new();
    while let Some(kv) = iter.next().await.unwrap() {
        seen.push(kv.key);
    }

    let expected: Vec<Vec<u8>> = (0..total)
        .map(|i| format!("doc/{:05}", i).into_bytes())
        .collect();

    assert_eq!(
        seen.len(),
        expected.len(),
        "prefix scan spanning {} chunks returned the wrong count",
        total / crate::chunked::DEFAULT_CHUNK_SIZE
    );
    assert_eq!(seen, expected, "prefix not held across a refill");
}

/// Test iterator boundary: single item at exact start bound
pub async fn test_iterator_single_item_at_start<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"only_key", b"value").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new()
        .with_start(b"only_key".to_vec())
        .with_end(b"only_keyz".to_vec());
    let mut iter = txn.iterator(opts).await.unwrap();

    let kv = iter.next().await.unwrap();
    assert!(kv.is_some());
    assert_eq!(kv.unwrap().key_bytes(), b"only_key");

    assert!(iter.next().await.unwrap().is_none());
}

/// Test iterator with item exactly at end bound (should be excluded)
pub async fn test_iterator_item_at_end_excluded<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"a", b"1").await.unwrap();
    txn.set(b"b", b"2").await.unwrap();
    txn.set(b"c", b"3").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new()
        .with_start(b"a".to_vec())
        .with_end(b"c".to_vec()); // c should be excluded
    let mut iter = txn.iterator(opts).await.unwrap();

    let mut keys = vec![];
    while let Some(kv) = iter.next().await.unwrap() {
        keys.push(String::from_utf8_lossy(kv.key_bytes()).to_string());
    }

    assert_eq!(keys, vec!["a", "b"], "End bound should be exclusive");
}

/// Test iterator with prefix that has no matching keys (edge case)
pub async fn test_iterator_prefix_between_keys<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"aaa", b"1").await.unwrap();
    txn.set(b"ccc", b"2").await.unwrap();
    txn.commit().await.unwrap();

    // Prefix "b" should match nothing (between aaa and ccc)
    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new().with_prefix(b"b".to_vec());
    let mut iter = txn.iterator(opts).await.unwrap();

    assert!(iter.next().await.unwrap().is_none());
}

/// Test iterator with overlapping start and prefix (prefix should win for scoping)
pub async fn test_iterator_prefix_with_start<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"pre/a", b"1").await.unwrap();
    txn.set(b"pre/b", b"2").await.unwrap();
    txn.set(b"pre/c", b"3").await.unwrap();
    txn.set(b"other/x", b"4").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new()
        .with_prefix(b"pre/".to_vec())
        .with_start(b"pre/b".to_vec()); // Start at b within prefix
    let mut iter = txn.iterator(opts).await.unwrap();

    let mut keys = vec![];
    while let Some(kv) = iter.next().await.unwrap() {
        keys.push(String::from_utf8_lossy(kv.key_bytes()).to_string());
    }

    assert_eq!(keys, vec!["pre/b", "pre/c"]);
}

/// Test multiple iterators on same transaction
pub async fn test_multiple_iterators<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"a", b"1").await.unwrap();
    txn.set(b"b", b"2").await.unwrap();
    txn.set(b"c", b"3").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();

    // Create two iterators on the same transaction
    let mut iter1 = txn.iterator(IterOptions::new()).await.unwrap();
    let mut iter2 = txn
        .iterator(IterOptions::new().with_reverse(true))
        .await
        .unwrap();

    // iter1 should go forward
    let kv1 = iter1.next().await.unwrap().unwrap();
    assert_eq!(kv1.key_bytes(), b"a");

    // iter2 should go backward (independent of iter1)
    let kv2 = iter2.next().await.unwrap().unwrap();
    assert_eq!(kv2.key_bytes(), b"c");

    // Continue iter1
    let kv1 = iter1.next().await.unwrap().unwrap();
    assert_eq!(kv1.key_bytes(), b"b");

    // Continue iter2
    let kv2 = iter2.next().await.unwrap().unwrap();
    assert_eq!(kv2.key_bytes(), b"b");
}

/// Test that empty values are handled correctly (distinct from non-existent keys).
pub async fn test_empty_value_handling<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"empty_value", b"").await.unwrap();
    txn.set(b"normal_value", b"hello").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();

    // Empty value key should exist
    assert!(txn.has(b"empty_value").await.unwrap());
    assert_eq!(txn.get(b"empty_value").await.unwrap(), Some(vec![]));

    // Non-existent key should be None, not empty vec
    assert!(!txn.has(b"nonexistent").await.unwrap());
    assert_eq!(txn.get(b"nonexistent").await.unwrap(), None);
}

/// Test iterator with empty values.
pub async fn test_iterator_empty_values<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"k1", b"v1").await.unwrap();
    txn.set(b"k2", b"").await.unwrap(); // Empty value
    txn.set(b"k3", b"v3").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();

    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"k1");
    assert_eq!(kv.value_bytes(), b"v1");

    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"k2");
    assert_eq!(
        kv.value_bytes(),
        b"",
        "Empty value should be empty slice, not skipped"
    );

    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"k3");
    assert_eq!(kv.value_bytes(), b"v3");
}

/// A range whose start sits above its end yields nothing rather than panicking.
pub async fn test_iterator_inverted_bounds_yield_empty<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"a", b"1").await.unwrap();
    txn.set(b"m", b"2").await.unwrap();
    txn.set(b"z", b"3").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new()
        .with_start(b"z".to_vec())
        .with_end(b"a".to_vec());
    let mut iter = txn.iterator(opts).await.unwrap();

    assert!(iter.next().await.unwrap().is_none());
}

/// The same, in reverse: reverse scans take a different code path in every
/// chunked backend.
pub async fn test_iterator_inverted_bounds_yield_empty_reverse<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"a", b"1").await.unwrap();
    txn.set(b"z", b"2").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new()
        .with_start(b"z".to_vec())
        .with_end(b"a".to_vec())
        .with_reverse(true);
    let mut iter = txn.iterator(opts).await.unwrap();

    assert!(iter.next().await.unwrap().is_none());
}

/// A start key past the end of the requested prefix cannot match anything.
pub async fn test_iterator_start_beyond_prefix_yields_empty<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"foo1", b"1").await.unwrap();
    txn.set(b"foo2", b"2").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new()
        .with_prefix(b"foo".to_vec())
        .with_start(b"z".to_vec());
    let mut iter = txn.iterator(opts).await.unwrap();

    assert!(iter.next().await.unwrap().is_none());
}

/// Uncommitted writes go through a second, separately-bounded range in every
/// backend; an inverted range must not panic there either.
pub async fn test_iterator_inverted_bounds_with_pending_writes<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"a", b"1").await.unwrap();
    txn.set(b"z", b"2").await.unwrap();

    let opts = IterOptions::new()
        .with_start(b"z".to_vec())
        .with_end(b"a".to_vec());
    let mut iter = txn.iterator(opts).await.unwrap();

    assert!(iter.next().await.unwrap().is_none());
}
