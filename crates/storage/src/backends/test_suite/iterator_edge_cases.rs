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
/// The window is 256 pairs, so this seeds 600 committed keys and then, in an
/// uncommitted transaction, inserts into the 255/256 seam, deletes the first
/// key of the second window, and overrides the key after it.
pub async fn test_iterator_pending_writes_at_chunk_boundary<S: Store>(store: &S) {
    let committed: Vec<Vec<u8>> = (0..600)
        .map(|i| format!("key_{:05}", i).into_bytes())
        .collect();

    let mut txn = store.new_txn(false).await.unwrap();
    for key in &committed {
        txn.set(key, b"v").await.unwrap();
    }
    txn.commit().await.unwrap();

    // Sorts after key_00255 and before key_00256, i.e. into the seam.
    let seam_key = b"key_00255x".to_vec();

    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(&seam_key, b"new").await.unwrap();
    txn.delete(&committed[256]).await.unwrap();
    txn.set(&committed[257], b"override").await.unwrap();

    let mut expected: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for (i, key) in committed.iter().enumerate() {
        match i {
            256 => continue, // deleted in this transaction
            257 => expected.push((key.clone(), b"override".to_vec())),
            _ => expected.push((key.clone(), b"v".to_vec())),
        }
        if i == 255 {
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
