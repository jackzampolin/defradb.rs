use crate::corekv::{Error, IterOptions, Store};

/// Test basic iteration
pub async fn test_iterator_basic<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"a", b"1").await.unwrap();
    txn.set(b"b", b"2").await.unwrap();
    txn.set(b"c", b"3").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();

    let mut keys = vec![];
    while let Some(kv) = iter.next().await.unwrap() {
        keys.push(String::from_utf8_lossy(kv.key_bytes()).to_string());
    }

    assert_eq!(keys, vec!["a", "b", "c"]);
}

/// Test prefix filtering
pub async fn test_iterator_prefix<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"user/1", b"alice").await.unwrap();
    txn.set(b"user/2", b"bob").await.unwrap();
    txn.set(b"post/1", b"hello").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new().with_prefix(b"user/".to_vec());
    let mut iter = txn.iterator(opts).await.unwrap();

    let mut count = 0;
    while let Some(kv) = iter.next().await.unwrap() {
        assert!(kv.key_bytes().starts_with(b"user/"));
        count += 1;
    }
    assert_eq!(count, 2);
}

/// Test reverse iteration
pub async fn test_iterator_reverse<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"a", b"1").await.unwrap();
    txn.set(b"b", b"2").await.unwrap();
    txn.set(b"c", b"3").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new().with_reverse(true);
    let mut iter = txn.iterator(opts).await.unwrap();

    let mut keys = vec![];
    while let Some(kv) = iter.next().await.unwrap() {
        keys.push(String::from_utf8_lossy(kv.key_bytes()).to_string());
    }

    assert_eq!(keys, vec!["c", "b", "a"]);
}

/// Test start/end range
pub async fn test_iterator_range<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    for key in [b"a", b"b", b"c", b"d", b"e"] {
        txn.set(key, b"value").await.unwrap();
    }
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new()
        .with_start(b"b".to_vec())
        .with_end(b"e".to_vec());
    let mut iter = txn.iterator(opts).await.unwrap();

    let mut keys = vec![];
    while let Some(kv) = iter.next().await.unwrap() {
        keys.push(String::from_utf8_lossy(kv.key_bytes()).to_string());
    }

    assert_eq!(keys, vec!["b", "c", "d"]);
}

/// Test keys-only mode returns empty values
pub async fn test_iterator_keys_only<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"key1", b"this_is_a_long_value").await.unwrap();
    txn.set(b"key2", b"another_long_value").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new().with_keys_only(true);
    let mut iter = txn.iterator(opts).await.unwrap();

    while let Some(kv) = iter.next().await.unwrap() {
        assert!(
            kv.value_bytes().is_empty(),
            "Keys-only iterator should return empty values"
        );
    }
}

/// Test closed iterator returns error
pub async fn test_iterator_closed<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"key", b"value").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();

    iter.close().await.unwrap();

    assert!(matches!(iter.next().await, Err(Error::Iterator(_))));
}

/// Test empty iterator (no data)
pub async fn test_iterator_empty_store<S: Store>(store: &S) {
    let txn = store.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();

    assert!(iter.next().await.unwrap().is_none());
}

/// Test start == end yields empty iterator
pub async fn test_iterator_start_equals_end<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"a", b"1").await.unwrap();
    txn.set(b"b", b"2").await.unwrap();
    txn.set(b"c", b"3").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new()
        .with_start(b"b".to_vec())
        .with_end(b"b".to_vec());
    let mut iter = txn.iterator(opts).await.unwrap();

    assert!(
        iter.next().await.unwrap().is_none(),
        "start == end should yield empty iterator"
    );
}

/// Test prefix with no matches
pub async fn test_iterator_prefix_no_match<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"users/1", b"alice").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new().with_prefix(b"posts/".to_vec());
    let mut iter = txn.iterator(opts).await.unwrap();

    assert!(iter.next().await.unwrap().is_none());
}

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

/// Test reverse iteration with prefix
pub async fn test_iterator_reverse_with_prefix<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"user/a", b"1").await.unwrap();
    txn.set(b"user/b", b"2").await.unwrap();
    txn.set(b"user/c", b"3").await.unwrap();
    txn.set(b"other/x", b"4").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new()
        .with_prefix(b"user/".to_vec())
        .with_reverse(true);
    let mut iter = txn.iterator(opts).await.unwrap();

    let mut keys = vec![];
    while let Some(kv) = iter.next().await.unwrap() {
        keys.push(String::from_utf8_lossy(kv.key_bytes()).to_string());
    }

    assert_eq!(keys, vec!["user/c", "user/b", "user/a"]);
}

// ============================================================================
// ITERATOR EDGE CASES (From Go test suite)
// ============================================================================

/// Test iterator with reverse and start/end bounds combined
pub async fn test_iterator_reverse_with_bounds<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    for c in b'a'..=b'z' {
        txn.set(&[c], &[c]).await.unwrap();
    }
    txn.commit().await.unwrap();

    // Reverse with start/end bounds: should get keys from d to m in reverse order
    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new()
        .with_start(b"d".to_vec())
        .with_end(b"n".to_vec()) // exclusive
        .with_reverse(true);
    let mut iter = txn.iterator(opts).await.unwrap();

    let mut keys = vec![];
    while let Some(kv) = iter.next().await.unwrap() {
        keys.push(kv.key_bytes()[0] as char);
    }

    // Should be m, l, k, j, i, h, g, f, e, d (reverse order, end exclusive)
    assert_eq!(keys, vec!['m', 'l', 'k', 'j', 'i', 'h', 'g', 'f', 'e', 'd']);
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

/// Test iterator with empty prefix (should return all keys)
pub async fn test_iterator_empty_prefix<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"a", b"1").await.unwrap();
    txn.set(b"b", b"2").await.unwrap();
    txn.set(b"c", b"3").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new().with_prefix(vec![]); // Empty prefix
    let mut iter = txn.iterator(opts).await.unwrap();

    let mut count = 0;
    while iter.next().await.unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 3, "Empty prefix should match all keys");
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

// ============================================================================
// ITERATOR SEEK AND RESET TESTS
// ============================================================================

/// Test iterator seek to existing key
pub async fn test_iterator_seek<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"a", b"1").await.unwrap();
    txn.set(b"b", b"2").await.unwrap();
    txn.set(b"c", b"3").await.unwrap();
    txn.set(b"d", b"4").await.unwrap();
    txn.set(b"e", b"5").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();

    // Seek to 'c'
    assert!(
        iter.seek(b"c").await.unwrap(),
        "Seek to existing key should return true"
    );

    // Next should return 'c'
    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"c");

    // Followed by 'd' and 'e'
    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"d");

    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"e");

    // No more items
    assert!(iter.next().await.unwrap().is_none());
}

/// Test iterator seek to key that doesn't exist (seeks to next key)
pub async fn test_iterator_seek_between_keys<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"aa", b"1").await.unwrap();
    txn.set(b"cc", b"2").await.unwrap();
    txn.set(b"ee", b"3").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();

    // Seek to 'bb' (doesn't exist, should position at 'cc')
    assert!(
        iter.seek(b"bb").await.unwrap(),
        "Seek to non-existing key should find next"
    );

    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(
        kv.key_bytes(),
        b"cc",
        "Should be positioned at next key >= seek target"
    );
}

/// Test iterator seek past all keys
pub async fn test_iterator_seek_past_end<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"a", b"1").await.unwrap();
    txn.set(b"b", b"2").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();

    // Seek past all keys
    assert!(
        !iter.seek(b"z").await.unwrap(),
        "Seek past end should return false"
    );

    // Iterator should be exhausted
    assert!(iter.next().await.unwrap().is_none());
}

/// Test iterator reset
pub async fn test_iterator_reset<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"a", b"1").await.unwrap();
    txn.set(b"b", b"2").await.unwrap();
    txn.set(b"c", b"3").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();

    // Consume entire iterator
    let mut count = 0;
    while iter.next().await.unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 3);

    // Reset
    iter.reset().await.unwrap();

    // Should be able to iterate again from the beginning
    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"a");

    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"b");

    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"c");

    assert!(iter.next().await.unwrap().is_none());
}

/// Test iterator seek after partial iteration
pub async fn test_iterator_seek_after_iteration<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"a", b"1").await.unwrap();
    txn.set(b"b", b"2").await.unwrap();
    txn.set(b"c", b"3").await.unwrap();
    txn.set(b"d", b"4").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();

    // Iterate partially
    iter.next().await.unwrap(); // a
    iter.next().await.unwrap(); // b

    // Seek back to 'a'
    assert!(iter.seek(b"a").await.unwrap());
    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"a", "Should seek backwards");
}

/// Test iterator seek and reset on closed iterator
pub async fn test_iterator_seek_reset_on_closed<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"key", b"value").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();

    iter.close().await.unwrap();

    // Both seek and reset should fail on closed iterator
    assert!(matches!(iter.seek(b"key").await, Err(Error::Iterator(_))));
    assert!(matches!(iter.reset().await, Err(Error::Iterator(_))));
}

// ============================================================================
// REVERSE ITERATOR EDGE CASES (From Go corekv test suite)
// ============================================================================

/// Test reverse iterator with start bound only.
/// In reverse mode, start is the LOWER bound - iteration goes from highest key DOWN to start.
pub async fn test_iterator_reverse_start_only<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"k1", b"v1").await.unwrap();
    txn.set(b"k2", b"v2").await.unwrap();
    txn.set(b"k3", b"v3").await.unwrap();
    txn.set(b"k4", b"v4").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new()
        .with_reverse(true)
        .with_start(b"k2".to_vec());
    let mut iter = txn.iterator(opts).await.unwrap();

    // Should get k4, k3, k2 in reverse order (k1 is below start bound)
    let mut keys = vec![];
    while let Some(kv) = iter.next().await.unwrap() {
        keys.push(String::from_utf8_lossy(kv.key_bytes()).to_string());
    }

    assert_eq!(
        keys,
        vec!["k4", "k3", "k2"],
        "Reverse with start should exclude keys below start"
    );
}

/// Test reverse iterator with end bound only.
/// In reverse mode, end is exclusive upper bound - iteration starts BELOW end.
pub async fn test_iterator_reverse_end_only<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"k1", b"v1").await.unwrap();
    txn.set(b"k2", b"v2").await.unwrap();
    txn.set(b"k3", b"v3").await.unwrap();
    txn.set(b"k4", b"v4").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new()
        .with_reverse(true)
        .with_end(b"k3".to_vec()); // k3 and k4 should be excluded
    let mut iter = txn.iterator(opts).await.unwrap();

    let mut keys = vec![];
    while let Some(kv) = iter.next().await.unwrap() {
        keys.push(String::from_utf8_lossy(kv.key_bytes()).to_string());
    }

    assert_eq!(
        keys,
        vec!["k2", "k1"],
        "Reverse with end should exclude keys >= end"
    );
}

/// Test reverse iterator with single item at/above end bound (should yield nothing).
/// This is a known edge case from Go implementation - single item >= end should not be yielded.
pub async fn test_iterator_reverse_end_single_item_out_of_bounds<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"k3", b"v3").await.unwrap(); // Only item, at the end bound
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new()
        .with_reverse(true)
        .with_end(b"k3".to_vec()); // k3 is exactly at end, should be excluded
    let mut iter = txn.iterator(opts).await.unwrap();

    assert!(
        iter.next().await.unwrap().is_none(),
        "Single item at end bound should not be yielded in reverse"
    );
}

/// Test reverse iterator with both start and end, where no items exist in range.
pub async fn test_iterator_reverse_start_end_no_items_in_range<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"k1", b"v1").await.unwrap();
    txn.set(b"k5", b"v5").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new()
        .with_reverse(true)
        .with_start(b"k2".to_vec())
        .with_end(b"k4".to_vec()); // Range [k2, k4) has no items
    let mut iter = txn.iterator(opts).await.unwrap();

    assert!(
        iter.next().await.unwrap().is_none(),
        "Reverse with start/end range containing no items should yield nothing"
    );
}

/// Test reverse iterator with seek operation.
pub async fn test_iterator_reverse_seek<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"k1", b"v1").await.unwrap();
    txn.set(b"k2", b"v2").await.unwrap();
    txn.set(b"k3", b"v3").await.unwrap();
    txn.set(b"k4", b"v4").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new().with_reverse(true);
    let mut iter = txn.iterator(opts).await.unwrap();

    // Seek to k2 in reverse iterator
    assert!(iter.seek(b"k2").await.unwrap());

    // In reverse, seek should position at k2, then next goes to k1
    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"k2");

    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"k1");

    assert!(iter.next().await.unwrap().is_none());
}

/// Test reverse iterator seek followed by next.
pub async fn test_iterator_reverse_seek_next<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"k1", b"v1").await.unwrap();
    txn.set(b"k2", b"v2").await.unwrap();
    txn.set(b"k3", b"v3").await.unwrap();
    txn.set(b"k4", b"v4").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new().with_reverse(true);
    let mut iter = txn.iterator(opts).await.unwrap();

    // First next() gets k4 (highest)
    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"k4");

    // Seek to k2
    assert!(iter.seek(b"k2").await.unwrap());

    // Next after seek should return k2
    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"k2");

    // Then k1
    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"k1");
}

/// Test reverse iterator with end bound and seek.
/// Seek should respect end bound even in reverse mode.
pub async fn test_iterator_reverse_end_seek<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"k1", b"v1").await.unwrap();
    txn.set(b"k2", b"v2").await.unwrap();
    txn.set(b"k3", b"v3").await.unwrap();
    txn.set(b"k4", b"v4").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new()
        .with_reverse(true)
        .with_end(b"k3".to_vec()); // Excludes k3 and k4
    let mut iter = txn.iterator(opts).await.unwrap();

    // Seek to k4 (which is outside bounds) should find k2 (highest in bounds)
    assert!(iter.seek(b"k4").await.unwrap());
    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(
        kv.key_bytes(),
        b"k2",
        "Seek outside end bound should find highest in-bounds key"
    );
}

/// Test reverse iterator with prefix.
pub async fn test_iterator_reverse_prefix_next<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"a/1", b"v1").await.unwrap();
    txn.set(b"a/2", b"v2").await.unwrap();
    txn.set(b"b/1", b"v3").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new()
        .with_reverse(true)
        .with_prefix(b"a/".to_vec());
    let mut iter = txn.iterator(opts).await.unwrap();

    // First next should get a/2 (highest with prefix)
    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"a/2");

    // Then a/1
    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"a/1");

    // No more
    assert!(iter.next().await.unwrap().is_none());
}

// ============================================================================
// ITERATOR STATE TRANSITION TESTS (From Go corekv test suite)
// ============================================================================

/// Test reset during partial iteration.
/// Iterate partway, reset, then iterate again from beginning.
pub async fn test_iterator_reset_partial_iteration<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"k1", b"v1").await.unwrap();
    txn.set(b"k2", b"v2").await.unwrap();
    txn.set(b"k3", b"v3").await.unwrap();
    txn.set(b"k4", b"v4").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();

    // Iterate first two items
    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.value_bytes(), b"v1");

    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.value_bytes(), b"v2");

    // Reset in the middle
    iter.reset().await.unwrap();

    // Should start over from k1
    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.value_bytes(), b"v1");

    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.value_bytes(), b"v2");

    // Continue to end
    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.value_bytes(), b"v3");

    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.value_bytes(), b"v4");

    assert!(iter.next().await.unwrap().is_none());
}

/// Test reset after full iteration.
/// Iterate to exhaustion, reset, then iterate again.
pub async fn test_iterator_reset_after_exhaustion<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"k1", b"v1").await.unwrap();
    txn.set(b"k2", b"v2").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();

    // Fully exhaust iterator
    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.value_bytes(), b"v1");
    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.value_bytes(), b"v2");
    assert!(iter.next().await.unwrap().is_none()); // Exhausted

    // Reset
    iter.reset().await.unwrap();

    // Should iterate again from beginning
    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.value_bytes(), b"v1");
    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.value_bytes(), b"v2");
    assert!(iter.next().await.unwrap().is_none());
}

/// Test reset followed by seek.
/// Reset, then seek to middle, continue from there.
pub async fn test_iterator_reset_then_seek<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"k1", b"v1").await.unwrap();
    txn.set(b"k2", b"v2").await.unwrap();
    txn.set(b"k3", b"v3").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();

    // Iterate some
    iter.next().await.unwrap();

    // Reset
    iter.reset().await.unwrap();

    // Seek to k2
    assert!(iter.seek(b"k2").await.unwrap());

    // Should get k2
    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"k2");

    // Then k3
    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"k3");

    assert!(iter.next().await.unwrap().is_none());
}

/// Test seek respects start bound.
/// If you seek to a key before start bound, should position at start.
pub async fn test_iterator_seek_respects_start_bound<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"k1", b"v1").await.unwrap();
    txn.set(b"k2", b"v2").await.unwrap();
    txn.set(b"k3", b"v3").await.unwrap();
    txn.set(b"k4", b"v4").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new().with_start(b"k2".to_vec());
    let mut iter = txn.iterator(opts).await.unwrap();

    // Seek to k1 (before start bound)
    // Should position at k2 (the start bound)
    assert!(iter.seek(b"k1").await.unwrap());

    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(
        kv.key_bytes(),
        b"k2",
        "Seek before start should position at start"
    );
}

/// Test multiple resets in sequence.
pub async fn test_iterator_multiple_resets<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"a", b"1").await.unwrap();
    txn.set(b"b", b"2").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();

    for _ in 0..3 {
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(kv.key_bytes(), b"a");
        iter.reset().await.unwrap();
    }

    // After final reset, should still work
    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"a");
    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"b");
}

// ============================================================================
// EMPTY VALUE HANDLING TESTS
// ============================================================================

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
