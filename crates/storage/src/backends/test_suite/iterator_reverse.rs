use crate::corekv::{IterOptions, Store};

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
