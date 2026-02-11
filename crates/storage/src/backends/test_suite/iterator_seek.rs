use crate::corekv::{Error, IterOptions, Store};

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
