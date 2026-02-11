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
