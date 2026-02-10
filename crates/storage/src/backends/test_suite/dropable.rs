use crate::corekv::Store;

/// Test drop_all clears all data
pub async fn test_drop_all<S: Store + crate::corekv::Dropable>(store: &S) {
    // Add some data
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"key1", b"value1").await.unwrap();
    txn.set(b"key2", b"value2").await.unwrap();
    txn.set(b"key3", b"value3").await.unwrap();
    txn.commit().await.unwrap();

    // Verify data exists
    let txn = store.new_txn(true).await.unwrap();
    assert!(txn.has(b"key1").await.unwrap());
    assert!(txn.has(b"key2").await.unwrap());
    assert!(txn.has(b"key3").await.unwrap());

    // Drop all
    store.drop_all().await.unwrap();

    // Verify data is gone
    let txn = store.new_txn(true).await.unwrap();
    assert!(
        !txn.has(b"key1").await.unwrap(),
        "key1 should be deleted after drop_all"
    );
    assert!(
        !txn.has(b"key2").await.unwrap(),
        "key2 should be deleted after drop_all"
    );
    assert!(
        !txn.has(b"key3").await.unwrap(),
        "key3 should be deleted after drop_all"
    );
}

/// Test drop_all followed by new writes
pub async fn test_drop_all_then_write<S: Store + crate::corekv::Dropable>(store: &S) {
    // Add initial data
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"old_key", b"old_value").await.unwrap();
    txn.commit().await.unwrap();

    // Drop all
    store.drop_all().await.unwrap();

    // Write new data
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"new_key", b"new_value").await.unwrap();
    txn.commit().await.unwrap();

    // Verify old is gone, new exists
    let txn = store.new_txn(true).await.unwrap();
    assert!(
        !txn.has(b"old_key").await.unwrap(),
        "old_key should be deleted"
    );
    assert_eq!(
        txn.get(b"new_key").await.unwrap(),
        Some(b"new_value".to_vec()),
        "new_key should exist"
    );
}

/// Test drop_all on empty store (should succeed)
pub async fn test_drop_all_empty_store<S: Store + crate::corekv::Dropable>(store: &S) {
    // Drop all on empty store should succeed
    store.drop_all().await.unwrap();

    // Store should still be usable
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"key", b"value").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    assert_eq!(txn.get(b"key").await.unwrap(), Some(b"value".to_vec()));
}
