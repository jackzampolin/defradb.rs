use crate::corekv::{Error, IterOptions, Store};

/// Test basic set/get operations
pub async fn test_basic_set_get<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();

    txn.set(b"key1", b"value1").await.unwrap();
    txn.set(b"key2", b"value2").await.unwrap();

    assert_eq!(txn.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
    assert_eq!(txn.get(b"key2").await.unwrap(), Some(b"value2".to_vec()));
    assert_eq!(txn.get(b"nonexistent").await.unwrap(), None);

    txn.commit().await.unwrap();

    // Verify after commit
    let txn = store.new_txn(true).await.unwrap();
    assert_eq!(txn.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
    assert_eq!(txn.get(b"key2").await.unwrap(), Some(b"value2".to_vec()));
}

/// Test delete operation
pub async fn test_delete<S: Store>(store: &S) {
    // Setup
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"to_delete", b"value").await.unwrap();
    txn.commit().await.unwrap();

    // Delete
    let mut txn = store.new_txn(false).await.unwrap();
    assert_eq!(
        txn.get(b"to_delete").await.unwrap(),
        Some(b"value".to_vec())
    );
    txn.delete(b"to_delete").await.unwrap();
    assert_eq!(txn.get(b"to_delete").await.unwrap(), None);
    txn.commit().await.unwrap();

    // Verify deletion persisted
    let txn = store.new_txn(true).await.unwrap();
    assert_eq!(txn.get(b"to_delete").await.unwrap(), None);
}

/// Test has operation
pub async fn test_has<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();

    assert!(!txn.has(b"key").await.unwrap());

    txn.set(b"key", b"value").await.unwrap();
    assert!(txn.has(b"key").await.unwrap());

    txn.delete(b"key").await.unwrap();
    assert!(!txn.has(b"key").await.unwrap());

    txn.commit().await.unwrap();
}

/// Test get_size operation
pub async fn test_get_size<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();

    // Non-existent key
    assert_eq!(txn.get_size(b"nonexistent").await.unwrap(), None);

    // Set a value and check size
    txn.set(b"key", b"hello").await.unwrap();
    assert_eq!(txn.get_size(b"key").await.unwrap(), Some(5));

    // Larger value
    let large_value = vec![0u8; 1000];
    txn.set(b"large", &large_value).await.unwrap();
    assert_eq!(txn.get_size(b"large").await.unwrap(), Some(1000));

    // Empty value
    txn.set(b"empty", b"").await.unwrap();
    assert_eq!(txn.get_size(b"empty").await.unwrap(), Some(0));

    txn.commit().await.unwrap();

    // Verify after commit
    let txn = store.new_txn(true).await.unwrap();
    assert_eq!(txn.get_size(b"key").await.unwrap(), Some(5));
    assert_eq!(txn.get_size(b"large").await.unwrap(), Some(1000));
    assert_eq!(txn.get_size(b"empty").await.unwrap(), Some(0));
    assert_eq!(txn.get_size(b"nonexistent").await.unwrap(), None);
}

/// Test get_size with pending deletes
pub async fn test_get_size_with_deletes<S: Store>(store: &S) {
    // Setup
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"to_delete", b"value").await.unwrap();
    txn.commit().await.unwrap();

    // Delete in new transaction
    let mut txn = store.new_txn(false).await.unwrap();
    assert_eq!(txn.get_size(b"to_delete").await.unwrap(), Some(5));
    txn.delete(b"to_delete").await.unwrap();
    assert_eq!(txn.get_size(b"to_delete").await.unwrap(), None);
    txn.commit().await.unwrap();

    // Verify
    let txn = store.new_txn(true).await.unwrap();
    assert_eq!(txn.get_size(b"to_delete").await.unwrap(), None);
}

/// Test read-your-writes within a transaction
pub async fn test_read_your_writes<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();

    // Write
    txn.set(b"key", b"value1").await.unwrap();
    assert_eq!(txn.get(b"key").await.unwrap(), Some(b"value1".to_vec()));

    // Update
    txn.set(b"key", b"value2").await.unwrap();
    assert_eq!(txn.get(b"key").await.unwrap(), Some(b"value2".to_vec()));

    // Delete
    txn.delete(b"key").await.unwrap();
    assert_eq!(txn.get(b"key").await.unwrap(), None);

    // Re-add
    txn.set(b"key", b"value3").await.unwrap();
    assert_eq!(txn.get(b"key").await.unwrap(), Some(b"value3".to_vec()));

    txn.commit().await.unwrap();
}

/// Test binary data handling - keys and values with null bytes and high bytes
pub async fn test_binary_data<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();

    // Key with null byte in middle
    let key_with_null = b"key\x00with\x00nulls";
    txn.set(key_with_null, b"value1").await.unwrap();

    // Key with high bytes (0xFF)
    let key_with_high = b"\xff\xfe\xfd";
    txn.set(key_with_high, b"value2").await.unwrap();

    // Value with null bytes
    let value_with_null = b"value\x00with\x00nulls";
    txn.set(b"normal_key", value_with_null).await.unwrap();

    // Value with all byte values 0x00-0xFF
    let mut all_bytes: Vec<u8> = (0u8..=255u8).collect();
    txn.set(b"all_bytes_key", &all_bytes).await.unwrap();

    txn.commit().await.unwrap();

    // Verify all data persisted correctly
    let txn = store.new_txn(true).await.unwrap();

    assert_eq!(
        txn.get(key_with_null).await.unwrap(),
        Some(b"value1".to_vec()),
        "Key with null bytes should work"
    );

    assert_eq!(
        txn.get(key_with_high).await.unwrap(),
        Some(b"value2".to_vec()),
        "Key with high bytes should work"
    );

    assert_eq!(
        txn.get(b"normal_key").await.unwrap(),
        Some(value_with_null.to_vec()),
        "Value with null bytes should work"
    );

    all_bytes = (0u8..=255u8).collect();
    assert_eq!(
        txn.get(b"all_bytes_key").await.unwrap(),
        Some(all_bytes),
        "Value with all byte values should work"
    );
}

/// Test that binary keys maintain correct sort order in iterators
pub async fn test_binary_key_ordering<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();

    // Insert keys that would sort differently if treated as strings vs bytes
    // Byte order: 0x00 < 0x41 ('A') < 0x61 ('a') < 0xFF
    txn.set(b"\x00", b"first").await.unwrap();
    txn.set(b"A", b"second").await.unwrap(); // 0x41
    txn.set(b"a", b"third").await.unwrap(); // 0x61
    txn.set(b"\xff", b"fourth").await.unwrap();

    txn.commit().await.unwrap();

    // Iterate and verify byte order
    let txn = store.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();

    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"\x00", "0x00 should come first");

    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"A", "0x41 ('A') should come second");

    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"a", "0x61 ('a') should come third");

    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"\xff", "0xFF should come fourth");

    assert!(iter.next().await.unwrap().is_none());
}

/// Test empty key rejection
pub async fn test_empty_key_rejected<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();

    assert!(matches!(txn.set(b"", b"value").await, Err(Error::EmptyKey)));
    assert!(matches!(txn.get(b"").await, Err(Error::EmptyKey)));
    assert!(matches!(txn.delete(b"").await, Err(Error::EmptyKey)));
    assert!(matches!(txn.has(b"").await, Err(Error::EmptyKey)));
}

/// Test read-only transaction enforcement
pub async fn test_readonly_transaction<S: Store>(store: &S) {
    let mut txn = store.new_txn(true).await.unwrap();

    assert!(matches!(
        txn.set(b"key", b"value").await,
        Err(Error::ReadOnlyTxn)
    ));
    assert!(matches!(txn.delete(b"key").await, Err(Error::ReadOnlyTxn)));

    // Read operations should work
    assert!(txn.get(b"key").await.is_ok());
    assert!(txn.has(b"key").await.is_ok());
}

/// Test closed store rejection
pub async fn test_closed_store_rejected<S: Store>(store: &S) {
    store.close().await.unwrap();

    assert!(matches!(store.new_txn(false).await, Err(Error::DBClosed)));
    assert!(matches!(store.new_txn(true).await, Err(Error::DBClosed)));
}
