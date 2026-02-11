use super::*;
use tempfile::TempDir;

#[tokio::test]
async fn test_redb_iterator_prefix_filtering() {
    let temp_dir = TempDir::new().unwrap();
    let store = RedbStore::open(temp_dir.path().join("test.redb")).unwrap();

    // Insert keys with different prefixes
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"user:1", b"alice").await.unwrap();
    txn.set(b"user:2", b"bob").await.unwrap();
    txn.set(b"user:3", b"carol").await.unwrap();
    txn.set(b"doc:1", b"document1").await.unwrap();
    txn.set(b"doc:2", b"document2").await.unwrap();
    txn.commit().await.unwrap();

    // Test prefix iteration
    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new().with_prefix(b"user:".to_vec());
    let mut iter = txn.iterator(opts).await.unwrap();

    let mut count = 0;
    while let Some(kv) = iter.next().await.unwrap() {
        assert!(kv.key.starts_with(b"user:"), "Key should have prefix");
        count += 1;
    }
    assert_eq!(count, 3, "Should have 3 user keys");

    // Test doc prefix
    let opts = IterOptions::new().with_prefix(b"doc:".to_vec());
    let mut iter = txn.iterator(opts).await.unwrap();

    let mut count = 0;
    while let Some(kv) = iter.next().await.unwrap() {
        assert!(kv.key.starts_with(b"doc:"), "Key should have prefix");
        count += 1;
    }
    assert_eq!(count, 2, "Should have 2 doc keys");
}

#[tokio::test]
async fn test_redb_iterator_range_filtering() {
    let temp_dir = TempDir::new().unwrap();
    let store = RedbStore::open(temp_dir.path().join("test.redb")).unwrap();

    // Insert alphabetically ordered keys
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"a", b"1").await.unwrap();
    txn.set(b"b", b"2").await.unwrap();
    txn.set(b"c", b"3").await.unwrap();
    txn.set(b"d", b"4").await.unwrap();
    txn.set(b"e", b"5").await.unwrap();
    txn.commit().await.unwrap();

    // Test range iteration [b, d)
    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new()
        .with_start(b"b".to_vec())
        .with_end(b"d".to_vec());
    let mut iter = txn.iterator(opts).await.unwrap();

    let keys: Vec<_> = {
        let mut keys = vec![];
        while let Some(kv) = iter.next().await.unwrap() {
            keys.push(kv.key);
        }
        keys
    };

    assert_eq!(keys.len(), 2, "Should have keys b and c");
    assert_eq!(keys[0], b"b");
    assert_eq!(keys[1], b"c");
}

#[tokio::test]
async fn test_redb_iterator_reverse() {
    let temp_dir = TempDir::new().unwrap();
    let store = RedbStore::open(temp_dir.path().join("test.redb")).unwrap();

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
        keys.push(kv.key);
    }

    assert_eq!(keys, vec![b"c".to_vec(), b"b".to_vec(), b"a".to_vec()]);
}

#[tokio::test]
async fn test_redb_empty_key_rejected() {
    let temp_dir = TempDir::new().unwrap();
    let store = RedbStore::open(temp_dir.path().join("test.redb")).unwrap();

    let mut txn = store.new_txn(false).await.unwrap();

    // Empty key should be rejected
    let result = txn.set(b"", b"value").await;
    assert!(result.is_err(), "Empty key should be rejected");

    let result = txn.get(b"").await;
    assert!(result.is_err(), "Empty key get should be rejected");

    let result = txn.delete(b"").await;
    assert!(result.is_err(), "Empty key delete should be rejected");
}

#[tokio::test]
async fn test_redb_read_only_txn_rejects_writes() {
    let temp_dir = TempDir::new().unwrap();
    let store = RedbStore::open(temp_dir.path().join("test.redb")).unwrap();

    let mut txn = store.new_txn(true).await.unwrap(); // read-only

    let result = txn.set(b"key", b"value").await;
    assert!(result.is_err(), "Read-only txn should reject set");

    let result = txn.delete(b"key").await;
    assert!(result.is_err(), "Read-only txn should reject delete");
}

#[tokio::test]
async fn test_redb_pending_changes_merged_with_snapshot() {
    let temp_dir = TempDir::new().unwrap();
    let store = RedbStore::open(temp_dir.path().join("test.redb")).unwrap();

    // Setup initial data
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"existing", b"original").await.unwrap();
    txn.commit().await.unwrap();

    // Start a new transaction with pending changes
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"existing", b"modified").await.unwrap();
    txn.set(b"new_key", b"new_value").await.unwrap();

    // Pending changes should be visible
    assert_eq!(
        txn.get(b"existing").await.unwrap(),
        Some(b"modified".to_vec()),
        "Should see pending modification"
    );
    assert_eq!(
        txn.get(b"new_key").await.unwrap(),
        Some(b"new_value".to_vec()),
        "Should see pending new key"
    );

    // Iterator should also merge pending changes
    let opts = IterOptions::new();
    let mut iter = txn.iterator(opts).await.unwrap();
    let mut found_keys = std::collections::HashSet::new();
    while let Some(kv) = iter.next().await.unwrap() {
        found_keys.insert(kv.key);
    }
    assert!(found_keys.contains(&b"existing".to_vec()));
    assert!(found_keys.contains(&b"new_key".to_vec()));
}

#[tokio::test]
async fn test_redb_pending_delete_removes_from_snapshot() {
    let temp_dir = TempDir::new().unwrap();
    let store = RedbStore::open(temp_dir.path().join("test.redb")).unwrap();

    // Setup initial data
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"to_delete", b"value").await.unwrap();
    txn.set(b"to_keep", b"value").await.unwrap();
    txn.commit().await.unwrap();

    // Delete in a new transaction
    let mut txn = store.new_txn(false).await.unwrap();
    txn.delete(b"to_delete").await.unwrap();

    // Deleted key should not be visible
    assert_eq!(
        txn.get(b"to_delete").await.unwrap(),
        None,
        "Deleted key should not be visible"
    );
    assert!(!txn.has(b"to_delete").await.unwrap());

    // Iterator should not include deleted key
    let opts = IterOptions::new();
    let mut iter = txn.iterator(opts).await.unwrap();
    while let Some(kv) = iter.next().await.unwrap() {
        assert_ne!(
            kv.key, b"to_delete",
            "Deleted key should not appear in iterator"
        );
    }
}

#[tokio::test]
async fn test_redb_iterator_seek_on_empty_store() {
    let temp_dir = TempDir::new().unwrap();
    let store = RedbStore::open(temp_dir.path().join("test.redb")).unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new();
    let mut iter = txn.iterator(opts).await.unwrap();

    // Seek on empty store should return false
    let found = iter.seek(b"any_key").await.unwrap();
    assert!(!found, "Seek on empty store should return false");

    // Next should return None
    let item = iter.next().await.unwrap();
    assert!(item.is_none(), "Next on empty store should return None");

    // Reset should succeed
    iter.reset().await.unwrap();
    assert!(
        iter.is_valid(),
        "Iterator should still be valid after reset"
    );
}

#[tokio::test]
async fn test_redb_many_keys_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("test.redb");

    // Write 1000 keys
    {
        let store = RedbStore::open(&path).unwrap();
        let mut txn = store.new_txn(false).await.unwrap();
        for i in 0..1000u32 {
            let key = format!("key_{:05}", i);
            let value = format!("value_{}", i);
            txn.set(key.as_bytes(), value.as_bytes()).await.unwrap();
        }
        txn.commit().await.unwrap();
        store.close().await.unwrap();
    }

    // Reopen and verify all keys
    {
        let store = RedbStore::open(&path).unwrap();
        let txn = store.new_txn(true).await.unwrap();

        // Verify specific keys
        for i in [0, 100, 500, 999].iter() {
            let key = format!("key_{:05}", i);
            let expected = format!("value_{}", i);
            let value = txn.get(key.as_bytes()).await.unwrap();
            assert_eq!(
                value,
                Some(expected.into_bytes()),
                "Key {} should be retrievable after persistence",
                key
            );
        }

        // Verify count via iterator
        let opts = IterOptions::new();
        let mut iter = txn.iterator(opts).await.unwrap();
        let mut count = 0;
        while iter.next().await.unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 1000, "Should have 1000 keys after persistence");
    }
}
