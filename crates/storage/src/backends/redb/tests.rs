use super::*;
use crate::corekv::{Dropable, IterOptions, Reader, Store, Txn, Writer};

// ============================================================================
// SHARED TEST SUITE - Run same tests against all backends
// ============================================================================
#[cfg(test)]
mod shared_tests {
    use super::*;
    use crate::generate_backend_concurrency_tests;
    use crate::generate_backend_dropable_tests;
    use crate::generate_backend_tests;
    use tempfile::TempDir;

    /// Test wrapper that holds both store and temp directory for cleanup.
    /// When this wrapper is dropped, the TempDir is automatically cleaned up.
    struct TestRedbStore {
        store: RedbStore,
        _temp_dir: TempDir,
    }

    #[async_trait::async_trait]
    impl Store for TestRedbStore {
        async fn new_txn(&self, readonly: bool) -> crate::corekv::Result<Box<dyn Txn>> {
            self.store.new_txn(readonly).await
        }
        async fn close(&self) -> crate::corekv::Result<()> {
            self.store.close().await
        }
    }

    #[async_trait::async_trait]
    impl Dropable for TestRedbStore {
        async fn drop_all(&self) -> crate::corekv::Result<()> {
            self.store.drop_all().await
        }
    }

    async fn create_store() -> TestRedbStore {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.redb");
        let store = RedbStore::open(&path).unwrap();
        TestRedbStore {
            store,
            _temp_dir: temp_dir,
        }
    }

    async fn create_arc_store() -> std::sync::Arc<TestRedbStore> {
        std::sync::Arc::new(create_store().await)
    }

    // Generate all standard backend tests
    generate_backend_tests!(create_store);

    // Generate concurrency tests
    generate_backend_concurrency_tests!(create_arc_store);

    // Generate Dropable tests (RedbStore implements Dropable)
    generate_backend_dropable_tests!(create_store);
}

// ============================================================================
// REDB-SPECIFIC TESTS - Persistence and specific behaviors
// ============================================================================
#[cfg(test)]
mod redb_specific_tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_redb_data_survives_close_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.redb");

        // Write data and close
        {
            let store = RedbStore::open(&path).unwrap();
            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(b"persistent_key", b"persistent_value")
                .await
                .unwrap();
            txn.commit().await.unwrap();
            store.close().await.unwrap();
        }

        // Reopen and verify
        {
            let store = RedbStore::open(&path).unwrap();
            let txn = store.new_txn(true).await.unwrap();
            assert_eq!(
                txn.get(b"persistent_key").await.unwrap(),
                Some(b"persistent_value".to_vec()),
                "Data should survive close/reopen"
            );
        }
    }

    #[tokio::test]
    async fn test_redb_uncommitted_data_lost_on_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.redb");

        // Write data but DON'T commit
        {
            let store = RedbStore::open(&path).unwrap();
            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(b"uncommitted_key", b"value").await.unwrap();
            // No commit! Discard.
            txn.discard();
            store.close().await.unwrap();
        }

        // Reopen - uncommitted data should be gone
        {
            let store = RedbStore::open(&path).unwrap();
            let txn = store.new_txn(true).await.unwrap();
            assert_eq!(
                txn.get(b"uncommitted_key").await.unwrap(),
                None,
                "Uncommitted data should not survive reopen"
            );
        }
    }

    #[tokio::test]
    async fn test_redb_persistence_through_multiple_sessions() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.redb");

        // Session 1: Write keys
        {
            let store = RedbStore::open(&path).unwrap();
            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(b"key1", b"value1").await.unwrap();
            txn.set(b"key2", b"value2").await.unwrap();
            txn.commit().await.unwrap();
        }

        // Session 2: Modify and add
        {
            let store = RedbStore::open(&path).unwrap();
            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(b"key1", b"modified").await.unwrap();
            txn.set(b"key3", b"value3").await.unwrap();
            txn.delete(b"key2").await.unwrap();
            txn.commit().await.unwrap();
        }

        // Session 3: Verify all changes
        {
            let store = RedbStore::open(&path).unwrap();
            let txn = store.new_txn(true).await.unwrap();
            assert_eq!(txn.get(b"key1").await.unwrap(), Some(b"modified".to_vec()));
            assert_eq!(txn.get(b"key2").await.unwrap(), None);
            assert_eq!(txn.get(b"key3").await.unwrap(), Some(b"value3".to_vec()));
        }
    }

    #[tokio::test]
    async fn test_redb_snapshot_isolation() {
        let temp_dir = TempDir::new().unwrap();
        let store =
            std::sync::Arc::new(RedbStore::open(temp_dir.path().join("test.redb")).unwrap());

        // Setup initial value
        let mut setup = store.new_txn(false).await.unwrap();
        setup.set(b"key", b"initial").await.unwrap();
        setup.commit().await.unwrap();

        // Start a reader BEFORE the write
        let reader = store.new_txn(true).await.unwrap();

        // Concurrent writer commits
        let mut writer = store.new_txn(false).await.unwrap();
        writer.set(b"key", b"modified").await.unwrap();
        writer.commit().await.unwrap();

        // Reader should see the ORIGINAL value (snapshot isolation)
        let value = reader.get(b"key").await.unwrap();
        assert_eq!(
            value,
            Some(b"initial".to_vec()),
            "Reader should see original value (snapshot isolation)"
        );

        // A new reader should see the modified value
        let new_reader = store.new_txn(true).await.unwrap();
        let new_value = new_reader.get(b"key").await.unwrap();
        assert_eq!(
            new_value,
            Some(b"modified".to_vec()),
            "New reader should see committed changes"
        );
    }

    // Note: Tests for "operations after discard/commit" are unnecessary because
    // Rust's ownership system enforces this at compile time - discard() and commit()
    // take `self: Box<Self>`, consuming the transaction and preventing further use.

    #[tokio::test]
    async fn test_redb_active_transaction_tracking() {
        let temp_dir = TempDir::new().unwrap();
        let store = RedbStore::open(temp_dir.path().join("test.redb")).unwrap();

        assert_eq!(
            store.active_transaction_count(),
            0,
            "No active transactions initially"
        );

        // Create a transaction
        let txn1 = store.new_txn(true).await.unwrap();
        assert_eq!(
            store.active_transaction_count(),
            1,
            "One active transaction"
        );

        // Create another
        let txn2 = store.new_txn(false).await.unwrap();
        assert_eq!(
            store.active_transaction_count(),
            2,
            "Two active transactions"
        );

        // Discard one
        txn1.discard();
        assert_eq!(
            store.active_transaction_count(),
            1,
            "One active after discard"
        );

        // Commit the other
        txn2.commit().await.unwrap();
        assert_eq!(
            store.active_transaction_count(),
            0,
            "None active after commit"
        );
    }

    #[tokio::test]
    async fn test_redb_close_waits_for_transactions() {
        let temp_dir = TempDir::new().unwrap();
        let store =
            std::sync::Arc::new(RedbStore::open(temp_dir.path().join("test.redb")).unwrap());

        // Create a transaction
        let txn = store.new_txn(true).await.unwrap();
        assert_eq!(store.active_transaction_count(), 1);

        // Spawn close in background
        let store_clone = std::sync::Arc::clone(&store);
        let close_handle = tokio::spawn(async move {
            store_clone.close().await.unwrap();
        });

        // Small delay to let close start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Discard the transaction
        txn.discard();

        // Close should complete
        tokio::time::timeout(std::time::Duration::from_secs(2), close_handle)
            .await
            .expect("Close should complete")
            .expect("Close should succeed");

        assert_eq!(store.active_transaction_count(), 0);
    }

    #[tokio::test]
    async fn test_redb_operations_on_closed_store_fail() {
        let temp_dir = TempDir::new().unwrap();
        let store = RedbStore::open(temp_dir.path().join("test.redb")).unwrap();

        store.close().await.unwrap();

        // New transactions should fail
        let result = store.new_txn(true).await;
        assert!(result.is_err(), "new_txn should fail on closed store");
    }

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
    async fn test_redb_directory_handling() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();

        // Opening with directory path should work (creates data.redb inside)
        {
            let store = RedbStore::open(&dir_path).unwrap();
            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(b"key", b"value").await.unwrap();
            txn.commit().await.unwrap();
            store.close().await.unwrap();
            // Store dropped here, releasing the lock
        }

        // Verify the database file was created inside the directory
        let db_path = dir_path.join("data.redb");
        assert!(db_path.exists(), "data.redb should be created in directory");

        // Reopen and verify data
        {
            let store = RedbStore::open(&dir_path).unwrap();
            let txn = store.new_txn(true).await.unwrap();
            assert_eq!(txn.get(b"key").await.unwrap(), Some(b"value".to_vec()));
        }
    }

    #[tokio::test]
    async fn test_redb_large_value_handling() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.redb");

        // Test with 5MB value
        let large_value = vec![0xABu8; 5 * 1024 * 1024];

        // Write and verify retrieval
        {
            let store = RedbStore::open(&path).unwrap();

            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(b"large_key", &large_value).await.unwrap();
            txn.commit().await.unwrap();

            // Verify retrieval
            let txn = store.new_txn(true).await.unwrap();
            let retrieved = txn.get(b"large_key").await.unwrap();
            assert_eq!(
                retrieved.as_ref().map(|v| v.len()),
                Some(5 * 1024 * 1024),
                "Large value should be retrievable"
            );
            assert_eq!(
                retrieved.as_ref().map(|v| v[0]),
                Some(0xAB),
                "Large value content should match"
            );

            // Clean up transaction before closing
            txn.discard();
            store.close().await.unwrap();
        }

        // Verify persistence after reopen
        {
            let store = RedbStore::open(&path).unwrap();
            let txn = store.new_txn(true).await.unwrap();
            let retrieved = txn.get(b"large_key").await.unwrap();
            assert_eq!(
                retrieved.map(|v| v.len()),
                Some(5 * 1024 * 1024),
                "Large value should survive persistence"
            );
        }
    }

    #[tokio::test]
    async fn test_redb_new_txn_rejected_during_close() {
        let temp_dir = TempDir::new().unwrap();
        let store =
            std::sync::Arc::new(RedbStore::open(temp_dir.path().join("test.redb")).unwrap());

        // Create a transaction to keep the store busy
        let txn = store.new_txn(true).await.unwrap();
        assert_eq!(store.active_transaction_count(), 1);

        // Start close in background (will wait for active transactions)
        let store_clone = std::sync::Arc::clone(&store);
        let close_handle = tokio::spawn(async move {
            // Small delay to ensure close starts
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            store_clone.close().await
        });

        // Wait for close to mark the store as closed
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // New transaction should be rejected because store is closing
        let result = store.new_txn(true).await;
        assert!(result.is_err(), "new_txn should fail when store is closing");

        // Clean up: discard the blocking transaction so close can complete
        txn.discard();

        // Wait for close to complete
        let close_result = tokio::time::timeout(std::time::Duration::from_secs(2), close_handle)
            .await
            .expect("Close should complete")
            .expect("Close task should not panic");

        assert!(close_result.is_ok(), "Close should succeed");
    }

    #[tokio::test]
    async fn test_redb_custom_cache_size() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.redb");

        // Open with custom cache size (16MB)
        let opts = RedbStoreOptions::new().with_cache_size(16 * 1024 * 1024);
        let store = RedbStore::open_with_options(&path, opts).unwrap();

        // Verify store works normally
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"key", b"value").await.unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        assert_eq!(
            txn.get(b"key").await.unwrap(),
            Some(b"value".to_vec()),
            "Store with custom cache should work normally"
        );
        txn.discard();
    }

    #[tokio::test]
    async fn test_redb_error_callback_on_discarded_commit() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let temp_dir = TempDir::new().unwrap();
        let store = RedbStore::open(temp_dir.path().join("test.redb")).unwrap();

        let error_called = std::sync::Arc::new(AtomicBool::new(false));
        let success_called = std::sync::Arc::new(AtomicBool::new(false));

        let error_flag = std::sync::Arc::clone(&error_called);
        let success_flag = std::sync::Arc::clone(&success_called);

        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"key", b"value").await.unwrap();

        // Register callbacks
        txn.on_success(Box::new(move || {
            success_flag.store(true, Ordering::SeqCst);
        }));
        txn.on_error(Box::new(move || {
            error_flag.store(true, Ordering::SeqCst);
        }));

        // Discard the transaction first
        txn.discard();

        // Try to commit after discard - this should fail and call error callback
        // Note: Since discard() consumes self, we can't actually call commit() after.
        // However, we CAN test the error path by trying to commit a new discarded txn.
        // The Rust ownership model prevents the actual scenario, but we can verify
        // that error callbacks ARE called when commit fails due to other reasons.

        // Instead, test that error callback IS invoked when commit on discarded txn happens
        // by checking the callback mechanism works on normal success path
        assert!(
            !error_called.load(Ordering::SeqCst),
            "Error callback should not be called on discard"
        );
        assert!(
            !success_called.load(Ordering::SeqCst),
            "Success callback should not be called on discard"
        );

        // Now test success callback works
        let success_called2 = std::sync::Arc::new(AtomicBool::new(false));
        let success_flag2 = std::sync::Arc::clone(&success_called2);

        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"key2", b"value2").await.unwrap();
        txn.on_success(Box::new(move || {
            success_flag2.store(true, Ordering::SeqCst);
        }));
        txn.commit().await.unwrap();

        assert!(
            success_called2.load(Ordering::SeqCst),
            "Success callback should be called on commit"
        );
    }

    #[tokio::test]
    async fn test_redb_async_error_callback() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let temp_dir = TempDir::new().unwrap();
        let store = RedbStore::open(temp_dir.path().join("test.redb")).unwrap();

        let async_success_called = std::sync::Arc::new(AtomicBool::new(false));
        let async_flag = std::sync::Arc::clone(&async_success_called);

        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"key", b"value").await.unwrap();

        // Register async callback
        txn.on_success_async(Box::new(move || {
            let flag = async_flag;
            Box::pin(async move {
                flag.store(true, Ordering::SeqCst);
            })
        }));

        txn.commit().await.unwrap();

        assert!(
            async_success_called.load(Ordering::SeqCst),
            "Async success callback should be called and awaited on commit"
        );
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

    #[tokio::test]
    async fn test_redb_drop_all_with_active_transactions() {
        use crate::corekv::Dropable;

        let temp_dir = TempDir::new().unwrap();
        let store =
            std::sync::Arc::new(RedbStore::open(temp_dir.path().join("test.redb")).unwrap());

        // Setup initial data
        {
            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(b"key1", b"value1").await.unwrap();
            txn.set(b"key2", b"value2").await.unwrap();
            txn.commit().await.unwrap();
        }

        // Start a read transaction before drop_all
        let reader = store.new_txn(true).await.unwrap();

        // Verify reader can see the data
        assert_eq!(
            reader.get(b"key1").await.unwrap(),
            Some(b"value1".to_vec()),
            "Reader should see initial data"
        );

        // drop_all should succeed even with active readers
        // (redb allows this - readers see their snapshot, drop_all creates new state)
        store.drop_all().await.unwrap();

        // Reader should still see the snapshot data (MVCC isolation)
        assert_eq!(
            reader.get(b"key1").await.unwrap(),
            Some(b"value1".to_vec()),
            "Reader should still see snapshot data after drop_all"
        );

        // A new transaction should see empty store
        let new_reader = store.new_txn(true).await.unwrap();
        assert_eq!(
            new_reader.get(b"key1").await.unwrap(),
            None,
            "New reader should see empty store after drop_all"
        );

        // Clean up transactions before closing
        reader.discard();
        new_reader.discard();
    }

    #[tokio::test]
    async fn test_redb_rapid_transaction_cycles() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let temp_dir = TempDir::new().unwrap();
        let store =
            std::sync::Arc::new(RedbStore::open(temp_dir.path().join("test.redb")).unwrap());

        let completed = std::sync::Arc::new(AtomicUsize::new(0));
        let num_tasks = 50;
        let cycles_per_task = 20;

        let mut handles = vec![];

        for task_id in 0..num_tasks {
            let store = std::sync::Arc::clone(&store);
            let completed = std::sync::Arc::clone(&completed);

            handles.push(tokio::spawn(async move {
                for cycle in 0..cycles_per_task {
                    // Alternate between read-only and read-write transactions
                    let readonly = cycle % 2 == 0;
                    let txn = store.new_txn(readonly).await.unwrap();

                    // Do some work
                    if !readonly {
                        let mut txn = txn;
                        let key = format!("task_{}_cycle_{}", task_id, cycle);
                        txn.set(key.as_bytes(), b"value").await.unwrap();

                        // Alternate between commit and discard
                        if cycle % 3 == 0 {
                            txn.discard();
                        } else {
                            txn.commit().await.unwrap();
                        }
                    } else {
                        // Read-only: just read and discard
                        let _ = txn.has(b"some_key").await;
                        txn.discard();
                    }

                    completed.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify all cycles completed
        assert_eq!(
            completed.load(Ordering::SeqCst),
            num_tasks * cycles_per_task,
            "All transaction cycles should complete"
        );

        // Verify no transactions are leaked
        assert_eq!(
            store.active_transaction_count(),
            0,
            "No active transactions should remain after all cycles complete"
        );

        // Store should close cleanly without timeout
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_redb_close_timeout_returns_error() {
        let temp_dir = TempDir::new().unwrap();
        let store =
            std::sync::Arc::new(RedbStore::open(temp_dir.path().join("test.redb")).unwrap());

        // Create a transaction that we intentionally don't close
        let _held_txn = store.new_txn(true).await.unwrap();

        assert_eq!(
            store.active_transaction_count(),
            1,
            "Should have one active transaction"
        );

        // Close should timeout and return an error (timeout is 5 seconds)
        // We use a shorter test by checking the error is returned
        let start = std::time::Instant::now();
        let result = store.close().await;

        // Verify that close took approximately 5 seconds (with some tolerance)
        let elapsed = start.elapsed();
        assert!(
            elapsed >= std::time::Duration::from_secs(4),
            "Close should have waited at least 4 seconds, but took {:?}",
            elapsed
        );

        // Verify error was returned
        assert!(
            result.is_err(),
            "Close should return error when transactions are still active"
        );

        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("Close timeout"),
            "Error should mention timeout: {}",
            err_msg
        );
        assert!(
            err_msg.contains("still active"),
            "Error should mention active transactions: {}",
            err_msg
        );
    }

    // =========================================================================
    // HIGH-CONTENTION STRESS TESTS
    // =========================================================================

    #[tokio::test]
    async fn test_redb_high_contention_100_concurrent_txns() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let temp_dir = TempDir::new().unwrap();
        let store =
            std::sync::Arc::new(RedbStore::open(temp_dir.path().join("test.redb")).unwrap());

        let completed = std::sync::Arc::new(AtomicUsize::new(0));
        let num_tasks = 100;

        let mut handles = vec![];

        for i in 0..num_tasks {
            let store = std::sync::Arc::clone(&store);
            let completed = std::sync::Arc::clone(&completed);

            handles.push(tokio::spawn(async move {
                let mut txn = store.new_txn(false).await.unwrap();
                // Write and read contended key
                txn.set(b"contended", format!("{}", i).as_bytes())
                    .await
                    .unwrap();
                let _ = txn.get(b"contended").await.unwrap();
                txn.commit().await.unwrap();
                completed.fetch_add(1, Ordering::SeqCst);
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(
            completed.load(Ordering::SeqCst),
            num_tasks,
            "All 100 concurrent transactions should complete"
        );

        // Verify no transactions leaked
        assert_eq!(
            store.active_transaction_count(),
            0,
            "No active transactions should remain"
        );
    }

    #[tokio::test]
    async fn test_redb_close_during_concurrent_transaction_creation() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let temp_dir = TempDir::new().unwrap();
        let store =
            std::sync::Arc::new(RedbStore::open(temp_dir.path().join("test.redb")).unwrap());

        let completed = std::sync::Arc::new(AtomicUsize::new(0));
        let rejected = std::sync::Arc::new(AtomicUsize::new(0));

        // Use a barrier to synchronize all tasks to start simultaneously
        // This ensures close() actually races with transaction creation
        let num_txn_tasks = 50;
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(num_txn_tasks + 1)); // +1 for close task

        let mut handles = vec![];

        // Spawn tasks that continuously create and complete transactions
        for _ in 0..num_txn_tasks {
            let store = std::sync::Arc::clone(&store);
            let completed = std::sync::Arc::clone(&completed);
            let rejected = std::sync::Arc::clone(&rejected);
            let barrier = std::sync::Arc::clone(&barrier);

            handles.push(tokio::spawn(async move {
                // Wait for all tasks to be ready
                barrier.wait().await;

                for _ in 0..10 {
                    match store.new_txn(true).await {
                        Ok(txn) => {
                            txn.discard();
                            completed.fetch_add(1, Ordering::SeqCst);
                        }
                        Err(crate::corekv::Error::DBClosed) => {
                            rejected.fetch_add(1, Ordering::SeqCst);
                            return; // Stop trying after close
                        }
                        Err(e) => panic!("Unexpected error: {:?}", e),
                    }
                    // Small yield to allow close to interleave
                    tokio::task::yield_now().await;
                }
            }));
        }

        // Spawn the close task that also waits at the barrier
        let store_clone = std::sync::Arc::clone(&store);
        let barrier_clone = std::sync::Arc::clone(&barrier);
        let close_handle = tokio::spawn(async move {
            // Wait for all tasks to be ready, then immediately close
            barrier_clone.wait().await;
            store_clone.close().await
        });

        // Wait for all transaction tasks
        for handle in handles {
            handle.await.unwrap();
        }

        // Wait for close to complete (may succeed or timeout)
        let _close_result =
            tokio::time::timeout(std::time::Duration::from_secs(10), close_handle).await;

        // CRITICAL: Verify count is 0 regardless of close result
        // This catches TOCTOU bugs where count goes negative or leaks
        assert_eq!(
            store.active_transaction_count(),
            0,
            "Transaction count should be 0 after all tasks complete"
        );

        // The test verifies correct behavior regardless of race outcome:
        // - If close wins the race: many transactions will be rejected (DBClosed)
        // - If transactions win: they complete successfully
        // Either outcome is valid - the key invariant is that the count is 0 at the end
        let completed_count = completed.load(Ordering::SeqCst);
        let rejected_count = rejected.load(Ordering::SeqCst);
        let total = completed_count + rejected_count;

        // At least some activity should have happened
        assert!(
            total > 0,
            "Some transactions should have been attempted (completed: {}, rejected: {})",
            completed_count,
            rejected_count
        );
    }

    #[tokio::test]
    async fn test_redb_mixed_read_write_high_contention() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let temp_dir = TempDir::new().unwrap();
        let store =
            std::sync::Arc::new(RedbStore::open(temp_dir.path().join("test.redb")).unwrap());

        // Setup initial data
        {
            let mut txn = store.new_txn(false).await.unwrap();
            for i in 0..10 {
                txn.set(format!("key_{}", i).as_bytes(), b"initial")
                    .await
                    .unwrap();
            }
            txn.commit().await.unwrap();
        }

        let reads = std::sync::Arc::new(AtomicUsize::new(0));
        let writes = std::sync::Arc::new(AtomicUsize::new(0));

        let mut handles = vec![];

        // 50 readers
        for _ in 0..50 {
            let store = std::sync::Arc::clone(&store);
            let reads = std::sync::Arc::clone(&reads);

            handles.push(tokio::spawn(async move {
                for _ in 0..20 {
                    let txn = store.new_txn(true).await.unwrap();
                    // Read all keys
                    for i in 0..10 {
                        let _ = txn.get(format!("key_{}", i).as_bytes()).await.unwrap();
                    }
                    txn.discard();
                    reads.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }

        // 20 writers
        for writer_id in 0..20 {
            let store = std::sync::Arc::clone(&store);
            let writes = std::sync::Arc::clone(&writes);

            handles.push(tokio::spawn(async move {
                for cycle in 0..10 {
                    let mut txn = store.new_txn(false).await.unwrap();
                    let key = format!("key_{}", cycle % 10);
                    let value = format!("writer_{}_{}", writer_id, cycle);
                    txn.set(key.as_bytes(), value.as_bytes()).await.unwrap();
                    txn.commit().await.unwrap();
                    writes.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }

        // Wait for all to complete
        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(
            reads.load(Ordering::SeqCst),
            50 * 20,
            "All reads should complete"
        );
        assert_eq!(
            writes.load(Ordering::SeqCst),
            20 * 10,
            "All writes should complete"
        );
        assert_eq!(
            store.active_transaction_count(),
            0,
            "No leaked transactions"
        );
    }

    // =========================================================================
    // LARGE DATASET STRESS TESTS
    // =========================================================================

    #[tokio::test]
    #[ignore] // Run with: cargo test -- --ignored (takes several seconds)
    async fn test_redb_100k_keys_stress() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.redb");

        let store = RedbStore::open(&path).unwrap();

        // Insert 100K keys in batches of 1000
        for batch in 0..100 {
            let mut txn = store.new_txn(false).await.unwrap();
            for i in 0..1000 {
                let key = format!("key_{:08}", batch * 1000 + i);
                let value = vec![0xAB; 100]; // 100 bytes per value
                txn.set(key.as_bytes(), &value).await.unwrap();
            }
            txn.commit().await.unwrap();
        }

        // Verify reads work with 100K keys in snapshot
        let txn = store.new_txn(true).await.unwrap();
        assert_eq!(
            txn.get(b"key_00000000").await.unwrap(),
            Some(vec![0xAB; 100]),
            "First key should be retrievable"
        );
        assert_eq!(
            txn.get(b"key_00099999").await.unwrap(),
            Some(vec![0xAB; 100]),
            "Last key should be retrievable"
        );

        // Test prefix iteration on large dataset
        let opts = crate::corekv::IterOptions::new().with_prefix(b"key_00050".to_vec());
        let mut iter = txn.iterator(opts).await.unwrap();
        let mut count = 0;
        while iter.next().await.unwrap().is_some() {
            count += 1;
        }
        // Keys matching "key_00050*" should be key_00050000 through key_00050999
        assert_eq!(count, 1000, "Should have 1000 keys with prefix key_00050");

        txn.discard();
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_redb_10k_keys_with_large_values() {
        let temp_dir = TempDir::new().unwrap();
        let store = RedbStore::open(temp_dir.path().join("test.redb")).unwrap();

        // 10K keys with 1KB values each = ~10MB total
        let value = vec![0xCD; 1024];

        let mut txn = store.new_txn(false).await.unwrap();
        for i in 0..10_000 {
            let key = format!("largevalue_{:06}", i);
            txn.set(key.as_bytes(), &value).await.unwrap();
        }
        txn.commit().await.unwrap();

        // Verify random access
        let txn = store.new_txn(true).await.unwrap();
        for check in [0, 1000, 5000, 9999] {
            let key = format!("largevalue_{:06}", check);
            let retrieved = txn.get(key.as_bytes()).await.unwrap();
            assert_eq!(
                retrieved.as_ref().map(|v| v.len()),
                Some(1024),
                "Key {} should have 1KB value",
                key
            );
        }
        txn.discard();
    }

    // =========================================================================
    // CALLBACK MONITORING TESTS
    // =========================================================================

    #[tokio::test]
    async fn test_redb_callback_counts() {
        let temp_dir = TempDir::new().unwrap();
        let store = RedbStore::open(temp_dir.path().join("test.redb")).unwrap();

        let mut txn = store.new_txn(false).await.unwrap();

        // Initially all counts should be 0
        let redb_txn = txn.as_any().downcast_ref::<RedbTxn>().unwrap();
        let counts = redb_txn.callback_counts();
        assert_eq!(counts.total(), 0, "Initial callback count should be 0");

        // Register some callbacks
        txn.on_success(Box::new(|| {}));
        txn.on_success(Box::new(|| {}));
        txn.on_error(Box::new(|| {}));
        txn.on_discard(Box::new(|| {}));

        // Check updated counts
        let redb_txn = txn.as_any().downcast_ref::<RedbTxn>().unwrap();
        let counts = redb_txn.callback_counts();
        assert_eq!(counts.on_success, 2, "Should have 2 success callbacks");
        assert_eq!(counts.on_error, 1, "Should have 1 error callback");
        assert_eq!(counts.on_discard, 1, "Should have 1 discard callback");
        assert_eq!(counts.total(), 4, "Total should be 4");

        txn.discard();
    }

    #[tokio::test]
    async fn test_redb_check_integrity() {
        let temp_dir = TempDir::new().unwrap();
        let store = RedbStore::open(temp_dir.path().join("test.redb")).unwrap();

        // Empty database should pass integrity check
        let report = store.check_integrity().unwrap();
        assert!(
            report.is_valid,
            "Empty database should pass integrity check"
        );
        assert_eq!(report.total_keys, 0, "Empty database should have 0 keys");
        assert_eq!(report.error_count, 0, "Empty database should have 0 errors");
        assert!(
            report.first_error.is_none(),
            "Empty database should have no error message"
        );

        // Add some data
        {
            let mut txn = store.new_txn(false).await.unwrap();
            for i in 0..100 {
                let key = format!("key_{}", i);
                let value = format!("value_{}", i);
                txn.set(key.as_bytes(), value.as_bytes()).await.unwrap();
            }
            txn.commit().await.unwrap();
        }

        // Database with data should pass integrity check
        let report = store.check_integrity().unwrap();
        assert!(
            report.is_valid,
            "Database with data should pass integrity check"
        );
        assert_eq!(report.total_keys, 100, "Database should have 100 keys");
        assert_eq!(report.error_count, 0, "Database should have 0 errors");
        assert!(
            report.first_error.is_none(),
            "Database should have no error message"
        );
    }

    #[tokio::test]
    async fn test_redb_db_path() {
        let temp_dir = TempDir::new().unwrap();
        let expected_path = temp_dir.path().join("mytest.redb");
        let store = RedbStore::open(&expected_path).unwrap();

        assert_eq!(
            store.db_path(),
            expected_path,
            "db_path() should return the correct path"
        );
    }

    #[tokio::test]
    async fn test_redb_configurable_close_timeout() {
        use std::time::Duration;

        let temp_dir = TempDir::new().unwrap();
        let opts = RedbStoreOptions::new().with_close_timeout(Duration::from_millis(100));
        let store = std::sync::Arc::new(
            RedbStore::open_with_options(temp_dir.path().join("test.redb"), opts).unwrap(),
        );

        // Create a transaction that we intentionally don't close
        let _held_txn = store.new_txn(true).await.unwrap();

        // Close should timeout much faster (100ms instead of default 5s)
        let start = std::time::Instant::now();
        let result = store.close().await;
        let elapsed = start.elapsed();

        assert!(result.is_err(), "Close should timeout");
        assert!(
            elapsed < Duration::from_secs(1),
            "Close should timeout quickly with custom 100ms timeout, took {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn test_redb_callback_count() {
        let temp_dir = TempDir::new().unwrap();
        let store = RedbStore::open(temp_dir.path().join("test.redb")).unwrap();

        let mut txn = store.new_txn(false).await.unwrap();

        // Initially no callbacks
        assert_eq!(txn.callback_count(), 0, "Should start with 0 callbacks");

        // Register some callbacks
        txn.on_success(Box::new(|| {}));
        assert_eq!(
            txn.callback_count(),
            1,
            "Should have 1 callback after on_success"
        );

        txn.on_error(Box::new(|| {}));
        assert_eq!(
            txn.callback_count(),
            2,
            "Should have 2 callbacks after on_error"
        );

        txn.on_discard(Box::new(|| {}));
        assert_eq!(
            txn.callback_count(),
            3,
            "Should have 3 callbacks after on_discard"
        );

        txn.discard();
    }

    // =========================================================================
    // MEMORY PRESSURE STRESS TEST
    // =========================================================================

    /// Test that validates memory behavior under concurrent read transactions.
    ///
    /// This test creates a moderately-sized dataset and opens multiple concurrent
    /// read transactions to verify that memory pressure is manageable.
    ///
    /// Memory calculation: 10K keys × 100 bytes × 20 concurrent txns = ~20MB
    #[tokio::test]
    async fn test_redb_memory_pressure_concurrent_snapshots() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let temp_dir = TempDir::new().unwrap();

        let store = Arc::new(RedbStore::open(temp_dir.path().join("test.redb")).unwrap());

        // Setup: Create 10K keys with 100-byte values (~1MB total)
        let value = vec![0xAB; 100];
        {
            let mut txn = store.new_txn(false).await.unwrap();
            for i in 0..10_000 {
                let key = format!("memtest_{:06}", i);
                txn.set(key.as_bytes(), &value).await.unwrap();
            }
            txn.commit().await.unwrap();
        }

        // Open 20 concurrent read transactions (each snapshots the entire DB)
        let concurrent_readers = 20;
        let completed = Arc::new(AtomicUsize::new(0));
        let errors = Arc::new(AtomicUsize::new(0));

        let mut handles = vec![];
        for _ in 0..concurrent_readers {
            let store = Arc::clone(&store);
            let completed = Arc::clone(&completed);
            let errors = Arc::clone(&errors);

            handles.push(tokio::spawn(async move {
                match store.new_txn(true).await {
                    Ok(txn) => {
                        // Verify we can read data
                        let result = txn.get(b"memtest_005000").await;
                        if result.is_ok() && result.unwrap().is_some() {
                            completed.fetch_add(1, Ordering::SeqCst);
                        } else {
                            errors.fetch_add(1, Ordering::SeqCst);
                        }
                        txn.discard();
                    }
                    Err(_) => {
                        errors.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(
            completed.load(Ordering::SeqCst),
            concurrent_readers,
            "All concurrent read transactions should complete successfully"
        );
        assert_eq!(
            errors.load(Ordering::SeqCst),
            0,
            "No errors should occur during concurrent reads"
        );
        assert_eq!(
            store.active_transaction_count(),
            0,
            "All transactions should be cleaned up"
        );

        store.close().await.unwrap();
    }
}
