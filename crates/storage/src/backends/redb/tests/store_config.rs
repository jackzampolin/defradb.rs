use super::*;
use tempfile::TempDir;

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
    let store = std::sync::Arc::new(RedbStore::open(temp_dir.path().join("test.redb")).unwrap());

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
