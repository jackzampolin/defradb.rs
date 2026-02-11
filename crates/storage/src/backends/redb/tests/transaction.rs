use super::*;
use tempfile::TempDir;

#[tokio::test]
async fn test_redb_snapshot_isolation() {
    let temp_dir = TempDir::new().unwrap();
    let store = std::sync::Arc::new(RedbStore::open(temp_dir.path().join("test.redb")).unwrap());

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
    let store = std::sync::Arc::new(RedbStore::open(temp_dir.path().join("test.redb")).unwrap());

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
async fn test_redb_drop_all_with_active_transactions() {
    use crate::corekv::Dropable;

    let temp_dir = TempDir::new().unwrap();
    let store = std::sync::Arc::new(RedbStore::open(temp_dir.path().join("test.redb")).unwrap());

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
async fn test_redb_close_timeout_returns_error() {
    let temp_dir = TempDir::new().unwrap();
    let store = std::sync::Arc::new(RedbStore::open(temp_dir.path().join("test.redb")).unwrap());

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
