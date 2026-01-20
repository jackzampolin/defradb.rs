//! Tests for DbTxn struct.

use std::sync::Arc;

use datastore::BasicTxn;
use db::txn::DbTxn;
use db::Error;
use storage::backends::MemoryStore;

#[tokio::test]
async fn test_db_txn_basic() {
    let store = Arc::new(MemoryStore::new());
    let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
    let txn = DbTxn::new(basic_txn, store.clone());

    assert_eq!(txn.id().unwrap(), 1);
    assert!(!txn.is_readonly().unwrap());
    assert!(!txn.is_explicit());
}

#[tokio::test]
async fn test_db_txn_explicit() {
    let store = Arc::new(MemoryStore::new());
    let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
    let txn = DbTxn::new_explicit(basic_txn, store.clone());

    assert!(txn.is_explicit());
}

#[tokio::test]
async fn test_db_txn_make_explicit() {
    let store = Arc::new(MemoryStore::new());
    let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
    let mut txn = DbTxn::new(basic_txn, store.clone());

    assert!(!txn.is_explicit());
    txn.make_explicit();
    assert!(txn.is_explicit());
}

#[tokio::test]
async fn test_db_txn_write_and_commit() {
    let store = Arc::new(MemoryStore::new());
    let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
    let txn = DbTxn::new(basic_txn, store.clone());

    // Write data
    txn.datastore()
        .unwrap()
        .set(b"key", b"value")
        .await
        .unwrap();

    // Commit
    txn.commit().await.unwrap();

    // Verify data persisted
    let basic_txn = BasicTxn::new(&*store, 2, true).await.unwrap();
    let txn = DbTxn::new(basic_txn, store.clone());
    let value = txn.datastore().unwrap().get(b"key").await.unwrap();
    assert_eq!(value, Some(b"value".to_vec()));
}

#[tokio::test]
async fn test_db_txn_write_and_discard() {
    let store = Arc::new(MemoryStore::new());
    let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
    let txn = DbTxn::new(basic_txn, store.clone());

    // Write data
    txn.datastore()
        .unwrap()
        .set(b"key", b"value")
        .await
        .unwrap();

    // Discard
    txn.discard().unwrap();

    // Verify data NOT persisted
    let basic_txn = BasicTxn::new(&*store, 2, true).await.unwrap();
    let txn = DbTxn::new(basic_txn, store.clone());
    let value = txn.datastore().unwrap().get(b"key").await.unwrap();
    assert_eq!(value, None);
}

#[tokio::test]
async fn test_db_txn_force_commit() {
    let store = Arc::new(MemoryStore::new());
    let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
    let txn = DbTxn::new_explicit(basic_txn, store.clone());

    // Write data
    txn.datastore()
        .unwrap()
        .set(b"key", b"value")
        .await
        .unwrap();

    // Force commit even though explicit
    txn.force_commit().await.unwrap();

    // Verify data persisted
    let basic_txn = BasicTxn::new(&*store, 2, true).await.unwrap();
    let txn = DbTxn::new(basic_txn, store.clone());
    let value = txn.datastore().unwrap().get(b"key").await.unwrap();
    assert_eq!(value, Some(b"value".to_vec()));
}

// Negative tests for error conditions

#[tokio::test]
async fn test_db_txn_explicit_commit_returns_error() {
    let store = Arc::new(MemoryStore::new());
    let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
    let txn = DbTxn::new_explicit(basic_txn, store.clone());

    // Commit on explicit transaction should return error
    let result = txn.commit().await;
    assert!(matches!(result, Err(Error::ExplicitTxnMustUseForce)));
}

#[tokio::test]
async fn test_db_txn_explicit_discard_returns_error() {
    let store = Arc::new(MemoryStore::new());
    let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
    let txn = DbTxn::new_explicit(basic_txn, store.clone());

    // Discard on explicit transaction should return error
    let result = txn.discard();
    assert!(matches!(result, Err(Error::ExplicitTxnMustUseForce)));
}

#[tokio::test]
async fn test_db_txn_force_discard() {
    let store = Arc::new(MemoryStore::new());
    let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
    let txn = DbTxn::new_explicit(basic_txn, store.clone());

    // Write data
    txn.datastore()
        .unwrap()
        .set(b"key", b"value")
        .await
        .unwrap();

    // Force discard even though explicit
    txn.force_discard().unwrap();

    // Verify data NOT persisted
    let basic_txn = BasicTxn::new(&*store, 2, true).await.unwrap();
    let txn = DbTxn::new(basic_txn, store.clone());
    let value = txn.datastore().unwrap().get(b"key").await.unwrap();
    assert_eq!(value, None);
}

#[tokio::test]
async fn test_db_txn_accessor_returns_all_stores() {
    let store = Arc::new(MemoryStore::new());
    let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
    let txn = DbTxn::new(basic_txn, store.clone());

    // All accessor methods should succeed on active transaction
    assert!(txn.blockstore().is_ok());
    assert!(txn.datastore().is_ok());
    assert!(txn.encstore().is_ok());
    assert!(txn.headstore().is_ok());
    assert!(txn.peerstore().is_ok());
    assert!(txn.systemstore().is_ok());
    assert!(txn.rootstore().is_ok());
}

// Transaction state and callback tests

#[tokio::test]
async fn test_db_txn_callbacks_executed_on_commit() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let store = Arc::new(MemoryStore::new());
    let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
    let mut txn = DbTxn::new(basic_txn, store.clone());

    let success_called = Arc::new(AtomicBool::new(false));
    let success_clone = success_called.clone();
    txn.on_success(Box::new(move || {
        success_clone.store(true, Ordering::SeqCst);
    }))
    .unwrap();

    txn.commit().await.unwrap();
    assert!(success_called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_db_txn_callbacks_executed_on_discard() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let store = Arc::new(MemoryStore::new());
    let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
    let mut txn = DbTxn::new(basic_txn, store.clone());

    let discard_called = Arc::new(AtomicBool::new(false));
    let discard_clone = discard_called.clone();
    txn.on_discard(Box::new(move || {
        discard_clone.store(true, Ordering::SeqCst);
    }))
    .unwrap();

    txn.discard().unwrap();
    assert!(discard_called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_db_txn_readonly_cannot_write() {
    let store = Arc::new(MemoryStore::new());
    let basic_txn = BasicTxn::new(&*store, 1, true).await.unwrap();
    let txn = DbTxn::new(basic_txn, store.clone());

    assert!(txn.is_readonly().unwrap());

    // Attempting to write should fail
    let result = txn.datastore().unwrap().set(b"key", b"value").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_db_txn_id_increments() {
    let store = Arc::new(MemoryStore::new());

    let basic_txn1 = BasicTxn::new(&*store, 1, false).await.unwrap();
    let txn1 = DbTxn::new(basic_txn1, store.clone());
    assert_eq!(txn1.id().unwrap(), 1);

    let basic_txn2 = BasicTxn::new(&*store, 2, false).await.unwrap();
    let txn2 = DbTxn::new(basic_txn2, store.clone());
    assert_eq!(txn2.id().unwrap(), 2);

    let basic_txn3 = BasicTxn::new(&*store, 100, false).await.unwrap();
    let txn3 = DbTxn::new(basic_txn3, store.clone());
    assert_eq!(txn3.id().unwrap(), 100);
}

#[tokio::test]
async fn test_db_txn_multiple_writes_single_commit() {
    let store = Arc::new(MemoryStore::new());
    let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
    let txn = DbTxn::new(basic_txn, store.clone());

    // Multiple writes in single transaction
    txn.datastore()
        .unwrap()
        .set(b"key1", b"value1")
        .await
        .unwrap();
    txn.datastore()
        .unwrap()
        .set(b"key2", b"value2")
        .await
        .unwrap();
    txn.datastore()
        .unwrap()
        .set(b"key3", b"value3")
        .await
        .unwrap();

    txn.commit().await.unwrap();

    // Verify all persisted
    let basic_txn = BasicTxn::new(&*store, 2, true).await.unwrap();
    let txn = DbTxn::new(basic_txn, store.clone());
    assert_eq!(
        txn.datastore().unwrap().get(b"key1").await.unwrap(),
        Some(b"value1".to_vec())
    );
    assert_eq!(
        txn.datastore().unwrap().get(b"key2").await.unwrap(),
        Some(b"value2".to_vec())
    );
    assert_eq!(
        txn.datastore().unwrap().get(b"key3").await.unwrap(),
        Some(b"value3".to_vec())
    );
}

#[tokio::test]
async fn test_db_txn_overwrite_value() {
    let store = Arc::new(MemoryStore::new());

    // Write initial value
    let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
    let txn = DbTxn::new(basic_txn, store.clone());
    txn.datastore()
        .unwrap()
        .set(b"key", b"initial")
        .await
        .unwrap();
    txn.commit().await.unwrap();

    // Overwrite value
    let basic_txn = BasicTxn::new(&*store, 2, false).await.unwrap();
    let txn = DbTxn::new(basic_txn, store.clone());
    txn.datastore()
        .unwrap()
        .set(b"key", b"updated")
        .await
        .unwrap();
    txn.commit().await.unwrap();

    // Verify updated value
    let basic_txn = BasicTxn::new(&*store, 3, true).await.unwrap();
    let txn = DbTxn::new(basic_txn, store.clone());
    assert_eq!(
        txn.datastore().unwrap().get(b"key").await.unwrap(),
        Some(b"updated".to_vec())
    );
}

#[tokio::test]
async fn test_db_txn_delete_value() {
    let store = Arc::new(MemoryStore::new());

    // Write initial value
    let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
    let txn = DbTxn::new(basic_txn, store.clone());
    txn.datastore()
        .unwrap()
        .set(b"key", b"value")
        .await
        .unwrap();
    txn.commit().await.unwrap();

    // Delete value
    let basic_txn = BasicTxn::new(&*store, 2, false).await.unwrap();
    let txn = DbTxn::new(basic_txn, store.clone());
    txn.datastore().unwrap().delete(b"key").await.unwrap();
    txn.commit().await.unwrap();

    // Verify deleted
    let basic_txn = BasicTxn::new(&*store, 3, true).await.unwrap();
    let txn = DbTxn::new(basic_txn, store.clone());
    assert_eq!(txn.datastore().unwrap().get(b"key").await.unwrap(), None);
}
