//! Tests for DbTxn struct.

use std::sync::Arc;

use datastore::BasicTxn;
use db::collection::ensure_persisted_collection_short_id;
use db::txn::DbTxn;
use db::Error;
use storage::backends::MemoryStore;
use storage::corekv::Key;
use storage::keys::systemstore::{CollectionID, CollectionIDSequenceKey};

fn new_txn(basic_txn: BasicTxn) -> DbTxn<MemoryStore> {
    DbTxn::new(basic_txn)
}

fn new_explicit_txn(basic_txn: BasicTxn) -> DbTxn<MemoryStore> {
    DbTxn::new_explicit(basic_txn)
}

#[tokio::test]
async fn test_db_txn_basic() {
    let store = Arc::new(MemoryStore::new());
    let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
    let txn = new_txn(basic_txn);

    assert_eq!(txn.id().unwrap(), 1);
    assert!(!txn.is_readonly().unwrap());
    assert!(!txn.is_explicit());
}

#[tokio::test]
async fn test_db_txn_explicit() {
    let store = Arc::new(MemoryStore::new());
    let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
    let txn = new_explicit_txn(basic_txn);

    assert!(txn.is_explicit());
}

#[tokio::test]
async fn test_db_txn_make_explicit() {
    let store = Arc::new(MemoryStore::new());
    let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
    let mut txn = new_txn(basic_txn);

    assert!(!txn.is_explicit());
    txn.make_explicit();
    assert!(txn.is_explicit());
}

#[tokio::test]
async fn test_db_txn_write_and_commit() {
    let store = Arc::new(MemoryStore::new());
    let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
    let txn = new_txn(basic_txn);

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
    let txn = new_txn(basic_txn);
    let value = txn.datastore().unwrap().get(b"key").await.unwrap();
    assert_eq!(value, Some(b"value".to_vec()));
}

#[tokio::test]
async fn test_db_txn_write_and_discard() {
    let store = Arc::new(MemoryStore::new());
    let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
    let txn = new_txn(basic_txn);

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
    let txn = new_txn(basic_txn);
    let value = txn.datastore().unwrap().get(b"key").await.unwrap();
    assert_eq!(value, None);
}

#[tokio::test]
async fn test_db_txn_force_commit() {
    let store = Arc::new(MemoryStore::new());
    let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
    let txn = new_explicit_txn(basic_txn);

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
    let txn = new_txn(basic_txn);
    let value = txn.datastore().unwrap().get(b"key").await.unwrap();
    assert_eq!(value, Some(b"value".to_vec()));
}

// Negative tests for error conditions

#[tokio::test]
async fn test_db_txn_explicit_commit_returns_error() {
    let store = Arc::new(MemoryStore::new());
    let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
    let txn = new_explicit_txn(basic_txn);

    // Commit on explicit transaction should return error
    let result = txn.commit().await;
    assert!(matches!(result, Err(Error::ExplicitTxnMustUseForce)));
}

#[tokio::test]
async fn test_db_txn_explicit_discard_returns_error() {
    let store = Arc::new(MemoryStore::new());
    let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
    let txn = new_explicit_txn(basic_txn);

    // Discard on explicit transaction should return error
    let result = txn.discard();
    assert!(matches!(result, Err(Error::ExplicitTxnMustUseForce)));
}

#[tokio::test]
async fn test_db_txn_force_discard() {
    let store = Arc::new(MemoryStore::new());
    let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
    let txn = new_explicit_txn(basic_txn);

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
    let txn = new_txn(basic_txn);
    let value = txn.datastore().unwrap().get(b"key").await.unwrap();
    assert_eq!(value, None);
}

#[tokio::test]
async fn test_db_txn_accessor_returns_all_stores() {
    let store = Arc::new(MemoryStore::new());
    let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
    let txn = new_txn(basic_txn);

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
    let mut txn = new_txn(basic_txn);

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
    let mut txn = new_txn(basic_txn);

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
    let txn = new_txn(basic_txn);

    assert!(txn.is_readonly().unwrap());

    // Attempting to write should fail
    let result = txn.datastore().unwrap().set(b"key", b"value").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_db_txn_id_increments() {
    let store = Arc::new(MemoryStore::new());

    let basic_txn1 = BasicTxn::new(&*store, 1, false).await.unwrap();
    let txn1 = new_txn(basic_txn1);
    assert_eq!(txn1.id().unwrap(), 1);

    let basic_txn2 = BasicTxn::new(&*store, 2, false).await.unwrap();
    let txn2 = new_txn(basic_txn2);
    assert_eq!(txn2.id().unwrap(), 2);

    let basic_txn3 = BasicTxn::new(&*store, 100, false).await.unwrap();
    let txn3 = new_txn(basic_txn3);
    assert_eq!(txn3.id().unwrap(), 100);
}

#[tokio::test]
async fn test_persisted_collection_root_id_allocation_conflicts_instead_of_duplicating() {
    let store = Arc::new(MemoryStore::new());

    let txn1 = DbTxn::new(
        BasicTxn::new(&*store, 1, false).await.unwrap(),
        store.clone(),
    );
    let txn2 = DbTxn::new(
        BasicTxn::new(&*store, 2, false).await.unwrap(),
        store.clone(),
    );

    let short_id_1 = {
        let systemstore1 = txn1.systemstore().unwrap();
        ensure_persisted_collection_short_id(&systemstore1, "collection-a")
            .await
            .unwrap()
    };
    let short_id_2 = {
        let systemstore2 = txn2.systemstore().unwrap();
        ensure_persisted_collection_short_id(&systemstore2, "collection-b")
            .await
            .unwrap()
    };

    assert_eq!(short_id_1, 1);
    assert_eq!(
        short_id_2, 1,
        "concurrent snapshots may tentatively pick the same next ID"
    );

    txn1.commit().await.unwrap();

    let err = txn2.commit().await.unwrap_err();
    assert!(
        matches!(
            err,
            Error::Datastore(datastore::Error::Storage(storage::Error::TxnConflict))
        ),
        "expected write-write conflict, got: {err}"
    );

    let retry_txn = DbTxn::new(
        BasicTxn::new(&*store, 3, false).await.unwrap(),
        store.clone(),
    );
    let retried_short_id = {
        let retry_systemstore = retry_txn.systemstore().unwrap();
        ensure_persisted_collection_short_id(&retry_systemstore, "collection-b")
            .await
            .unwrap()
    };
    assert_eq!(retried_short_id, 2);
    retry_txn.commit().await.unwrap();

    let read_txn = DbTxn::new(
        BasicTxn::new(&*store, 4, true).await.unwrap(),
        store.clone(),
    );
    let (collection_a_short_id, collection_b_short_id, sequence_value) = {
        let read_systemstore = read_txn.systemstore().unwrap();
        let sequence_key = CollectionIDSequenceKey;
        let collection_a_short_id = read_systemstore
            .get(&CollectionID::new("collection-a").bytes())
            .await
            .unwrap();
        let collection_b_short_id = read_systemstore
            .get(&CollectionID::new("collection-b").bytes())
            .await
            .unwrap();
        let sequence_value = read_systemstore.get(&sequence_key.bytes()).await.unwrap();
        (collection_a_short_id, collection_b_short_id, sequence_value)
    };

    assert_eq!(collection_a_short_id, Some(b"1".to_vec()));
    assert_eq!(collection_b_short_id, Some(b"2".to_vec()));
    assert_eq!(sequence_value, Some(2u32.to_be_bytes().to_vec()));
}

#[tokio::test]
async fn test_db_txn_multiple_writes_single_commit() {
    let store = Arc::new(MemoryStore::new());
    let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
    let txn = new_txn(basic_txn);

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
    let txn = new_txn(basic_txn);
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
    let txn = new_txn(basic_txn);
    txn.datastore()
        .unwrap()
        .set(b"key", b"initial")
        .await
        .unwrap();
    txn.commit().await.unwrap();

    // Overwrite value
    let basic_txn = BasicTxn::new(&*store, 2, false).await.unwrap();
    let txn = new_txn(basic_txn);
    txn.datastore()
        .unwrap()
        .set(b"key", b"updated")
        .await
        .unwrap();
    txn.commit().await.unwrap();

    // Verify updated value
    let basic_txn = BasicTxn::new(&*store, 3, true).await.unwrap();
    let txn = new_txn(basic_txn);
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
    let txn = new_txn(basic_txn);
    txn.datastore()
        .unwrap()
        .set(b"key", b"value")
        .await
        .unwrap();
    txn.commit().await.unwrap();

    // Delete value
    let basic_txn = BasicTxn::new(&*store, 2, false).await.unwrap();
    let txn = new_txn(basic_txn);
    txn.datastore().unwrap().delete(b"key").await.unwrap();
    txn.commit().await.unwrap();

    // Verify deleted
    let basic_txn = BasicTxn::new(&*store, 3, true).await.unwrap();
    let txn = new_txn(basic_txn);
    assert_eq!(txn.datastore().unwrap().get(b"key").await.unwrap(), None);
}
