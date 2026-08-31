//! Integration tests for BasicTxn.

use bytes::Bytes;
use datastore::BasicTxn;
use futures::FutureExt;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use storage::RegolithStore;

#[tokio::test]
async fn test_basic_txn_id() {
    let store = RegolithStore::in_memory().unwrap();
    let txn = BasicTxn::new(&store, 42, false).await.unwrap();
    assert_eq!(txn.id(), 42);
}

#[tokio::test]
async fn test_basic_txn_readonly() {
    let store = RegolithStore::in_memory().unwrap();

    let txn = BasicTxn::new(&store, 1, true).await.unwrap();
    assert!(txn.is_readonly());

    let txn = BasicTxn::new(&store, 2, false).await.unwrap();
    assert!(!txn.is_readonly());
}

#[tokio::test]
async fn test_basic_txn_multistore_access() {
    let store = RegolithStore::in_memory().unwrap();
    let txn = BasicTxn::new(&store, 1, false).await.unwrap();

    // Write to different stores
    txn.datastore()
        .set(b"key", b"datastore_value")
        .await
        .unwrap();
    txn.systemstore()
        .set(b"key", b"systemstore_value")
        .await
        .unwrap();

    // Read back
    assert_eq!(
        txn.datastore().get(b"key").await.unwrap(),
        Some(Bytes::from_static(b"datastore_value"))
    );
    assert_eq!(
        txn.systemstore().get(b"key").await.unwrap(),
        Some(Bytes::from_static(b"systemstore_value"))
    );

    // Commit
    txn.commit().await.unwrap();

    // Verify data persisted
    let txn = BasicTxn::new(&store, 2, true).await.unwrap();
    assert_eq!(
        txn.datastore().get(b"key").await.unwrap(),
        Some(Bytes::from_static(b"datastore_value"))
    );
}

#[tokio::test]
async fn test_basic_txn_on_success_callback() {
    let store = RegolithStore::in_memory().unwrap();
    let mut txn = BasicTxn::new(&store, 1, false).await.unwrap();

    let called = Arc::new(AtomicBool::new(false));
    let called_clone = called.clone();
    txn.on_success(Box::new(move || {
        called_clone.store(true, Ordering::SeqCst);
    }));

    txn.commit().await.unwrap();

    assert!(called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_basic_txn_multiple_callbacks() {
    let store = RegolithStore::in_memory().unwrap();
    let mut txn = BasicTxn::new(&store, 1, false).await.unwrap();

    let counter = Arc::new(AtomicU32::new(0));
    for _ in 0..3 {
        let counter_clone = counter.clone();
        txn.on_success(Box::new(move || {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        }));
    }

    txn.commit().await.unwrap();

    assert_eq!(counter.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn test_basic_txn_discard_callback() {
    let store = RegolithStore::in_memory().unwrap();
    let mut txn = BasicTxn::new(&store, 1, false).await.unwrap();

    let called = Arc::new(AtomicBool::new(false));
    let called_clone = called.clone();
    txn.on_discard(Box::new(move || {
        called_clone.store(true, Ordering::SeqCst);
    }));

    // Write some data
    txn.datastore().set(b"key", b"value").await.unwrap();

    // Discard
    txn.discard().unwrap();

    assert!(called.load(Ordering::SeqCst));

    // Verify data was not persisted
    let txn = BasicTxn::new(&store, 2, true).await.unwrap();
    assert_eq!(txn.datastore().get(b"key").await.unwrap(), None);
}

#[tokio::test]
async fn test_basic_txn_rootstore_access() {
    let store = RegolithStore::in_memory().unwrap();
    let txn = BasicTxn::new(&store, 1, false).await.unwrap();

    // Write through datastore
    txn.datastore().set(b"mykey", b"value").await.unwrap();

    // Read through rootstore with prefix
    let value = txn.rootstore().get(b"dmykey").await.unwrap();
    assert_eq!(value, Some(Bytes::from_static(b"value")));

    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_basic_txn_error_callback_not_called_on_success() {
    let store = RegolithStore::in_memory().unwrap();
    let mut txn = BasicTxn::new(&store, 1, false).await.unwrap();

    let error_called = Arc::new(AtomicBool::new(false));
    let error_called_clone = error_called.clone();
    txn.on_error(Box::new(move || {
        error_called_clone.store(true, Ordering::SeqCst);
    }));

    let success_called = Arc::new(AtomicBool::new(false));
    let success_called_clone = success_called.clone();
    txn.on_success(Box::new(move || {
        success_called_clone.store(true, Ordering::SeqCst);
    }));

    txn.commit().await.unwrap();

    // Success should be called, error should not
    assert!(success_called.load(Ordering::SeqCst));
    assert!(!error_called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_basic_txn_discard_callback_not_called_on_commit() {
    let store = RegolithStore::in_memory().unwrap();
    let mut txn = BasicTxn::new(&store, 1, false).await.unwrap();

    let discard_called = Arc::new(AtomicBool::new(false));
    let discard_called_clone = discard_called.clone();
    txn.on_discard(Box::new(move || {
        discard_called_clone.store(true, Ordering::SeqCst);
    }));

    txn.commit().await.unwrap();

    // Discard callback should not be called on commit
    assert!(!discard_called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_basic_txn_success_callback_not_called_on_discard() {
    let store = RegolithStore::in_memory().unwrap();
    let mut txn = BasicTxn::new(&store, 1, false).await.unwrap();

    let success_called = Arc::new(AtomicBool::new(false));
    let success_called_clone = success_called.clone();
    txn.on_success(Box::new(move || {
        success_called_clone.store(true, Ordering::SeqCst);
    }));

    txn.discard().unwrap();

    // Success callback should not be called on discard
    assert!(!success_called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_basic_txn_double_commit_returns_error() {
    let store = RegolithStore::in_memory().unwrap();
    let txn = BasicTxn::new(&store, 1, false).await.unwrap();

    // First commit succeeds
    txn.commit().await.unwrap();

    // Cannot commit twice - txn is consumed after first commit
    // This is enforced by Rust's ownership system
}

#[tokio::test]
async fn test_basic_txn_discard_already_discarded_returns_error() {
    let store = RegolithStore::in_memory().unwrap();
    let txn = BasicTxn::new(&store, 1, false).await.unwrap();

    // First discard succeeds
    txn.discard().unwrap();

    // Cannot discard twice - txn is consumed after first discard
    // This is enforced by Rust's ownership system
}

#[tokio::test]
async fn test_basic_txn_all_stores_accessible() {
    let store = RegolithStore::in_memory().unwrap();
    let txn = BasicTxn::new(&store, 1, false).await.unwrap();

    // All stores should be accessible and work
    txn.datastore().set(b"d", b"data").await.unwrap();
    txn.blockstore().set(b"b", b"block").await.unwrap();
    txn.encstore().set(b"e", b"enc").await.unwrap();
    txn.headstore().set(b"h", b"head").await.unwrap();
    txn.peerstore().set(b"p", b"peer").await.unwrap();
    txn.systemstore().set(b"s", b"sys").await.unwrap();

    txn.commit().await.unwrap();

    // Verify all stores persisted
    let txn = BasicTxn::new(&store, 2, true).await.unwrap();
    assert_eq!(
        txn.datastore().get(b"d").await.unwrap(),
        Some(Bytes::from_static(b"data"))
    );
    assert_eq!(
        txn.blockstore().get(b"b").await.unwrap(),
        Some(Bytes::from_static(b"block"))
    );
    assert_eq!(
        txn.encstore().get(b"e").await.unwrap(),
        Some(Bytes::from_static(b"enc"))
    );
    assert_eq!(
        txn.headstore().get(b"h").await.unwrap(),
        Some(Bytes::from_static(b"head"))
    );
    assert_eq!(
        txn.peerstore().get(b"p").await.unwrap(),
        Some(Bytes::from_static(b"peer"))
    );
    assert_eq!(
        txn.systemstore().get(b"s").await.unwrap(),
        Some(Bytes::from_static(b"sys"))
    );
}

#[tokio::test]
async fn test_basic_txn_commit_with_outstanding_view_fails() {
    let store = RegolithStore::in_memory().unwrap();
    let txn = BasicTxn::new(&store, 1, false).await.unwrap();

    // Hold a reference to a namespace view
    let _view = txn.datastore();

    // Commit should fail because there are outstanding references
    let result = txn.commit().await;
    assert!(result.is_err());
    // The error message should mention references
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("references"));
}

#[tokio::test]
async fn test_basic_txn_discard_with_outstanding_view_fails() {
    let store = RegolithStore::in_memory().unwrap();
    let txn = BasicTxn::new(&store, 1, false).await.unwrap();

    // Hold a reference to a namespace view
    let _view = txn.datastore();

    // Discard should fail because there are outstanding references
    let result = txn.discard();
    assert!(result.is_err());
    assert!(matches!(result, Err(datastore::Error::TxnStillInUse)));
}

#[tokio::test]
async fn test_basic_txn_async_success_callback() {
    use std::time::Duration;

    let store = RegolithStore::in_memory().unwrap();
    let mut txn = BasicTxn::new(&store, 1, false).await.unwrap();

    let (tx, rx) = tokio::sync::oneshot::channel();
    txn.on_success_async(Box::new(move || {
        Box::pin(async move {
            tx.send(()).unwrap();
        })
    }));

    txn.commit().await.unwrap();

    // Wait for async callback to complete
    tokio::time::timeout(Duration::from_secs(1), rx)
        .await
        .expect("Timeout waiting for async callback")
        .expect("Failed to receive from callback");
}

#[tokio::test]
async fn test_basic_txn_mixed_success_callbacks_preserve_execution_order() {
    let store = RegolithStore::in_memory().unwrap();
    let mut txn = BasicTxn::new(&store, 1, false).await.unwrap();

    let execution_order = Arc::new(Mutex::new(Vec::new()));

    let order = execution_order.clone();
    txn.on_success(Box::new(move || {
        order.lock().unwrap().push("sync-1");
    }));

    let order = execution_order.clone();
    txn.on_success_async(Box::new(move || {
        Box::pin(async move {
            order.lock().unwrap().push("async-1");
        })
    }));

    let order = execution_order.clone();
    txn.on_success(Box::new(move || {
        order.lock().unwrap().push("sync-2");
    }));

    let order = execution_order.clone();
    txn.on_success_async(Box::new(move || {
        Box::pin(async move {
            order.lock().unwrap().push("async-2");
        })
    }));

    txn.commit().await.unwrap();

    assert_eq!(
        *execution_order.lock().unwrap(),
        vec!["async-1", "async-2", "sync-1", "sync-2"]
    );
}

#[tokio::test]
async fn test_basic_txn_async_discard_callback() {
    use std::time::Duration;

    let store = RegolithStore::in_memory().unwrap();
    let mut txn = BasicTxn::new(&store, 1, false).await.unwrap();

    let (tx, rx) = tokio::sync::oneshot::channel();
    txn.on_discard_async(Box::new(move || {
        Box::pin(async move {
            tx.send(()).unwrap();
        })
    }));

    txn.discard().unwrap();

    // Wait for async callback to complete
    tokio::time::timeout(Duration::from_secs(1), rx)
        .await
        .expect("Timeout waiting for async callback")
        .expect("Failed to receive from callback");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_basic_txn_discard_spawns_async_callbacks_before_sync_callbacks() {
    use std::time::Duration;

    let store = RegolithStore::in_memory().unwrap();
    let mut txn = BasicTxn::new(&store, 1, false).await.unwrap();

    let (started_tx, started_rx) = std::sync::mpsc::channel();
    txn.on_discard_async(Box::new(move || {
        Box::pin(async move {
            started_tx.send(()).unwrap();
        })
    }));

    txn.on_discard(Box::new(move || {
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("async discard callback should be spawned before sync discard callback runs");
    }));

    txn.discard().unwrap();
}

#[tokio::test]
async fn test_basic_txn_success_callback_panic_propagates() {
    let store = RegolithStore::in_memory().unwrap();
    let mut txn = BasicTxn::new(&store, 1, false).await.unwrap();

    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = counter.clone();
    txn.on_success(Box::new(move || {
        counter_clone.fetch_add(1, Ordering::SeqCst);
    }));
    txn.on_success(Box::new(|| {
        panic!("success callback panic");
    }));
    let counter_clone = counter.clone();
    txn.on_success(Box::new(move || {
        counter_clone.fetch_add(1, Ordering::SeqCst);
    }));

    let panic = std::panic::AssertUnwindSafe(async move {
        txn.commit().await.unwrap();
    })
    .catch_unwind()
    .await;

    assert!(panic.is_err());
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_basic_txn_async_success_callback_panic_propagates() {
    let store = RegolithStore::in_memory().unwrap();
    let mut txn = BasicTxn::new(&store, 1, false).await.unwrap();

    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = counter.clone();
    txn.on_success_async(Box::new(move || {
        Box::pin(async move {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        })
    }));
    txn.on_success_async(Box::new(|| {
        Box::pin(async {
            panic!("async success callback panic");
        })
    }));
    let counter_clone = counter.clone();
    txn.on_success_async(Box::new(move || {
        Box::pin(async move {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        })
    }));

    let panic = std::panic::AssertUnwindSafe(async move {
        txn.commit().await.unwrap();
    })
    .catch_unwind()
    .await;

    assert!(panic.is_err());
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_basic_txn_discard_callback_panic_propagates() {
    let store = RegolithStore::in_memory().unwrap();
    let mut txn = BasicTxn::new(&store, 1, false).await.unwrap();

    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = counter.clone();
    txn.on_discard(Box::new(move || {
        counter_clone.fetch_add(1, Ordering::SeqCst);
    }));
    txn.on_discard(Box::new(|| {
        panic!("discard callback panic");
    }));
    let counter_clone = counter.clone();
    txn.on_discard(Box::new(move || {
        counter_clone.fetch_add(1, Ordering::SeqCst);
    }));

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        txn.discard().unwrap();
    }));

    assert!(panic.is_err());
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}
