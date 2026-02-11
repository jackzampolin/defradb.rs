use crate::corekv::Store;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

/// Test discard prevents persistence
pub async fn test_discard_prevents_persistence<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"discarded_key", b"value").await.unwrap();
    txn.discard();

    let txn = store.new_txn(true).await.unwrap();
    assert_eq!(
        txn.get(b"discarded_key").await.unwrap(),
        None,
        "Discarded transaction changes must not persist"
    );
}

/// Test success callback is invoked on commit
pub async fn test_success_callback<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();

    let called = Arc::new(AtomicBool::new(false));
    let called_clone = called.clone();

    txn.on_success(Box::new(move || {
        called_clone.store(true, Ordering::SeqCst);
    }));

    txn.set(b"key", b"value").await.unwrap();
    txn.commit().await.unwrap();

    assert!(
        called.load(Ordering::SeqCst),
        "Success callback should be invoked"
    );
}

/// Test discard callback is invoked
pub async fn test_discard_callback<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();

    let called = Arc::new(AtomicBool::new(false));
    let called_clone = called.clone();

    txn.on_discard(Box::new(move || {
        called_clone.store(true, Ordering::SeqCst);
    }));

    txn.set(b"key", b"value").await.unwrap();
    txn.discard();

    assert!(
        called.load(Ordering::SeqCst),
        "Discard callback should be invoked"
    );
}

/// Test async success callback
pub async fn test_async_success_callback<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();

    let called = Arc::new(AtomicBool::new(false));
    let called_clone = called.clone();

    txn.on_success_async(Box::new(move || {
        let flag = called_clone.clone();
        Box::pin(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            flag.store(true, Ordering::SeqCst);
        })
    }));

    txn.set(b"key", b"value").await.unwrap();
    txn.commit().await.unwrap();

    assert!(
        called.load(Ordering::SeqCst),
        "Async callback should be awaited during commit"
    );
}

/// Test that one callback panic doesn't stop others
pub async fn test_callback_panic_safety<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();

    let count = Arc::new(AtomicUsize::new(0));

    // First callback - increments
    let count1 = count.clone();
    txn.on_success(Box::new(move || {
        count1.fetch_add(1, Ordering::SeqCst);
    }));

    // Second callback - PANICS
    txn.on_success(Box::new(|| {
        panic!("Intentional panic in test");
    }));

    // Third callback - should still run
    let count3 = count.clone();
    txn.on_success(Box::new(move || {
        count3.fetch_add(1, Ordering::SeqCst);
    }));

    txn.set(b"key", b"value").await.unwrap();
    txn.commit().await.unwrap();

    assert_eq!(
        count.load(Ordering::SeqCst),
        2,
        "Both non-panicking callbacks should execute despite middle callback panic"
    );
}

/// Test async callback panic safety
pub async fn test_async_callback_panic_safety<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();

    let count = Arc::new(AtomicUsize::new(0));

    let count1 = count.clone();
    txn.on_success_async(Box::new(move || {
        let c = count1.clone();
        Box::pin(async move {
            c.fetch_add(1, Ordering::SeqCst);
        })
    }));

    txn.on_success_async(Box::new(|| {
        Box::pin(async {
            panic!("Intentional async panic");
        })
    }));

    let count3 = count.clone();
    txn.on_success_async(Box::new(move || {
        let c = count3.clone();
        Box::pin(async move {
            c.fetch_add(1, Ordering::SeqCst);
        })
    }));

    txn.set(b"key", b"value").await.unwrap();
    txn.commit().await.unwrap();

    assert_eq!(count.load(Ordering::SeqCst), 2);
}

/// Test discard callback panic safety
pub async fn test_discard_callback_panic_safety<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();

    let count = Arc::new(AtomicUsize::new(0));

    let count1 = count.clone();
    txn.on_discard(Box::new(move || {
        count1.fetch_add(1, Ordering::SeqCst);
    }));

    txn.on_discard(Box::new(|| {
        panic!("Discard callback panic");
    }));

    let count3 = count.clone();
    txn.on_discard(Box::new(move || {
        count3.fetch_add(1, Ordering::SeqCst);
    }));

    txn.set(b"key", b"value").await.unwrap();
    txn.discard();

    assert_eq!(count.load(Ordering::SeqCst), 2);
}
