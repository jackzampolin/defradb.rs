/// Shared test suite for all backend implementations.
///
/// This module provides a comprehensive test suite that verifies backend correctness.
/// All backends MUST pass these tests to ensure consistent behavior.
///
/// # Usage
///
/// Each backend module should include:
/// ```ignore
/// #[cfg(test)]
/// mod shared_tests {
///     use super::*;
///     use crate::backends::test_suite::*;
///
///     async fn create_store() -> impl Store {
///         MemoryStore::new()
///     }
///
///     // Then invoke the test macros or call test functions
/// }
/// ```

use crate::corekv::{Error, IterOptions, Store};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};

// ============================================================================
// BASIC OPERATIONS
// ============================================================================

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
    assert_eq!(txn.get(b"to_delete").await.unwrap(), Some(b"value".to_vec()));
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

// ============================================================================
// ERROR HANDLING
// ============================================================================

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

    assert!(matches!(txn.set(b"key", b"value").await, Err(Error::ReadOnlyTxn)));
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

// ============================================================================
// TRANSACTION LIFECYCLE
// ============================================================================

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

    assert!(called.load(Ordering::SeqCst), "Success callback should be invoked");
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

    assert!(called.load(Ordering::SeqCst), "Discard callback should be invoked");
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

    assert!(called.load(Ordering::SeqCst), "Async callback should be awaited during commit");
}

// ============================================================================
// CALLBACK PANIC SAFETY
// ============================================================================

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
        Box::pin(async move { c.fetch_add(1, Ordering::SeqCst); })
    }));

    txn.on_success_async(Box::new(|| {
        Box::pin(async { panic!("Intentional async panic"); })
    }));

    let count3 = count.clone();
    txn.on_success_async(Box::new(move || {
        let c = count3.clone();
        Box::pin(async move { c.fetch_add(1, Ordering::SeqCst); })
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

// ============================================================================
// ITERATOR TESTS
// ============================================================================

/// Test basic iteration
pub async fn test_iterator_basic<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"a", b"1").await.unwrap();
    txn.set(b"b", b"2").await.unwrap();
    txn.set(b"c", b"3").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();

    let mut keys = vec![];
    while let Some(kv) = iter.next().await.unwrap() {
        keys.push(String::from_utf8_lossy(kv.key_bytes()).to_string());
    }

    assert_eq!(keys, vec!["a", "b", "c"]);
}

/// Test prefix filtering
pub async fn test_iterator_prefix<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"user/1", b"alice").await.unwrap();
    txn.set(b"user/2", b"bob").await.unwrap();
    txn.set(b"post/1", b"hello").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new().with_prefix(b"user/".to_vec());
    let mut iter = txn.iterator(opts).await.unwrap();

    let mut count = 0;
    while let Some(kv) = iter.next().await.unwrap() {
        assert!(kv.key_bytes().starts_with(b"user/"));
        count += 1;
    }
    assert_eq!(count, 2);
}

/// Test reverse iteration
pub async fn test_iterator_reverse<S: Store>(store: &S) {
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
        keys.push(String::from_utf8_lossy(kv.key_bytes()).to_string());
    }

    assert_eq!(keys, vec!["c", "b", "a"]);
}

/// Test start/end range
pub async fn test_iterator_range<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    for key in [b"a", b"b", b"c", b"d", b"e"] {
        txn.set(key, b"value").await.unwrap();
    }
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new()
        .with_start(b"b".to_vec())
        .with_end(b"e".to_vec());
    let mut iter = txn.iterator(opts).await.unwrap();

    let mut keys = vec![];
    while let Some(kv) = iter.next().await.unwrap() {
        keys.push(String::from_utf8_lossy(kv.key_bytes()).to_string());
    }

    assert_eq!(keys, vec!["b", "c", "d"]);
}

/// Test keys-only mode returns empty values
pub async fn test_iterator_keys_only<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"key1", b"this_is_a_long_value").await.unwrap();
    txn.set(b"key2", b"another_long_value").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new().with_keys_only(true);
    let mut iter = txn.iterator(opts).await.unwrap();

    while let Some(kv) = iter.next().await.unwrap() {
        assert!(
            kv.value_bytes().is_empty(),
            "Keys-only iterator should return empty values"
        );
    }
}

/// Test closed iterator returns error
pub async fn test_iterator_closed<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"key", b"value").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();

    iter.close().await.unwrap();

    assert!(matches!(iter.next().await, Err(Error::Iterator(_))));
}

/// Test empty iterator (no data)
pub async fn test_iterator_empty_store<S: Store>(store: &S) {
    let txn = store.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();

    assert!(iter.next().await.unwrap().is_none());
}

/// Test start == end yields empty iterator
pub async fn test_iterator_start_equals_end<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"a", b"1").await.unwrap();
    txn.set(b"b", b"2").await.unwrap();
    txn.set(b"c", b"3").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new()
        .with_start(b"b".to_vec())
        .with_end(b"b".to_vec());
    let mut iter = txn.iterator(opts).await.unwrap();

    assert!(iter.next().await.unwrap().is_none(), "start == end should yield empty iterator");
}

/// Test prefix with no matches
pub async fn test_iterator_prefix_no_match<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"users/1", b"alice").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new().with_prefix(b"posts/".to_vec());
    let mut iter = txn.iterator(opts).await.unwrap();

    assert!(iter.next().await.unwrap().is_none());
}

/// Test iterator sees pending (uncommitted) writes
pub async fn test_iterator_sees_pending_writes<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"a", b"1").await.unwrap();
    txn.set(b"b", b"2").await.unwrap();

    // Iterator BEFORE commit should see pending writes
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();

    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"a");

    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"b");

    assert!(iter.next().await.unwrap().is_none());
}

/// Test iterator sees pending deletes
pub async fn test_iterator_sees_pending_deletes<S: Store>(store: &S) {
    // Setup
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"a", b"1").await.unwrap();
    txn.set(b"b", b"2").await.unwrap();
    txn.set(b"c", b"3").await.unwrap();
    txn.commit().await.unwrap();

    // Delete b (uncommitted)
    let mut txn = store.new_txn(false).await.unwrap();
    txn.delete(b"b").await.unwrap();

    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();

    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"a");

    let kv = iter.next().await.unwrap().unwrap();
    assert_eq!(kv.key_bytes(), b"c", "Deleted 'b' should be skipped");

    assert!(iter.next().await.unwrap().is_none());
}

/// Test reverse iteration with prefix
pub async fn test_iterator_reverse_with_prefix<S: Store>(store: &S) {
    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"user/a", b"1").await.unwrap();
    txn.set(b"user/b", b"2").await.unwrap();
    txn.set(b"user/c", b"3").await.unwrap();
    txn.set(b"other/x", b"4").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let opts = IterOptions::new()
        .with_prefix(b"user/".to_vec())
        .with_reverse(true);
    let mut iter = txn.iterator(opts).await.unwrap();

    let mut keys = vec![];
    while let Some(kv) = iter.next().await.unwrap() {
        keys.push(String::from_utf8_lossy(kv.key_bytes()).to_string());
    }

    assert_eq!(keys, vec!["user/c", "user/b", "user/a"]);
}

// ============================================================================
// CONCURRENCY TESTS
// ============================================================================

/// Test concurrent writes to different keys (should all succeed)
pub async fn test_concurrent_writes_different_keys<S: Store + 'static>(store: Arc<S>) {
    let commit_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for i in 0..20 {
        let store = store.clone();
        let commit_count = commit_count.clone();
        handles.push(tokio::spawn(async move {
            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(format!("key_{}", i).as_bytes(), b"value").await.unwrap();
            txn.commit().await.unwrap();
            commit_count.fetch_add(1, Ordering::SeqCst);
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    assert_eq!(commit_count.load(Ordering::SeqCst), 20);

    // Verify all keys exist
    let txn = store.new_txn(true).await.unwrap();
    for i in 0..20 {
        assert!(
            txn.has(format!("key_{}", i).as_bytes()).await.unwrap(),
            "key_{} should exist",
            i
        );
    }
}

/// Test concurrent writes to the SAME key (last writer wins)
pub async fn test_concurrent_writes_same_key<S: Store + 'static>(store: Arc<S>) {
    let commit_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for i in 0..10 {
        let store = store.clone();
        let commit_count = commit_count.clone();
        handles.push(tokio::spawn(async move {
            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(b"contended_key", format!("value_{}", i).as_bytes())
                .await
                .unwrap();
            txn.commit().await.unwrap();
            commit_count.fetch_add(1, Ordering::SeqCst);
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // All commits should succeed (no OCC = no conflicts)
    assert_eq!(commit_count.load(Ordering::SeqCst), 10);

    // Key should have SOME value
    let txn = store.new_txn(true).await.unwrap();
    assert!(txn.get(b"contended_key").await.unwrap().is_some());
}

/// Test last-writer-wins semantics
pub async fn test_last_writer_wins<S: Store + 'static>(store: Arc<S>) {
    let mut txn1 = store.new_txn(false).await.unwrap();
    let mut txn2 = store.new_txn(false).await.unwrap();

    txn1.set(b"shared", b"from_txn1").await.unwrap();
    txn2.set(b"shared", b"from_txn2").await.unwrap();

    // Commit order determines winner
    txn1.commit().await.unwrap();
    txn2.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    assert_eq!(
        txn.get(b"shared").await.unwrap(),
        Some(b"from_txn2".to_vec()),
        "Last commit should win"
    );
}

/// Test reverse commit order
pub async fn test_last_writer_wins_reverse<S: Store + 'static>(store: Arc<S>) {
    let mut txn1 = store.new_txn(false).await.unwrap();
    let mut txn2 = store.new_txn(false).await.unwrap();

    txn1.set(b"shared", b"from_txn1").await.unwrap();
    txn2.set(b"shared", b"from_txn2").await.unwrap();

    // Reverse order
    txn2.commit().await.unwrap();
    txn1.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    assert_eq!(
        txn.get(b"shared").await.unwrap(),
        Some(b"from_txn1".to_vec())
    );
}

/// Stress test with many parallel commits
pub async fn test_parallel_stress<S: Store + 'static>(store: Arc<S>) {
    let commit_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for i in 0..50 {
        let store = store.clone();
        let commit_count = commit_count.clone();
        handles.push(tokio::spawn(async move {
            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(format!("stress_key_{}", i).as_bytes(), b"value")
                .await
                .unwrap();
            txn.commit().await.unwrap();
            commit_count.fetch_add(1, Ordering::SeqCst);
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    assert_eq!(commit_count.load(Ordering::SeqCst), 50);

    // Verify all keys exist
    let txn = store.new_txn(true).await.unwrap();
    for i in 0..50 {
        assert!(
            txn.has(format!("stress_key_{}", i).as_bytes())
                .await
                .unwrap(),
            "Key stress_{} should exist",
            i
        );
    }
}

// ============================================================================
// SNAPSHOT ISOLATION TESTS (CONCURRENT)
// ============================================================================

/// Test snapshot isolation with concurrent readers and writers.
///
/// This test verifies that readers see a consistent snapshot even when
/// concurrent writers are actively modifying data. Each reader should see
/// the database state as it was when the transaction started.
pub async fn test_snapshot_isolation_concurrent<S: Store + 'static>(store: Arc<S>) {
    // Setup: write initial value
    let mut setup = store.new_txn(false).await.unwrap();
    setup.set(b"snapshot_key", b"initial").await.unwrap();
    setup.commit().await.unwrap();

    // Track violations
    let violations = Arc::new(AtomicUsize::new(0));
    let total_checks = Arc::new(AtomicUsize::new(0));

    // Barrier to synchronize start
    let barrier = Arc::new(tokio::sync::Barrier::new(21)); // 10 readers + 10 writers + 1 main

    let mut handles = vec![];

    // Spawn 10 reader tasks - each reads initial value, waits, then reads again
    for _ in 0..10 {
        let store = store.clone();
        let violations = violations.clone();
        let total_checks = total_checks.clone();
        let barrier = barrier.clone();

        handles.push(tokio::spawn(async move {
            barrier.wait().await; // Synchronize start

            // Start a read transaction
            let reader = store.new_txn(true).await.unwrap();

            // Read initial value
            let first_read = reader.get(b"snapshot_key").await.unwrap();

            // Wait a bit for writers to do their work
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;

            // Read again - should see SAME value (snapshot isolation)
            let second_read = reader.get(b"snapshot_key").await.unwrap();

            total_checks.fetch_add(1, Ordering::SeqCst);

            if first_read != second_read {
                violations.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }

    // Spawn 10 writer tasks - each writes a unique value
    for i in 0..10 {
        let store = store.clone();
        let barrier = barrier.clone();

        handles.push(tokio::spawn(async move {
            barrier.wait().await; // Synchronize start

            // Small random delay to interleave with readers
            tokio::time::sleep(std::time::Duration::from_millis(i * 5)).await;

            let mut writer = store.new_txn(false).await.unwrap();
            writer.set(b"snapshot_key", format!("writer_{}", i).as_bytes()).await.unwrap();
            writer.commit().await.unwrap();
        }));
    }

    // Start everyone
    barrier.wait().await;

    // Wait for all tasks
    for handle in handles {
        handle.await.unwrap();
    }

    assert_eq!(
        violations.load(Ordering::SeqCst),
        0,
        "Snapshot isolation violated: readers saw different values within same transaction"
    );
    assert_eq!(total_checks.load(Ordering::SeqCst), 10, "All readers should complete");
}

/// Test that long-running readers maintain isolation despite many writes.
///
/// A reader starts, then 100 rapid writes happen, then the reader reads.
/// The reader should still see the original value.
pub async fn test_snapshot_isolation_long_running_reader<S: Store + 'static>(store: Arc<S>) {
    // Setup initial value
    let mut setup = store.new_txn(false).await.unwrap();
    setup.set(b"long_read_key", b"original").await.unwrap();
    setup.commit().await.unwrap();

    // Start a long-running reader
    let reader = store.new_txn(true).await.unwrap();
    let initial_value = reader.get(b"long_read_key").await.unwrap();
    assert_eq!(initial_value, Some(b"original".to_vec()));

    // Perform 100 rapid writes
    for i in 0..100 {
        let mut writer = store.new_txn(false).await.unwrap();
        writer.set(b"long_read_key", format!("write_{}", i).as_bytes()).await.unwrap();
        writer.commit().await.unwrap();
    }

    // Reader should STILL see original value
    let final_value = reader.get(b"long_read_key").await.unwrap();
    assert_eq!(
        final_value,
        Some(b"original".to_vec()),
        "Long-running reader should maintain snapshot isolation"
    );

    // New reader should see latest write
    let new_reader = store.new_txn(true).await.unwrap();
    let new_value = new_reader.get(b"long_read_key").await.unwrap();
    assert_eq!(
        new_value,
        Some(b"write_99".to_vec()),
        "New reader should see latest committed value"
    );
}

/// Test snapshot isolation with iterator under concurrent modification.
///
/// A reader starts iteration, concurrent writers add/modify keys,
/// the iterator should only see keys that existed at transaction start.
pub async fn test_snapshot_isolation_iterator<S: Store + 'static>(store: Arc<S>) {
    // Setup: write 5 keys
    let mut setup = store.new_txn(false).await.unwrap();
    for i in 0..5 {
        setup.set(format!("iter_key_{}", i).as_bytes(), b"original").await.unwrap();
    }
    setup.commit().await.unwrap();

    // Start a reader transaction
    let reader = store.new_txn(true).await.unwrap();

    // Concurrent writer adds more keys and modifies existing
    let mut writer = store.new_txn(false).await.unwrap();
    for i in 5..10 {
        writer.set(format!("iter_key_{}", i).as_bytes(), b"new").await.unwrap();
    }
    writer.set(b"iter_key_0", b"modified").await.unwrap();
    writer.commit().await.unwrap();

    // Iterate with the reader - should only see original 5 keys with original values
    use crate::corekv::IterOptions;
    let opts = IterOptions::new().with_prefix(b"iter_key_".to_vec());
    let mut iter = reader.iterator(opts).await.unwrap();

    let mut count = 0;
    let mut saw_modified = false;
    while let Some(kv) = iter.next().await.unwrap() {
        count += 1;
        if kv.value_bytes() == b"modified" || kv.value_bytes() == b"new" {
            saw_modified = true;
        }
    }

    assert_eq!(count, 5, "Iterator should only see 5 original keys");
    assert!(
        !saw_modified,
        "Iterator should not see any modified/new values"
    );
}

// ============================================================================
// MACRO FOR RUNNING ALL TESTS
// ============================================================================

/// Macro to generate test functions for a specific store type
#[macro_export]
macro_rules! generate_backend_tests {
    ($store_fn:expr) => {
        use $crate::backends::test_suite;

        #[tokio::test]
        async fn shared_test_basic_set_get() {
            let store = $store_fn().await;
            test_suite::test_basic_set_get(&store).await;
        }

        #[tokio::test]
        async fn shared_test_delete() {
            let store = $store_fn().await;
            test_suite::test_delete(&store).await;
        }

        #[tokio::test]
        async fn shared_test_has() {
            let store = $store_fn().await;
            test_suite::test_has(&store).await;
        }

        #[tokio::test]
        async fn shared_test_read_your_writes() {
            let store = $store_fn().await;
            test_suite::test_read_your_writes(&store).await;
        }

        #[tokio::test]
        async fn shared_test_empty_key_rejected() {
            let store = $store_fn().await;
            test_suite::test_empty_key_rejected(&store).await;
        }

        #[tokio::test]
        async fn shared_test_readonly_transaction() {
            let store = $store_fn().await;
            test_suite::test_readonly_transaction(&store).await;
        }

        #[tokio::test]
        async fn shared_test_closed_store_rejected() {
            let store = $store_fn().await;
            test_suite::test_closed_store_rejected(&store).await;
        }

        #[tokio::test]
        async fn shared_test_discard_prevents_persistence() {
            let store = $store_fn().await;
            test_suite::test_discard_prevents_persistence(&store).await;
        }

        #[tokio::test]
        async fn shared_test_success_callback() {
            let store = $store_fn().await;
            test_suite::test_success_callback(&store).await;
        }

        #[tokio::test]
        async fn shared_test_discard_callback() {
            let store = $store_fn().await;
            test_suite::test_discard_callback(&store).await;
        }

        #[tokio::test]
        async fn shared_test_async_success_callback() {
            let store = $store_fn().await;
            test_suite::test_async_success_callback(&store).await;
        }

        #[tokio::test]
        async fn shared_test_callback_panic_safety() {
            let store = $store_fn().await;
            test_suite::test_callback_panic_safety(&store).await;
        }

        #[tokio::test]
        async fn shared_test_async_callback_panic_safety() {
            let store = $store_fn().await;
            test_suite::test_async_callback_panic_safety(&store).await;
        }

        #[tokio::test]
        async fn shared_test_discard_callback_panic_safety() {
            let store = $store_fn().await;
            test_suite::test_discard_callback_panic_safety(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_basic() {
            let store = $store_fn().await;
            test_suite::test_iterator_basic(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_prefix() {
            let store = $store_fn().await;
            test_suite::test_iterator_prefix(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_reverse() {
            let store = $store_fn().await;
            test_suite::test_iterator_reverse(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_range() {
            let store = $store_fn().await;
            test_suite::test_iterator_range(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_keys_only() {
            let store = $store_fn().await;
            test_suite::test_iterator_keys_only(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_closed() {
            let store = $store_fn().await;
            test_suite::test_iterator_closed(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_empty_store() {
            let store = $store_fn().await;
            test_suite::test_iterator_empty_store(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_start_equals_end() {
            let store = $store_fn().await;
            test_suite::test_iterator_start_equals_end(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_prefix_no_match() {
            let store = $store_fn().await;
            test_suite::test_iterator_prefix_no_match(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_sees_pending_writes() {
            let store = $store_fn().await;
            test_suite::test_iterator_sees_pending_writes(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_sees_pending_deletes() {
            let store = $store_fn().await;
            test_suite::test_iterator_sees_pending_deletes(&store).await;
        }

        #[tokio::test]
        async fn shared_test_iterator_reverse_with_prefix() {
            let store = $store_fn().await;
            test_suite::test_iterator_reverse_with_prefix(&store).await;
        }
    };
}

/// Macro for concurrency tests that need Arc<Store>
/// NOTE: This macro assumes generate_backend_tests! was already called, which imports test_suite
#[macro_export]
macro_rules! generate_backend_concurrency_tests {
    ($arc_store_fn:expr) => {
        #[tokio::test]
        async fn shared_test_concurrent_writes_different_keys() {
            let store = $arc_store_fn().await;
            test_suite::test_concurrent_writes_different_keys(store).await;
        }

        #[tokio::test]
        async fn shared_test_concurrent_writes_same_key() {
            let store = $arc_store_fn().await;
            test_suite::test_concurrent_writes_same_key(store).await;
        }

        #[tokio::test]
        async fn shared_test_last_writer_wins() {
            let store = $arc_store_fn().await;
            test_suite::test_last_writer_wins(store).await;
        }

        #[tokio::test]
        async fn shared_test_last_writer_wins_reverse() {
            let store = $arc_store_fn().await;
            test_suite::test_last_writer_wins_reverse(store).await;
        }

        #[tokio::test]
        async fn shared_test_parallel_stress() {
            let store = $arc_store_fn().await;
            test_suite::test_parallel_stress(store).await;
        }

        #[tokio::test]
        async fn shared_test_snapshot_isolation_concurrent() {
            let store = $arc_store_fn().await;
            test_suite::test_snapshot_isolation_concurrent(store).await;
        }

        #[tokio::test]
        async fn shared_test_snapshot_isolation_long_running_reader() {
            let store = $arc_store_fn().await;
            test_suite::test_snapshot_isolation_long_running_reader(store).await;
        }

        #[tokio::test]
        async fn shared_test_snapshot_isolation_iterator() {
            let store = $arc_store_fn().await;
            test_suite::test_snapshot_isolation_iterator(store).await;
        }
    };
}

pub use generate_backend_tests;
pub use generate_backend_concurrency_tests;
