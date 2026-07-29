use crate::corekv::{Error, IterOptions, Store, Txn};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Test concurrent writes to different keys (should all succeed)
pub async fn test_concurrent_writes_different_keys<S: Store + 'static>(store: Arc<S>) {
    let commit_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for i in 0..20 {
        let store = store.clone();
        let commit_count = commit_count.clone();
        handles.push(tokio::spawn(async move {
            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(format!("key_{}", i).as_bytes(), b"value")
                .await
                .unwrap();
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

async fn replace_collection_heads(txn: &mut Box<dyn Txn>, child: &[u8]) {
    let mut iterator = txn
        .iterator(
            IterOptions::new()
                .with_prefix(b"h/c/7/".to_vec())
                .with_commutative_set(),
        )
        .await
        .unwrap();
    let mut old_heads = Vec::new();
    while let Some(pair) = iterator.next().await.unwrap() {
        old_heads.push(pair.key);
    }
    iterator.close().await.unwrap();

    for old_head in old_heads {
        txn.delete(&old_head).await.unwrap();
    }
    txn.set(child, b"2").await.unwrap();
}

/// Test concurrent observed-remove/add transitions on a shared set prefix.
pub async fn test_commutative_set_transitions<S: Store + 'static>(store: Arc<S>) {
    let mut seed = store.new_txn(false).await.unwrap();
    seed.set(b"h/c/7/root", b"1").await.unwrap();
    seed.commit().await.unwrap();

    let mut first = store.new_txn(false).await.unwrap();
    let mut second = store.new_txn(false).await.unwrap();
    replace_collection_heads(&mut first, b"h/c/7/first").await;
    replace_collection_heads(&mut second, b"h/c/7/second").await;

    first.commit().await.unwrap();
    second.commit().await.unwrap();

    let read = store.new_txn(true).await.unwrap();
    let mut iterator = read
        .iterator(IterOptions::new().with_prefix(b"h/c/7/".to_vec()))
        .await
        .unwrap();
    let mut heads = Vec::new();
    while let Some(pair) = iterator.next().await.unwrap() {
        heads.push(pair.key);
    }
    iterator.close().await.unwrap();
    heads.sort();
    assert_eq!(
        heads,
        vec![b"h/c/7/first".to_vec(), b"h/c/7/second".to_vec()]
    );
}

/// Test concurrent writes to the SAME key (last writer wins)
pub async fn test_concurrent_writes_same_key<S: Store + 'static>(store: Arc<S>) {
    let success_count = Arc::new(AtomicUsize::new(0));
    let conflict_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for i in 0..10 {
        let store = store.clone();
        let success_count = success_count.clone();
        let conflict_count = conflict_count.clone();
        handles.push(tokio::spawn(async move {
            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(b"contended_key", format!("value_{}", i).as_bytes())
                .await
                .unwrap();
            match txn.commit().await {
                Ok(()) => {
                    success_count.fetch_add(1, Ordering::SeqCst);
                }
                Err(Error::TxnConflict) => {
                    conflict_count.fetch_add(1, Ordering::SeqCst);
                }
                Err(e) => panic!("unexpected error: {}", e),
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // At least one commit should succeed, others may conflict
    let successes = success_count.load(Ordering::SeqCst);
    let conflicts = conflict_count.load(Ordering::SeqCst);
    assert!(successes >= 1, "At least one commit should succeed");
    assert_eq!(
        successes + conflicts,
        10,
        "All transactions should complete"
    );

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

    // First commit succeeds, second detects conflict
    txn1.commit().await.unwrap();
    let result = txn2.commit().await;
    assert!(result.is_err(), "Second commit should fail with conflict");
    assert!(
        matches!(result.unwrap_err(), Error::TxnConflict),
        "Error should be TxnConflict"
    );

    let txn = store.new_txn(true).await.unwrap();
    assert_eq!(
        txn.get(b"shared").await.unwrap(),
        Some(b"from_txn1".to_vec()),
        "First commit should win (second conflicted)"
    );
}

/// Test reverse commit order - second commit conflicts
pub async fn test_last_writer_wins_reverse<S: Store + 'static>(store: Arc<S>) {
    let mut txn1 = store.new_txn(false).await.unwrap();
    let mut txn2 = store.new_txn(false).await.unwrap();

    txn1.set(b"shared", b"from_txn1").await.unwrap();
    txn2.set(b"shared", b"from_txn2").await.unwrap();

    // Reverse order: txn2 commits first, txn1 conflicts
    txn2.commit().await.unwrap();
    let result = txn1.commit().await;
    assert!(result.is_err(), "Second commit should fail with conflict");
    assert!(
        matches!(result.unwrap_err(), Error::TxnConflict),
        "Error should be TxnConflict"
    );

    let txn = store.new_txn(true).await.unwrap();
    assert_eq!(
        txn.get(b"shared").await.unwrap(),
        Some(b"from_txn2".to_vec()),
        "First commit should win (second conflicted)"
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

            // Retry on TxnConflict since all writers target the same key
            loop {
                let mut writer = store.new_txn(false).await.unwrap();
                writer
                    .set(b"snapshot_key", format!("writer_{}", i).as_bytes())
                    .await
                    .unwrap();
                match writer.commit().await {
                    Ok(()) => break,
                    Err(crate::corekv::Error::TxnConflict) => continue,
                    Err(e) => panic!("Unexpected commit error: {:?}", e),
                }
            }
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
    assert_eq!(
        total_checks.load(Ordering::SeqCst),
        10,
        "All readers should complete"
    );
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
        writer
            .set(b"long_read_key", format!("write_{}", i).as_bytes())
            .await
            .unwrap();
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

/// Test write-write isolation - writers don't see each other's uncommitted data.
///
/// Two concurrent writers should not see each other's pending changes.
/// Only committed data should be visible to new transactions.
pub async fn test_write_write_isolation<S: Store + 'static>(store: Arc<S>) {
    // Setup initial state
    let mut setup = store.new_txn(false).await.unwrap();
    setup.set(b"shared_key", b"initial").await.unwrap();
    setup.commit().await.unwrap();

    // Start two writer transactions
    let mut writer1 = store.new_txn(false).await.unwrap();
    let mut writer2 = store.new_txn(false).await.unwrap();

    // Writer1 makes a change (uncommitted)
    writer1.set(b"shared_key", b"from_writer1").await.unwrap();
    writer1.set(b"writer1_only", b"exclusive").await.unwrap();

    // Writer2 should NOT see writer1's uncommitted changes
    assert_eq!(
        writer2.get(b"shared_key").await.unwrap(),
        Some(b"initial".to_vec()),
        "Writer2 should see original value, not writer1's uncommitted change"
    );
    assert_eq!(
        writer2.get(b"writer1_only").await.unwrap(),
        None,
        "Writer2 should not see writer1's uncommitted new key"
    );

    // Writer2 makes its own changes
    writer2.set(b"shared_key", b"from_writer2").await.unwrap();
    writer2.set(b"writer2_only", b"exclusive").await.unwrap();

    // Writer1 should NOT see writer2's uncommitted changes
    assert_eq!(
        writer1.get(b"writer2_only").await.unwrap(),
        None,
        "Writer1 should not see writer2's uncommitted new key"
    );

    // Commit writer1 first
    writer1.commit().await.unwrap();

    // Writer2 still shouldn't see writer1's committed changes (snapshot isolation)
    assert_eq!(
        writer2.get(b"shared_key").await.unwrap(),
        Some(b"from_writer2".to_vec()), // Its own pending write
        "Writer2 should see its own write, not writer1's commit"
    );

    // Commit writer2 - conflicts on shared_key which writer1 already committed
    let result = writer2.commit().await;
    assert!(
        result.is_err(),
        "Writer2 commit should fail due to conflict on shared_key"
    );
    assert!(
        matches!(result.unwrap_err(), Error::TxnConflict),
        "Error should be TxnConflict"
    );

    // New transaction should see writer1's state (writer2 was rejected)
    let reader = store.new_txn(true).await.unwrap();
    assert_eq!(
        reader.get(b"shared_key").await.unwrap(),
        Some(b"from_writer1".to_vec()),
        "Final value should be from writer1 (writer2 conflicted)"
    );
    assert_eq!(
        reader.get(b"writer1_only").await.unwrap(),
        Some(b"exclusive".to_vec()),
        "writer1_only key should exist"
    );
    assert_eq!(
        reader.get(b"writer2_only").await.unwrap(),
        None,
        "writer2_only key should not exist (writer2 conflicted)"
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
        setup
            .set(format!("iter_key_{}", i).as_bytes(), b"original")
            .await
            .unwrap();
    }
    setup.commit().await.unwrap();

    // Start a reader transaction
    let reader = store.new_txn(true).await.unwrap();

    // Concurrent writer adds more keys and modifies existing
    let mut writer = store.new_txn(false).await.unwrap();
    for i in 5..10 {
        writer
            .set(format!("iter_key_{}", i).as_bytes(), b"new")
            .await
            .unwrap();
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
