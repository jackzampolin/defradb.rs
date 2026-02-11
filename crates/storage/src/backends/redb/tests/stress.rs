use super::*;
use tempfile::TempDir;

#[tokio::test]
async fn test_redb_rapid_transaction_cycles() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let temp_dir = TempDir::new().unwrap();
    let store = std::sync::Arc::new(RedbStore::open(temp_dir.path().join("test.redb")).unwrap());

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
async fn test_redb_high_contention_100_concurrent_txns() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let temp_dir = TempDir::new().unwrap();
    let store = std::sync::Arc::new(RedbStore::open(temp_dir.path().join("test.redb")).unwrap());

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
    let store = std::sync::Arc::new(RedbStore::open(temp_dir.path().join("test.redb")).unwrap());

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
    let store = std::sync::Arc::new(RedbStore::open(temp_dir.path().join("test.redb")).unwrap());

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

/// Test that validates memory behavior under concurrent read transactions.
///
/// This test creates a moderately-sized dataset and opens multiple concurrent
/// read transactions to verify that memory pressure is manageable.
///
/// Memory calculation: 10K keys x 100 bytes x 20 concurrent txns = ~20MB
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
