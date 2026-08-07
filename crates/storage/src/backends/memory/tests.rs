use super::*;
use crate::corekv::{IterOptions, Store};
use std::sync::Arc;

mod chunked_scan;

// ============================================================================
// SHARED TEST SUITE - Run same tests against all backends
// ============================================================================
mod shared_tests {
    use super::*;
    use crate::generate_backend_concurrency_tests;
    use crate::generate_backend_dropable_tests;
    use crate::generate_backend_tests;

    async fn create_store() -> MemoryStore {
        MemoryStore::new()
    }

    async fn create_arc_store() -> Arc<MemoryStore> {
        Arc::new(MemoryStore::new())
    }

    // Generate all standard backend tests
    generate_backend_tests!(create_store);

    // Generate concurrency tests
    generate_backend_concurrency_tests!(create_arc_store);

    // Generate Dropable tests (MemoryStore implements Dropable)
    generate_backend_dropable_tests!(create_store);
}

// ============================================================================
// MEMORY-SPECIFIC TESTS - Tests unique to MemoryStore behavior
// ============================================================================
mod memory_specific_tests {
    use super::*;

    /// Test MVCC snapshot isolation
    ///
    /// MemoryStore provides true MVCC snapshot isolation where readers
    /// get a snapshot at transaction start and never see concurrent commits.
    #[tokio::test]
    async fn test_memory_snapshot_isolation() {
        let store = Arc::new(MemoryStore::new());

        // Setup: write initial value
        let mut setup_txn = store.new_txn(false).await.unwrap();
        setup_txn.set(b"key", b"initial_value").await.unwrap();
        setup_txn.commit().await.unwrap();

        // Start reader transaction (gets snapshot with initial value)
        let reader = store.new_txn(true).await.unwrap();

        // Concurrent writer modifies and commits
        let mut writer = store.new_txn(false).await.unwrap();
        writer.set(b"key", b"modified_value").await.unwrap();
        writer.commit().await.unwrap();

        // Reader should STILL see initial value (true MVCC snapshot isolation)
        assert_eq!(
            reader.get(b"key").await.unwrap(),
            Some(b"initial_value".to_vec()),
            "MemoryStore reader should maintain snapshot isolation"
        );

        // New reader sees committed value
        let new_reader = store.new_txn(true).await.unwrap();
        assert_eq!(
            new_reader.get(b"key").await.unwrap(),
            Some(b"modified_value".to_vec())
        );
    }

    /// Test concurrent delete with snapshot isolation
    #[tokio::test]
    async fn test_memory_snapshot_preserves_deleted_keys() {
        let store = Arc::new(MemoryStore::new());

        // Setup
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"to_delete", b"exists").await.unwrap();
        txn.commit().await.unwrap();

        // Reader starts (snapshot has the key)
        let reader = store.new_txn(true).await.unwrap();

        // Deleter runs concurrently
        let mut deleter = store.new_txn(false).await.unwrap();
        deleter.delete(b"to_delete").await.unwrap();
        deleter.commit().await.unwrap();

        // Reader should still see the key (snapshot isolation)
        assert_eq!(
            reader.get(b"to_delete").await.unwrap(),
            Some(b"exists".to_vec()),
            "Reader snapshot should preserve deleted key"
        );

        // New transaction sees deletion
        let new_txn = store.new_txn(true).await.unwrap();
        assert_eq!(
            new_txn.get(b"to_delete").await.unwrap(),
            None,
            "New transaction should see deletion"
        );
    }

    /// Stress test: 50 parallel transactions
    #[tokio::test]
    async fn test_memory_parallel_commits_stress() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let store = Arc::new(MemoryStore::new());
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

        // Verify all 50 keys exist
        let txn = store.new_txn(true).await.unwrap();
        for i in 0..50 {
            assert!(
                txn.has(format!("stress_key_{}", i).as_bytes())
                    .await
                    .unwrap(),
                "Key {} should exist after concurrent commits",
                i
            );
        }
    }

    #[tokio::test]
    async fn test_memory_long_lived_create_cannot_overwrite_committed_document() {
        let store = Arc::new(MemoryStore::new());

        let mut stale = store.new_txn(false).await.unwrap();
        assert_eq!(stale.get(b"/seq/doc").await.unwrap(), None);

        let mut winner = store.new_txn(false).await.unwrap();
        winner.set(b"/seq/doc", &1_u64.to_be_bytes()).await.unwrap();
        winner.set(b"/d/s/1", b"winner").await.unwrap();
        winner.set(b"/d/p/winner", b"1").await.unwrap();
        winner.set(b"/data/1", b"winner-body").await.unwrap();
        winner.commit().await.unwrap();

        for i in 0..1000 {
            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(format!("unrelated-{i}").as_bytes(), b"value")
                .await
                .unwrap();
            txn.commit().await.unwrap();
        }

        stale.set(b"/seq/doc", &1_u64.to_be_bytes()).await.unwrap();
        stale.set(b"/d/s/1", b"stale").await.unwrap();
        stale.set(b"/d/p/stale", b"1").await.unwrap();
        stale.set(b"/data/1", b"stale-body").await.unwrap();
        assert!(matches!(
            stale.commit().await,
            Err(crate::corekv::Error::TxnConflict)
        ));

        let reader = store.new_txn(true).await.unwrap();
        assert_eq!(
            reader.get(b"/d/s/1").await.unwrap(),
            Some(b"winner".to_vec())
        );
        assert_eq!(
            reader.get(b"/data/1").await.unwrap(),
            Some(b"winner-body".to_vec())
        );
    }
}
