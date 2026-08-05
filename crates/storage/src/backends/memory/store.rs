use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

use super::transaction::MemoryTxn;
use crate::backends::shared::{CallbackManager, ConflictTracker};
use crate::corekv::{Dropable, Error, Result, Store, Txn};

/// In-memory key-value store using BTreeMap.
///
/// Data is stored in a BTreeMap wrapped in Arc<RwLock<>> for thread-safe
/// concurrent access. The store provides snapshot isolation for transactions
/// with optimistic write-write conflict detection.
#[derive(Clone)]
pub struct MemoryStore {
    data: Arc<RwLock<BTreeMap<Vec<u8>, Vec<u8>>>>,
    closed: Arc<AtomicBool>,
    conflict_tracker: Arc<ConflictTracker>,
    /// Read-locks pair versions with snapshots; write-locks pair conflict
    /// publication with physical commits.
    commit_gate: Arc<RwLock<()>>,
}

impl MemoryStore {
    /// Create a new empty memory store.
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(BTreeMap::new())),
            closed: Arc::new(AtomicBool::new(false)),
            conflict_tracker: Arc::new(ConflictTracker::for_backend("memory")),
            commit_gate: Arc::new(RwLock::new(())),
        }
    }

    /// Check if the store is closed.
    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl crate::corekv::private::Sealed for MemoryStore {}

#[async_trait]
impl Store for MemoryStore {
    fn transaction_stats_handle(&self) -> Option<crate::backends::TransactionStatsHandle> {
        Some(self.conflict_tracker.stats_handle())
    }

    async fn new_txn(&self, readonly: bool) -> Result<Box<dyn Txn>> {
        if self.is_closed() {
            return Err(Error::DBClosed);
        }

        // Read-only transactions skip the gate: they never conflict-check,
        // and the data read-lock below is what guarantees the snapshot is
        // not torn by an in-flight commit's mutation.
        let _commit_guard = if readonly {
            None
        } else {
            Some(self.commit_gate.read().await)
        };
        let data = self.data.read().await;
        let conflict_snapshot = (!readonly).then(|| self.conflict_tracker.begin_snapshot());
        let read_version = conflict_snapshot.as_ref().map_or_else(
            || self.conflict_tracker.current_version(),
            |snapshot| snapshot.version(),
        );
        let snapshot = data.clone();

        Ok(Box::new(MemoryTxn {
            store: Arc::clone(&self.data),
            conflict_tracker: Arc::clone(&self.conflict_tracker),
            _conflict_snapshot: conflict_snapshot,
            commit_gate: Arc::clone(&self.commit_gate),
            read_version,
            snapshot,
            pending: Mutex::new(BTreeMap::new()),
            read_set: Mutex::new(crate::backends::shared::ReadSet::default()),
            readonly,
            discarded: AtomicBool::new(false),
            committed: AtomicBool::new(false),
            callbacks: CallbackManager::new(),
        }))
    }

    async fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::Release);
        Ok(())
    }
}

#[async_trait]
impl Dropable for MemoryStore {
    async fn drop_all(&self) -> Result<()> {
        if self.is_closed() {
            return Err(Error::DBClosed);
        }

        // Clear all data
        let mut data = self.data.write().await;
        data.clear();
        Ok(())
    }
}

#[cfg(test)]
mod pairing_tests {
    use super::*;
    use crate::backends::shared::ReadSet;
    use crate::corekv::{Reader, Writer};
    use std::time::Duration;

    #[tokio::test]
    async fn snapshot_waits_while_successful_commit_is_published() {
        let store = Arc::new(MemoryStore::new());
        let key = b"paired-snapshot".to_vec();
        let value = b"committed".to_vec();

        let gate = Arc::clone(&store.commit_gate);
        let commit_guard = gate.write().await;
        let reservation = store
            .conflict_tracker
            .reserve(
                store.conflict_tracker.current_version(),
                std::slice::from_ref(&key).iter(),
                &ReadSet::default(),
            )
            .unwrap();
        store.data.write().await.insert(key.clone(), value.clone());
        reservation.publish();

        let snapshot_store = Arc::clone(&store);
        let mut snapshot_task = tokio::spawn(async move { snapshot_store.new_txn(false).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut snapshot_task)
                .await
                .is_err(),
            "new transaction took a snapshot during version publication"
        );

        drop(commit_guard);

        let snapshot = tokio::time::timeout(Duration::from_secs(1), snapshot_task)
            .await
            .expect("snapshot remained blocked after commit")
            .expect("snapshot task panicked")
            .expect("snapshot creation failed");
        assert_eq!(snapshot.get(&key).await.unwrap(), Some(value));
    }

    #[tokio::test]
    async fn physical_write_waits_for_snapshot_pairing() {
        let store = Arc::new(MemoryStore::new());
        let key = b"paired-commit".to_vec();
        let mut writer = store.new_txn(false).await.unwrap();
        writer.set(&key, b"committed").await.unwrap();

        let gate = Arc::clone(&store.commit_gate);
        let snapshot_guard = gate.read().await;
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let mut commit_task = tokio::spawn(async move {
            started_tx.send(()).unwrap();
            writer.commit().await
        });
        started_rx.await.unwrap();

        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut commit_task)
                .await
                .is_err(),
            "physical commit did not wait for snapshot pairing"
        );
        assert!(!store.data.read().await.contains_key(&key));

        drop(snapshot_guard);
        tokio::time::timeout(Duration::from_secs(1), commit_task)
            .await
            .expect("commit remained blocked after snapshot pairing")
            .expect("commit task panicked")
            .expect("commit failed");
        assert_eq!(
            store.data.read().await.get(&key).cloned(),
            Some(b"committed".to_vec())
        );
    }

    #[tokio::test]
    async fn readonly_txn_skips_commit_gate() {
        let store = Arc::new(MemoryStore::new());
        let gate = Arc::clone(&store.commit_gate);
        let commit_guard = gate.write().await;

        // A read-only transaction must not queue behind an in-flight commit.
        let readonly = tokio::time::timeout(Duration::from_secs(1), store.new_txn(true))
            .await
            .expect("read-only transaction blocked behind the commit gate")
            .expect("read-only transaction failed");
        drop(readonly);

        // Writers still pair version and snapshot behind the gate.
        let writer_store = Arc::clone(&store);
        let mut writer_task = tokio::spawn(async move { writer_store.new_txn(false).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut writer_task)
                .await
                .is_err(),
            "write transaction skipped the commit gate"
        );

        drop(commit_guard);
        tokio::time::timeout(Duration::from_secs(1), writer_task)
            .await
            .expect("write transaction remained blocked after gate release")
            .expect("writer task panicked")
            .expect("write transaction failed");
    }

    #[tokio::test]
    async fn transaction_stats_capture_commit_gate_waits() {
        let store = MemoryStore::new();
        let handle = store.transaction_stats_handle().unwrap();
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"key", b"value").await.unwrap();
        txn.commit().await.unwrap();

        let stats = handle.snapshot();
        assert_eq!(stats.backend, "memory");
        assert_eq!(stats.commit_gate_waits, 1);
        assert_eq!(stats.tracker.current_version, 1);
    }

    #[tokio::test]
    async fn cancelling_commit_before_physical_lock_does_not_advance_version() {
        let store = Arc::new(MemoryStore::new());
        let mut writer = store.new_txn(false).await.unwrap();
        writer.set(b"cancelled", b"value").await.unwrap();

        let data_guard = store.data.write().await;
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let commit_task = tokio::spawn(async move {
            started_tx.send(()).unwrap();
            writer.commit().await
        });
        started_rx.await.unwrap();

        let gate = Arc::clone(&store.commit_gate);
        assert!(
            tokio::time::timeout(Duration::from_millis(25), gate.read())
                .await
                .is_err(),
            "commit did not reach the physical data lock"
        );

        commit_task.abort();
        match commit_task.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(_) => panic!("commit completed instead of being cancelled"),
        }
        drop(data_guard);

        assert_eq!(store.conflict_tracker.current_version(), 0);
        assert!(!store
            .data
            .read()
            .await
            .contains_key(b"cancelled".as_slice()));
    }
}
