use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use super::config::FjallStoreOptions;
use super::transaction::FjallTxn;
use crate::backends::shared::{CallbackManager, ConflictTracker};
use crate::corekv::{Dropable, Error, Result, Store, Txn};

/// Fjall-backed key-value store (LSM-tree).
///
/// This store wraps a fjall Database and Keyspace, providing concurrent
/// write access without a global write lock (unlike redb's COW B+tree).
///
/// # Active Transaction Tracking
///
/// The store tracks the number of active transactions. When closing, the store
/// will reject new transactions and wait for existing ones to complete.
pub struct FjallStore {
    db: fjall::Database,
    keyspace: fjall::Keyspace,
    closed: AtomicBool,
    conflict_tracker: Arc<ConflictTracker>,
    /// Read-locks pair versions with snapshots; write-locks pair conflict
    /// publication with physical commits.
    commit_gate: Arc<tokio::sync::RwLock<()>>,
    db_path: std::path::PathBuf,
    active_txn_count: Arc<AtomicUsize>,
    close_timeout: std::time::Duration,
    durability: crate::backends::shared::DurabilityMode,
}

impl FjallStore {
    /// Open a fjall database at the specified path with default options.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_options(path, FjallStoreOptions::default())
    }

    /// Open a fjall database at the specified path with custom options.
    pub fn open_with_options<P: AsRef<Path>>(path: P, opts: FjallStoreOptions) -> Result<Self> {
        let path = path.as_ref();
        let db_path = if path.extension().is_some() {
            path.parent().unwrap_or(path).join("data.fjall")
        } else {
            path.join("data.fjall")
        };

        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                Error::Backend(format!(
                    "failed to create directory '{}': {}",
                    parent.display(),
                    e
                ))
            })?;
        }

        let mut builder = fjall::Database::builder(&db_path)
            .cache_size(opts.cache_size())
            .max_journaling_size(opts.max_journal_size());

        if opts.worker_threads() > 0 {
            builder = builder.worker_threads(opts.worker_threads());
        }

        let db = builder.open().map_err(|e| {
            tracing::error!(
                db_path = %db_path.display(),
                error = %e,
                "Failed to open fjall database"
            );
            let err: Error = e.into();
            err
        })?;

        let l0_threshold = opts.l0_threshold();
        let max_memtable_size = opts.max_memtable_size();
        let kv_separation = opts.kv_separation();
        let keyspace = db
            .keyspace("kv", move || {
                let mut ks_opts = fjall::KeyspaceCreateOptions::default()
                    .max_memtable_size(max_memtable_size)
                    .compaction_strategy(Arc::new(
                        fjall::compaction::Leveled::default().with_l0_threshold(l0_threshold),
                    ));

                if kv_separation {
                    ks_opts = ks_opts.with_kv_separation(Some(
                        fjall::KvSeparationOptions::default().separation_threshold(256),
                    ));
                }

                ks_opts
            })
            .map_err(|e| {
                tracing::error!(error = %e, "Failed to create/open fjall keyspace");
                let err: Error = e.into();
                err
            })?;

        let is_separated = keyspace.is_kv_separated();
        tracing::info!(
            kv_separated = is_separated,
            kv_separation_requested = kv_separation,
            db_path = %db_path.display(),
            "Fjall keyspace opened"
        );

        if kv_separation && !is_separated {
            return Err(Error::Backend(format!(
                "KV separation requested but keyspace at '{}' was created without it. \
                 Delete the data directory and restart, or set kv_separation=false.",
                db_path.display()
            )));
        }

        Ok(Self {
            db,
            keyspace,
            closed: AtomicBool::new(false),
            conflict_tracker: Arc::new(ConflictTracker::new()),
            commit_gate: Arc::new(tokio::sync::RwLock::new(())),
            db_path,
            active_txn_count: Arc::new(AtomicUsize::new(0)),
            close_timeout: opts.close_timeout(),
            durability: opts.durability(),
        })
    }

    /// Get the database file path.
    pub fn db_path(&self) -> &std::path::Path {
        &self.db_path
    }

    /// Get the current count of active transactions.
    pub fn active_transaction_count(&self) -> usize {
        self.active_txn_count.load(Ordering::Acquire)
    }

    /// Returns true if the underlying keyspace uses KV separation (blob storage).
    pub fn is_kv_separated(&self) -> bool {
        self.keyspace.is_kv_separated()
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

impl crate::corekv::private::Sealed for FjallStore {}

#[async_trait]
impl Store for FjallStore {
    async fn new_txn(&self, readonly: bool) -> Result<Box<dyn Txn>> {
        // CAS-based TOCTOU protection: increment count, then verify not closed.
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::DBClosed);
        }
        self.active_txn_count.fetch_add(1, Ordering::AcqRel);
        if self.closed.load(Ordering::Acquire) {
            self.active_txn_count.fetch_sub(1, Ordering::AcqRel);
            return Err(Error::DBClosed);
        }

        // Guard to decrement count on panic or early return
        struct NewTxnGuard<'a>(&'a AtomicUsize, bool);
        impl Drop for NewTxnGuard<'_> {
            fn drop(&mut self) {
                if !self.1 {
                    self.0.fetch_sub(1, Ordering::AcqRel);
                }
            }
        }
        let mut guard = NewTxnGuard(&self.active_txn_count, false);

        // Pair the published conflict version with the Fjall snapshot. A
        // pending physical write may already be visible, but its reservation
        // remains a conservative conflict until publication. Read-only
        // transactions never conflict-check and skip this gate.
        let _commit_guard = if readonly {
            None
        } else {
            Some(self.commit_gate.read().await)
        };
        let conflict_snapshot = (!readonly).then(|| self.conflict_tracker.begin_snapshot());
        let read_version = conflict_snapshot.as_ref().map_or_else(
            || self.conflict_tracker.current_version(),
            |snapshot| snapshot.version(),
        );
        let snapshot = self.db.snapshot();

        // Defuse guard — transaction will manage its own count via Drop
        guard.1 = true;

        Ok(Box::new(FjallTxn {
            db: self.db.clone(),
            keyspace: self.keyspace.clone(),
            conflict_tracker: Arc::clone(&self.conflict_tracker),
            _conflict_snapshot: conflict_snapshot,
            commit_gate: Arc::clone(&self.commit_gate),
            active_txn_count: Arc::clone(&self.active_txn_count),
            read_version,
            snapshot,
            pending: Mutex::new(BTreeMap::new()),
            read_set: Mutex::new(crate::backends::shared::ReadSet::default()),
            readonly,
            discarded: AtomicBool::new(false),
            committed: AtomicBool::new(false),
            callbacks: CallbackManager::new(),
            durability: self.durability,
        }))
    }

    async fn close(&self) -> Result<()> {
        // Swap closed to true; if already true, another close() won.
        if self.closed.swap(true, Ordering::Release) {
            return Ok(());
        }

        let active = self.active_txn_count.load(Ordering::Acquire);
        if active > 0 {
            tracing::info!(
                active_transactions = active,
                db_path = %self.db_path.display(),
                "Store closing with active transactions - waiting for completion"
            );

            let start = std::time::Instant::now();
            let timeout = self.close_timeout;
            while self.active_txn_count.load(Ordering::Acquire) > 0 {
                if start.elapsed() > timeout {
                    let remaining = self.active_txn_count.load(Ordering::Acquire);
                    tracing::error!(
                        remaining_transactions = remaining,
                        timeout_secs = timeout.as_secs(),
                        db_path = %self.db_path.display(),
                        "Failed to close store - transactions still active after timeout"
                    );
                    return Err(Error::Other(format!(
                        "Close timeout: {} transaction(s) still active after {}s (db: {})",
                        remaining,
                        timeout.as_secs(),
                        self.db_path.display()
                    )));
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Dropable for FjallStore {
    async fn drop_all(&self) -> Result<()> {
        if self.is_closed() {
            return Err(Error::DBClosed);
        }

        self.keyspace.clear().map_err(|e| {
            tracing::error!(error = %e, "Failed to clear fjall keyspace");
            let err: Error = e.into();
            err
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod pairing_tests {
    use super::*;
    use crate::backends::shared::ReadSet;
    use crate::corekv::{Reader, Writer};
    use fjall::Readable;
    use std::time::Duration;

    fn physical_value(store: &FjallStore, key: &[u8]) -> Option<Vec<u8>> {
        store
            .db
            .snapshot()
            .get(&store.keyspace, key)
            .unwrap()
            .map(|value| value.to_vec())
    }

    #[tokio::test]
    async fn snapshot_waits_while_successful_commit_is_published() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let store = Arc::new(FjallStore::open(temp_dir.path()).unwrap());
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
        let mut batch = store.db.batch();
        batch.insert(&store.keyspace, key.as_slice(), value.as_slice());
        batch.commit().unwrap();
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
    async fn physical_write_does_not_wait_for_snapshot_pairing() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let store = Arc::new(FjallStore::open(temp_dir.path()).unwrap());
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

        tokio::time::timeout(Duration::from_secs(1), async {
            while physical_value(&store, &key).is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("physical write remained blocked by snapshot pairing");
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut commit_task)
                .await
                .is_err(),
            "commit completed before its conflict version was published"
        );

        drop(snapshot_guard);
        tokio::time::timeout(Duration::from_secs(1), commit_task)
            .await
            .expect("commit remained blocked after publication gate release")
            .expect("commit task panicked")
            .expect("commit failed");
        assert_eq!(physical_value(&store, &key), Some(b"committed".to_vec()));
    }

    #[tokio::test]
    async fn readonly_txn_skips_commit_gate() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let store = Arc::new(FjallStore::open(temp_dir.path()).unwrap());
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
    async fn commit_conflict_checks_against_pinned_records() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let store = Arc::new(FjallStore::open(temp_dir.path()).unwrap());
        let key = b"contended-key".to_vec();

        // Txn A snapshots at version 0 and stages a write to `key`.
        let mut txn_a = store.new_txn(false).await.unwrap();
        txn_a.set(&key, b"stale-A").await.unwrap();

        // Txn B commits a write to the same key -> version 1 recorded.
        let mut txn_b = store.new_txn(false).await.unwrap();
        txn_b.set(&key, b"committed-B").await.unwrap();
        txn_b.commit().await.unwrap();
        assert_eq!(physical_value(&store, &key), Some(b"committed-B".to_vec()));

        let error = txn_a.commit().await.unwrap_err();
        assert!(
            error.is_txn_conflict(),
            "expected TxnConflict, got: {error}"
        );

        assert_eq!(
            physical_value(&store, &key),
            Some(b"committed-B".to_vec()),
            "cancelled commit overwrote a conflicting committed write"
        );
    }

    #[tokio::test]
    async fn cancelling_snapshot_wait_does_not_leak_active_transaction_count() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let store = Arc::new(FjallStore::open(temp_dir.path()).unwrap());
        let gate = Arc::clone(&store.commit_gate);
        let commit_guard = gate.write().await;

        let snapshot_store = Arc::clone(&store);
        let snapshot_task = tokio::spawn(async move { snapshot_store.new_txn(false).await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while store.active_txn_count.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("snapshot task did not reach the commit gate");

        snapshot_task.abort();
        match snapshot_task.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(_) => panic!("snapshot task completed instead of being cancelled"),
        }
        drop(commit_guard);
        assert_eq!(store.active_txn_count.load(Ordering::Acquire), 0);
    }
}
