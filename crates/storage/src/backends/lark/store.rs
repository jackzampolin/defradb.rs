use async_trait::async_trait;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use super::config::LarkStoreOptions;
use super::transaction::LarkTxn;
use crate::backends::shared::{ConflictTracker, DurabilityMode};
use crate::corekv::{Dropable, Error, Result, Store, Txn};

/// Pure Rust LSM-tree key-value store backed by lark-kv.
pub struct LarkStore {
    db: Arc<lark_kv::Db>,
    closed: AtomicBool,
    conflict_tracker: Arc<ConflictTracker>,
    /// Read-locks pair versions with snapshots; write-locks pair conflict
    /// publication with physical commits.
    commit_gate: Arc<tokio::sync::RwLock<()>>,
    db_path: std::path::PathBuf,
    active_txn_count: Arc<AtomicUsize>,
    close_timeout: std::time::Duration,
    durability: DurabilityMode,
}

impl LarkStore {
    /// Open a Lark database at the specified path with default options.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_options(path, LarkStoreOptions::default())
    }

    /// Open a Lark database at the specified path with custom options.
    pub fn open_with_options<P: AsRef<Path>>(path: P, opts: LarkStoreOptions) -> Result<Self> {
        let path = path.as_ref();
        let db_path = if path.extension().is_some() {
            path.parent().unwrap_or(path).join("data.lark")
        } else {
            path.join("data.lark")
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

        let lark_opts = opts.to_lark_options();
        let db = lark_kv::Db::open(&db_path, lark_opts).map_err(|e| {
            tracing::error!(
                db_path = %db_path.display(),
                error = %e,
                "Failed to open Lark database"
            );
            Error::Backend(format!("failed to open lark db: {}", e))
        })?;

        Ok(Self {
            db: Arc::new(db),
            closed: AtomicBool::new(false),
            conflict_tracker: Arc::new(ConflictTracker::new()),
            commit_gate: Arc::new(tokio::sync::RwLock::new(())),
            db_path,
            active_txn_count: Arc::new(AtomicUsize::new(0)),
            close_timeout: opts.close_timeout(),
            durability: opts.durability(),
        })
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

impl crate::corekv::private::Sealed for LarkStore {}

#[async_trait]
impl Store for LarkStore {
    async fn new_txn(&self, readonly: bool) -> Result<Box<dyn Txn>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::DBClosed);
        }
        self.active_txn_count.fetch_add(1, Ordering::AcqRel);
        if self.closed.load(Ordering::Acquire) {
            self.active_txn_count.fetch_sub(1, Ordering::AcqRel);
            return Err(Error::DBClosed);
        }

        struct NewTxnGuard<'a>(&'a AtomicUsize, bool);
        impl Drop for NewTxnGuard<'_> {
            fn drop(&mut self) {
                if !self.1 {
                    self.0.fetch_sub(1, Ordering::AcqRel);
                }
            }
        }
        let mut guard = NewTxnGuard(&self.active_txn_count, false);

        // Pair the conflict version and lark snapshot without a commit
        // becoming visible between them. Read-only transactions skip the
        // gate: lark now publishes its read horizon only after a batch is
        // applied, so an ungated snapshot can no longer observe a torn
        // batch, and read-only transactions never conflict-check.
        let _commit_guard = if readonly {
            None
        } else {
            Some(self.commit_gate.read().await)
        };
        let txn = LarkTxn::new(
            Arc::clone(&self.db),
            Arc::clone(&self.conflict_tracker),
            Arc::clone(&self.commit_gate),
            Arc::clone(&self.active_txn_count),
            readonly,
            self.durability,
        );

        guard.1 = true;
        Ok(Box::new(txn))
    }

    async fn close(&self) -> Result<()> {
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

        self.db
            .close()
            .map_err(|e| Error::Backend(format!("failed to close lark: {}", e)))?;

        Ok(())
    }
}

#[async_trait]
impl Dropable for LarkStore {
    async fn drop_all(&self) -> Result<()> {
        if self.is_closed() {
            return Err(Error::DBClosed);
        }

        self.db
            .drop_all()
            .map_err(|e| Error::Backend(format!("failed to drop all: {}", e)))?;

        Ok(())
    }
}

#[cfg(test)]
mod pairing_tests {
    use super::*;
    use crate::backends::shared::ReadSet;
    use crate::corekv::{Reader, Writer};
    use std::time::Duration;

    fn physical_value(store: &LarkStore, key: &[u8]) -> Option<Vec<u8>> {
        store.db.snapshot().get(key).unwrap()
    }

    #[tokio::test]
    async fn snapshot_waits_for_physical_write_after_conflict_version_advances() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let store = Arc::new(LarkStore::open(temp_dir.path()).unwrap());
        let key = b"paired-snapshot".to_vec();
        let value = b"committed".to_vec();

        let gate = Arc::clone(&store.commit_gate);
        let commit_guard = gate.write().await;
        store
            .conflict_tracker
            .check_and_record(
                store.conflict_tracker.current_version(),
                std::slice::from_ref(&key).iter(),
                &ReadSet::default(),
            )
            .unwrap();

        let snapshot_store = Arc::clone(&store);
        let mut snapshot_task = tokio::spawn(async move { snapshot_store.new_txn(false).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut snapshot_task)
                .await
                .is_err(),
            "new transaction took a snapshot during an in-flight physical commit"
        );

        let mut batch = lark_kv::WriteBatch::new();
        batch.put(&key, &value);
        store
            .db
            .write_with_durability(batch, lark_kv::DurabilityMode::Eventual)
            .unwrap();
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
        let temp_dir = tempfile::TempDir::new().unwrap();
        let store = Arc::new(LarkStore::open(temp_dir.path()).unwrap());
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
        assert_eq!(physical_value(&store, &key), None);

        drop(snapshot_guard);
        tokio::time::timeout(Duration::from_secs(1), commit_task)
            .await
            .expect("commit remained blocked after snapshot pairing")
            .expect("commit task panicked")
            .expect("commit failed");
        assert_eq!(physical_value(&store, &key), Some(b"committed".to_vec()));
    }

    #[tokio::test]
    async fn readonly_txn_skips_the_commit_gate() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let store = Arc::new(LarkStore::open(temp_dir.path()).unwrap());
        let gate = Arc::clone(&store.commit_gate);
        let commit_guard = gate.write().await;

        // lark now publishes its read horizon only after a batch is applied,
        // so a read-only snapshot can no longer observe a torn batch and does
        // not need the gate — matching rocksdb/redb/fjall/memory. It must
        // acquire even while a writer holds the gate.
        let readonly_store = Arc::clone(&store);
        let readonly_task = tokio::spawn(async move { readonly_store.new_txn(true).await });
        tokio::time::timeout(Duration::from_secs(1), readonly_task)
            .await
            .expect("read-only transaction blocked on the commit gate")
            .expect("readonly task panicked")
            .expect("read-only transaction failed");

        drop(commit_guard);
    }

    #[tokio::test]
    async fn cancelling_snapshot_wait_does_not_leak_active_transaction_count() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let store = Arc::new(LarkStore::open(temp_dir.path()).unwrap());
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
