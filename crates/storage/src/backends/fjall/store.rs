use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

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
    closed: Arc<RwLock<bool>>,
    conflict_tracker: Arc<ConflictTracker>,
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
            closed: Arc::new(RwLock::new(false)),
            conflict_tracker: Arc::new(ConflictTracker::new()),
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
        self.active_txn_count.load(Ordering::SeqCst)
    }

    /// Returns true if the underlying keyspace uses KV separation (blob storage).
    pub fn is_kv_separated(&self) -> bool {
        self.keyspace.is_kv_separated()
    }

    async fn is_closed(&self) -> bool {
        *self.closed.read().await
    }
}

#[async_trait]
impl Store for FjallStore {
    async fn new_txn(&self, readonly: bool) -> Result<Box<dyn Txn>> {
        {
            let closed = self.closed.read().await;
            if *closed {
                return Err(Error::DBClosed);
            }
            self.active_txn_count.fetch_add(1, Ordering::SeqCst);
        }

        // Guard to decrement count on panic or early return
        struct NewTxnGuard<'a>(&'a AtomicUsize, bool);
        impl Drop for NewTxnGuard<'_> {
            fn drop(&mut self) {
                if !self.1 {
                    self.0.fetch_sub(1, Ordering::SeqCst);
                }
            }
        }
        let mut guard = NewTxnGuard(&self.active_txn_count, false);

        let read_version = self.conflict_tracker.current_version();
        let snapshot = self.db.snapshot();

        // Defuse guard — transaction will manage its own count via Drop
        guard.1 = true;

        Ok(Box::new(FjallTxn {
            db: self.db.clone(),
            keyspace: self.keyspace.clone(),
            conflict_tracker: Arc::clone(&self.conflict_tracker),
            active_txn_count: Arc::clone(&self.active_txn_count),
            read_version,
            snapshot,
            pending: Mutex::new(BTreeMap::new()),
            readonly,
            discarded: Mutex::new(false),
            committed: Mutex::new(false),
            callbacks: CallbackManager::new(),
            durability: self.durability,
        }))
    }

    async fn close(&self) -> Result<()> {
        {
            let mut closed = self.closed.write().await;
            if *closed {
                return Ok(());
            }
            *closed = true;
        }

        let active = self.active_txn_count.load(Ordering::SeqCst);
        if active > 0 {
            tracing::info!(
                active_transactions = active,
                db_path = %self.db_path.display(),
                "Store closing with active transactions - waiting for completion"
            );

            let start = std::time::Instant::now();
            let timeout = self.close_timeout;
            while self.active_txn_count.load(Ordering::SeqCst) > 0 {
                if start.elapsed() > timeout {
                    let remaining = self.active_txn_count.load(Ordering::SeqCst);
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
        if self.is_closed().await {
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
