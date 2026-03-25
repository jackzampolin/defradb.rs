use async_trait::async_trait;
use parking_lot::Mutex;
use redb::{Database, ReadableTable};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use super::config::{DurabilityMode, RedbStoreOptions};
use super::group_commit::GroupCommitBuffer;
use super::transaction::RedbTxn;
use super::KV_TABLE;
use crate::backends::shared::{CallbackManager, ConflictTracker};
use crate::corekv::{Dropable, Error, Result, Store, Txn};

/// Redb-backed key-value store.
///
/// This store wraps a redb Database instance and provides MVCC transaction
/// support through snapshots and buffered writes.
///
/// # Active Transaction Tracking
///
/// The store tracks the number of active transactions. When closing, the store
/// will reject new transactions and wait for existing ones to complete.
pub struct RedbStore {
    db: Arc<Database>,
    closed: AtomicBool,
    /// Count of active transactions (for graceful shutdown)
    active_txn_count: Arc<AtomicUsize>,
    /// Close timeout duration
    close_timeout: std::time::Duration,
    /// Database file path (for error messages)
    db_path: std::path::PathBuf,
    /// Conflict tracker for write-write conflict detection
    conflict_tracker: Arc<ConflictTracker>,
    /// Durability mode for write transactions
    durability: DurabilityMode,
    /// Group commit buffer for coalescing write transactions
    group_commit: Option<Arc<GroupCommitBuffer>>,
}

impl RedbStore {
    /// Open a redb database at the specified path with default options.
    ///
    /// If the database doesn't exist, it will be created.
    /// If the path is a directory, creates `data.redb` inside it.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the database file or directory
    ///
    /// # Returns
    ///
    /// * `Ok(RedbStore)` on success
    /// * `Err(Error)` if the database cannot be opened
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_options(path, RedbStoreOptions::default())
    }

    /// Open a redb database at the specified path with custom options.
    ///
    /// If the database doesn't exist, it will be created.
    /// If the path is a directory, creates `data.redb` inside it.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the database file or directory
    /// * `opts` - Configuration options for the database
    ///
    /// # Returns
    ///
    /// * `Ok(RedbStore)` on success
    /// * `Err(Error)` if the database cannot be opened
    ///
    /// # Example
    ///
    /// ```ignore
    /// use storage::backends::{RedbStore, RedbStoreOptions};
    ///
    /// let opts = RedbStoreOptions::new()
    ///     .with_cache_size(64 * 1024 * 1024); // 64MB cache
    ///
    /// let store = RedbStore::open_with_options("/path/to/db", opts)?;
    /// ```
    pub fn open_with_options<P: AsRef<Path>>(path: P, opts: RedbStoreOptions) -> Result<Self> {
        let path = path.as_ref();
        let db_path = if path.is_dir() {
            path.join("data.redb")
        } else {
            path.to_path_buf()
        };

        let mut builder = redb::Builder::new();
        if let Some(cache_size) = opts.cache_size() {
            builder.set_cache_size(cache_size);
        }

        // Open database with path context in error messages
        let db = builder.create(&db_path).map_err(|e| {
            // Add file path context to common errors
            match &e {
                redb::DatabaseError::DatabaseAlreadyOpen => {
                    tracing::warn!(db_path = %db_path.display(), "Database is locked by another process");
                    Error::Backend(format!(
                        "database '{}' is locked by another process. \
                         Troubleshooting: (1) check for other running processes using 'lsof {}', \
                         (2) if no process found, the lock file may be stale - check for .lock files, \
                         (3) ensure previous instance shut down cleanly",
                        db_path.display(),
                        db_path.display()
                    ))
                }
                redb::DatabaseError::UpgradeRequired(version) => {
                    tracing::warn!(
                        db_path = %db_path.display(),
                        version = version,
                        "Database file format upgrade required"
                    );
                    Error::Backend(format!(
                        "database '{}' uses file format version {} which requires upgrade. \
                         To upgrade: (1) backup your database first, \
                         (2) use 'redb-cli upgrade {}' or equivalent tool, \
                         (3) see redb documentation for migration details",
                        db_path.display(),
                        version,
                        db_path.display()
                    ))
                }
                _ => {
                    tracing::error!(
                        db_path = %db_path.display(),
                        error = %e,
                        "Failed to open database"
                    );
                    e.into()
                }
            }
        })?;

        // Ensure the KV table exists by opening a write transaction
        {
            let write_txn = db.begin_write()?;
            let _ = write_txn.open_table(KV_TABLE)?;
            write_txn.commit()?;
        }

        let db = Arc::new(db);
        let conflict_tracker = Arc::new(ConflictTracker::new());

        // Create group commit buffer if a tokio runtime is available.
        // This coalesces multiple transaction commits into single redb writes.
        let group_commit = tokio::runtime::Handle::try_current().ok().map(|_| {
            Arc::new(GroupCommitBuffer::new(
                Arc::clone(&db),
                opts.durability(),
                Arc::clone(&conflict_tracker),
            ))
        });

        Ok(Self {
            db,
            closed: AtomicBool::new(false),
            active_txn_count: Arc::new(AtomicUsize::new(0)),
            close_timeout: opts.close_timeout(),
            db_path,
            conflict_tracker,
            durability: opts.durability(),
            group_commit,
        })
    }

    /// Get the current count of active transactions.
    pub fn active_transaction_count(&self) -> usize {
        self.active_txn_count.load(Ordering::Acquire)
    }

    /// Get the database file path.
    pub fn db_path(&self) -> &std::path::Path {
        &self.db_path
    }

    /// Check database integrity.
    ///
    /// This performs a read-only scan of the database to verify that all data
    /// is readable and the internal structure is consistent. It does NOT repair
    /// any issues found.
    ///
    /// # Returns
    ///
    /// * `Ok(IntegrityReport)` with detailed results of the check
    /// * `Err(Error)` if the check could not be completed
    ///
    /// # Note
    ///
    /// This operation may be slow on large databases as it reads all data.
    pub fn check_integrity(&self) -> Result<IntegrityReport> {
        // Attempt to read all keys to verify data integrity
        let read_txn = self.db.begin_read()?;

        let table = match read_txn.open_table(KV_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => {
                // Empty database is valid
                return Ok(IntegrityReport {
                    is_valid: true,
                    total_keys: 0,
                    error_count: 0,
                    first_error: None,
                });
            }
            Err(e) => {
                tracing::error!(error = %e, "Integrity check failed: cannot open table");
                return Err(e.into());
            }
        };

        let mut key_count = 0u64;
        let mut error_count = 0u64;
        let mut first_error: Option<String> = None;

        let range = match table.range::<&[u8]>(..) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(error = %e, "Integrity check failed: cannot create range iterator");
                return Err(e.into());
            }
        };

        for result in range {
            match result {
                Ok((key, value)) => {
                    // Try to read the key and value to verify they're accessible
                    let _ = key.value();
                    let _ = value.value();
                    key_count += 1;
                }
                Err(e) => {
                    let error_msg = e.to_string();
                    tracing::error!(
                        error = %e,
                        keys_checked = key_count,
                        "Integrity check found error while reading key"
                    );
                    if first_error.is_none() {
                        first_error = Some(error_msg);
                    }
                    error_count += 1;
                }
            }
        }

        let is_valid = error_count == 0;
        if !is_valid {
            tracing::warn!(
                total_keys = key_count,
                errors = error_count,
                "Integrity check completed with errors"
            );
        } else {
            tracing::info!(total_keys = key_count, "Integrity check passed");
        }

        Ok(IntegrityReport {
            is_valid,
            total_keys: key_count,
            error_count,
            first_error,
        })
    }

    /// Check if the store is closed.
    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

#[async_trait]
impl Store for RedbStore {
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

        // Use a local guard to ensure count is decremented on panic or early return.
        // The guard is defused (set to true) only when the transaction is fully constructed.
        struct NewTxnGuard<'a>(&'a AtomicUsize, bool);
        impl Drop for NewTxnGuard<'_> {
            fn drop(&mut self) {
                if !self.1 {
                    self.0.fetch_sub(1, Ordering::AcqRel);
                }
            }
        }
        let mut guard = NewTxnGuard(&self.active_txn_count, false);

        // Record version before taking snapshot for conflict detection
        let read_version = self.conflict_tracker.current_version();

        // Use redb's MVCC ReadTransaction for snapshot isolation (O(1) creation)
        let read_txn = self.db.begin_read()?;

        // Defuse the guard - transaction will manage its own count via its Drop impl
        guard.1 = true;

        Ok(Box::new(RedbTxn {
            db: Arc::clone(&self.db),
            active_txn_count: Arc::clone(&self.active_txn_count),
            conflict_tracker: Arc::clone(&self.conflict_tracker),
            read_version,
            read_txn,
            pending: Mutex::new(BTreeMap::new()),
            readonly,
            durability: self.durability,
            discarded: AtomicBool::new(false),
            committed: AtomicBool::new(false),
            callbacks: CallbackManager::new(),
            group_commit: self.group_commit.clone(),
        }))
    }

    async fn close(&self) -> Result<()> {
        // Swap closed to true; if already true, another close() won.
        if self.closed.swap(true, Ordering::Release) {
            return Ok(());
        }

        // Wait for active transactions to complete (with timeout)
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
                        "Close timeout: {} transaction(s) still active after {}s (db: {}). \
                         Possible causes: (1) Transactions not calling commit()/discard() - check for missing cleanup, \
                         (2) Long-running I/O operations - transactions may still be processing large commits or snapshots. \
                         Use RedbStoreOptions::with_close_timeout() to increase timeout if needed.",
                        remaining,
                        timeout.as_secs(),
                        self.db_path.display()
                    )));
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }

        // Shut down group commit flush loop so it releases its Arc<Database>
        if let Some(ref gc) = self.group_commit {
            gc.shutdown().await;
        }

        Ok(())
    }
}

#[async_trait]
impl Dropable for RedbStore {
    async fn drop_all(&self) -> Result<()> {
        if self.is_closed() {
            return Err(Error::DBClosed);
        }

        // Open a write transaction and delete all entries
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(KV_TABLE)?;
            // Collect all keys first to avoid borrow issues
            let keys: Vec<Vec<u8>> = {
                let range = table.range::<&[u8]>(..)?;
                range
                    .map(|result| result.map(|(k, _)| k.value().to_vec()))
                    .collect::<std::result::Result<Vec<_>, _>>()?
            };
            // Delete all keys
            for key in keys {
                table.remove(key.as_slice())?;
            }
        }
        write_txn.commit()?;

        Ok(())
    }
}

/// Report from database integrity check.
///
/// Provides detailed information about the integrity check results,
/// including total keys scanned and any errors encountered.
#[derive(Debug, Clone)]
pub struct IntegrityReport {
    /// Whether the integrity check passed (no errors found).
    pub is_valid: bool,
    /// Total number of keys scanned during the check.
    pub total_keys: u64,
    /// Number of errors encountered during the check.
    pub error_count: u64,
    /// First error message encountered, if any (for debugging).
    pub first_error: Option<String>,
}

impl IntegrityReport {
    /// Check if the database passed the integrity check.
    pub fn is_valid(&self) -> bool {
        self.is_valid
    }
}
