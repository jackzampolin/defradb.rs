use async_trait::async_trait;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

use super::config::RocksDbStoreOptions;
use super::transaction::RocksDbTxn;
use crate::backends::shared::ConflictTracker;
use crate::corekv::{Dropable, Error, Result, Store, Txn};

/// RocksDB-backed key-value store.
///
/// Uses OptimisticTransactionDB for OCC (optimistic concurrency control),
/// providing concurrent writes with conflict detection at commit time.
pub struct RocksDbStore {
    db: Arc<rocksdb::OptimisticTransactionDB>,
    closed: Arc<RwLock<bool>>,
    conflict_tracker: Arc<ConflictTracker>,
    db_path: std::path::PathBuf,
    active_txn_count: Arc<AtomicUsize>,
    close_timeout: std::time::Duration,
    durability: crate::backends::shared::DurabilityMode,
}

impl RocksDbStore {
    /// Open a RocksDB database at the specified path with default options.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_options(path, RocksDbStoreOptions::default())
    }

    /// Open a RocksDB database at the specified path with custom options.
    pub fn open_with_options<P: AsRef<Path>>(path: P, opts: RocksDbStoreOptions) -> Result<Self> {
        let path = path.as_ref();
        let db_path = if path.extension().is_some() {
            path.parent().unwrap_or(path).join("data.rocksdb")
        } else {
            path.join("data.rocksdb")
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

        let mut db_opts = rocksdb::Options::default();
        db_opts.create_if_missing(true);
        db_opts.set_max_background_jobs(
            opts.max_background_compactions() + opts.max_background_flushes(),
        );
        db_opts.set_write_buffer_size(opts.write_buffer_size());
        db_opts.set_max_write_buffer_number(opts.max_write_buffer_number());
        db_opts.set_level_zero_slowdown_writes_trigger(opts.l0_slowdown_writes_trigger());
        db_opts.set_level_zero_stop_writes_trigger(opts.l0_stop_writes_trigger());

        // Block-based table options with cache
        let mut block_opts = rocksdb::BlockBasedOptions::default();
        let cache = rocksdb::Cache::new_lru_cache(opts.block_cache_size());
        block_opts.set_block_cache(&cache);
        block_opts.set_bloom_filter(10.0, false);
        db_opts.set_block_based_table_factory(&block_opts);

        // BlobDB for large values
        if opts.enable_blob_files() {
            db_opts.set_enable_blob_files(true);
            db_opts.set_min_blob_size(opts.min_blob_size());
            db_opts.set_enable_blob_gc(true);
        }

        let db: rocksdb::OptimisticTransactionDB =
            rocksdb::OptimisticTransactionDB::open(&db_opts, &db_path).map_err(|e| {
                tracing::error!(
                    db_path = %db_path.display(),
                    error = %e,
                    "Failed to open RocksDB database"
                );
                let err: Error = e.into();
                err
            })?;

        Ok(Self {
            db: Arc::new(db),
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

    async fn is_closed(&self) -> bool {
        *self.closed.read().await
    }
}

#[async_trait]
impl Store for RocksDbStore {
    async fn new_txn(&self, readonly: bool) -> Result<Box<dyn Txn>> {
        {
            let closed = self.closed.read().await;
            if *closed {
                return Err(Error::DBClosed);
            }
            self.active_txn_count.fetch_add(1, Ordering::SeqCst);
        }

        Ok(Box::new(RocksDbTxn::new(
            Arc::clone(&self.db),
            Arc::clone(&self.conflict_tracker),
            Arc::clone(&self.active_txn_count),
            readonly,
            self.durability,
        )))
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
impl Dropable for RocksDbStore {
    async fn drop_all(&self) -> Result<()> {
        if self.is_closed().await {
            return Err(Error::DBClosed);
        }

        // Delete all keys via iterator
        let mut iter = self.db.raw_iterator();
        iter.seek_to_first();
        let mut batch = rocksdb::WriteBatchWithTransaction::<true>::default();
        while iter.valid() {
            if let Some(key) = iter.key() {
                batch.delete(key);
            }
            iter.next();
        }
        self.db.write(batch).map_err(|e| {
            tracing::error!(error = %e, "Failed to clear RocksDB");
            let err: Error = e.into();
            err
        })?;

        Ok(())
    }
}
