use async_trait::async_trait;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use super::config::RocksDbStoreOptions;
use super::metrics::{RocksDbStatsHandle, RocksDbStatsSnapshot, RocksDbTransactionMetrics};
use super::transaction::RocksDbTxn;
use crate::backends::shared::ConflictTracker;
use crate::corekv::{Dropable, Error, Result, Store, Txn};

/// RocksDB-backed key-value store.
///
/// Uses OptimisticTransactionDB for OCC (optimistic concurrency control),
/// providing concurrent writes with conflict detection at commit time.
pub struct RocksDbStore {
    db: Arc<rocksdb::OptimisticTransactionDB>,
    closed: AtomicBool,
    conflict_tracker: Arc<ConflictTracker>,
    commit_gate: Arc<tokio::sync::RwLock<()>>,
    db_path: std::path::PathBuf,
    active_txn_count: Arc<AtomicUsize>,
    close_timeout: std::time::Duration,
    durability: crate::backends::shared::DurabilityMode,
    stats: RocksDbStatsHandle,
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
        db_opts.set_target_file_size_base(opts.target_file_size_base());
        db_opts.set_max_bytes_for_level_base(opts.max_bytes_for_level_base());
        if opts.statistics_enabled() {
            db_opts.enable_statistics();
            db_opts.set_statistics_level(rocksdb::statistics::StatsLevel::ExceptDetailedTimers);
            db_opts.set_stats_dump_period_sec(0);
        }

        use super::config::{CompactionStyle, CompressionType};
        let rocksdb_compression = match opts.compression() {
            CompressionType::None => rocksdb::DBCompressionType::None,
            CompressionType::Snappy => rocksdb::DBCompressionType::Snappy,
            CompressionType::Zstd => rocksdb::DBCompressionType::Zstd,
            CompressionType::Lz4 => rocksdb::DBCompressionType::Lz4,
        };
        db_opts.set_compression_type(rocksdb_compression);

        match opts.compaction_style() {
            CompactionStyle::Level => {
                db_opts.set_compaction_style(rocksdb::DBCompactionStyle::Level);
                // Auto-adjust level sizes based on actual DB size. Critical for
                // databases that grow from empty to 100GB+. Achieves ~1.11x space
                // amplification and reduces write amplification during growth.
                db_opts.set_level_compaction_dynamic_level_bytes(true);
            }
            CompactionStyle::Universal => {
                db_opts.set_compaction_style(rocksdb::DBCompactionStyle::Universal);
            }
        }

        // Zstd for bottommost level (best ratio where data is rewritten least often).
        db_opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);

        // Smooth out I/O: sync every 1MB instead of bursting at compaction end.
        db_opts.set_bytes_per_sync(1_048_576);
        // Explicit readahead for compaction (reduces syscall overhead on all drives).
        db_opts.set_compaction_readahead_size(2 * 1024 * 1024);

        // Block-based table options with cache
        let mut block_opts = rocksdb::BlockBasedOptions::default();
        let cache = rocksdb::Cache::new_lru_cache(opts.block_cache_size());
        block_opts.set_block_cache(&cache);
        block_opts.set_bloom_filter(10.0, false);
        block_opts.set_block_size(opts.block_size());
        // Count index/filter blocks against block cache budget to prevent
        // uncontrolled memory growth outside the cache.
        block_opts.set_cache_index_and_filter_blocks(true);
        block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
        block_opts.set_format_version(5);
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
        let db = Arc::new(db);
        let statistics = opts.statistics_enabled().then(|| Arc::new(db_opts));
        let transaction_metrics = Arc::new(RocksDbTransactionMetrics::default());
        let stats = RocksDbStatsHandle::new(
            Arc::clone(&db),
            cache,
            statistics,
            Arc::clone(&transaction_metrics),
        );

        Ok(Self {
            db,
            closed: AtomicBool::new(false),
            conflict_tracker: Arc::new(ConflictTracker::for_backend("rocksdb")),
            commit_gate: Arc::new(tokio::sync::RwLock::new(())),
            db_path,
            active_txn_count: Arc::new(AtomicUsize::new(0)),
            close_timeout: opts.close_timeout(),
            durability: opts.durability(),
            stats,
        })
    }

    /// Get the database file path.
    pub fn db_path(&self) -> &std::path::Path {
        &self.db_path
    }

    /// Return a cloneable diagnostics handle that remains valid for this store's lifetime.
    pub fn stats_handle(&self) -> RocksDbStatsHandle {
        self.stats.clone()
    }

    /// Capture current RocksDB gauges and process-lifetime counters.
    pub fn stats(&self) -> Result<RocksDbStatsSnapshot> {
        self.stats.snapshot()
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

impl crate::corekv::private::Sealed for RocksDbStore {}

#[async_trait]
impl Store for RocksDbStore {
    fn transaction_stats_handle(&self) -> Option<crate::backends::TransactionStatsHandle> {
        Some(self.conflict_tracker.stats_handle())
    }

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

        // Ensure cancellation while waiting for the commit gate does not leak
        // the active transaction count and prevent store shutdown.
        struct NewTxnGuard<'a>(&'a AtomicUsize, bool);
        impl Drop for NewTxnGuard<'_> {
            fn drop(&mut self) {
                if !self.1 {
                    self.0.fetch_sub(1, Ordering::AcqRel);
                }
            }
        }
        let mut guard = NewTxnGuard(&self.active_txn_count, false);

        // Pair the published conflict version with the RocksDB snapshot. A
        // pending physical write may already be visible, but its reservation
        // remains a conservative conflict until publication. Read-only
        // transactions never conflict-check and skip this gate.
        let _commit_guard = if readonly {
            None
        } else {
            let started = std::time::Instant::now();
            let guard = self.commit_gate.read().await;
            self.stats
                .transactions
                .record_snapshot_gate_wait(started.elapsed());
            Some(guard)
        };
        let txn = RocksDbTxn::new(
            Arc::clone(&self.db),
            Arc::clone(&self.conflict_tracker),
            Arc::clone(&self.commit_gate),
            Arc::clone(&self.active_txn_count),
            readonly,
            self.durability,
            Arc::clone(&self.stats.transactions),
        );

        // The transaction now owns the active-count decrement through Drop.
        guard.1 = true;
        Ok(Box::new(txn))
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
impl Dropable for RocksDbStore {
    async fn drop_all(&self) -> Result<()> {
        if self.is_closed() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::shared::ReadSet;
    use std::time::Duration;

    #[tokio::test]
    async fn snapshot_waits_while_successful_commit_is_published() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let store = Arc::new(RocksDbStore::open(temp_dir.path()).unwrap());
        let sequence_key = b"/seq/doc".to_vec();
        let sequence_value = 14_u64.to_be_bytes();

        let gate = Arc::clone(&store.commit_gate);
        let commit_guard = gate.write().await;
        let reservation = store
            .conflict_tracker
            .reserve(
                store.conflict_tracker.current_version(),
                std::slice::from_ref(&sequence_key).iter(),
                &ReadSet::default(),
            )
            .unwrap();
        store.db.put(&sequence_key, sequence_value).unwrap();
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
        assert_eq!(
            snapshot.get(&sequence_key).await.unwrap(),
            Some(sequence_value.to_vec())
        );
    }

    #[tokio::test]
    async fn physical_write_does_not_wait_for_snapshot_pairing() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let store = Arc::new(RocksDbStore::open(temp_dir.path()).unwrap());
        let key = b"commit-gate-key".to_vec();

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
            while store.db.get(&key).unwrap().is_none() {
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
        assert_eq!(store.db.get(&key).unwrap(), Some(b"committed".to_vec()));
    }

    #[tokio::test]
    async fn readonly_txn_skips_commit_gate() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let store = Arc::new(RocksDbStore::open(temp_dir.path()).unwrap());
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
    async fn cancelled_commit_still_runs_success_callbacks() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let store = Arc::new(RocksDbStore::open(temp_dir.path()).unwrap());
        let key = b"cancelled-callback-key".to_vec();

        let fired = Arc::new(AtomicBool::new(false));
        let async_fired = Arc::new(AtomicBool::new(false));

        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(&key, b"committed").await.unwrap();
        {
            let fired = Arc::clone(&fired);
            txn.on_success(Box::new(move || fired.store(true, Ordering::Release)));
        }
        {
            let async_fired = Arc::clone(&async_fired);
            txn.on_success_async(Box::new(move || {
                Box::pin(async move {
                    async_fired.store(true, Ordering::Release);
                })
            }));
        }

        // Park version publication after the blocking task writes.
        let gate = Arc::clone(&store.commit_gate);
        let commit_guard = gate.write().await;

        let commit_task = tokio::spawn(async move { txn.commit().await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Drop the caller's future while publication is still parked.
        commit_task.abort();
        assert!(
            commit_task.await.is_err(),
            "commit task should have been aborted"
        );

        // Releasing the gate lets the detached task publish and start the
        // callbacks; they are spawned, so poll until they land.
        drop(commit_guard);
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !(fired.load(Ordering::Acquire) && async_fired.load(Ordering::Acquire)) {
            assert!(
                std::time::Instant::now() < deadline,
                "callbacks never ran for a commit that landed (sync={}, async={})",
                fired.load(Ordering::Acquire),
                async_fired.load(Ordering::Acquire)
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        assert_eq!(
            store.db.get(&key).unwrap(),
            Some(b"committed".to_vec()),
            "cancelled commit lost a write it had already started"
        );
    }

    #[tokio::test]
    async fn commit_conflict_checks_against_pinned_records() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let store = Arc::new(RocksDbStore::open(temp_dir.path()).unwrap());
        let key = b"contended-key".to_vec();

        // Txn A snapshots at version 0 and stages a write to `key`.
        let mut txn_a = store.new_txn(false).await.unwrap();
        txn_a.set(&key, b"stale-A").await.unwrap();

        // Txn B commits a write to the same key -> version 1 recorded.
        let mut txn_b = store.new_txn(false).await.unwrap();
        txn_b.set(&key, b"committed-B").await.unwrap();
        txn_b.commit().await.unwrap();
        assert_eq!(store.db.get(&key).unwrap(), Some(b"committed-B".to_vec()));

        let error = txn_a.commit().await.unwrap_err();
        assert!(
            error.is_txn_conflict(),
            "expected TxnConflict, got: {error}"
        );

        assert_eq!(
            store.db.get(&key).unwrap(),
            Some(b"committed-B".to_vec()),
            "cancelled commit overwrote a conflicting committed write"
        );
    }

    #[tokio::test]
    async fn cancelling_snapshot_wait_does_not_leak_active_transaction_count() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let store = Arc::new(RocksDbStore::open(temp_dir.path()).unwrap());
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

    #[tokio::test]
    async fn stats_keep_expensive_rocksdb_counters_opt_in() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let store = RocksDbStore::open_with_options(
            temp_dir.path(),
            RocksDbStoreOptions::new().with_block_cache_size(1024 * 1024),
        )
        .unwrap();

        let stats = store.stats().unwrap();
        assert_eq!(stats.block_cache.capacity_bytes, 1024 * 1024);
        assert!(stats.counters.is_none());
        assert_eq!(stats.transactions.conflicts, 0);
    }

    #[tokio::test]
    async fn stats_report_live_cache_and_cumulative_reads() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let store = RocksDbStore::open_with_options(
            temp_dir.path(),
            RocksDbStoreOptions::new()
                .with_block_cache_size(1024 * 1024)
                .with_statistics_enabled(true),
        )
        .unwrap();

        store.db.put(b"stats-key", b"stats-value").unwrap();
        store.db.flush().unwrap();
        assert_eq!(
            store.db.get(b"stats-key").unwrap(),
            Some(b"stats-value".to_vec())
        );
        assert_eq!(
            store.db.get(b"stats-key").unwrap(),
            Some(b"stats-value".to_vec())
        );

        let stats = store.stats_handle().snapshot().unwrap();
        let counters = stats
            .counters
            .as_ref()
            .expect("explicitly enabled counters should be present");
        assert!(counters.io.keys_read >= 2);
        assert!(counters.block_cache.hits + counters.block_cache.misses >= 1);
        assert!(stats.block_cache.usage_bytes <= stats.block_cache.capacity_bytes);
        serde_json::to_value(stats).expect("diagnostics snapshot should serialize as JSON");
    }

    #[tokio::test]
    async fn stats_count_gate_waits_and_transaction_conflicts() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let store = RocksDbStore::open(temp_dir.path()).unwrap();
        let key = b"stats-conflict-key";

        let mut stale = store.new_txn(false).await.unwrap();
        stale.get(key).await.unwrap();
        stale.set(key, b"stale").await.unwrap();

        let mut winner = store.new_txn(false).await.unwrap();
        winner.set(key, b"winner").await.unwrap();
        winner.commit().await.unwrap();

        assert!(matches!(stale.commit().await, Err(Error::TxnConflict)));

        let stats = store.stats().unwrap().transactions;
        assert_eq!(stats.conflicts, 1);
        assert_eq!(stats.snapshot_gate_waits, 2);
        assert_eq!(stats.commit_gate_waits, 1);
    }
}
