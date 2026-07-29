use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rocksdb::statistics::{Histogram, Ticker};
use serde::Serialize;

use crate::corekv::{Error, Result};

/// Point-in-time and process-lifetime diagnostics for a RocksDB store.
///
/// Cache and LSM fields are gauges sampled when [`RocksDbStatsHandle::snapshot`]
/// is called. `counters` and `transactions` are cumulative from the time the
/// current process opened the database; restarting the process resets them.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RocksDbStatsSnapshot {
    pub block_cache: RocksDbBlockCacheStats,
    pub lsm: RocksDbLsmStats,
    pub counters: Option<RocksDbCumulativeStats>,
    pub transactions: RocksDbTransactionStats,
}

/// Current block-cache residency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RocksDbBlockCacheStats {
    pub capacity_bytes: u64,
    pub usage_bytes: u64,
    pub pinned_usage_bytes: u64,
}

/// Current memtable, flush, compaction, and SST state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RocksDbLsmStats {
    pub l0_file_count: u64,
    pub pending_compaction_bytes: u64,
    pub compaction_pending: bool,
    pub running_compactions: u64,
    pub flush_pending: bool,
    pub running_flushes: u64,
    pub immutable_memtable_count: u64,
    pub active_memtable_bytes: u64,
    pub unflushed_memtable_bytes: u64,
    pub total_memtable_bytes: u64,
    pub table_reader_bytes: u64,
    pub live_sst_bytes: u64,
    pub delayed_write_rate_bytes_per_second: u64,
    pub writes_stopped: bool,
    pub background_errors: u64,
}

/// Cumulative RocksDB counters collected only when statistics are enabled.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RocksDbCumulativeStats {
    pub block_cache: RocksDbBlockCacheCounters,
    pub bloom_filter: RocksDbBloomFilterCounters,
    pub io: RocksDbIoCounters,
    pub write_stalls: RocksDbWriteStallCounters,
    pub flushes: RocksDbFlushCounters,
    pub compactions: RocksDbCompactionCounters,
}

/// RocksDB exposes cache admissions but no exact general LRU-eviction ticker.
/// Compare admissions and misses with the point-in-time usage gauges instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RocksDbBlockCacheCounters {
    pub hits: u64,
    pub misses: u64,
    pub data_hits: u64,
    pub data_misses: u64,
    pub index_hits: u64,
    pub index_misses: u64,
    pub filter_hits: u64,
    pub filter_misses: u64,
    pub admissions: u64,
    pub admission_failures: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RocksDbBloomFilterCounters {
    pub useful: u64,
    pub full_positive: u64,
    pub full_true_positive: u64,
    pub prefix_checked: u64,
    pub prefix_useful: u64,
    pub prefix_true_positive: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RocksDbIoCounters {
    pub keys_read: u64,
    pub bytes_read: u64,
    pub iterator_bytes_read: u64,
    pub file_opens: u64,
    pub file_errors: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RocksDbWriteStallCounters {
    pub total_micros: u64,
    pub latency_micros: RocksDbHistogramStats,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RocksDbFlushCounters {
    pub bytes_written: u64,
    pub latency_micros: RocksDbHistogramStats,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RocksDbCompactionCounters {
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub cpu_micros: u64,
    pub cancelled: u64,
    pub latency_micros: RocksDbHistogramStats,
}

/// A cumulative RocksDB histogram summary.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RocksDbHistogramStats {
    pub count: u64,
    pub sum: u64,
    pub median: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
}

/// DefraDB transaction diagnostics not supplied by RocksDB itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RocksDbTransactionStats {
    pub conflicts: u64,
    pub snapshot_gate_waits: u64,
    pub snapshot_gate_wait_micros: u64,
    pub snapshot_gate_wait_max_micros: u64,
    pub commit_gate_waits: u64,
    pub commit_gate_wait_micros: u64,
    pub commit_gate_wait_max_micros: u64,
}

#[derive(Default)]
pub(crate) struct RocksDbTransactionMetrics {
    conflicts: AtomicU64,
    snapshot_gate_waits: AtomicU64,
    snapshot_gate_wait_micros: AtomicU64,
    snapshot_gate_wait_max_micros: AtomicU64,
    commit_gate_waits: AtomicU64,
    commit_gate_wait_micros: AtomicU64,
    commit_gate_wait_max_micros: AtomicU64,
}

impl RocksDbTransactionMetrics {
    pub(crate) fn record_conflict(&self) {
        self.conflicts.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_snapshot_gate_wait(&self, elapsed: Duration) {
        record_wait(
            elapsed,
            &self.snapshot_gate_waits,
            &self.snapshot_gate_wait_micros,
            &self.snapshot_gate_wait_max_micros,
        );
    }

    pub(crate) fn record_commit_gate_wait(&self, elapsed: Duration) {
        record_wait(
            elapsed,
            &self.commit_gate_waits,
            &self.commit_gate_wait_micros,
            &self.commit_gate_wait_max_micros,
        );
    }

    fn snapshot(&self) -> RocksDbTransactionStats {
        RocksDbTransactionStats {
            conflicts: self.conflicts.load(Ordering::Relaxed),
            snapshot_gate_waits: self.snapshot_gate_waits.load(Ordering::Relaxed),
            snapshot_gate_wait_micros: self.snapshot_gate_wait_micros.load(Ordering::Relaxed),
            snapshot_gate_wait_max_micros: self
                .snapshot_gate_wait_max_micros
                .load(Ordering::Relaxed),
            commit_gate_waits: self.commit_gate_waits.load(Ordering::Relaxed),
            commit_gate_wait_micros: self.commit_gate_wait_micros.load(Ordering::Relaxed),
            commit_gate_wait_max_micros: self.commit_gate_wait_max_micros.load(Ordering::Relaxed),
        }
    }
}

/// Cloneable access to a live RocksDB store's diagnostics.
#[derive(Clone)]
pub struct RocksDbStatsHandle {
    db: Arc<rocksdb::OptimisticTransactionDB>,
    cache: rocksdb::Cache,
    statistics: Option<Arc<rocksdb::Options>>,
    pub(crate) transactions: Arc<RocksDbTransactionMetrics>,
}

impl RocksDbStatsHandle {
    pub(crate) fn new(
        db: Arc<rocksdb::OptimisticTransactionDB>,
        cache: rocksdb::Cache,
        statistics: Option<Arc<rocksdb::Options>>,
        transactions: Arc<RocksDbTransactionMetrics>,
    ) -> Self {
        Self {
            db,
            cache,
            statistics,
            transactions,
        }
    }

    /// Capture current gauges and cumulative counters without reading user data.
    pub fn snapshot(&self) -> Result<RocksDbStatsSnapshot> {
        Ok(RocksDbStatsSnapshot {
            block_cache: RocksDbBlockCacheStats {
                capacity_bytes: self.property(rocksdb::properties::BLOCK_CACHE_CAPACITY)?,
                usage_bytes: self.cache.get_usage() as u64,
                pinned_usage_bytes: self.cache.get_pinned_usage() as u64,
            },
            lsm: RocksDbLsmStats {
                l0_file_count: self.property(&rocksdb::properties::num_files_at_level(0))?,
                pending_compaction_bytes: self
                    .property(rocksdb::properties::ESTIMATE_PENDING_COMPACTION_BYTES)?,
                compaction_pending: self.property(rocksdb::properties::COMPACTION_PENDING)? != 0,
                running_compactions: self.property(rocksdb::properties::NUM_RUNNING_COMPACTIONS)?,
                flush_pending: self.property(rocksdb::properties::MEM_TABLE_FLUSH_PENDING)? != 0,
                running_flushes: self.property(rocksdb::properties::NUM_RUNNING_FLUSHES)?,
                immutable_memtable_count: self
                    .property(rocksdb::properties::NUM_IMMUTABLE_MEM_TABLE)?,
                active_memtable_bytes: self
                    .property(rocksdb::properties::CUR_SIZE_ACTIVE_MEM_TABLE)?,
                unflushed_memtable_bytes: self
                    .property(rocksdb::properties::CUR_SIZE_ALL_MEM_TABLES)?,
                total_memtable_bytes: self.property(rocksdb::properties::SIZE_ALL_MEM_TABLES)?,
                table_reader_bytes: self
                    .property(rocksdb::properties::ESTIMATE_TABLE_READERS_MEM)?,
                live_sst_bytes: self.property(rocksdb::properties::LIVE_SST_FILES_SIZE)?,
                delayed_write_rate_bytes_per_second: self
                    .property(rocksdb::properties::ACTUAL_DELAYED_WRITE_RATE)?,
                writes_stopped: self.property(rocksdb::properties::IS_WRITE_STOPPED)? != 0,
                background_errors: self.property(rocksdb::properties::BACKGROUND_ERRORS)?,
            },
            counters: self
                .statistics
                .as_ref()
                .map(|statistics| RocksDbCumulativeStats {
                    block_cache: RocksDbBlockCacheCounters {
                        hits: ticker(statistics, Ticker::BlockCacheHit),
                        misses: ticker(statistics, Ticker::BlockCacheMiss),
                        data_hits: ticker(statistics, Ticker::BlockCacheDataHit),
                        data_misses: ticker(statistics, Ticker::BlockCacheDataMiss),
                        index_hits: ticker(statistics, Ticker::BlockCacheIndexHit),
                        index_misses: ticker(statistics, Ticker::BlockCacheIndexMiss),
                        filter_hits: ticker(statistics, Ticker::BlockCacheFilterHit),
                        filter_misses: ticker(statistics, Ticker::BlockCacheFilterMiss),
                        admissions: ticker(statistics, Ticker::BlockCacheAdd),
                        admission_failures: ticker(statistics, Ticker::BlockCacheAddFailures),
                        bytes_read: ticker(statistics, Ticker::BlockCacheBytesRead),
                        bytes_written: ticker(statistics, Ticker::BlockCacheBytesWrite),
                    },
                    bloom_filter: RocksDbBloomFilterCounters {
                        useful: ticker(statistics, Ticker::BloomFilterUseful),
                        full_positive: ticker(statistics, Ticker::BloomFilterFullPositive),
                        full_true_positive: ticker(statistics, Ticker::BloomFilterFullTruePositive),
                        prefix_checked: ticker(statistics, Ticker::BloomFilterPrefixChecked),
                        prefix_useful: ticker(statistics, Ticker::BloomFilterPrefixUseful),
                        prefix_true_positive: ticker(
                            statistics,
                            Ticker::BloomFilterPrefixTruePositive,
                        ),
                    },
                    io: RocksDbIoCounters {
                        keys_read: ticker(statistics, Ticker::NumberKeysRead),
                        bytes_read: ticker(statistics, Ticker::BytesRead),
                        iterator_bytes_read: ticker(statistics, Ticker::IterBytesRead),
                        file_opens: ticker(statistics, Ticker::NoFileOpens),
                        file_errors: ticker(statistics, Ticker::NoFileErrors),
                    },
                    write_stalls: RocksDbWriteStallCounters {
                        total_micros: ticker(statistics, Ticker::StallMicros),
                        latency_micros: histogram(statistics, Histogram::WriteStall),
                    },
                    flushes: RocksDbFlushCounters {
                        bytes_written: ticker(statistics, Ticker::FlushWriteBytes),
                        latency_micros: histogram(statistics, Histogram::FlushTime),
                    },
                    compactions: RocksDbCompactionCounters {
                        bytes_read: ticker(statistics, Ticker::CompactReadBytes),
                        bytes_written: ticker(statistics, Ticker::CompactWriteBytes),
                        cpu_micros: ticker(statistics, Ticker::CompactionCpuTotalTime),
                        cancelled: ticker(statistics, Ticker::CompactionCancelled),
                        latency_micros: histogram(statistics, Histogram::CompactionTime),
                    },
                }),
            transactions: self.transactions.snapshot(),
        })
    }

    fn property(&self, name: &rocksdb::properties::PropName) -> Result<u64> {
        self.db
            .property_int_value(name)
            .map_err(Error::from)?
            .ok_or_else(|| Error::Backend(format!("RocksDB property unavailable: {name}")))
    }
}

fn ticker(statistics: &rocksdb::Options, ticker: Ticker) -> u64 {
    statistics.get_ticker_count(ticker)
}

fn histogram(statistics: &rocksdb::Options, histogram: Histogram) -> RocksDbHistogramStats {
    let data = statistics.get_histogram_data(histogram);
    RocksDbHistogramStats {
        count: data.count(),
        sum: data.sum(),
        median: data.median(),
        p95: data.p95(),
        p99: data.p99(),
        max: data.max(),
    }
}

fn record_wait(elapsed: Duration, count: &AtomicU64, total: &AtomicU64, max: &AtomicU64) {
    let micros = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
    count.fetch_add(1, Ordering::Relaxed);
    total.fetch_add(micros, Ordering::Relaxed);
    max.fetch_max(micros, Ordering::Relaxed);
}
