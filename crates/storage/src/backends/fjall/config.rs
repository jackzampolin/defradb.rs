use std::time::Duration;

use crate::backends::shared::DurabilityMode;

/// Default close timeout in seconds.
pub const DEFAULT_CLOSE_TIMEOUT_SECS: u64 = 5;

/// Default block cache size: 512 MiB.
/// Fjall docs recommend 20-25% of available memory.
const DEFAULT_CACHE_SIZE: u64 = 512 * 1024 * 1024;

/// Default maximum journal size: 1 GiB.
const DEFAULT_MAX_JOURNAL_SIZE: u64 = 1024 * 1024 * 1024;

/// Default compaction worker threads. 0 = use fjall default (min(cores, 4)).
/// For high-concurrency write workloads, set to match or exceed writer count.
const DEFAULT_WORKER_THREADS: usize = 8;

/// Default L0 compaction threshold. Fjall default is 4.
/// Backpressure is hardcoded at L0 >= 20, halt at >= 30.
/// Lower threshold = compaction starts sooner = fewer L0 runs.
const DEFAULT_L0_THRESHOLD: u8 = 4;

/// Default memtable size: 256 MiB. Fjall default is 64 MiB. Larger memtables
/// reduce L0 flush frequency, helping sustain high write throughput.
const DEFAULT_MAX_MEMTABLE_SIZE: u64 = 256 * 1024 * 1024;

/// Configuration options for FjallStore.
#[derive(Debug, Clone)]
pub struct FjallStoreOptions {
    cache_size: u64,
    max_journal_size: u64,
    close_timeout: Duration,
    durability: DurabilityMode,
    worker_threads: usize,
    l0_threshold: u8,
    max_memtable_size: u64,
    kv_separation: bool,
}

impl Default for FjallStoreOptions {
    fn default() -> Self {
        Self {
            cache_size: DEFAULT_CACHE_SIZE,
            max_journal_size: DEFAULT_MAX_JOURNAL_SIZE,
            close_timeout: Duration::from_secs(DEFAULT_CLOSE_TIMEOUT_SECS),
            durability: DurabilityMode::Eventual,
            worker_threads: DEFAULT_WORKER_THREADS,
            l0_threshold: DEFAULT_L0_THRESHOLD,
            max_memtable_size: DEFAULT_MAX_MEMTABLE_SIZE,
            kv_separation: true,
        }
    }
}

impl FjallStoreOptions {
    /// Create a new options struct with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the block cache size in bytes.
    ///
    /// Fjall uses this to control memory for cached LSM-tree blocks.
    /// Recommended: 20-25% of available memory.
    ///
    /// Default: 512 MiB.
    pub fn with_cache_size(mut self, bytes: u64) -> Self {
        self.cache_size = bytes;
        self
    }

    /// Get the configured cache size.
    pub fn cache_size(&self) -> u64 {
        self.cache_size
    }

    /// Set the maximum journal size in bytes.
    ///
    /// Larger journals allow more write batching before flush,
    /// reducing write amplification for high-throughput workloads.
    ///
    /// Default: 1 GiB. Minimum: 64 MiB (enforced by fjall).
    pub fn with_max_journal_size(mut self, bytes: u64) -> Self {
        self.max_journal_size = bytes;
        self
    }

    /// Get the configured max journal size.
    pub fn max_journal_size(&self) -> u64 {
        self.max_journal_size
    }

    /// Set the close timeout duration.
    ///
    /// When `close()` is called, the store will wait up to this duration for
    /// active transactions to complete before returning an error.
    ///
    /// Default: 5 seconds
    pub fn with_close_timeout(mut self, timeout: Duration) -> Self {
        self.close_timeout = timeout;
        self
    }

    /// Get the configured close timeout.
    pub fn close_timeout(&self) -> Duration {
        self.close_timeout
    }

    /// Set the durability mode.
    ///
    /// `DurabilityMode::Eventual` (default) flushes to OS buffers,
    /// matching Go DefraDB's BadgerDB defaults.
    /// `DurabilityMode::Immediate` fsyncs on every commit.
    pub fn with_durability(mut self, mode: DurabilityMode) -> Self {
        self.durability = mode;
        self
    }

    /// Get the configured durability mode.
    pub fn durability(&self) -> DurabilityMode {
        self.durability
    }

    /// Set the number of compaction/flush worker threads.
    /// 0 uses fjall's default (min(cores, 4)).
    pub fn with_worker_threads(mut self, n: usize) -> Self {
        self.worker_threads = n;
        self
    }

    /// Get the configured worker thread count.
    pub fn worker_threads(&self) -> usize {
        self.worker_threads
    }

    /// Set the L0 compaction threshold.
    /// Higher values batch more L0 runs per compaction, reducing frequency.
    pub fn with_l0_threshold(mut self, threshold: u8) -> Self {
        self.l0_threshold = threshold;
        self
    }

    /// Get the configured L0 threshold.
    pub fn l0_threshold(&self) -> u8 {
        self.l0_threshold
    }

    /// Set the maximum memtable size in bytes.
    /// Larger memtables reduce L0 flush frequency.
    pub fn with_max_memtable_size(mut self, bytes: u64) -> Self {
        self.max_memtable_size = bytes;
        self
    }

    /// Get the configured max memtable size.
    pub fn max_memtable_size(&self) -> u64 {
        self.max_memtable_size
    }

    /// Enable/disable KV separation (blob storage for large values).
    /// When enabled, values above the separation threshold are stored in
    /// separate blob files, reducing LSM compaction write amplification.
    pub fn with_kv_separation(mut self, enabled: bool) -> Self {
        self.kv_separation = enabled;
        self
    }

    /// Get whether KV separation is enabled.
    pub fn kv_separation(&self) -> bool {
        self.kv_separation
    }
}
