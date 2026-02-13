use std::time::Duration;

use crate::backends::shared::DurabilityMode;

/// Default close timeout in seconds.
pub const DEFAULT_CLOSE_TIMEOUT_SECS: u64 = 5;

/// Default block cache size: 512 MiB.
/// Fjall docs recommend 20-25% of available memory.
const DEFAULT_CACHE_SIZE: u64 = 512 * 1024 * 1024;

/// Default maximum journal size: 1 GiB.
const DEFAULT_MAX_JOURNAL_SIZE: u64 = 1024 * 1024 * 1024;

/// Configuration options for FjallStore.
#[derive(Debug, Clone)]
pub struct FjallStoreOptions {
    cache_size: u64,
    max_journal_size: u64,
    close_timeout: Duration,
    durability: DurabilityMode,
}

impl Default for FjallStoreOptions {
    fn default() -> Self {
        Self {
            cache_size: DEFAULT_CACHE_SIZE,
            max_journal_size: DEFAULT_MAX_JOURNAL_SIZE,
            close_timeout: Duration::from_secs(DEFAULT_CLOSE_TIMEOUT_SECS),
            durability: DurabilityMode::Eventual,
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
}
