use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Default close timeout in seconds.
pub const DEFAULT_CLOSE_TIMEOUT_SECS: u64 = 5;

/// Configuration options for RedbStore.
///
/// This struct provides a builder pattern for configuring redb database options.
/// Use `RedbStoreOptions::default()` for sensible defaults, or customize
/// via the builder methods.
///
/// # Example
///
/// ```ignore
/// use storage::backends::RedbStoreOptions;
/// use std::time::Duration;
///
/// let opts = RedbStoreOptions::new()
///     .with_cache_size(64 * 1024 * 1024) // 64MB cache
///     .with_close_timeout(Duration::from_secs(10)); // 10 second close timeout
///
/// let store = RedbStore::open_with_options("/path/to/db", opts)?;
/// ```
#[derive(Debug, Clone)]
pub struct RedbStoreOptions {
    cache_size: Option<usize>,
    close_timeout: Duration,
    durability: DurabilityMode,
}

/// Controls when data is flushed to disk after a commit.
///
/// Default is `Eventual`, matching Go DefraDB's BadgerDB behavior
/// (`SyncWrites = false`). Process crashes are safe due to redb's WAL;
/// only OS crashes risk data loss.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DurabilityMode {
    /// Flush to disk on every commit. Safe against process and OS crashes.
    Immediate,
    /// Rely on the OS to flush eventually (default). Matches Go DefraDB's
    /// BadgerDB defaults. Process crash is still safe due to redb's WAL.
    #[default]
    Eventual,
}

impl Default for RedbStoreOptions {
    fn default() -> Self {
        Self {
            cache_size: None,
            close_timeout: Duration::from_secs(DEFAULT_CLOSE_TIMEOUT_SECS),
            durability: DurabilityMode::Eventual,
        }
    }
}

impl RedbStoreOptions {
    /// Create a new options struct with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the cache size in bytes.
    ///
    /// Redb uses this to control memory usage for caching pages.
    /// Larger values can improve read performance for frequently accessed data.
    ///
    /// If not set, redb defaults to 1 GiB (1,073,741,824 bytes).
    /// A value of 0 disables caching entirely, which may significantly
    /// degrade performance.
    pub fn with_cache_size(mut self, bytes: usize) -> Self {
        self.cache_size = Some(bytes);
        self
    }

    /// Get the configured cache size, if set.
    pub fn cache_size(&self) -> Option<usize> {
        self.cache_size
    }

    /// Set the close timeout duration.
    ///
    /// When `close()` is called, the store will wait up to this duration for
    /// active transactions to complete before returning an error.
    ///
    /// Default: 5 seconds
    ///
    /// # Arguments
    ///
    /// * `timeout` - Maximum time to wait for transactions to complete
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
    /// `DurabilityMode::Eventual` (default) defers flushing to the OS,
    /// matching Go DefraDB's BadgerDB defaults.
    /// `DurabilityMode::Immediate` flushes to disk on every commit.
    pub fn with_durability(mut self, mode: DurabilityMode) -> Self {
        self.durability = mode;
        self
    }

    /// Get the configured durability mode.
    pub fn durability(&self) -> DurabilityMode {
        self.durability
    }
}
