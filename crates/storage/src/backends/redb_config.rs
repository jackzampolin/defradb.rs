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
///     .with_close_timeout(Duration::from_secs(10)) // 10 second close timeout
///     .with_max_snapshot_keys(1_000_000); // Limit snapshots to 1M keys
///
/// let store = RedbStore::open_with_options("/path/to/db", opts)?;
/// ```
#[derive(Debug, Clone)]
pub struct RedbStoreOptions {
    cache_size: Option<usize>,
    close_timeout: Duration,
    max_snapshot_keys: Option<usize>,
}

/// Default maximum snapshot keys (1 million).
///
/// This provides reasonable OOM protection while allowing most use cases.
/// Can be increased via `with_max_snapshot_keys()` or removed with `with_max_snapshot_keys(usize::MAX)`.
pub const DEFAULT_MAX_SNAPSHOT_KEYS: usize = 1_000_000;

impl Default for RedbStoreOptions {
    fn default() -> Self {
        Self {
            cache_size: None,
            close_timeout: Duration::from_secs(DEFAULT_CLOSE_TIMEOUT_SECS),
            max_snapshot_keys: Some(DEFAULT_MAX_SNAPSHOT_KEYS),
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

    /// Set the maximum number of keys allowed in a transaction snapshot.
    ///
    /// When a transaction is created, it captures a snapshot of the database
    /// into memory. This option limits the snapshot size to prevent OOM
    /// conditions with large databases.
    ///
    /// If the database contains more keys than this limit when creating a
    /// transaction, `new_txn()` will return an error instead of loading
    /// all keys into memory.
    ///
    /// Default: 1,000,000 keys (provides OOM protection)
    ///
    /// # Recommended Values
    ///
    /// - Small databases (<100K keys): Default is fine
    /// - Medium databases (100K-1M keys): Default is fine
    /// - Large databases: Consider a different storage backend, or use
    ///   `with_unlimited_snapshot_keys()` if you have sufficient RAM
    ///
    /// # Example
    ///
    /// ```ignore
    /// let opts = RedbStoreOptions::new()
    ///     .with_max_snapshot_keys(500_000); // Limit to 500K keys
    /// ```
    pub fn with_max_snapshot_keys(mut self, max_keys: usize) -> Self {
        self.max_snapshot_keys = Some(max_keys);
        self
    }

    /// Remove the snapshot key limit (use with caution).
    ///
    /// This disables OOM protection and allows unlimited keys in snapshots.
    /// Only use this if you have sufficient RAM for your database size
    /// multiplied by the expected number of concurrent transactions.
    ///
    /// # Warning
    ///
    /// Memory usage = database_size × concurrent_transactions
    pub fn with_unlimited_snapshot_keys(mut self) -> Self {
        self.max_snapshot_keys = None;
        self
    }

    /// Get the configured maximum snapshot keys, if set.
    pub fn max_snapshot_keys(&self) -> Option<usize> {
        self.max_snapshot_keys
    }
}
