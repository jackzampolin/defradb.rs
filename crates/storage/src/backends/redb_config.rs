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
///
/// let opts = RedbStoreOptions::new()
///     .with_cache_size(64 * 1024 * 1024); // 64MB cache
///
/// let store = RedbStore::open_with_options("/path/to/db", opts)?;
/// ```
#[derive(Debug, Clone, Default)]
pub struct RedbStoreOptions {
    cache_size: Option<usize>,
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
    pub fn with_cache_size(mut self, bytes: usize) -> Self {
        self.cache_size = Some(bytes);
        self
    }

    /// Get the configured cache size, if set.
    pub fn cache_size(&self) -> Option<usize> {
        self.cache_size
    }
}
