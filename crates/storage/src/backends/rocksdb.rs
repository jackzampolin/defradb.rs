/// RocksDB backend implementation with write batch atomicity and snapshot isolation.
///
/// This backend provides a production-ready persistent key-value store using
/// RocksDB. It uses write batches for atomic commits, read-your-writes
/// consistency within transactions, and snapshot isolation for reads.
///
/// # Features
///
/// - Persistent storage with LSM-tree architecture
/// - Atomic writes via write batches
/// - Read-your-writes consistency within transactions
/// - **Snapshot isolation**: Readers see a consistent view of data from transaction start
/// - High performance with configurable caching and compaction
/// - Crash recovery with write-ahead logging (WAL)
///
/// # MVCC Behavior
///
/// When a transaction is created, it captures a RocksDB snapshot. All reads
/// within the transaction see the database state as it was at that moment,
/// regardless of concurrent writes by other transactions.
///
/// # Use Cases
///
/// - Production deployments
/// - Large datasets that don't fit in memory
/// - Applications requiring persistence
/// - High-throughput workloads with concurrent readers/writers
///
/// # Example
///
/// ```ignore
/// use storage::backends::rocksdb::RocksDBStore;
/// use storage::corekv::{Store, Reader, Writer};
///
/// let store = RocksDBStore::open("/path/to/db")?;
/// let mut txn = store.new_txn(false).await?;
/// txn.set(b"key", b"value").await?;
/// txn.commit().await?;
/// ```

use async_trait::async_trait;
use parking_lot::Mutex;
use rocksdb::{
    DBWithThreadMode, MultiThreaded, Options, Snapshot, WriteBatch, WriteOptions,
};
use std::mem::ManuallyDrop;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::corekv::{
    AsyncTxnCallback, Error, Iterator, IterOptions, KvPair, Reader, Result, Store, Txn,
    TxnCallback, Writer,
};

/// RocksDB-backed key-value store.
///
/// This store wraps a RocksDB database instance and provides MVCC transaction
/// support through RocksDB's snapshot and write batch mechanisms.
pub struct RocksDBStore {
    db: Arc<DBWithThreadMode<MultiThreaded>>,
    closed: Arc<RwLock<bool>>,
}

impl RocksDBStore {
    /// Open a RocksDB database at the specified path.
    ///
    /// If the database doesn't exist, it will be created. The database will be
    /// configured with sensible defaults for DefraDB usage.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the database directory
    ///
    /// # Returns
    ///
    /// * `Ok(RocksDBStore)` on success
    /// * `Err(Error)` if the database cannot be opened
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // Configure for write-heavy workloads
        opts.set_max_write_buffer_number(3);
        opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB

        // Enable bloom filters for faster lookups
        opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(4));

        let db = DBWithThreadMode::open(&opts, path)?;

        Ok(Self {
            db: Arc::new(db),
            closed: Arc::new(RwLock::new(false)),
        })
    }

    /// Open a RocksDB database with custom options.
    ///
    /// This allows full control over RocksDB configuration.
    pub fn open_with_opts<P: AsRef<Path>>(path: P, opts: Options) -> Result<Self> {
        let db = DBWithThreadMode::open(&opts, path)?;

        Ok(Self {
            db: Arc::new(db),
            closed: Arc::new(RwLock::new(false)),
        })
    }

    /// Check if the store is closed.
    async fn is_closed(&self) -> bool {
        *self.closed.read().await
    }
}

#[async_trait]
impl Store for RocksDBStore {
    async fn new_txn(&self, readonly: bool) -> Result<Box<dyn Txn>> {
        if self.is_closed().await {
            return Err(Error::DBClosed);
        }

        let db = Arc::clone(&self.db);

        // Create a snapshot for read isolation.
        // SAFETY: We transmute the snapshot lifetime to 'static because:
        // 1. The db Arc is stored in the same struct as the snapshot
        // 2. We implement Drop to ensure snapshot is dropped first
        // 3. The Arc ensures the DB won't be dropped while we hold a reference
        let snapshot: Snapshot<'static> = unsafe { std::mem::transmute(db.snapshot()) };

        Ok(Box::new(RocksDBTxn {
            db,
            snapshot: ManuallyDrop::new(snapshot),
            batch: Mutex::new(WriteBatch::default()),
            pending: Mutex::new(std::collections::HashMap::new()),
            readonly,
            discarded: Mutex::new(false),
            on_success: Mutex::new(Vec::new()),
            on_success_async: Mutex::new(Vec::new()),
            on_error: Mutex::new(Vec::new()),
            on_error_async: Mutex::new(Vec::new()),
            on_discard: Mutex::new(Vec::new()),
            on_discard_async: Mutex::new(Vec::new()),
        }))
    }

    async fn close(&self) -> Result<()> {
        let mut closed = self.closed.write().await;
        *closed = true;
        // RocksDB will be closed when the Arc drops
        Ok(())
    }
}

/// RocksDB transaction with write batching and snapshot isolation.
///
/// Transactions use RocksDB write batches for atomic writes and snapshots
/// for consistent reads. The snapshot captures the database state at
/// transaction creation time, providing true MVCC isolation.
///
/// # Safety
///
/// The snapshot holds a reference to the database. We use ManuallyDrop
/// and implement Drop to ensure the snapshot is released before the db
/// Arc could potentially drop the database.
struct RocksDBTxn {
    /// Reference to the RocksDB database
    db: Arc<DBWithThreadMode<MultiThreaded>>,

    /// Snapshot for read isolation - captures DB state at transaction start.
    ///
    /// SAFETY: The lifetime is transmuted to 'static, but the snapshot is
    /// always dropped before the db Arc in our Drop implementation.
    /// ManuallyDrop ensures we control the drop order.
    snapshot: ManuallyDrop<Snapshot<'static>>,

    /// Write batch for pending changes
    batch: Mutex<WriteBatch>,

    /// Pending writes tracker for read-your-writes support
    /// (Some(value) = set, None = delete)
    pending: Mutex<std::collections::HashMap<Vec<u8>, Option<Vec<u8>>>>,

    /// Whether this is a read-only transaction
    readonly: bool,

    /// Whether the transaction has been discarded
    discarded: Mutex<bool>,

    /// Callbacks for successful commit
    on_success: Mutex<Vec<TxnCallback>>,
    on_success_async: Mutex<Vec<AsyncTxnCallback>>,

    /// Callbacks for failed commit
    on_error: Mutex<Vec<TxnCallback>>,
    on_error_async: Mutex<Vec<AsyncTxnCallback>>,

    /// Callbacks for discard
    on_discard: Mutex<Vec<TxnCallback>>,
    on_discard_async: Mutex<Vec<AsyncTxnCallback>>,
}

impl Drop for RocksDBTxn {
    fn drop(&mut self) {
        // SAFETY: We must drop the snapshot before the db Arc could drop the DB.
        // ManuallyDrop ensures we control this ordering.
        // The snapshot was created from self.db and must be released first.
        unsafe {
            ManuallyDrop::drop(&mut self.snapshot);
        }
        // Now db Arc can safely drop (though it likely won't since other refs exist)
    }
}

impl RocksDBTxn {
    /// Execute sync callbacks with panic protection.
    ///
    /// Each callback is wrapped in catch_unwind to ensure that a panic in one
    /// callback doesn't prevent execution of subsequent callbacks.
    fn execute_callbacks(callbacks: Vec<TxnCallback>) {
        for (i, callback) in callbacks.into_iter().enumerate() {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback));
            if let Err(panic_info) = result {
                tracing::error!(
                    callback_index = i,
                    panic = ?panic_info,
                    "Transaction callback panicked - continuing with remaining callbacks"
                );
            }
        }
    }

    /// Execute async callbacks with panic protection.
    ///
    /// Each callback is executed sequentially with panic catching via FutureExt::catch_unwind.
    async fn execute_async_callbacks(callbacks: Vec<AsyncTxnCallback>) {
        use futures::FutureExt;

        for (i, callback) in callbacks.into_iter().enumerate() {
            let future = callback();
            // Wrap the future in AssertUnwindSafe and catch panics
            let result = std::panic::AssertUnwindSafe(future).catch_unwind().await;
            if let Err(panic_info) = result {
                tracing::error!(
                    callback_index = i,
                    panic = ?panic_info,
                    "Async callback panicked during execution - continuing with remaining callbacks"
                );
            }
        }
    }
}

#[async_trait]
impl Reader for RocksDBTxn {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if *self.discarded.lock() {
            return Err(Error::DiscardedTxn);
        }

        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        // Check pending writes first (read-your-writes)
        let pending = self.pending.lock();
        if let Some(pending_value) = pending.get(key) {
            return Ok(pending_value.clone());
        }
        drop(pending);

        // Read from snapshot for isolation (sees DB state at txn start)
        match self.snapshot.get(key)? {
            Some(value) => Ok(Some(value.to_vec())),
            None => Ok(None),
        }
    }

    async fn has(&self, key: &[u8]) -> Result<bool> {
        if *self.discarded.lock() {
            return Err(Error::DiscardedTxn);
        }

        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        // Check pending writes first
        let pending = self.pending.lock();
        if let Some(pending_value) = pending.get(key) {
            return Ok(pending_value.is_some());
        }
        drop(pending);

        // Read from snapshot for isolation
        Ok(self.snapshot.get(key)?.is_some())
    }

    async fn get_size(&self, key: &[u8]) -> Result<Option<usize>> {
        if *self.discarded.lock() {
            return Err(Error::DiscardedTxn);
        }

        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        // Check pending writes first
        let pending = self.pending.lock();
        if let Some(pending_value) = pending.get(key) {
            return Ok(pending_value.as_ref().map(|v| v.len()));
        }
        drop(pending);

        // Read from snapshot for isolation
        match self.snapshot.get(key)? {
            Some(value) => Ok(Some(value.len())),
            None => Ok(None),
        }
    }

    async fn iterator(&self, opts: IterOptions) -> Result<Box<dyn Iterator>> {
        if *self.discarded.lock() {
            return Err(Error::DiscardedTxn);
        }

        // Use a BTreeMap to merge DB data with pending writes
        // Only collect keys that match the filter criteria to avoid loading entire DB
        let mut merged: std::collections::BTreeMap<Vec<u8>, Vec<u8>> = std::collections::BTreeMap::new();

        // Determine iteration bounds based on options
        let (start_bound, end_bound) = if let Some(prefix) = opts.prefix() {
            // Use prefix to limit iteration range
            let start = prefix.to_vec();
            // Calculate end bound: increment last byte of prefix (or append 0xFF if all 0xFF)
            let mut end = prefix.to_vec();
            let mut found_non_ff = false;
            for i in (0..end.len()).rev() {
                if end[i] < 0xFF {
                    end[i] += 1;
                    end.truncate(i + 1);
                    found_non_ff = true;
                    break;
                }
            }
            if !found_non_ff {
                // All bytes are 0xFF, append 0xFF to create upper bound
                end.push(0xFF);
            }
            (Some(start), Some(end))
        } else if opts.start().is_some() || opts.end().is_some() {
            (opts.start().map(|s| s.to_vec()), opts.end().map(|e| e.to_vec()))
        } else {
            (None, None)
        };

        // Use appropriate iterator mode based on bounds
        // Use snapshot iterator for consistent reads (snapshot isolation)
        let db_iter = match (&start_bound, opts.reverse()) {
            (Some(start), false) => self.snapshot.iterator(rocksdb::IteratorMode::From(start, rocksdb::Direction::Forward)),
            (Some(_), true) => {
                // For reverse with start bound, we need to start from end_bound or prefix end
                if let Some(ref end) = end_bound {
                    self.snapshot.iterator(rocksdb::IteratorMode::From(end, rocksdb::Direction::Reverse))
                } else {
                    self.snapshot.iterator(rocksdb::IteratorMode::End)
                }
            }
            (None, false) => self.snapshot.iterator(rocksdb::IteratorMode::Start),
            (None, true) => self.snapshot.iterator(rocksdb::IteratorMode::End),
        };

        // Collect from DB with early termination based on bounds
        for item in db_iter {
            let (key, value) = item?;
            let key_vec = key.to_vec();

            // Apply prefix filter
            if let Some(prefix) = opts.prefix() {
                if !key_vec.starts_with(prefix) {
                    // For forward iteration, if we've passed the prefix range, stop
                    if !opts.reverse() && key_vec.as_slice() >= end_bound.as_ref().map(|e| e.as_slice()).unwrap_or(&[0xFF]) {
                        break;
                    }
                    // For reverse iteration, if we're before the prefix range, stop
                    if opts.reverse() && key_vec.as_slice() < start_bound.as_ref().map(|s| s.as_slice()).unwrap_or(&[]) {
                        break;
                    }
                    continue;
                }
            }

            // Apply start bound
            if let Some(start) = opts.start() {
                if key_vec.as_slice() < start {
                    if opts.reverse() {
                        break; // Past our range in reverse
                    }
                    continue;
                }
            }

            // Apply end bound
            if let Some(end) = opts.end() {
                if key_vec.as_slice() >= end {
                    if !opts.reverse() {
                        break; // Past our range in forward
                    }
                    continue;
                }
            }

            merged.insert(key_vec, value.to_vec());
        }

        // Then, merge pending writes (overwrites DB values)
        // Only include pending writes that match the filter criteria
        let pending = self.pending.lock();
        for (key, value) in pending.iter() {
            // Apply prefix filter
            if let Some(prefix) = opts.prefix() {
                if !key.starts_with(prefix) {
                    continue;
                }
            }

            // Apply start bound
            if let Some(start) = opts.start() {
                if key.as_slice() < start {
                    continue;
                }
            }

            // Apply end bound
            if let Some(end) = opts.end() {
                if key.as_slice() >= end {
                    continue;
                }
            }

            match value {
                Some(v) => {
                    merged.insert(key.clone(), v.clone());
                }
                None => {
                    merged.remove(key);
                }
            }
        }
        drop(pending);

        // Convert to vector and apply ordering
        let mut data: Vec<KvPair> = merged
            .into_iter()
            .map(|(key, value)| {
                if opts.keys_only() {
                    KvPair::key_only(key)
                } else {
                    KvPair::new(key, value)
                }
            })
            .collect();

        if opts.reverse() {
            data.reverse();
        }

        Ok(Box::new(SimpleIterator { data, position: 0, closed: false }))
    }
}

#[async_trait]
impl Writer for RocksDBTxn {
    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if *self.discarded.lock() {
            return Err(Error::DiscardedTxn);
        }

        if self.readonly {
            return Err(Error::ReadOnlyTxn);
        }

        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        // Track in pending for read-your-writes
        self.pending.lock().insert(key.to_vec(), Some(value.to_vec()));

        // Add to batch
        self.batch.lock().put(key, value);
        Ok(())
    }

    async fn delete(&mut self, key: &[u8]) -> Result<()> {
        if *self.discarded.lock() {
            return Err(Error::DiscardedTxn);
        }

        if self.readonly {
            return Err(Error::ReadOnlyTxn);
        }

        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        // Track in pending (None = deleted)
        self.pending.lock().insert(key.to_vec(), None);

        // Add to batch
        self.batch.lock().delete(key);
        Ok(())
    }
}

#[async_trait]
impl Txn for RocksDBTxn {
    async fn commit(self: Box<Self>) -> Result<()> {
        if *self.discarded.lock() {
            return Err(Error::DiscardedTxn);
        }

        // Write the batch atomically
        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(false); // Use WAL without fsync for better performance

        // Take ownership of the batch
        let batch = std::mem::replace(&mut *self.batch.lock(), WriteBatch::default());

        match self.db.write_opt(batch, &write_opts) {
            Ok(_) => {
                // Execute success callbacks
                let on_success = std::mem::take(&mut *self.on_success.lock());
                let on_success_async = std::mem::take(&mut *self.on_success_async.lock());
                Self::execute_callbacks(on_success);
                Self::execute_async_callbacks(on_success_async).await;
                Ok(())
            }
            Err(e) => {
                // Execute error callbacks
                let on_error = std::mem::take(&mut *self.on_error.lock());
                let on_error_async = std::mem::take(&mut *self.on_error_async.lock());
                Self::execute_callbacks(on_error);
                Self::execute_async_callbacks(on_error_async).await;
                Err(e.into())
            }
        }
    }

    fn discard(self: Box<Self>) {
        *self.discarded.lock() = true;

        // Execute sync discard callbacks
        let on_discard = std::mem::take(&mut *self.on_discard.lock());
        Self::execute_callbacks(on_discard);

        // Handle async callbacks: spawn them in background with warning
        let on_discard_async = std::mem::take(&mut *self.on_discard_async.lock());
        if !on_discard_async.is_empty() {
            let callback_count = on_discard_async.len();
            tracing::warn!(
                count = callback_count,
                "Transaction has async discard callbacks. Spawning in background - they may not complete if process exits. Consider using commit() instead of discard() when async callbacks are registered."
            );

            // Spawn async callbacks in background with error tracking
            // NOTE: These may not complete if the process exits before they finish
            tokio::spawn(async move {
                Self::execute_async_callbacks(on_discard_async).await;
                tracing::debug!(
                    count = callback_count,
                    "Async discard callbacks completed"
                );
            });
        }
    }

    fn on_success(&mut self, callback: TxnCallback) {
        self.on_success.lock().push(callback);
    }

    fn on_success_async(&mut self, callback: AsyncTxnCallback) {
        self.on_success_async.lock().push(callback);
    }

    fn on_error(&mut self, callback: TxnCallback) {
        self.on_error.lock().push(callback);
    }

    fn on_error_async(&mut self, callback: AsyncTxnCallback) {
        self.on_error_async.lock().push(callback);
    }

    fn on_discard(&mut self, callback: TxnCallback) {
        self.on_discard.lock().push(callback);
    }

    fn on_discard_async(&mut self, callback: AsyncTxnCallback) {
        self.on_discard_async.lock().push(callback);
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn is_readonly(&self) -> bool {
        self.readonly
    }
}

/// Simple in-memory iterator for RocksDB results.
///
/// This collects matching keys into memory based on prefix/range filters.
/// The iterator tracks its closed state to prevent use after close.
struct SimpleIterator {
    data: Vec<KvPair>,
    position: usize,
    closed: bool,
}

#[async_trait]
impl Iterator for SimpleIterator {
    async fn next(&mut self) -> Result<Option<KvPair>> {
        if self.closed {
            return Err(Error::Iterator("Iterator has been closed".into()));
        }

        if self.position >= self.data.len() {
            return Ok(None);
        }

        let kv = self.data[self.position].clone();
        self.position += 1;
        Ok(Some(kv))
    }

    async fn close(&mut self) -> Result<()> {
        self.closed = true;
        // Clear data to free memory
        self.data.clear();
        Ok(())
    }

    async fn seek(&mut self, key: &[u8]) -> Result<bool> {
        if self.closed {
            return Err(Error::Iterator("Iterator has been closed".into()));
        }

        // Find first key >= seek_key
        let pos = self.data.iter().position(|kv| kv.key_bytes() >= key);

        match pos {
            Some(p) => {
                self.position = p;
                Ok(true)
            }
            None => {
                self.position = self.data.len();
                Ok(false)
            }
        }
    }

    async fn reset(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::Iterator("Iterator has been closed".into()));
        }

        self.position = 0;
        Ok(())
    }

    fn is_valid(&self) -> bool {
        !self.closed && self.position < self.data.len()
    }
}

// ============================================================================
// SHARED TEST SUITE - Run same tests against all backends
// ============================================================================
#[cfg(test)]
mod shared_tests {
    use super::*;
    use crate::generate_backend_tests;
    use crate::generate_backend_concurrency_tests;
    use tempfile::TempDir;

    // Each test gets a fresh store - TempDir cleanup is automatic
    async fn create_store() -> RocksDBStore {
        let temp_dir = TempDir::new().unwrap();
        // Keep the TempDir so it lives for the duration of the test
        let path = temp_dir.path().to_path_buf();
        std::mem::forget(temp_dir);  // Prevent cleanup until test ends
        RocksDBStore::open(&path).unwrap()
    }

    async fn create_arc_store() -> std::sync::Arc<RocksDBStore> {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();
        std::mem::forget(temp_dir);
        std::sync::Arc::new(RocksDBStore::open(&path).unwrap())
    }

    // Generate all standard backend tests
    generate_backend_tests!(create_store);

    // Generate concurrency tests
    generate_backend_concurrency_tests!(create_arc_store);
}

// ============================================================================
// ROCKSDB-SPECIFIC TESTS - Persistence and known limitations
// ============================================================================
#[cfg(test)]
mod rocksdb_specific_tests {
    use super::*;
    use tempfile::TempDir;

    // =========================================================================
    // PERSISTENCE TESTS - RocksDB-specific durability (Memory doesn't have this)
    // =========================================================================

    #[tokio::test]
    async fn test_rocksdb_data_survives_close_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Write data and close
        {
            let store = RocksDBStore::open(&path).unwrap();
            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(b"persistent_key", b"persistent_value")
                .await
                .unwrap();
            txn.commit().await.unwrap();
            store.close().await.unwrap();
        }

        // Reopen and verify
        {
            let store = RocksDBStore::open(&path).unwrap();
            let txn = store.new_txn(true).await.unwrap();
            assert_eq!(
                txn.get(b"persistent_key").await.unwrap(),
                Some(b"persistent_value".to_vec()),
                "Data should survive close/reopen"
            );
        }
    }

    #[tokio::test]
    async fn test_rocksdb_uncommitted_data_lost_on_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Write data but DON'T commit
        {
            let store = RocksDBStore::open(&path).unwrap();
            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(b"uncommitted_key", b"value").await.unwrap();
            // No commit! Discard.
            txn.discard();
            store.close().await.unwrap();
        }

        // Reopen - uncommitted data should be gone
        {
            let store = RocksDBStore::open(&path).unwrap();
            let txn = store.new_txn(true).await.unwrap();
            assert_eq!(
                txn.get(b"uncommitted_key").await.unwrap(),
                None,
                "Uncommitted data should not survive reopen"
            );
        }
    }

    #[tokio::test]
    async fn test_rocksdb_persistence_through_multiple_sessions() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Session 1: Write keys
        {
            let store = RocksDBStore::open(&path).unwrap();
            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(b"key1", b"value1").await.unwrap();
            txn.set(b"key2", b"value2").await.unwrap();
            txn.commit().await.unwrap();
        }

        // Session 2: Modify and add
        {
            let store = RocksDBStore::open(&path).unwrap();
            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(b"key1", b"modified").await.unwrap();
            txn.set(b"key3", b"value3").await.unwrap();
            txn.delete(b"key2").await.unwrap();
            txn.commit().await.unwrap();
        }

        // Session 3: Verify all changes
        {
            let store = RocksDBStore::open(&path).unwrap();
            let txn = store.new_txn(true).await.unwrap();
            assert_eq!(txn.get(b"key1").await.unwrap(), Some(b"modified".to_vec()));
            assert_eq!(txn.get(b"key2").await.unwrap(), None);
            assert_eq!(txn.get(b"key3").await.unwrap(), Some(b"value3".to_vec()));
        }
    }

    // =========================================================================
    // SNAPSHOT ISOLATION TESTS
    // Verify RocksDB transactions provide proper MVCC isolation
    // =========================================================================

    /// Test that RocksDB transactions have snapshot isolation.
    /// Readers see a consistent view from transaction start, not concurrent commits.
    #[tokio::test]
    async fn test_rocksdb_snapshot_isolation() {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(RocksDBStore::open(temp_dir.path()).unwrap());

        // Setup initial value
        let mut setup = store.new_txn(false).await.unwrap();
        setup.set(b"key", b"initial").await.unwrap();
        setup.commit().await.unwrap();

        // Start a reader BEFORE the write
        let reader = store.new_txn(true).await.unwrap();

        // Concurrent writer commits
        let mut writer = store.new_txn(false).await.unwrap();
        writer.set(b"key", b"modified").await.unwrap();
        writer.commit().await.unwrap();

        // Reader should see the ORIGINAL value (snapshot isolation)
        let value = reader.get(b"key").await.unwrap();
        assert_eq!(
            value,
            Some(b"initial".to_vec()),
            "Reader should see original value (snapshot isolation)"
        );

        // A new reader should see the modified value
        let new_reader = store.new_txn(true).await.unwrap();
        let new_value = new_reader.get(b"key").await.unwrap();
        assert_eq!(
            new_value,
            Some(b"modified".to_vec()),
            "New reader should see committed changes"
        );
    }

    /// Test snapshot isolation with multiple concurrent writers
    #[tokio::test]
    async fn test_rocksdb_snapshot_isolation_multiple_writers() {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(RocksDBStore::open(temp_dir.path()).unwrap());

        // Setup
        let mut setup = store.new_txn(false).await.unwrap();
        setup.set(b"key", b"v0").await.unwrap();
        setup.commit().await.unwrap();

        // Reader starts - sees v0
        let reader = store.new_txn(true).await.unwrap();

        // Writer 1 changes to v1
        let mut w1 = store.new_txn(false).await.unwrap();
        w1.set(b"key", b"v1").await.unwrap();
        w1.commit().await.unwrap();

        // Writer 2 changes to v2
        let mut w2 = store.new_txn(false).await.unwrap();
        w2.set(b"key", b"v2").await.unwrap();
        w2.commit().await.unwrap();

        // Original reader still sees v0
        assert_eq!(
            reader.get(b"key").await.unwrap(),
            Some(b"v0".to_vec()),
            "Original reader should still see v0"
        );

        // New reader sees v2
        let new_reader = store.new_txn(true).await.unwrap();
        assert_eq!(
            new_reader.get(b"key").await.unwrap(),
            Some(b"v2".to_vec()),
            "New reader should see latest value v2"
        );
    }

    /// Test that snapshot isolation works correctly with deletes
    #[tokio::test]
    async fn test_rocksdb_snapshot_isolation_with_deletes() {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(RocksDBStore::open(temp_dir.path()).unwrap());

        // Setup
        let mut setup = store.new_txn(false).await.unwrap();
        setup.set(b"key", b"exists").await.unwrap();
        setup.commit().await.unwrap();

        // Reader starts - sees key exists
        let reader = store.new_txn(true).await.unwrap();

        // Writer deletes the key
        let mut writer = store.new_txn(false).await.unwrap();
        writer.delete(b"key").await.unwrap();
        writer.commit().await.unwrap();

        // Original reader still sees the key
        assert_eq!(
            reader.get(b"key").await.unwrap(),
            Some(b"exists".to_vec()),
            "Reader should still see deleted key (snapshot isolation)"
        );
        assert!(
            reader.has(b"key").await.unwrap(),
            "Reader.has() should return true for deleted key"
        );

        // New reader sees key as deleted
        let new_reader = store.new_txn(true).await.unwrap();
        assert_eq!(
            new_reader.get(b"key").await.unwrap(),
            None,
            "New reader should not see deleted key"
        );
    }
}
