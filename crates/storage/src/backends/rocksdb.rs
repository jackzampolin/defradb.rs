/// RocksDB backend implementation with write batch atomicity.
///
/// This backend provides a production-ready persistent key-value store using
/// RocksDB. It uses write batches for atomic commits with read-your-writes
/// consistency within transactions.
///
/// # Features
///
/// - Persistent storage with LSM-tree architecture
/// - Atomic writes via write batches
/// - Read-your-writes consistency within transactions
/// - High performance with configurable caching and compaction
/// - Crash recovery with write-ahead logging (WAL)
///
/// # Limitations
///
/// - No full snapshot isolation (reads may see concurrent writes)
/// - No optimistic concurrency control (conflicts not detected)
/// - Future versions will add proper MVCC support
///
/// # Use Cases
///
/// - Production deployments
/// - Large datasets that don't fit in memory
/// - Applications requiring persistence
/// - High-throughput workloads
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
    DBWithThreadMode, MultiThreaded, Options, WriteBatch, WriteOptions,
};
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

        Ok(Box::new(RocksDBTxn {
            db: Arc::clone(&self.db),
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

/// RocksDB transaction with write batching.
///
/// Transactions use RocksDB write batches for atomic writes.
/// Note: This is a simplified implementation without full snapshot isolation.
/// Future versions will add proper MVCC support.
struct RocksDBTxn {
    /// Reference to the RocksDB database
    db: Arc<DBWithThreadMode<MultiThreaded>>,

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

        // Read from DB
        match self.db.get(key)? {
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

        Ok(self.db.get(key)?.is_some())
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
        let db_iter = match (&start_bound, opts.reverse()) {
            (Some(start), false) => self.db.iterator(rocksdb::IteratorMode::From(start, rocksdb::Direction::Forward)),
            (Some(_), true) => {
                // For reverse with start bound, we need to start from end_bound or prefix end
                if let Some(ref end) = end_bound {
                    self.db.iterator(rocksdb::IteratorMode::From(end, rocksdb::Direction::Reverse))
                } else {
                    self.db.iterator(rocksdb::IteratorMode::End)
                }
            }
            (None, false) => self.db.iterator(rocksdb::IteratorMode::Start),
            (None, true) => self.db.iterator(rocksdb::IteratorMode::End),
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

    fn is_valid(&self) -> bool {
        !self.closed && self.position < self.data.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn create_test_store() -> (RocksDBStore, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let store = RocksDBStore::open(temp_dir.path()).unwrap();
        (store, temp_dir)
    }

    #[tokio::test]
    async fn test_rocksdb_store_basic() {
        let (store, _temp_dir) = create_test_store().await;
        let mut txn = store.new_txn(false).await.unwrap();

        txn.set(b"key1", b"value1").await.unwrap();
        txn.set(b"key2", b"value2").await.unwrap();

        assert_eq!(txn.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
        assert_eq!(txn.get(b"key2").await.unwrap(), Some(b"value2".to_vec()));

        txn.commit().await.unwrap();
    }

    #[tokio::test]
    async fn test_rocksdb_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Write and close
        {
            let store = RocksDBStore::open(&path).unwrap();
            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(b"persistent", b"data").await.unwrap();
            txn.commit().await.unwrap();
        }

        // Reopen and verify
        {
            let store = RocksDBStore::open(&path).unwrap();
            let txn = store.new_txn(true).await.unwrap();
            assert_eq!(
                txn.get(b"persistent").await.unwrap(),
                Some(b"data".to_vec())
            );
        }
    }

    #[tokio::test]
    async fn test_rocksdb_delete() {
        let (store, _temp_dir) = create_test_store().await;

        // Set a value
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"key", b"value").await.unwrap();
        txn.commit().await.unwrap();

        // Delete it
        let mut txn = store.new_txn(false).await.unwrap();
        txn.delete(b"key").await.unwrap();
        txn.commit().await.unwrap();

        // Verify deletion
        let txn = store.new_txn(true).await.unwrap();
        assert_eq!(txn.get(b"key").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_rocksdb_readonly() {
        let (store, _temp_dir) = create_test_store().await;
        let mut txn = store.new_txn(true).await.unwrap();

        let result = txn.set(b"key", b"value").await;
        assert!(matches!(result, Err(Error::ReadOnlyTxn)));
    }

    #[tokio::test]
    async fn test_rocksdb_discard() {
        let (store, _temp_dir) = create_test_store().await;
        let mut txn = store.new_txn(false).await.unwrap();

        txn.set(b"key", b"value").await.unwrap();
        txn.discard();

        // Value shouldn't be persisted
        let txn = store.new_txn(true).await.unwrap();
        assert_eq!(txn.get(b"key").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_rocksdb_iterator() {
        let (store, _temp_dir) = create_test_store().await;
        let mut txn = store.new_txn(false).await.unwrap();

        txn.set(b"key1", b"value1").await.unwrap();
        txn.set(b"key2", b"value2").await.unwrap();
        txn.set(b"key3", b"value3").await.unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let mut iter = txn.iterator(IterOptions::new()).await.unwrap();

        let mut count = 0;
        while let Some(_kv) = iter.next().await.unwrap() {
            count += 1;
        }
        assert_eq!(count, 3);

        iter.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_rocksdb_empty_key_rejected() {
        let (store, _temp_dir) = create_test_store().await;
        let mut txn = store.new_txn(false).await.unwrap();

        // Empty key should be rejected for set
        let result = txn.set(b"", b"value").await;
        assert!(matches!(result, Err(Error::EmptyKey)));

        // Empty key should be rejected for get
        let result = txn.get(b"").await;
        assert!(matches!(result, Err(Error::EmptyKey)));

        // Empty key should be rejected for delete
        let result = txn.delete(b"").await;
        assert!(matches!(result, Err(Error::EmptyKey)));

        // Empty key should be rejected for has
        let result = txn.has(b"").await;
        assert!(matches!(result, Err(Error::EmptyKey)));
    }

    #[tokio::test]
    async fn test_rocksdb_closed_store_rejected() {
        let (store, _temp_dir) = create_test_store().await;

        // Close the store
        store.close().await.unwrap();

        // Attempting to create a transaction on closed store should fail
        let result = store.new_txn(false).await;
        assert!(matches!(result, Err(Error::DBClosed)));

        // Read-only transaction should also fail
        let result = store.new_txn(true).await;
        assert!(matches!(result, Err(Error::DBClosed)));
    }

    #[tokio::test]
    async fn test_rocksdb_has_operation() {
        let (store, _temp_dir) = create_test_store().await;

        // has() should return false for non-existent key
        let txn = store.new_txn(true).await.unwrap();
        assert!(!txn.has(b"nonexistent").await.unwrap());
        drop(txn);

        // Set a key and verify has() returns true
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"test_key", b"test_value").await.unwrap();
        assert!(txn.has(b"test_key").await.unwrap());
        txn.commit().await.unwrap();

        // Verify has() returns true after commit
        let txn = store.new_txn(true).await.unwrap();
        assert!(txn.has(b"test_key").await.unwrap());
        drop(txn);

        // Delete the key and verify has() returns false
        let mut txn = store.new_txn(false).await.unwrap();
        txn.delete(b"test_key").await.unwrap();
        assert!(!txn.has(b"test_key").await.unwrap());
        txn.commit().await.unwrap();

        // Verify has() returns false after delete
        let txn = store.new_txn(true).await.unwrap();
        assert!(!txn.has(b"test_key").await.unwrap());
    }

    #[tokio::test]
    async fn test_rocksdb_read_your_writes() {
        let (store, _temp_dir) = create_test_store().await;

        // Test read-your-writes consistency within a transaction
        let mut txn = store.new_txn(false).await.unwrap();

        // Write a value
        txn.set(b"key", b"value1").await.unwrap();

        // Should be able to read the uncommitted value
        assert_eq!(txn.get(b"key").await.unwrap(), Some(b"value1".to_vec()));

        // Update the value
        txn.set(b"key", b"value2").await.unwrap();

        // Should see the updated value
        assert_eq!(txn.get(b"key").await.unwrap(), Some(b"value2".to_vec()));

        // Delete the key
        txn.delete(b"key").await.unwrap();

        // Should see deletion
        assert_eq!(txn.get(b"key").await.unwrap(), None);

        txn.commit().await.unwrap();
    }

    #[tokio::test]
    async fn test_rocksdb_callbacks() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let (store, _temp_dir) = create_test_store().await;
        let mut txn = store.new_txn(false).await.unwrap();

        let success_called = Arc::new(AtomicBool::new(false));
        let success_called_clone = Arc::clone(&success_called);

        txn.on_success(Box::new(move || {
            success_called_clone.store(true, Ordering::SeqCst);
        }));

        txn.set(b"key", b"value").await.unwrap();
        txn.commit().await.unwrap();

        assert!(success_called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_rocksdb_iterator_closed_returns_error() {
        let (store, _temp_dir) = create_test_store().await;
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"key1", b"value1").await.unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let mut iter = txn.iterator(IterOptions::new()).await.unwrap();

        // Close the iterator
        iter.close().await.unwrap();

        // Using closed iterator should return an error
        let result = iter.next().await;
        assert!(matches!(result, Err(Error::Iterator(_))));

        // is_valid should return false
        assert!(!iter.is_valid());
    }

    #[tokio::test]
    async fn test_rocksdb_async_callback_execution() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        use std::time::Duration;

        let (store, _temp_dir) = create_test_store().await;
        let mut txn = store.new_txn(false).await.unwrap();

        let async_success_called = Arc::new(AtomicBool::new(false));
        let async_success_called_clone = Arc::clone(&async_success_called);

        txn.on_success_async(Box::new(move || {
            let flag = Arc::clone(&async_success_called_clone);
            Box::pin(async move {
                // Simulate async work
                tokio::time::sleep(Duration::from_millis(10)).await;
                flag.store(true, Ordering::SeqCst);
            })
        }));

        txn.set(b"key", b"value").await.unwrap();
        txn.commit().await.unwrap();

        // Async callback should have been awaited during commit
        assert!(async_success_called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_rocksdb_iterator_prefix_filtering() {
        let (store, _temp_dir) = create_test_store().await;
        let mut txn = store.new_txn(false).await.unwrap();

        // Insert keys with different prefixes
        txn.set(b"user/1", b"alice").await.unwrap();
        txn.set(b"user/2", b"bob").await.unwrap();
        txn.set(b"post/1", b"hello").await.unwrap();
        txn.set(b"post/2", b"world").await.unwrap();
        txn.commit().await.unwrap();

        // Iterate with user/ prefix
        let txn = store.new_txn(true).await.unwrap();
        let opts = IterOptions::new().with_prefix(b"user/".to_vec());
        let mut iter = txn.iterator(opts).await.unwrap();

        let mut user_keys: Vec<String> = vec![];
        while let Some(kv) = iter.next().await.unwrap() {
            user_keys.push(kv.key_str());
        }

        assert_eq!(user_keys.len(), 2);
        assert!(user_keys.contains(&"user/1".to_string()));
        assert!(user_keys.contains(&"user/2".to_string()));

        // Iterate with post/ prefix
        let opts = IterOptions::new().with_prefix(b"post/".to_vec());
        let mut iter = txn.iterator(opts).await.unwrap();

        let mut post_keys: Vec<String> = vec![];
        while let Some(kv) = iter.next().await.unwrap() {
            post_keys.push(kv.key_str());
        }

        assert_eq!(post_keys.len(), 2);
        assert!(post_keys.contains(&"post/1".to_string()));
        assert!(post_keys.contains(&"post/2".to_string()));
    }

    #[tokio::test]
    async fn test_rocksdb_iterator_start_end_range() {
        let (store, _temp_dir) = create_test_store().await;
        let mut txn = store.new_txn(false).await.unwrap();

        // Insert keys: a, b, c, d, e
        for key in [b"a", b"b", b"c", b"d", b"e"] {
            txn.set(key, b"value").await.unwrap();
        }
        txn.commit().await.unwrap();

        // Test start and end bounds
        let txn = store.new_txn(true).await.unwrap();
        let opts = IterOptions::new()
            .with_start(b"b".to_vec())
            .with_end(b"e".to_vec());
        let mut iter = txn.iterator(opts).await.unwrap();

        let mut keys: Vec<String> = vec![];
        while let Some(kv) = iter.next().await.unwrap() {
            keys.push(kv.key_str());
        }

        assert_eq!(keys, vec!["b", "c", "d"]);
    }
}
