/// Redb backend implementation with snapshot isolation and ACID transactions.
///
/// This backend provides a pure Rust, WASM-compatible persistent key-value store
/// using redb. It uses read snapshots for isolation and buffered writes for
/// read-your-writes consistency within transactions.
///
/// # Features
///
/// - Pure Rust implementation (no C/C++ dependencies)
/// - WASM-compatible
/// - ACID transactions with snapshot isolation
/// - Persistent storage with crash recovery
/// - Single-writer model (matches Go DefraDB's LevelDB semantics)
///
/// # MVCC Behavior
///
/// When a transaction is created, it captures a snapshot of the database state.
/// All reads within the transaction see the database state as it was at that moment,
/// regardless of concurrent writes by other transactions.
///
/// # Memory Considerations
///
/// **WARNING**: This implementation loads the entire database snapshot into memory
/// (via `BTreeMap`) when a transaction is created. This approach provides correct
/// snapshot isolation but has significant memory implications:
///
/// - Memory usage scales linearly with database size
/// - Multiple concurrent read transactions multiply memory usage
/// - Not suitable for databases larger than available RAM
///
/// For large datasets, consider using a different backend or implementing
/// lazy-loading snapshots.
///
/// # Use Cases
///
/// - Production deployments requiring WASM compatibility
/// - Embedded applications
/// - Cross-platform storage
///
/// # Example
///
/// ```ignore
/// use storage::backends::redb::RedbStore;
/// use storage::corekv::{Store, Reader, Writer};
///
/// let store = RedbStore::open("/path/to/db")?;
/// let mut txn = store.new_txn(false).await?;
/// txn.set(b"key", b"value").await?;
/// txn.commit().await?;
/// ```
use async_trait::async_trait;
use parking_lot::Mutex;
use redb::{Database, ReadTransaction, ReadableTable, TableDefinition};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::corekv::{
    AsyncTxnCallback, Dropable, Error, IterOptions, Iterator, KvPair, Reader, Result, Store, Txn,
    TxnCallback, Writer,
};

/// Table definition for the main key-value store.
const KV_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("kv");

/// Redb-backed key-value store.
///
/// This store wraps a redb Database instance and provides MVCC transaction
/// support through snapshots and buffered writes.
pub struct RedbStore {
    db: Arc<Database>,
    closed: Arc<RwLock<bool>>,
}

impl RedbStore {
    /// Open a redb database at the specified path.
    ///
    /// If the database doesn't exist, it will be created.
    /// If the path is a directory, creates `data.redb` inside it.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the database file or directory
    ///
    /// # Returns
    ///
    /// * `Ok(RedbStore)` on success
    /// * `Err(Error)` if the database cannot be opened
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let db_path = if path.is_dir() {
            path.join("data.redb")
        } else {
            path.to_path_buf()
        };
        let db = Database::create(db_path)?;

        // Ensure the KV table exists by opening a write transaction
        {
            let write_txn = db.begin_write()?;
            let _ = write_txn.open_table(KV_TABLE)?;
            write_txn.commit()?;
        }

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
impl Store for RedbStore {
    async fn new_txn(&self, readonly: bool) -> Result<Box<dyn Txn>> {
        if self.is_closed().await {
            return Err(Error::DBClosed);
        }

        // Capture a snapshot for read isolation
        let read_txn = self.db.begin_read()?;
        let snapshot = capture_snapshot(&read_txn)?;

        Ok(Box::new(RedbTxn {
            db: Arc::clone(&self.db),
            snapshot,
            pending: Mutex::new(BTreeMap::new()),
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
        Ok(())
    }
}

#[async_trait]
impl Dropable for RedbStore {
    async fn drop_all(&self) -> Result<()> {
        if self.is_closed().await {
            return Err(Error::DBClosed);
        }

        // Open a write transaction and delete all entries
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(KV_TABLE)?;
            // Collect all keys first to avoid borrow issues
            let keys: Vec<Vec<u8>> = {
                let range = table.range::<&[u8]>(..)?;
                range
                    .map(|result| result.map(|(k, _)| k.value().to_vec()))
                    .collect::<std::result::Result<Vec<_>, _>>()?
            };
            // Delete all keys
            for key in keys {
                table.remove(key.as_slice())?;
            }
        }
        write_txn.commit()?;

        Ok(())
    }
}

/// Capture a snapshot of the current database state into memory.
fn capture_snapshot(read_txn: &ReadTransaction) -> Result<BTreeMap<Vec<u8>, Vec<u8>>> {
    let mut snapshot = BTreeMap::new();

    let table = match read_txn.open_table(KV_TABLE) {
        Ok(t) => t,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(snapshot),
        Err(e) => return Err(e.into()),
    };

    let range = table.range::<&[u8]>(..)?;
    for result in range {
        let (key, value) = result?;
        snapshot.insert(key.value().to_vec(), value.value().to_vec());
    }

    Ok(snapshot)
}

/// Redb transaction with snapshot isolation and buffered writes.
///
/// Transactions maintain a snapshot of the store at creation time and track
/// pending changes. Changes are applied atomically on commit.
struct RedbTxn {
    /// Reference to the redb database
    db: Arc<Database>,

    /// Snapshot of store at transaction start (for reads)
    snapshot: BTreeMap<Vec<u8>, Vec<u8>>,

    /// Pending changes (Some(value) = set, None = delete)
    pending: Mutex<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,

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

impl RedbTxn {
    /// Get a value, checking pending changes first, then snapshot.
    fn get_internal(&self, key: &[u8]) -> Option<Vec<u8>> {
        // Check pending changes first
        let pending = self.pending.lock();
        if let Some(pending_value) = pending.get(key) {
            return pending_value.clone();
        }

        // Fall back to snapshot
        self.snapshot.get(key).cloned()
    }

    /// Check if a key exists.
    fn has_internal(&self, key: &[u8]) -> bool {
        // Check pending changes first
        let pending = self.pending.lock();
        if let Some(pending_value) = pending.get(key) {
            return pending_value.is_some();
        }

        // Fall back to snapshot
        self.snapshot.contains_key(key)
    }

    /// Execute sync callbacks with panic protection.
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
    async fn execute_async_callbacks(callbacks: Vec<AsyncTxnCallback>) {
        use futures::FutureExt;

        for (i, callback) in callbacks.into_iter().enumerate() {
            let future = callback();
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
impl Reader for RedbTxn {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if *self.discarded.lock() {
            return Err(Error::DiscardedTxn);
        }

        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        Ok(self.get_internal(key))
    }

    async fn has(&self, key: &[u8]) -> Result<bool> {
        if *self.discarded.lock() {
            return Err(Error::DiscardedTxn);
        }

        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        Ok(self.has_internal(key))
    }

    async fn get_size(&self, key: &[u8]) -> Result<Option<usize>> {
        if *self.discarded.lock() {
            return Err(Error::DiscardedTxn);
        }

        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        Ok(self.get_internal(key).map(|v| v.len()))
    }

    async fn iterator(&self, opts: IterOptions) -> Result<Box<dyn Iterator>> {
        if *self.discarded.lock() {
            return Err(Error::DiscardedTxn);
        }

        // Merge snapshot and pending changes
        let mut merged = self.snapshot.clone();
        let pending = self.pending.lock();
        for (key, value) in pending.iter() {
            match value {
                Some(v) => {
                    merged.insert(key.clone(), v.clone());
                }
                None => {
                    merged.remove(key);
                }
            }
        }

        Ok(Box::new(RedbIterator::new(merged, opts)?))
    }
}

#[async_trait]
impl Writer for RedbTxn {
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

        self.pending
            .lock()
            .insert(key.to_vec(), Some(value.to_vec()));
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

        self.pending.lock().insert(key.to_vec(), None);
        Ok(())
    }
}

#[async_trait]
impl Txn for RedbTxn {
    async fn commit(self: Box<Self>) -> Result<()> {
        if *self.discarded.lock() {
            tracing::warn!("Attempted to commit a discarded transaction");
            let on_error = std::mem::take(&mut *self.on_error.lock());
            let on_error_async = std::mem::take(&mut *self.on_error_async.lock());
            Self::execute_callbacks(on_error);
            Self::execute_async_callbacks(on_error_async).await;
            return Err(Error::DiscardedTxn);
        }

        // Clone pending changes before any async operations
        let pending = self.pending.lock().clone();

        // Apply pending changes to the database if there are any
        if !pending.is_empty() {
            let write_txn = match self.db.begin_write() {
                Ok(txn) => txn,
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        pending_changes = pending.len(),
                        "Failed to begin write transaction during commit"
                    );
                    let on_error = std::mem::take(&mut *self.on_error.lock());
                    let on_error_async = std::mem::take(&mut *self.on_error_async.lock());
                    Self::execute_callbacks(on_error);
                    Self::execute_async_callbacks(on_error_async).await;
                    return Err(e.into());
                }
            };

            {
                let mut table = match write_txn.open_table(KV_TABLE) {
                    Ok(t) => t,
                    Err(e) => {
                        tracing::error!(
                            error = %e,
                            "Failed to open KV table during commit"
                        );
                        let on_error = std::mem::take(&mut *self.on_error.lock());
                        let on_error_async = std::mem::take(&mut *self.on_error_async.lock());
                        Self::execute_callbacks(on_error);
                        Self::execute_async_callbacks(on_error_async).await;
                        return Err(e.into());
                    }
                };

                for (key, value) in pending.iter() {
                    match value {
                        Some(v) => {
                            if let Err(e) = table.insert(key.as_slice(), v.as_slice()) {
                                tracing::error!(
                                    error = %e,
                                    key_len = key.len(),
                                    value_len = v.len(),
                                    "Failed to insert key during commit"
                                );
                                let on_error = std::mem::take(&mut *self.on_error.lock());
                                let on_error_async =
                                    std::mem::take(&mut *self.on_error_async.lock());
                                Self::execute_callbacks(on_error);
                                Self::execute_async_callbacks(on_error_async).await;
                                return Err(e.into());
                            }
                        }
                        None => {
                            if let Err(e) = table.remove(key.as_slice()) {
                                tracing::error!(
                                    error = %e,
                                    key_len = key.len(),
                                    "Failed to delete key during commit"
                                );
                                let on_error = std::mem::take(&mut *self.on_error.lock());
                                let on_error_async =
                                    std::mem::take(&mut *self.on_error_async.lock());
                                Self::execute_callbacks(on_error);
                                Self::execute_async_callbacks(on_error_async).await;
                                return Err(e.into());
                            }
                        }
                    }
                }
            }

            if let Err(e) = write_txn.commit() {
                tracing::error!(
                    error = %e,
                    pending_changes = pending.len(),
                    "Failed to finalize commit"
                );
                let on_error = std::mem::take(&mut *self.on_error.lock());
                let on_error_async = std::mem::take(&mut *self.on_error_async.lock());
                Self::execute_callbacks(on_error);
                Self::execute_async_callbacks(on_error_async).await;
                return Err(e.into());
            }
        }

        // Execute success callbacks
        let on_success = std::mem::take(&mut *self.on_success.lock());
        let on_success_async = std::mem::take(&mut *self.on_success_async.lock());
        Self::execute_callbacks(on_success);
        Self::execute_async_callbacks(on_success_async).await;

        Ok(())
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

            tokio::spawn(async move {
                Self::execute_async_callbacks(on_discard_async).await;
                tracing::debug!(count = callback_count, "Async discard callbacks completed");
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

/// Iterator over redb key-value pairs.
struct RedbIterator {
    /// Sorted vector of key-value pairs
    data: Vec<(Vec<u8>, Vec<u8>)>,

    /// Current position in the iterator
    position: usize,

    /// Whether the iterator is closed
    closed: bool,

    /// Whether this is a keys-only iterator
    keys_only: bool,

    /// Whether this iterator is in reverse mode
    reverse: bool,
}

impl RedbIterator {
    fn new(data: BTreeMap<Vec<u8>, Vec<u8>>, opts: IterOptions) -> Result<Self> {
        // Apply filters and convert to Vec
        let mut filtered: Vec<_> = data
            .into_iter()
            .filter(|(k, _)| {
                // Apply prefix filter
                if let Some(prefix) = opts.prefix() {
                    if !k.starts_with(prefix) {
                        return false;
                    }
                }

                // Apply start filter
                if let Some(start) = opts.start() {
                    if k.as_slice() < start {
                        return false;
                    }
                }

                // Apply end filter
                if let Some(end) = opts.end() {
                    if k.as_slice() >= end {
                        return false;
                    }
                }

                true
            })
            .collect();

        // Apply reverse ordering
        let reverse = opts.reverse();
        if reverse {
            filtered.reverse();
        }

        Ok(Self {
            data: filtered,
            position: 0,
            closed: false,
            keys_only: opts.keys_only(),
            reverse,
        })
    }
}

#[async_trait]
impl Iterator for RedbIterator {
    async fn next(&mut self) -> Result<Option<KvPair>> {
        if self.closed {
            return Err(Error::Iterator("Iterator has been closed".into()));
        }

        if self.position >= self.data.len() {
            return Ok(None);
        }

        let (key, value) = &self.data[self.position];
        self.position += 1;

        if self.keys_only {
            Ok(Some(KvPair::key_only(key.clone())))
        } else {
            Ok(Some(KvPair::new(key.clone(), value.clone())))
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.closed = true;
        Ok(())
    }

    async fn seek(&mut self, key: &[u8]) -> Result<bool> {
        if self.closed {
            return Err(Error::Iterator("Iterator has been closed".into()));
        }

        // Find the position to seek to based on iteration direction.
        let pos = if self.reverse {
            // Reverse mode: data is [k4, k3, k2, k1] (descending)
            // seek(k2) should find the first key <= k2
            self.data.iter().position(|(k, _)| k.as_slice() <= key)
        } else {
            // Forward mode: data is [k1, k2, k3, k4] (ascending)
            // seek(k2) should find the first key >= k2
            self.data.iter().position(|(k, _)| k.as_slice() >= key)
        };

        match pos {
            Some(p) => {
                self.position = p;
                Ok(true)
            }
            None => {
                // No matching key found, position at end
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
        !self.closed
    }
}

// Convert redb errors to our error type with context
impl From<redb::Error> for Error {
    fn from(err: redb::Error) -> Self {
        Error::Backend(format!("redb error: {}", err))
    }
}

impl From<redb::DatabaseError> for Error {
    fn from(err: redb::DatabaseError) -> Self {
        Error::Backend(format!("redb database error: {}", err))
    }
}

impl From<redb::TransactionError> for Error {
    fn from(err: redb::TransactionError) -> Self {
        Error::Backend(format!("redb transaction error: {}", err))
    }
}

impl From<redb::TableError> for Error {
    fn from(err: redb::TableError) -> Self {
        Error::Backend(format!("redb table error: {}", err))
    }
}

impl From<redb::StorageError> for Error {
    fn from(err: redb::StorageError) -> Self {
        Error::Backend(format!("redb storage error: {}", err))
    }
}

impl From<redb::CommitError> for Error {
    fn from(err: redb::CommitError) -> Self {
        Error::Backend(format!("redb commit error: {}", err))
    }
}

// ============================================================================
// SHARED TEST SUITE - Run same tests against all backends
// ============================================================================
#[cfg(test)]
mod shared_tests {
    use super::*;
    use crate::generate_backend_concurrency_tests;
    use crate::generate_backend_dropable_tests;
    use crate::generate_backend_tests;
    use tempfile::TempDir;

    async fn create_store() -> RedbStore {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.redb");
        std::mem::forget(temp_dir); // Prevent cleanup until test ends
        RedbStore::open(&path).unwrap()
    }

    async fn create_arc_store() -> Arc<RedbStore> {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.redb");
        std::mem::forget(temp_dir);
        Arc::new(RedbStore::open(&path).unwrap())
    }

    // Generate all standard backend tests
    generate_backend_tests!(create_store);

    // Generate concurrency tests
    generate_backend_concurrency_tests!(create_arc_store);

    // Generate Dropable tests (RedbStore implements Dropable)
    generate_backend_dropable_tests!(create_store);
}

// ============================================================================
// REDB-SPECIFIC TESTS - Persistence and specific behaviors
// ============================================================================
#[cfg(test)]
mod redb_specific_tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_redb_data_survives_close_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.redb");

        // Write data and close
        {
            let store = RedbStore::open(&path).unwrap();
            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(b"persistent_key", b"persistent_value")
                .await
                .unwrap();
            txn.commit().await.unwrap();
            store.close().await.unwrap();
        }

        // Reopen and verify
        {
            let store = RedbStore::open(&path).unwrap();
            let txn = store.new_txn(true).await.unwrap();
            assert_eq!(
                txn.get(b"persistent_key").await.unwrap(),
                Some(b"persistent_value".to_vec()),
                "Data should survive close/reopen"
            );
        }
    }

    #[tokio::test]
    async fn test_redb_uncommitted_data_lost_on_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.redb");

        // Write data but DON'T commit
        {
            let store = RedbStore::open(&path).unwrap();
            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(b"uncommitted_key", b"value").await.unwrap();
            // No commit! Discard.
            txn.discard();
            store.close().await.unwrap();
        }

        // Reopen - uncommitted data should be gone
        {
            let store = RedbStore::open(&path).unwrap();
            let txn = store.new_txn(true).await.unwrap();
            assert_eq!(
                txn.get(b"uncommitted_key").await.unwrap(),
                None,
                "Uncommitted data should not survive reopen"
            );
        }
    }

    #[tokio::test]
    async fn test_redb_persistence_through_multiple_sessions() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.redb");

        // Session 1: Write keys
        {
            let store = RedbStore::open(&path).unwrap();
            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(b"key1", b"value1").await.unwrap();
            txn.set(b"key2", b"value2").await.unwrap();
            txn.commit().await.unwrap();
        }

        // Session 2: Modify and add
        {
            let store = RedbStore::open(&path).unwrap();
            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(b"key1", b"modified").await.unwrap();
            txn.set(b"key3", b"value3").await.unwrap();
            txn.delete(b"key2").await.unwrap();
            txn.commit().await.unwrap();
        }

        // Session 3: Verify all changes
        {
            let store = RedbStore::open(&path).unwrap();
            let txn = store.new_txn(true).await.unwrap();
            assert_eq!(txn.get(b"key1").await.unwrap(), Some(b"modified".to_vec()));
            assert_eq!(txn.get(b"key2").await.unwrap(), None);
            assert_eq!(txn.get(b"key3").await.unwrap(), Some(b"value3".to_vec()));
        }
    }

    #[tokio::test]
    async fn test_redb_snapshot_isolation() {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(RedbStore::open(temp_dir.path().join("test.redb")).unwrap());

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

    // Note: Tests for "operations after discard/commit" are unnecessary because
    // Rust's ownership system enforces this at compile time - discard() and commit()
    // take `self: Box<Self>`, consuming the transaction and preventing further use.
}
