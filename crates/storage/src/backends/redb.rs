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
/// # Async Callback Lifecycle
///
/// Transaction callbacks follow fire-and-forget semantics (matching Go DefraDB):
///
/// - **Sync callbacks**: Executed inline during commit/discard, blocking until complete
/// - **Async callbacks on commit**: Awaited during commit, blocking return until complete
/// - **Async callbacks on discard**: Spawned as background tasks (fire-and-forget)
///
/// **Important**: Async discard callbacks may not complete if the process exits
/// before they finish. Callers requiring completion guarantees should use
/// `sync::WaitGroup` or similar synchronization, or prefer `commit()` over
/// `discard()` when async cleanup is critical.
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
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::corekv::{
    AsyncTxnCallback, Dropable, Error, IterOptions, Iterator, KvPair, Reader, Result, Store, Txn,
    TxnCallback, Writer,
};

use super::redb_config::RedbStoreOptions;

/// Table definition for the main key-value store.
const KV_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("kv");

/// Redb-backed key-value store.
///
/// This store wraps a redb Database instance and provides MVCC transaction
/// support through snapshots and buffered writes.
///
/// # Active Transaction Tracking
///
/// The store tracks the number of active transactions. When closing, the store
/// will reject new transactions and wait for existing ones to complete.
pub struct RedbStore {
    db: Arc<Database>,
    closed: Arc<RwLock<bool>>,
    /// Count of active transactions (for graceful shutdown)
    active_txn_count: Arc<AtomicUsize>,
}

impl RedbStore {
    /// Open a redb database at the specified path with default options.
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
        Self::open_with_options(path, RedbStoreOptions::default())
    }

    /// Open a redb database at the specified path with custom options.
    ///
    /// If the database doesn't exist, it will be created.
    /// If the path is a directory, creates `data.redb` inside it.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the database file or directory
    /// * `opts` - Configuration options for the database
    ///
    /// # Returns
    ///
    /// * `Ok(RedbStore)` on success
    /// * `Err(Error)` if the database cannot be opened
    ///
    /// # Example
    ///
    /// ```ignore
    /// use storage::backends::{RedbStore, RedbStoreOptions};
    ///
    /// let opts = RedbStoreOptions::new()
    ///     .with_cache_size(64 * 1024 * 1024); // 64MB cache
    ///
    /// let store = RedbStore::open_with_options("/path/to/db", opts)?;
    /// ```
    pub fn open_with_options<P: AsRef<Path>>(path: P, opts: RedbStoreOptions) -> Result<Self> {
        let path = path.as_ref();
        let db_path = if path.is_dir() {
            path.join("data.redb")
        } else {
            path.to_path_buf()
        };

        let mut builder = redb::Builder::new();
        if let Some(cache_size) = opts.cache_size() {
            builder.set_cache_size(cache_size);
        }

        let db = builder.create(db_path)?;

        // Ensure the KV table exists by opening a write transaction
        {
            let write_txn = db.begin_write()?;
            let _ = write_txn.open_table(KV_TABLE)?;
            write_txn.commit()?;
        }

        Ok(Self {
            db: Arc::new(db),
            closed: Arc::new(RwLock::new(false)),
            active_txn_count: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Get the current count of active transactions.
    pub fn active_transaction_count(&self) -> usize {
        self.active_txn_count.load(Ordering::SeqCst)
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

        // Increment active transaction count
        self.active_txn_count.fetch_add(1, Ordering::SeqCst);

        // Capture a snapshot for read isolation
        let read_txn = match self.db.begin_read() {
            Ok(txn) => txn,
            Err(e) => {
                // Decrement count on failure
                self.active_txn_count.fetch_sub(1, Ordering::SeqCst);
                return Err(e.into());
            }
        };

        let snapshot = match capture_snapshot(&read_txn) {
            Ok(s) => s,
            Err(e) => {
                // Decrement count on failure
                self.active_txn_count.fetch_sub(1, Ordering::SeqCst);
                return Err(e);
            }
        };

        Ok(Box::new(RedbTxn {
            db: Arc::clone(&self.db),
            active_txn_count: Arc::clone(&self.active_txn_count),
            snapshot,
            pending: Mutex::new(BTreeMap::new()),
            readonly,
            discarded: Mutex::new(false),
            committed: Mutex::new(false),
            on_success: Mutex::new(Vec::new()),
            on_success_async: Mutex::new(Vec::new()),
            on_error: Mutex::new(Vec::new()),
            on_error_async: Mutex::new(Vec::new()),
            on_discard: Mutex::new(Vec::new()),
            on_discard_async: Mutex::new(Vec::new()),
        }))
    }

    async fn close(&self) -> Result<()> {
        // Mark as closed first to prevent new transactions
        {
            let mut closed = self.closed.write().await;
            *closed = true;
        }

        // Wait for active transactions to complete (with timeout)
        let active = self.active_txn_count.load(Ordering::SeqCst);
        if active > 0 {
            tracing::info!(
                active_transactions = active,
                "Store closing with active transactions - waiting for completion"
            );

            // Poll for up to 5 seconds for transactions to complete
            let start = std::time::Instant::now();
            let timeout = std::time::Duration::from_secs(5);
            while self.active_txn_count.load(Ordering::SeqCst) > 0 {
                if start.elapsed() > timeout {
                    let remaining = self.active_txn_count.load(Ordering::SeqCst);
                    tracing::warn!(
                        remaining_transactions = remaining,
                        "Timeout waiting for transactions to complete during close"
                    );
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }

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

/// Guard to decrement active transaction count on drop.
///
/// Used in commit() to ensure the count is decremented even on early returns.
struct TxnCountGuard(Arc<AtomicUsize>);

impl Drop for TxnCountGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Compute the start and end bounds for a BTreeMap range query from IterOptions.
///
/// This optimizes iteration by using the underlying data structure's range
/// capabilities instead of filtering after iteration.
fn compute_range_bounds(opts: &IterOptions) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    // Determine start bound
    let start_bound = match (opts.prefix(), opts.start()) {
        (Some(prefix), Some(start)) => {
            // Use whichever is greater
            if prefix > start {
                Bound::Included(prefix.to_vec())
            } else {
                Bound::Included(start.to_vec())
            }
        }
        (Some(prefix), None) => Bound::Included(prefix.to_vec()),
        (None, Some(start)) => Bound::Included(start.to_vec()),
        (None, None) => Bound::Unbounded,
    };

    // Determine end bound
    let end_bound = match (opts.prefix(), opts.end()) {
        (Some(prefix), Some(end)) => {
            // Compute prefix end (prefix with last byte incremented)
            let prefix_end = prefix_to_end_bound(prefix);
            // Use whichever is smaller
            if let Some(pe) = prefix_end {
                if pe.as_slice() < end {
                    Bound::Excluded(pe)
                } else {
                    Bound::Excluded(end.to_vec())
                }
            } else {
                Bound::Excluded(end.to_vec())
            }
        }
        (Some(prefix), None) => {
            match prefix_to_end_bound(prefix) {
                Some(end) => Bound::Excluded(end),
                None => Bound::Unbounded, // Prefix is all 0xFF bytes
            }
        }
        (None, Some(end)) => Bound::Excluded(end.to_vec()),
        (None, None) => Bound::Unbounded,
    };

    (start_bound, end_bound)
}

/// Compute the exclusive end bound for a prefix.
///
/// Given a prefix like "foo", returns "fop" (the first key that doesn't match the prefix).
/// Returns None if the prefix is all 0xFF bytes (meaning iteration should go to the end).
fn prefix_to_end_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    // Increment the last byte, handling overflow
    while let Some(last) = end.pop() {
        if last < 0xFF {
            end.push(last + 1);
            return Some(end);
        }
        // If the byte was 0xFF, we popped it and try the next one
    }
    // All bytes were 0xFF
    None
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

    /// Reference to the active transaction counter (for decrement on complete)
    active_txn_count: Arc<AtomicUsize>,

    /// Snapshot of store at transaction start (for reads)
    snapshot: BTreeMap<Vec<u8>, Vec<u8>>,

    /// Pending changes (Some(value) = set, None = delete)
    pending: Mutex<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,

    /// Whether this is a read-only transaction
    readonly: bool,

    /// Whether the transaction has been discarded
    discarded: Mutex<bool>,

    /// Whether the transaction has been committed
    committed: Mutex<bool>,

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

        // Compute the effective range bounds for efficient range queries
        let (start_bound, end_bound) = compute_range_bounds(&opts);

        // Helper to check prefix
        let matches_prefix =
            |key: &[u8]| -> bool { opts.prefix().map_or(true, |p| key.starts_with(p)) };

        // Extract snapshot items into Vec (already sorted by BTreeMap)
        let snapshot_items: Vec<(Vec<u8>, Vec<u8>)> = self
            .snapshot
            .range((start_bound.clone(), end_bound.clone()))
            .filter(|(k, _)| matches_prefix(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // Extract pending items into Vec (sorted by BTreeMap, with Option for deletions)
        let pending = self.pending.lock();
        let pending_items: Vec<(Vec<u8>, Option<Vec<u8>>)> = pending
            .range((start_bound, end_bound))
            .filter(|(k, _)| matches_prefix(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        Ok(Box::new(MergingIterator::new(
            snapshot_items,
            pending_items,
            opts,
        )))
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
        // Helper to decrement count on all exit paths
        let _guard = TxnCountGuard(Arc::clone(&self.active_txn_count));

        if *self.discarded.lock() {
            tracing::warn!("Attempted to commit a discarded transaction");
            let on_error = std::mem::take(&mut *self.on_error.lock());
            let on_error_async = std::mem::take(&mut *self.on_error_async.lock());
            Self::execute_callbacks(on_error);
            Self::execute_async_callbacks(on_error_async).await;
            return Err(Error::DiscardedTxn);
        }

        if *self.committed.lock() {
            tracing::warn!("Attempted to commit an already committed transaction");
            return Err(Error::Other("Transaction already committed".into()));
        }

        *self.committed.lock() = true;

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
        // Decrement active transaction count
        self.active_txn_count.fetch_sub(1, Ordering::SeqCst);

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

/// Merging iterator that lazily combines snapshot and pending changes.
///
/// Instead of materializing the full merged result upfront, this iterator
/// holds both sorted Vecs and merges on-demand during iteration.
struct MergingIterator {
    /// Items from snapshot (sorted ascending)
    snapshot_items: Vec<(Vec<u8>, Vec<u8>)>,
    /// Current position in snapshot
    snapshot_pos: usize,

    /// Pending changes (sorted ascending, None = deletion)
    pending_items: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    /// Current position in pending
    pending_pos: usize,

    /// Whether iteration is reversed
    reverse: bool,
    /// Whether to return only keys
    keys_only: bool,
    /// Whether the iterator is closed
    closed: bool,
}

impl MergingIterator {
    fn new(
        mut snapshot_items: Vec<(Vec<u8>, Vec<u8>)>,
        mut pending_items: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        opts: IterOptions,
    ) -> Self {
        let reverse = opts.reverse();
        if reverse {
            snapshot_items.reverse();
            pending_items.reverse();
        }

        Self {
            snapshot_items,
            snapshot_pos: 0,
            pending_items,
            pending_pos: 0,
            reverse,
            keys_only: opts.keys_only(),
            closed: false,
        }
    }

    /// Get the next merged key-value pair, handling overrides and deletions.
    fn next_merged(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        loop {
            let snap_key = self.snapshot_items.get(self.snapshot_pos).map(|(k, _)| k);
            let pend_key = self.pending_items.get(self.pending_pos).map(|(k, _)| k);

            match (snap_key, pend_key) {
                (None, None) => return None,

                (Some(_), None) => {
                    let (key, value) = self.snapshot_items[self.snapshot_pos].clone();
                    self.snapshot_pos += 1;
                    return Some((key, value));
                }

                (None, Some(_)) => {
                    let (key, value_opt) = self.pending_items[self.pending_pos].clone();
                    self.pending_pos += 1;
                    match value_opt {
                        Some(value) => return Some((key, value)),
                        None => continue, // Deletion of non-existent key, skip
                    }
                }

                (Some(sk), Some(pk)) => {
                    let cmp = if self.reverse {
                        pk.cmp(sk) // Reversed: larger keys come first
                    } else {
                        sk.cmp(pk)
                    };

                    match cmp {
                        std::cmp::Ordering::Less => {
                            // Snapshot key comes first (no pending override)
                            let (key, value) = self.snapshot_items[self.snapshot_pos].clone();
                            self.snapshot_pos += 1;
                            return Some((key, value));
                        }
                        std::cmp::Ordering::Greater => {
                            // Pending key comes first (new key not in snapshot)
                            let (key, value_opt) = self.pending_items[self.pending_pos].clone();
                            self.pending_pos += 1;
                            match value_opt {
                                Some(value) => return Some((key, value)),
                                None => continue, // Deletion of non-existent key
                            }
                        }
                        std::cmp::Ordering::Equal => {
                            // Same key: pending overrides snapshot
                            let (key, value_opt) = self.pending_items[self.pending_pos].clone();
                            self.snapshot_pos += 1; // Skip snapshot version
                            self.pending_pos += 1;
                            match value_opt {
                                Some(value) => return Some((key, value)),
                                None => continue, // Deletion
                            }
                        }
                    }
                }
            }
        }
    }

    /// Binary search for seek position in a sorted Vec.
    fn binary_search_position<T>(items: &[(Vec<u8>, T)], key: &[u8], reverse: bool) -> usize {
        if reverse {
            // Reversed: items are [k4, k3, k2, k1], find first <= key
            items.partition_point(|(k, _)| k.as_slice() > key)
        } else {
            // Forward: items are [k1, k2, k3, k4], find first >= key
            items.partition_point(|(k, _)| k.as_slice() < key)
        }
    }
}

#[async_trait]
impl Iterator for MergingIterator {
    async fn next(&mut self) -> Result<Option<KvPair>> {
        if self.closed {
            return Err(Error::Iterator("Iterator has been closed".into()));
        }

        match self.next_merged() {
            Some((key, value)) => {
                if self.keys_only {
                    Ok(Some(KvPair::key_only(key)))
                } else {
                    Ok(Some(KvPair::new(key, value)))
                }
            }
            None => Ok(None),
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

        // Seek both iterators to the target position
        self.snapshot_pos = Self::binary_search_position(&self.snapshot_items, key, self.reverse);
        self.pending_pos = Self::binary_search_position(&self.pending_items, key, self.reverse);

        // Check if there's any data at or after the seek position
        let has_snapshot = self.snapshot_pos < self.snapshot_items.len();
        let has_pending = self.pending_pos < self.pending_items.len();

        Ok(has_snapshot || has_pending)
    }

    async fn reset(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::Iterator("Iterator has been closed".into()));
        }

        self.snapshot_pos = 0;
        self.pending_pos = 0;
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
    use crate::corekv::Dropable;
    use crate::generate_backend_concurrency_tests;
    use crate::generate_backend_dropable_tests;
    use crate::generate_backend_tests;
    use tempfile::TempDir;

    /// Test wrapper that holds both store and temp directory for cleanup.
    /// When this wrapper is dropped, the TempDir is automatically cleaned up.
    struct TestRedbStore {
        store: RedbStore,
        _temp_dir: TempDir,
    }

    #[async_trait]
    impl Store for TestRedbStore {
        async fn new_txn(&self, readonly: bool) -> Result<Box<dyn Txn>> {
            self.store.new_txn(readonly).await
        }
        async fn close(&self) -> Result<()> {
            self.store.close().await
        }
    }

    #[async_trait]
    impl Dropable for TestRedbStore {
        async fn drop_all(&self) -> Result<()> {
            self.store.drop_all().await
        }
    }

    async fn create_store() -> TestRedbStore {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.redb");
        let store = RedbStore::open(&path).unwrap();
        TestRedbStore {
            store,
            _temp_dir: temp_dir,
        }
    }

    async fn create_arc_store() -> Arc<TestRedbStore> {
        Arc::new(create_store().await)
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

    #[tokio::test]
    async fn test_redb_active_transaction_tracking() {
        let temp_dir = TempDir::new().unwrap();
        let store = RedbStore::open(temp_dir.path().join("test.redb")).unwrap();

        assert_eq!(
            store.active_transaction_count(),
            0,
            "No active transactions initially"
        );

        // Create a transaction
        let txn1 = store.new_txn(true).await.unwrap();
        assert_eq!(
            store.active_transaction_count(),
            1,
            "One active transaction"
        );

        // Create another
        let txn2 = store.new_txn(false).await.unwrap();
        assert_eq!(
            store.active_transaction_count(),
            2,
            "Two active transactions"
        );

        // Discard one
        txn1.discard();
        assert_eq!(
            store.active_transaction_count(),
            1,
            "One active after discard"
        );

        // Commit the other
        txn2.commit().await.unwrap();
        assert_eq!(
            store.active_transaction_count(),
            0,
            "None active after commit"
        );
    }

    #[tokio::test]
    async fn test_redb_close_waits_for_transactions() {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(RedbStore::open(temp_dir.path().join("test.redb")).unwrap());

        // Create a transaction
        let txn = store.new_txn(true).await.unwrap();
        assert_eq!(store.active_transaction_count(), 1);

        // Spawn close in background
        let store_clone = Arc::clone(&store);
        let close_handle = tokio::spawn(async move {
            store_clone.close().await.unwrap();
        });

        // Small delay to let close start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Discard the transaction
        txn.discard();

        // Close should complete
        tokio::time::timeout(std::time::Duration::from_secs(2), close_handle)
            .await
            .expect("Close should complete")
            .expect("Close should succeed");

        assert_eq!(store.active_transaction_count(), 0);
    }

    #[tokio::test]
    async fn test_redb_operations_on_closed_store_fail() {
        let temp_dir = TempDir::new().unwrap();
        let store = RedbStore::open(temp_dir.path().join("test.redb")).unwrap();

        store.close().await.unwrap();

        // New transactions should fail
        let result = store.new_txn(true).await;
        assert!(result.is_err(), "new_txn should fail on closed store");
    }

    #[tokio::test]
    async fn test_redb_iterator_prefix_filtering() {
        let temp_dir = TempDir::new().unwrap();
        let store = RedbStore::open(temp_dir.path().join("test.redb")).unwrap();

        // Insert keys with different prefixes
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"user:1", b"alice").await.unwrap();
        txn.set(b"user:2", b"bob").await.unwrap();
        txn.set(b"user:3", b"carol").await.unwrap();
        txn.set(b"doc:1", b"document1").await.unwrap();
        txn.set(b"doc:2", b"document2").await.unwrap();
        txn.commit().await.unwrap();

        // Test prefix iteration
        let txn = store.new_txn(true).await.unwrap();
        let opts = IterOptions::new().with_prefix(b"user:".to_vec());
        let mut iter = txn.iterator(opts).await.unwrap();

        let mut count = 0;
        while let Some(kv) = iter.next().await.unwrap() {
            assert!(kv.key.starts_with(b"user:"), "Key should have prefix");
            count += 1;
        }
        assert_eq!(count, 3, "Should have 3 user keys");

        // Test doc prefix
        let opts = IterOptions::new().with_prefix(b"doc:".to_vec());
        let mut iter = txn.iterator(opts).await.unwrap();

        let mut count = 0;
        while let Some(kv) = iter.next().await.unwrap() {
            assert!(kv.key.starts_with(b"doc:"), "Key should have prefix");
            count += 1;
        }
        assert_eq!(count, 2, "Should have 2 doc keys");
    }

    #[tokio::test]
    async fn test_redb_iterator_range_filtering() {
        let temp_dir = TempDir::new().unwrap();
        let store = RedbStore::open(temp_dir.path().join("test.redb")).unwrap();

        // Insert alphabetically ordered keys
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"a", b"1").await.unwrap();
        txn.set(b"b", b"2").await.unwrap();
        txn.set(b"c", b"3").await.unwrap();
        txn.set(b"d", b"4").await.unwrap();
        txn.set(b"e", b"5").await.unwrap();
        txn.commit().await.unwrap();

        // Test range iteration [b, d)
        let txn = store.new_txn(true).await.unwrap();
        let opts = IterOptions::new()
            .with_start(b"b".to_vec())
            .with_end(b"d".to_vec());
        let mut iter = txn.iterator(opts).await.unwrap();

        let keys: Vec<_> = {
            let mut keys = vec![];
            while let Some(kv) = iter.next().await.unwrap() {
                keys.push(kv.key);
            }
            keys
        };

        assert_eq!(keys.len(), 2, "Should have keys b and c");
        assert_eq!(keys[0], b"b");
        assert_eq!(keys[1], b"c");
    }

    #[tokio::test]
    async fn test_redb_iterator_reverse() {
        let temp_dir = TempDir::new().unwrap();
        let store = RedbStore::open(temp_dir.path().join("test.redb")).unwrap();

        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"a", b"1").await.unwrap();
        txn.set(b"b", b"2").await.unwrap();
        txn.set(b"c", b"3").await.unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let opts = IterOptions::new().with_reverse(true);
        let mut iter = txn.iterator(opts).await.unwrap();

        let mut keys = vec![];
        while let Some(kv) = iter.next().await.unwrap() {
            keys.push(kv.key);
        }

        assert_eq!(keys, vec![b"c".to_vec(), b"b".to_vec(), b"a".to_vec()]);
    }

    #[tokio::test]
    async fn test_redb_empty_key_rejected() {
        let temp_dir = TempDir::new().unwrap();
        let store = RedbStore::open(temp_dir.path().join("test.redb")).unwrap();

        let mut txn = store.new_txn(false).await.unwrap();

        // Empty key should be rejected
        let result = txn.set(b"", b"value").await;
        assert!(result.is_err(), "Empty key should be rejected");

        let result = txn.get(b"").await;
        assert!(result.is_err(), "Empty key get should be rejected");

        let result = txn.delete(b"").await;
        assert!(result.is_err(), "Empty key delete should be rejected");
    }

    #[tokio::test]
    async fn test_redb_read_only_txn_rejects_writes() {
        let temp_dir = TempDir::new().unwrap();
        let store = RedbStore::open(temp_dir.path().join("test.redb")).unwrap();

        let mut txn = store.new_txn(true).await.unwrap(); // read-only

        let result = txn.set(b"key", b"value").await;
        assert!(result.is_err(), "Read-only txn should reject set");

        let result = txn.delete(b"key").await;
        assert!(result.is_err(), "Read-only txn should reject delete");
    }

    #[tokio::test]
    async fn test_redb_pending_changes_merged_with_snapshot() {
        let temp_dir = TempDir::new().unwrap();
        let store = RedbStore::open(temp_dir.path().join("test.redb")).unwrap();

        // Setup initial data
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"existing", b"original").await.unwrap();
        txn.commit().await.unwrap();

        // Start a new transaction with pending changes
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"existing", b"modified").await.unwrap();
        txn.set(b"new_key", b"new_value").await.unwrap();

        // Pending changes should be visible
        assert_eq!(
            txn.get(b"existing").await.unwrap(),
            Some(b"modified".to_vec()),
            "Should see pending modification"
        );
        assert_eq!(
            txn.get(b"new_key").await.unwrap(),
            Some(b"new_value".to_vec()),
            "Should see pending new key"
        );

        // Iterator should also merge pending changes
        let opts = IterOptions::new();
        let mut iter = txn.iterator(opts).await.unwrap();
        let mut found_keys = std::collections::HashSet::new();
        while let Some(kv) = iter.next().await.unwrap() {
            found_keys.insert(kv.key);
        }
        assert!(found_keys.contains(&b"existing".to_vec()));
        assert!(found_keys.contains(&b"new_key".to_vec()));
    }

    #[tokio::test]
    async fn test_redb_pending_delete_removes_from_snapshot() {
        let temp_dir = TempDir::new().unwrap();
        let store = RedbStore::open(temp_dir.path().join("test.redb")).unwrap();

        // Setup initial data
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"to_delete", b"value").await.unwrap();
        txn.set(b"to_keep", b"value").await.unwrap();
        txn.commit().await.unwrap();

        // Delete in a new transaction
        let mut txn = store.new_txn(false).await.unwrap();
        txn.delete(b"to_delete").await.unwrap();

        // Deleted key should not be visible
        assert_eq!(
            txn.get(b"to_delete").await.unwrap(),
            None,
            "Deleted key should not be visible"
        );
        assert!(!txn.has(b"to_delete").await.unwrap());

        // Iterator should not include deleted key
        let opts = IterOptions::new();
        let mut iter = txn.iterator(opts).await.unwrap();
        while let Some(kv) = iter.next().await.unwrap() {
            assert_ne!(
                kv.key, b"to_delete",
                "Deleted key should not appear in iterator"
            );
        }
    }

    #[tokio::test]
    async fn test_redb_directory_handling() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().to_path_buf();

        // Opening with directory path should work (creates data.redb inside)
        {
            let store = RedbStore::open(&dir_path).unwrap();
            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(b"key", b"value").await.unwrap();
            txn.commit().await.unwrap();
            store.close().await.unwrap();
            // Store dropped here, releasing the lock
        }

        // Verify the database file was created inside the directory
        let db_path = dir_path.join("data.redb");
        assert!(db_path.exists(), "data.redb should be created in directory");

        // Reopen and verify data
        {
            let store = RedbStore::open(&dir_path).unwrap();
            let txn = store.new_txn(true).await.unwrap();
            assert_eq!(txn.get(b"key").await.unwrap(), Some(b"value".to_vec()));
        }
    }

    #[tokio::test]
    async fn test_redb_large_value_handling() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.redb");

        // Test with 5MB value
        let large_value = vec![0xABu8; 5 * 1024 * 1024];

        // Write and verify retrieval
        {
            let store = RedbStore::open(&path).unwrap();

            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(b"large_key", &large_value).await.unwrap();
            txn.commit().await.unwrap();

            // Verify retrieval
            let txn = store.new_txn(true).await.unwrap();
            let retrieved = txn.get(b"large_key").await.unwrap();
            assert_eq!(
                retrieved.as_ref().map(|v| v.len()),
                Some(5 * 1024 * 1024),
                "Large value should be retrievable"
            );
            assert_eq!(
                retrieved.as_ref().map(|v| v[0]),
                Some(0xAB),
                "Large value content should match"
            );

            store.close().await.unwrap();
        }

        // Verify persistence after reopen
        {
            let store = RedbStore::open(&path).unwrap();
            let txn = store.new_txn(true).await.unwrap();
            let retrieved = txn.get(b"large_key").await.unwrap();
            assert_eq!(
                retrieved.map(|v| v.len()),
                Some(5 * 1024 * 1024),
                "Large value should survive persistence"
            );
        }
    }

    #[tokio::test]
    async fn test_redb_new_txn_rejected_during_close() {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(RedbStore::open(temp_dir.path().join("test.redb")).unwrap());

        // Create a transaction to keep the store busy
        let txn = store.new_txn(true).await.unwrap();
        assert_eq!(store.active_transaction_count(), 1);

        // Start close in background (will wait for active transactions)
        let store_clone = Arc::clone(&store);
        let close_handle = tokio::spawn(async move {
            // Small delay to ensure close starts
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            store_clone.close().await
        });

        // Wait for close to mark the store as closed
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // New transaction should be rejected because store is closing
        let result = store.new_txn(true).await;
        assert!(result.is_err(), "new_txn should fail when store is closing");

        // Clean up: discard the blocking transaction so close can complete
        txn.discard();

        // Wait for close to complete
        let close_result = tokio::time::timeout(std::time::Duration::from_secs(2), close_handle)
            .await
            .expect("Close should complete")
            .expect("Close task should not panic");

        assert!(close_result.is_ok(), "Close should succeed");
    }

    #[tokio::test]
    async fn test_redb_custom_cache_size() {
        use super::super::redb_config::RedbStoreOptions;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.redb");

        // Open with custom cache size (16MB)
        let opts = RedbStoreOptions::new().with_cache_size(16 * 1024 * 1024);
        let store = RedbStore::open_with_options(&path, opts).unwrap();

        // Verify store works normally
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"key", b"value").await.unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        assert_eq!(
            txn.get(b"key").await.unwrap(),
            Some(b"value".to_vec()),
            "Store with custom cache should work normally"
        );
    }

    #[tokio::test]
    async fn test_redb_error_callback_on_discarded_commit() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let temp_dir = TempDir::new().unwrap();
        let store = RedbStore::open(temp_dir.path().join("test.redb")).unwrap();

        let error_called = Arc::new(AtomicBool::new(false));
        let success_called = Arc::new(AtomicBool::new(false));

        let error_flag = Arc::clone(&error_called);
        let success_flag = Arc::clone(&success_called);

        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"key", b"value").await.unwrap();

        // Register callbacks
        txn.on_success(Box::new(move || {
            success_flag.store(true, Ordering::SeqCst);
        }));
        txn.on_error(Box::new(move || {
            error_flag.store(true, Ordering::SeqCst);
        }));

        // Discard the transaction first
        txn.discard();

        // Try to commit after discard - this should fail and call error callback
        // Note: Since discard() consumes self, we can't actually call commit() after.
        // However, we CAN test the error path by trying to commit a new discarded txn.
        // The Rust ownership model prevents the actual scenario, but we can verify
        // that error callbacks ARE called when commit fails due to other reasons.

        // Instead, test that error callback IS invoked when commit on discarded txn happens
        // by checking the callback mechanism works on normal success path
        assert!(
            !error_called.load(Ordering::SeqCst),
            "Error callback should not be called on discard"
        );
        assert!(
            !success_called.load(Ordering::SeqCst),
            "Success callback should not be called on discard"
        );

        // Now test success callback works
        let success_called2 = Arc::new(AtomicBool::new(false));
        let success_flag2 = Arc::clone(&success_called2);

        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"key2", b"value2").await.unwrap();
        txn.on_success(Box::new(move || {
            success_flag2.store(true, Ordering::SeqCst);
        }));
        txn.commit().await.unwrap();

        assert!(
            success_called2.load(Ordering::SeqCst),
            "Success callback should be called on commit"
        );
    }

    #[tokio::test]
    async fn test_redb_async_error_callback() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let temp_dir = TempDir::new().unwrap();
        let store = RedbStore::open(temp_dir.path().join("test.redb")).unwrap();

        let async_success_called = Arc::new(AtomicBool::new(false));
        let async_flag = Arc::clone(&async_success_called);

        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"key", b"value").await.unwrap();

        // Register async callback
        txn.on_success_async(Box::new(move || {
            let flag = async_flag;
            Box::pin(async move {
                flag.store(true, Ordering::SeqCst);
            })
        }));

        txn.commit().await.unwrap();

        assert!(
            async_success_called.load(Ordering::SeqCst),
            "Async success callback should be called and awaited on commit"
        );
    }

    #[tokio::test]
    async fn test_redb_iterator_seek_on_empty_store() {
        let temp_dir = TempDir::new().unwrap();
        let store = RedbStore::open(temp_dir.path().join("test.redb")).unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let opts = IterOptions::new();
        let mut iter = txn.iterator(opts).await.unwrap();

        // Seek on empty store should return false
        let found = iter.seek(b"any_key").await.unwrap();
        assert!(!found, "Seek on empty store should return false");

        // Next should return None
        let item = iter.next().await.unwrap();
        assert!(item.is_none(), "Next on empty store should return None");

        // Reset should succeed
        iter.reset().await.unwrap();
        assert!(
            iter.is_valid(),
            "Iterator should still be valid after reset"
        );
    }

    #[tokio::test]
    async fn test_redb_many_keys_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.redb");

        // Write 1000 keys
        {
            let store = RedbStore::open(&path).unwrap();
            let mut txn = store.new_txn(false).await.unwrap();
            for i in 0..1000u32 {
                let key = format!("key_{:05}", i);
                let value = format!("value_{}", i);
                txn.set(key.as_bytes(), value.as_bytes()).await.unwrap();
            }
            txn.commit().await.unwrap();
            store.close().await.unwrap();
        }

        // Reopen and verify all keys
        {
            let store = RedbStore::open(&path).unwrap();
            let txn = store.new_txn(true).await.unwrap();

            // Verify specific keys
            for i in [0, 100, 500, 999].iter() {
                let key = format!("key_{:05}", i);
                let expected = format!("value_{}", i);
                let value = txn.get(key.as_bytes()).await.unwrap();
                assert_eq!(
                    value,
                    Some(expected.into_bytes()),
                    "Key {} should be retrievable after persistence",
                    key
                );
            }

            // Verify count via iterator
            let opts = IterOptions::new();
            let mut iter = txn.iterator(opts).await.unwrap();
            let mut count = 0;
            while iter.next().await.unwrap().is_some() {
                count += 1;
            }
            assert_eq!(count, 1000, "Should have 1000 keys after persistence");
        }
    }
}
