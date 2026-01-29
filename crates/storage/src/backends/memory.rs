/// In-memory backend implementation using BTreeMap.
///
/// This backend provides a simple, fast, in-memory key-value store suitable for
/// testing and development. It uses a BTreeMap for ordered storage and supports
/// full MVCC transactions with snapshot isolation.
///
/// # Features
///
/// - Ordered key-value storage with BTreeMap
/// - Full transaction support with snapshot isolation
/// - Concurrent read access with RwLock
/// - Zero persistence (data lost on process exit)
/// - No external dependencies beyond standard library
///
/// # Use Cases
///
/// - Unit testing
/// - Integration testing
/// - Development and prototyping
/// - Ephemeral caches
///
/// # Example
///
/// ```ignore
/// use storage::backends::memory::MemoryStore;
/// use storage::corekv::{Store, Reader, Writer};
///
/// let store = MemoryStore::new();
/// let mut txn = store.new_txn(false).await?;
/// txn.set(b"key", b"value").await?;
/// txn.commit().await?;
/// ```
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::corekv::{
    AsyncTxnCallback, Dropable, Error, IterOptions, Iterator, KvPair, Reader, Result, Store, Txn,
    TxnCallback, Writer,
};

/// Tracks committed write sets for optimistic conflict detection.
///
/// Each committed transaction's write set is recorded along with the version
/// at which it was committed. When a new transaction commits, it checks whether
/// any of its written keys were also written by transactions that committed
/// after this transaction's snapshot was taken.
pub(crate) struct ConflictTracker {
    /// Monotonically increasing version counter.
    version: AtomicU64,
    /// Write sets from committed transactions: (commit_version, keys_written).
    /// Protected by a mutex since we only access it during commit (not hot path).
    committed_writes: Mutex<Vec<(u64, HashSet<Vec<u8>>)>>,
}

impl ConflictTracker {
    pub(crate) fn new() -> Self {
        Self {
            version: AtomicU64::new(0),
            committed_writes: Mutex::new(Vec::new()),
        }
    }

    /// Get the current version for a new transaction's snapshot.
    pub(crate) fn current_version(&self) -> u64 {
        self.version.load(Ordering::SeqCst)
    }

    /// Check for conflicts and record the write set if no conflict.
    /// Returns Err(TxnConflict) if any key in `write_set` was written by a
    /// transaction that committed after `read_version`.
    pub(crate) fn check_and_record(
        &self,
        read_version: u64,
        write_set: HashSet<Vec<u8>>,
    ) -> std::result::Result<(), Error> {
        if write_set.is_empty() {
            return Ok(());
        }

        let mut committed = self.committed_writes.lock();

        // Check for conflicts: any key we wrote was also written by a
        // transaction committed after our snapshot
        for (commit_ver, keys) in committed.iter() {
            if *commit_ver > read_version {
                for key in &write_set {
                    if keys.contains(key) {
                        return Err(Error::TxnConflict);
                    }
                }
            }
        }

        // No conflict - record our write set
        let new_version = self.version.fetch_add(1, Ordering::SeqCst) + 1;
        committed.push((new_version, write_set));

        // Prune old entries that can no longer conflict (optional GC).
        // Keep entries that are newer than the oldest possible active transaction.
        // For simplicity, keep last 1000 entries.
        if committed.len() > 1000 {
            let drain_count = committed.len() - 1000;
            committed.drain(..drain_count);
        }

        Ok(())
    }
}

/// In-memory key-value store using BTreeMap.
///
/// Data is stored in a BTreeMap wrapped in Arc<RwLock<>> for thread-safe
/// concurrent access. The store provides snapshot isolation for transactions
/// with optimistic write-write conflict detection.
#[derive(Clone)]
pub struct MemoryStore {
    data: Arc<RwLock<BTreeMap<Vec<u8>, Vec<u8>>>>,
    closed: Arc<RwLock<bool>>,
    conflict_tracker: Arc<ConflictTracker>,
}

impl MemoryStore {
    /// Create a new empty memory store.
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(BTreeMap::new())),
            closed: Arc::new(RwLock::new(false)),
            conflict_tracker: Arc::new(ConflictTracker::new()),
        }
    }

    /// Check if the store is closed.
    async fn is_closed(&self) -> bool {
        *self.closed.read().await
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Store for MemoryStore {
    async fn new_txn(&self, readonly: bool) -> Result<Box<dyn Txn>> {
        if self.is_closed().await {
            return Err(Error::DBClosed);
        }

        // Record version before taking snapshot for conflict detection
        let read_version = self.conflict_tracker.current_version();

        // Take a snapshot of current data for isolation
        let snapshot = self.data.read().await.clone();

        Ok(Box::new(MemoryTxn {
            store: Arc::clone(&self.data),
            conflict_tracker: Arc::clone(&self.conflict_tracker),
            read_version,
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
        let mut closed = self.closed.write().await;
        *closed = true;
        Ok(())
    }
}

#[async_trait]
impl Dropable for MemoryStore {
    async fn drop_all(&self) -> Result<()> {
        if self.is_closed().await {
            return Err(Error::DBClosed);
        }

        // Clear all data
        let mut data = self.data.write().await;
        data.clear();
        Ok(())
    }
}

/// In-memory transaction with snapshot isolation and conflict detection.
///
/// Transactions maintain a snapshot of the store at creation time and track
/// pending changes. On commit, write-write conflicts are detected using
/// optimistic concurrency control.
struct MemoryTxn {
    /// Reference to the store's data
    store: Arc<RwLock<BTreeMap<Vec<u8>, Vec<u8>>>>,

    /// Conflict tracker for write-write conflict detection
    conflict_tracker: Arc<ConflictTracker>,

    /// Version at which this transaction's snapshot was taken
    read_version: u64,

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

impl MemoryTxn {
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
impl Reader for MemoryTxn {
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

        Ok(Box::new(MemoryIterator::new(merged, opts)?))
    }
}

#[async_trait]
impl Writer for MemoryTxn {
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
impl Txn for MemoryTxn {
    async fn commit(self: Box<Self>) -> Result<()> {
        // Check discarded first (matches redb backend order)
        if *self.discarded.lock() {
            tracing::warn!("Attempted to commit a discarded transaction");
            let on_error = std::mem::take(&mut *self.on_error.lock());
            let on_error_async = std::mem::take(&mut *self.on_error_async.lock());
            Self::execute_callbacks(on_error);
            Self::execute_async_callbacks(on_error_async).await;
            return Err(Error::DiscardedTxn);
        }

        // Check if already committed (defensive - ownership prevents this in normal usage)
        if *self.committed.lock() {
            tracing::warn!("Attempted to commit an already committed transaction");
            return Err(Error::Other("Transaction already committed".into()));
        }

        // Clone pending changes before awaiting (can't hold MutexGuard across await)
        let pending = self.pending.lock().clone();

        // Check for write-write conflicts before applying
        if !pending.is_empty() {
            let write_set: HashSet<Vec<u8>> = pending.keys().cloned().collect();
            if let Err(e) = self.conflict_tracker.check_and_record(self.read_version, write_set) {
                let on_error = std::mem::take(&mut *self.on_error.lock());
                let on_error_async = std::mem::take(&mut *self.on_error_async.lock());
                Self::execute_callbacks(on_error);
                Self::execute_async_callbacks(on_error_async).await;
                return Err(e);
            }
        }

        // Mark as committed
        *self.committed.lock() = true;

        // Apply pending changes to store
        if !pending.is_empty() {
            let mut store = self.store.write().await;

            for (key, value) in pending.iter() {
                match value {
                    Some(v) => {
                        store.insert(key.clone(), v.clone());
                    }
                    None => {
                        store.remove(key);
                    }
                }
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

            // Spawn async callbacks in background with error tracking
            // NOTE: These may not complete if the process exits before they finish
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

    fn callback_count(&self) -> usize {
        self.on_success.lock().len()
            + self.on_success_async.lock().len()
            + self.on_error.lock().len()
            + self.on_error_async.lock().len()
            + self.on_discard.lock().len()
            + self.on_discard_async.lock().len()
    }
}

/// Iterator over in-memory key-value pairs.
struct MemoryIterator {
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

impl MemoryIterator {
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
impl Iterator for MemoryIterator {
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
        // For forward iteration: find first key >= seek_key
        // For reverse iteration: find first key <= seek_key
        //
        // The data is stored in iteration order (reversed for reverse mode),
        // so in reverse mode we're looking for k <= key in descending order.
        let pos = if self.reverse {
            // Reverse mode: data is [k4, k3, k2, k1] (descending)
            // seek(k2) should find the first key <= k2, which is k2 at position 2
            self.data.iter().position(|(k, _)| k.as_slice() <= key)
        } else {
            // Forward mode: data is [k1, k2, k3, k4] (ascending)
            // seek(k2) should find the first key >= k2, which is k2 at position 1
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

// ============================================================================
// SHARED TEST SUITE - Run same tests against all backends
// ============================================================================
#[cfg(test)]
mod shared_tests {
    use super::*;
    use crate::generate_backend_concurrency_tests;
    use crate::generate_backend_dropable_tests;
    use crate::generate_backend_tests;

    async fn create_store() -> MemoryStore {
        MemoryStore::new()
    }

    async fn create_arc_store() -> Arc<MemoryStore> {
        Arc::new(MemoryStore::new())
    }

    // Generate all standard backend tests
    generate_backend_tests!(create_store);

    // Generate concurrency tests
    generate_backend_concurrency_tests!(create_arc_store);

    // Generate Dropable tests (MemoryStore implements Dropable)
    generate_backend_dropable_tests!(create_store);
}

// ============================================================================
// MEMORY-SPECIFIC TESTS - Tests unique to MemoryStore behavior
// ============================================================================
#[cfg(test)]
mod memory_specific_tests {
    use super::*;

    /// Test MVCC snapshot isolation
    ///
    /// MemoryStore provides true MVCC snapshot isolation where readers
    /// get a snapshot at transaction start and never see concurrent commits.
    #[tokio::test]
    async fn test_memory_snapshot_isolation() {
        let store = Arc::new(MemoryStore::new());

        // Setup: write initial value
        let mut setup_txn = store.new_txn(false).await.unwrap();
        setup_txn.set(b"key", b"initial_value").await.unwrap();
        setup_txn.commit().await.unwrap();

        // Start reader transaction (gets snapshot with initial value)
        let reader = store.new_txn(true).await.unwrap();

        // Concurrent writer modifies and commits
        let mut writer = store.new_txn(false).await.unwrap();
        writer.set(b"key", b"modified_value").await.unwrap();
        writer.commit().await.unwrap();

        // Reader should STILL see initial value (true MVCC snapshot isolation)
        assert_eq!(
            reader.get(b"key").await.unwrap(),
            Some(b"initial_value".to_vec()),
            "MemoryStore reader should maintain snapshot isolation"
        );

        // New reader sees committed value
        let new_reader = store.new_txn(true).await.unwrap();
        assert_eq!(
            new_reader.get(b"key").await.unwrap(),
            Some(b"modified_value".to_vec())
        );
    }

    /// Test concurrent delete with snapshot isolation
    #[tokio::test]
    async fn test_memory_snapshot_preserves_deleted_keys() {
        let store = Arc::new(MemoryStore::new());

        // Setup
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"to_delete", b"exists").await.unwrap();
        txn.commit().await.unwrap();

        // Reader starts (snapshot has the key)
        let reader = store.new_txn(true).await.unwrap();

        // Deleter runs concurrently
        let mut deleter = store.new_txn(false).await.unwrap();
        deleter.delete(b"to_delete").await.unwrap();
        deleter.commit().await.unwrap();

        // Reader should still see the key (snapshot isolation)
        assert_eq!(
            reader.get(b"to_delete").await.unwrap(),
            Some(b"exists".to_vec()),
            "Reader snapshot should preserve deleted key"
        );

        // New transaction sees deletion
        let new_txn = store.new_txn(true).await.unwrap();
        assert_eq!(
            new_txn.get(b"to_delete").await.unwrap(),
            None,
            "New transaction should see deletion"
        );
    }

    /// Stress test: 50 parallel transactions
    #[tokio::test]
    async fn test_memory_parallel_commits_stress() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let store = Arc::new(MemoryStore::new());
        let commit_count = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];

        for i in 0..50 {
            let store = store.clone();
            let commit_count = commit_count.clone();
            handles.push(tokio::spawn(async move {
                let mut txn = store.new_txn(false).await.unwrap();
                txn.set(format!("stress_key_{}", i).as_bytes(), b"value")
                    .await
                    .unwrap();
                txn.commit().await.unwrap();
                commit_count.fetch_add(1, Ordering::SeqCst);
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        assert_eq!(commit_count.load(Ordering::SeqCst), 50);

        // Verify all 50 keys exist
        let txn = store.new_txn(true).await.unwrap();
        for i in 0..50 {
            assert!(
                txn.has(format!("stress_key_{}", i).as_bytes())
                    .await
                    .unwrap(),
                "Key {} should exist after concurrent commits",
                i
            );
        }
    }
}
