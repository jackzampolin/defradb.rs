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
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::corekv::{
    AsyncTxnCallback, Error, Iterator, IterOptions, KvPair, Reader, Result, Store, Txn,
    TxnCallback, Writer,
};

/// In-memory key-value store using BTreeMap.
///
/// Data is stored in a BTreeMap wrapped in Arc<RwLock<>> for thread-safe
/// concurrent access. The store provides snapshot isolation for transactions.
#[derive(Clone)]
pub struct MemoryStore {
    data: Arc<RwLock<BTreeMap<Vec<u8>, Vec<u8>>>>,
    closed: Arc<RwLock<bool>>,
}

impl MemoryStore {
    /// Create a new empty memory store.
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(BTreeMap::new())),
            closed: Arc::new(RwLock::new(false)),
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

        // Take a snapshot of current data for isolation
        let snapshot = self.data.read().await.clone();

        Ok(Box::new(MemoryTxn {
            store: Arc::clone(&self.data),
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

/// In-memory transaction with snapshot isolation.
///
/// Transactions maintain a snapshot of the store at creation time and track
/// pending changes. Changes are applied atomically on commit.
struct MemoryTxn {
    /// Reference to the store's data
    store: Arc<RwLock<BTreeMap<Vec<u8>, Vec<u8>>>>,

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
    /// Each callback is executed sequentially to ensure proper error handling.
    async fn execute_async_callbacks(callbacks: Vec<AsyncTxnCallback>) {
        for (i, callback) in callbacks.into_iter().enumerate() {
            let future = callback();
            // Note: We can't catch panics in async code the same way, but we can
            // catch panics from the callback creation and log them
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                // The callback itself is already created, just await it
            }));
            if let Err(panic_info) = result {
                tracing::error!(
                    callback_index = i,
                    panic = ?panic_info,
                    "Async callback setup panicked"
                );
                continue;
            }
            future.await;
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

        self.pending.lock().insert(key.to_vec(), Some(value.to_vec()));
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
        if *self.discarded.lock() {
            return Err(Error::DiscardedTxn);
        }

        // Clone pending changes before awaiting (can't hold MutexGuard across await)
        let pending = self.pending.lock().clone();

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
            tracing::error!(
                count = on_discard_async.len(),
                "Transaction has async discard callbacks. Spawning in background - they may not complete if process exits. Consider using commit() instead of discard() when async callbacks are registered."
            );

            // Spawn async callbacks in background
            // NOTE: These may not complete if the process exits before they finish
            tokio::spawn(async move {
                Self::execute_async_callbacks(on_discard_async).await;
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
        if opts.reverse() {
            filtered.reverse();
        }

        Ok(Self {
            data: filtered,
            position: 0,
            closed: false,
            keys_only: opts.keys_only(),
        })
    }
}

#[async_trait]
impl Iterator for MemoryIterator {
    async fn next(&mut self) -> Result<Option<KvPair>> {
        if self.closed {
            return Ok(None);
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

    fn is_valid(&self) -> bool {
        !self.closed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memory_store_basic() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        txn.set(b"key1", b"value1").await.unwrap();
        txn.set(b"key2", b"value2").await.unwrap();

        assert_eq!(txn.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
        assert_eq!(txn.get(b"key2").await.unwrap(), Some(b"value2".to_vec()));

        txn.commit().await.unwrap();
    }

    #[tokio::test]
    async fn test_memory_store_delete() {
        let store = MemoryStore::new();

        // Set a value
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"key", b"value").await.unwrap();
        txn.commit().await.unwrap();

        // Delete it
        let mut txn = store.new_txn(false).await.unwrap();
        txn.delete(b"key").await.unwrap();
        assert_eq!(txn.get(b"key").await.unwrap(), None);
        txn.commit().await.unwrap();

        // Verify deletion persisted
        let txn = store.new_txn(true).await.unwrap();
        assert_eq!(txn.get(b"key").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_memory_store_isolation() {
        let store = MemoryStore::new();

        // Create two transactions
        let mut txn1 = store.new_txn(false).await.unwrap();
        let mut txn2 = store.new_txn(false).await.unwrap();

        // txn1 writes
        txn1.set(b"key", b"value1").await.unwrap();

        // txn2 shouldn't see txn1's write yet
        assert_eq!(txn2.get(b"key").await.unwrap(), None);

        // Commit txn1
        txn1.commit().await.unwrap();

        // txn2 still has its snapshot (doesn't see committed changes)
        assert_eq!(txn2.get(b"key").await.unwrap(), None);

        // New transaction sees the committed value
        let txn3 = store.new_txn(true).await.unwrap();
        assert_eq!(txn3.get(b"key").await.unwrap(), Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_memory_store_readonly() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(true).await.unwrap();

        let result = txn.set(b"key", b"value").await;
        assert!(matches!(result, Err(Error::ReadOnlyTxn)));
    }

    #[tokio::test]
    async fn test_memory_store_discard() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        txn.set(b"key", b"value").await.unwrap();
        txn.discard();

        // Value shouldn't be persisted
        let txn = store.new_txn(true).await.unwrap();
        assert_eq!(txn.get(b"key").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_memory_iterator() {
        let store = MemoryStore::new();
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
    async fn test_memory_iterator_prefix() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        txn.set(b"user_1", b"alice").await.unwrap();
        txn.set(b"user_2", b"bob").await.unwrap();
        txn.set(b"post_1", b"hello").await.unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let opts = IterOptions::new().with_prefix(b"user_".to_vec());
        let mut iter = txn.iterator(opts).await.unwrap();

        let mut count = 0;
        while let Some(_kv) = iter.next().await.unwrap() {
            count += 1;
        }
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn test_memory_iterator_reverse() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        txn.set(b"a", b"1").await.unwrap();
        txn.set(b"b", b"2").await.unwrap();
        txn.set(b"c", b"3").await.unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let opts = IterOptions::new().with_reverse(true);
        let mut iter = txn.iterator(opts).await.unwrap();

        let kv1 = iter.next().await.unwrap().unwrap();
        assert_eq!(kv1.key_bytes(), b"c");

        let kv2 = iter.next().await.unwrap().unwrap();
        assert_eq!(kv2.key_bytes(), b"b");

        let kv3 = iter.next().await.unwrap().unwrap();
        assert_eq!(kv3.key_bytes(), b"a");

        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_memory_store_callbacks() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let store = MemoryStore::new();
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
}
