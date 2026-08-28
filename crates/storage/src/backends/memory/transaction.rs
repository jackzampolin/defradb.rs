use bytes::Bytes;
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::backends::shared::{CallbackManager, ConflictSnapshot, ConflictTracker, ReadSet};
use crate::chunked::{ChunkedSnapshot, DEFAULT_CHUNK_SIZE};
use crate::corekv::{
    AsyncTxnCallback, Error, IterOptions, Iterator, Reader, Result, Txn, TxnCallback, Writer,
};
use crate::empty_iterator::EmptyIterator;
use crate::merging::MergingIterator;
use crate::range_bounds::compute_range_bounds;

/// In-memory transaction with snapshot isolation and conflict detection.
///
/// Transactions maintain a snapshot of the store at creation time and track
/// pending changes. On commit, write-write conflicts are detected using
/// optimistic concurrency control.
pub(crate) struct MemoryTxn {
    /// Reference to the store's data
    pub(crate) store: Arc<RwLock<BTreeMap<Vec<u8>, Vec<u8>>>>,

    /// Conflict tracker for write-write conflict detection
    pub(crate) conflict_tracker: Arc<ConflictTracker>,

    /// Keeps conflict history alive for this write transaction's snapshot.
    pub(crate) _conflict_snapshot: Option<ConflictSnapshot>,

    /// Keeps conflict versions aligned with physical snapshots and commits.
    pub(crate) commit_gate: Arc<RwLock<()>>,

    /// Version at which this transaction's snapshot was taken
    pub(crate) read_version: u64,

    /// Snapshot of store at transaction start (for reads).
    ///
    /// `Arc`-wrapped so `iterator()` can hand a cheap handle to a `'static`
    /// chunk-reading closure instead of cloning the whole map per call.
    pub(crate) snapshot: Arc<BTreeMap<Vec<u8>, Vec<u8>>>,

    /// Pending changes (Some(value) = set, None = delete)
    pub(crate) pending: Mutex<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,

    /// Point keys and ranges read by this transaction.
    pub(crate) read_set: Mutex<ReadSet>,

    /// Whether this is a read-only transaction
    pub(crate) readonly: bool,

    /// Whether the transaction has been discarded
    pub(crate) discarded: AtomicBool,

    /// Whether the transaction has been committed
    pub(crate) committed: AtomicBool,

    /// Transaction lifecycle callbacks
    pub(crate) callbacks: CallbackManager,
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
}

impl crate::corekv::private::Sealed for MemoryTxn {}

#[async_trait]
impl Reader for MemoryTxn {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if self.discarded.load(Ordering::Acquire) {
            return Err(Error::DiscardedTxn);
        }

        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        self.read_set.lock().record_key(key);
        Ok(self.get_internal(key).map(Bytes::from))
    }

    async fn has(&self, key: &[u8]) -> Result<bool> {
        if self.discarded.load(Ordering::Acquire) {
            return Err(Error::DiscardedTxn);
        }

        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        self.read_set.lock().record_key(key);
        Ok(self.has_internal(key))
    }

    async fn get_size(&self, key: &[u8]) -> Result<Option<usize>> {
        if self.discarded.load(Ordering::Acquire) {
            return Err(Error::DiscardedTxn);
        }

        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        self.read_set.lock().record_key(key);
        Ok(self.get_internal(key).map(|v| v.len()))
    }

    async fn iterator(&self, opts: IterOptions) -> Result<Box<dyn Iterator>> {
        if self.discarded.load(Ordering::Acquire) {
            return Err(Error::DiscardedTxn);
        }

        self.read_set.lock().record_iter_options(&opts);
        let Some((start_bound, end_bound)) = compute_range_bounds(&opts) else {
            return Ok(Box::new(EmptyIterator::new()));
        };
        let keys_only = opts.keys_only();
        let matches_prefix =
            |key: &[u8]| -> bool { opts.prefix().is_none_or(|p| key.starts_with(p)) };

        let snapshot = if opts.reverse() {
            // Chunked forward reads cannot be reversed after the fact, and
            // reverse scans are not on the limit hot path: read everything
            // matching eagerly, exactly as before.
            let mut items: Vec<(Vec<u8>, Vec<u8>)> = self
                .snapshot
                .range((start_bound.clone(), end_bound.clone()))
                .filter(|(k, _)| matches_prefix(k))
                .map(|(k, v)| (k.clone(), if keys_only { Vec::new() } else { v.clone() }))
                .collect();
            items.reverse();
            // Already fully materialized: the whole (reversed) result is
            // handed over as the one and only window.
            ChunkedSnapshot::from_window(items)
        } else {
            let end_bound_for_chunk = end_bound.clone();
            let start_bound_for_chunk = start_bound.clone();
            let prefix = opts.prefix().map(|p| p.to_vec());
            let snapshot = Arc::clone(&self.snapshot);
            ChunkedSnapshot::new(DEFAULT_CHUNK_SIZE, move |after: Option<Vec<u8>>| {
                // Resume strictly after the last key yielded: `last_key + 0x00`
                // is its exclusive successor, so nothing sorts between them.
                let lower_bound = match &after {
                    Some(k) => {
                        let mut succ = k.clone();
                        succ.push(0);
                        Bound::Included(succ)
                    }
                    None => start_bound_for_chunk.clone(),
                };
                let items: Vec<(Vec<u8>, Vec<u8>)> = snapshot
                    .range((lower_bound, end_bound_for_chunk.clone()))
                    .filter(|(k, _)| prefix.as_deref().is_none_or(|p| k.starts_with(p)))
                    .take(DEFAULT_CHUNK_SIZE)
                    .map(|(k, v)| (k.clone(), if keys_only { Vec::new() } else { v.clone() }))
                    .collect();
                async move { Ok(items) }
            })
        };

        // Extract pending items into Vec (sorted by BTreeMap, with Option for deletions)
        let pending_items: Vec<(Vec<u8>, Option<Vec<u8>>)> = if self.readonly {
            Vec::new()
        } else {
            let pending = self.pending.lock();
            pending
                .range((start_bound, end_bound))
                .filter(|(k, _)| matches_prefix(k))
                .map(|(k, v)| {
                    (
                        k.clone(),
                        v.as_ref()
                            .map(|value| if keys_only { Vec::new() } else { value.clone() }),
                    )
                })
                .collect()
        };

        Ok(Box::new(MergingIterator::new(
            snapshot,
            pending_items,
            opts,
        )))
    }
}

#[async_trait]
impl Writer for MemoryTxn {
    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.discarded.load(Ordering::Acquire) {
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
        if self.discarded.load(Ordering::Acquire) {
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
        if self.discarded.load(Ordering::Acquire) {
            tracing::warn!("Attempted to commit a discarded transaction");
            CallbackManager::execute_callbacks(self.callbacks.take_error());
            CallbackManager::execute_async_callbacks(self.callbacks.take_error_async()).await;
            return Err(Error::DiscardedTxn);
        }

        // Check if already committed (defensive - ownership prevents this in normal usage)
        if self.committed.load(Ordering::Acquire) {
            tracing::warn!("Attempted to commit an already committed transaction");
            return Err(Error::Other("Transaction already committed".into()));
        }

        // Clone pending changes before awaiting (can't hold MutexGuard across await)
        let pending = self.pending.lock().clone();
        let read_set = self.read_set.lock().clone();

        if !pending.is_empty() {
            let reservation =
                match self
                    .conflict_tracker
                    .reserve(self.read_version, pending.keys(), &read_set)
                {
                    Ok(reservation) => reservation,
                    Err(error) => {
                        CallbackManager::execute_callbacks(self.callbacks.take_error());
                        CallbackManager::execute_async_callbacks(self.callbacks.take_error_async())
                            .await;
                        return Err(error);
                    }
                };
            let wait_started = std::time::Instant::now();
            let commit_guard = self.commit_gate.write().await;
            self.conflict_tracker
                .record_commit_gate_wait(wait_started.elapsed());
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
            reservation.publish();
            drop(store);
            drop(commit_guard);
        }

        // Mark as committed
        self.committed.store(true, Ordering::Release);

        // Execute success callbacks. The sync ones cannot be skipped — nothing
        // awaits between the mutation above and here — but the async ones sit
        // behind an await, so a caller cancelled there would drop the events
        // for a mutation that already landed (#1185). Spawn them so they
        // survive that, and await the handle so they still finish before
        // commit() returns. There is no blocking task here to carry them, and
        // no guarantee of a runtime, so fall back to running them inline.
        CallbackManager::execute_callbacks(self.callbacks.take_success());
        let success_async = self.callbacks.take_success_async();
        if !success_async.is_empty() {
            match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    crate::backends::shared::join_commit_callbacks(
                        handle.spawn(CallbackManager::execute_async_callbacks(success_async)),
                    )
                    .await;
                }
                Err(_) => {
                    CallbackManager::execute_async_callbacks(success_async).await;
                }
            }
        }

        Ok(())
    }

    fn discard(self: Box<Self>) {
        self.discarded.store(true, Ordering::Release);

        // Execute sync discard callbacks
        CallbackManager::execute_callbacks(self.callbacks.take_discard());

        // Handle async callbacks: spawn them in background with warning
        let on_discard_async = self.callbacks.take_discard_async();
        if !on_discard_async.is_empty() {
            let callback_count = on_discard_async.len();
            tracing::warn!(
                count = callback_count,
                "Transaction has async discard callbacks. Spawning in background - they may not complete if process exits. Consider using commit() instead of discard() when async callbacks are registered."
            );

            // Spawn async callbacks in background with error tracking
            // NOTE: These may not complete if the process exits before they finish
            tokio::spawn(async move {
                CallbackManager::execute_async_callbacks(on_discard_async).await;
                tracing::debug!(count = callback_count, "Async discard callbacks completed");
            });
        }
    }

    fn on_success(&mut self, callback: TxnCallback) {
        self.callbacks.register_success(callback);
    }

    fn on_success_async(&mut self, callback: AsyncTxnCallback) {
        self.callbacks.register_success_async(callback);
    }

    fn on_error(&mut self, callback: TxnCallback) {
        self.callbacks.register_error(callback);
    }

    fn on_error_async(&mut self, callback: AsyncTxnCallback) {
        self.callbacks.register_error_async(callback);
    }

    fn on_discard(&mut self, callback: TxnCallback) {
        self.callbacks.register_discard(callback);
    }

    fn on_discard_async(&mut self, callback: AsyncTxnCallback) {
        self.callbacks.register_discard_async(callback);
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
        self.callbacks.count()
    }
}
