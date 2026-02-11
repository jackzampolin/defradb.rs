use async_trait::async_trait;
use parking_lot::Mutex;
use redb::{Database, ReadTransaction};
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::config::DurabilityMode;
use super::iterator::MergingIterator;
use super::{bound_as_ref, compute_range_bounds, KV_TABLE};
use crate::backends::shared::{CallbackCounts, CallbackManager, ConflictTracker};
use crate::corekv::{
    AsyncTxnCallback, Error, IterOptions, Iterator, Reader, Result, Txn, TxnCallback, Writer,
};

/// Redb transaction with snapshot isolation and buffered writes.
///
/// Transactions maintain a snapshot of the store at creation time and track
/// pending changes. Changes are applied atomically on commit.
///
/// # Drop Safety
///
/// If a transaction is dropped without calling `commit()` or `discard()`,
/// the Drop implementation will:
/// - Decrement the active transaction count (preventing store close hangs)
/// - Log a warning about the improper cleanup
///
/// This is a safety net - callers should always explicitly commit or discard.
pub(crate) struct RedbTxn {
    /// Reference to the redb database
    pub(crate) db: Arc<Database>,

    /// Reference to the active transaction counter (for decrement on complete)
    pub(crate) active_txn_count: Arc<AtomicUsize>,

    /// Conflict tracker for write-write conflict detection
    pub(crate) conflict_tracker: Arc<ConflictTracker>,

    /// Version at which this transaction's snapshot was taken
    pub(crate) read_version: u64,

    /// Redb MVCC read transaction for snapshot isolation (O(1) creation)
    pub(crate) read_txn: ReadTransaction,

    /// Pending changes (Some(value) = set, None = delete)
    pub(crate) pending: Mutex<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,

    /// Whether this is a read-only transaction
    pub(crate) readonly: bool,

    /// Durability mode for write commits
    pub(crate) durability: DurabilityMode,

    /// Whether the transaction has been discarded
    pub(crate) discarded: Mutex<bool>,

    /// Whether the transaction has been committed
    pub(crate) committed: Mutex<bool>,

    /// Transaction lifecycle callbacks
    pub(crate) callbacks: CallbackManager,
}

impl Drop for RedbTxn {
    fn drop(&mut self) {
        // Always decrement the active transaction count
        self.active_txn_count.fetch_sub(1, Ordering::SeqCst);

        // Log warning if dropped without explicit commit/discard
        let was_committed = *self.committed.lock();
        let was_discarded = *self.discarded.lock();
        if !was_committed && !was_discarded {
            // Count skipped callbacks to include in warning
            let total_skipped =
                self.callbacks.counts().on_discard + self.callbacks.counts().on_discard_async;

            if total_skipped > 0 {
                tracing::warn!(
                    skipped_callbacks = total_skipped,
                    "Transaction dropped without commit() or discard() - \
                     this may indicate a bug. Pending changes were lost and \
                     {} registered discard callback(s) were NOT executed.",
                    total_skipped
                );
            } else {
                tracing::warn!(
                    "Transaction dropped without commit() or discard() - \
                     this may indicate a bug. Pending changes were lost."
                );
            }
        }
    }
}

impl RedbTxn {
    /// Get the current count of registered callbacks.
    #[allow(dead_code)] // Part of public API - used externally for monitoring
    pub fn callback_counts(&self) -> CallbackCounts {
        self.callbacks.counts()
    }

    /// Get a value, checking pending changes first, then the read transaction.
    fn get_internal(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check pending changes first
        let pending = self.pending.lock();
        if let Some(pending_value) = pending.get(key) {
            return Ok(pending_value.clone());
        }
        drop(pending);

        // Fall back to redb ReadTransaction
        let table = match self.read_txn.open_table(KV_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        match table.get(key)? {
            Some(value) => Ok(Some(value.value().to_vec())),
            None => Ok(None),
        }
    }

    /// Check if a key exists.
    fn has_internal(&self, key: &[u8]) -> Result<bool> {
        // Check pending changes first
        let pending = self.pending.lock();
        if let Some(pending_value) = pending.get(key) {
            return Ok(pending_value.is_some());
        }
        drop(pending);

        // Fall back to redb ReadTransaction
        let table = match self.read_txn.open_table(KV_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(false),
            Err(e) => return Err(e.into()),
        };
        Ok(table.get(key)?.is_some())
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

        self.get_internal(key)
    }

    async fn has(&self, key: &[u8]) -> Result<bool> {
        if *self.discarded.lock() {
            return Err(Error::DiscardedTxn);
        }

        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        self.has_internal(key)
    }

    async fn get_size(&self, key: &[u8]) -> Result<Option<usize>> {
        if *self.discarded.lock() {
            return Err(Error::DiscardedTxn);
        }

        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        Ok(self.get_internal(key)?.map(|v| v.len()))
    }

    async fn iterator(&self, opts: IterOptions) -> Result<Box<dyn Iterator>> {
        if *self.discarded.lock() {
            return Err(Error::DiscardedTxn);
        }

        // Compute the effective range bounds for efficient range queries
        let (start_bound, end_bound) = compute_range_bounds(&opts);

        // Helper to check prefix
        let matches_prefix =
            |key: &[u8]| -> bool { opts.prefix().is_none_or(|p| key.starts_with(p)) };

        // Read matching items from the redb ReadTransaction (only matching range)
        let snapshot_items: Vec<(Vec<u8>, Vec<u8>)> = match self.read_txn.open_table(KV_TABLE) {
            Ok(table) => {
                let range =
                    table.range::<&[u8]>((bound_as_ref(&start_bound), bound_as_ref(&end_bound)))?;
                let mut items = Vec::new();
                for result in range {
                    let (key, value) = result?;
                    let k = key.value().to_vec();
                    if matches_prefix(&k) {
                        items.push((k, value.value().to_vec()));
                    }
                }
                items
            }
            Err(redb::TableError::TableDoesNotExist(_)) => Vec::new(),
            Err(e) => return Err(e.into()),
        };

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
        // Note: active_txn_count is decremented by Drop impl when self is dropped
        // at the end of this function (on any exit path).

        if *self.discarded.lock() {
            tracing::warn!("Attempted to commit a discarded transaction");
            CallbackManager::execute_callbacks(self.callbacks.take_error());
            CallbackManager::execute_async_callbacks(self.callbacks.take_error_async()).await;
            return Err(Error::DiscardedTxn);
        }

        if *self.committed.lock() {
            tracing::warn!("Attempted to commit an already committed transaction");
            return Err(Error::Other("Transaction already committed".into()));
        }

        // Take pending changes (avoids clone — commit consumes self via Box<Self>)
        let pending = std::mem::take(&mut *self.pending.lock());

        // Check for write-write conflicts before applying
        if !pending.is_empty() {
            let write_set: HashSet<Vec<u8>> = pending.keys().cloned().collect();
            if let Err(e) = self
                .conflict_tracker
                .check_and_record(self.read_version, write_set)
            {
                CallbackManager::execute_callbacks(self.callbacks.take_error());
                CallbackManager::execute_async_callbacks(self.callbacks.take_error_async()).await;
                return Err(e);
            }
        }

        // Apply pending changes to the database if there are any
        if !pending.is_empty() {
            let mut write_txn = match self.db.begin_write() {
                Ok(txn) => txn,
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        pending_changes = pending.len(),
                        "Failed to begin write transaction during commit"
                    );
                    CallbackManager::execute_callbacks(self.callbacks.take_error());
                    CallbackManager::execute_async_callbacks(self.callbacks.take_error_async())
                        .await;
                    return Err(e.into());
                }
            };

            write_txn.set_durability(match self.durability {
                DurabilityMode::Immediate => redb::Durability::Immediate,
                DurabilityMode::Eventual => redb::Durability::Eventual,
            });

            {
                let mut table = match write_txn.open_table(KV_TABLE) {
                    Ok(t) => t,
                    Err(e) => {
                        tracing::error!(
                            error = %e,
                            "Failed to open KV table during commit"
                        );
                        CallbackManager::execute_callbacks(self.callbacks.take_error());
                        CallbackManager::execute_async_callbacks(self.callbacks.take_error_async())
                            .await;
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
                                    "Failed to insert key during commit - transaction will be rolled back"
                                );
                                // Note: write_txn is automatically rolled back when dropped (redb guarantee)
                                tracing::debug!(
                                    pending_changes = pending.len(),
                                    "Write transaction aborted - no partial writes persisted"
                                );
                                CallbackManager::execute_callbacks(self.callbacks.take_error());
                                CallbackManager::execute_async_callbacks(
                                    self.callbacks.take_error_async(),
                                )
                                .await;
                                return Err(e.into());
                            }
                        }
                        None => {
                            if let Err(e) = table.remove(key.as_slice()) {
                                tracing::error!(
                                    error = %e,
                                    key_len = key.len(),
                                    "Failed to delete key during commit - transaction will be rolled back"
                                );
                                // Note: write_txn is automatically rolled back when dropped (redb guarantee)
                                tracing::debug!(
                                    pending_changes = pending.len(),
                                    "Write transaction aborted - no partial writes persisted"
                                );
                                CallbackManager::execute_callbacks(self.callbacks.take_error());
                                CallbackManager::execute_async_callbacks(
                                    self.callbacks.take_error_async(),
                                )
                                .await;
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
                    "Failed to finalize commit - all changes rolled back"
                );
                // Note: redb guarantees atomicity - if commit fails, no changes are persisted
                tracing::debug!("Commit failed at finalization stage - database state unchanged");
                CallbackManager::execute_callbacks(self.callbacks.take_error());
                CallbackManager::execute_async_callbacks(self.callbacks.take_error_async()).await;
                return Err(e.into());
            }
        }

        // Mark as committed AFTER successful database commit
        // This ensures the flag accurately reflects the transaction state
        *self.committed.lock() = true;

        // Execute success callbacks
        CallbackManager::execute_callbacks(self.callbacks.take_success());
        CallbackManager::execute_async_callbacks(self.callbacks.take_success_async()).await;

        Ok(())
    }

    fn discard(self: Box<Self>) {
        // Note: active_txn_count is decremented by Drop impl when self is dropped
        // at the end of this function.

        *self.discarded.lock() = true;

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
