use async_trait::async_trait;
use parking_lot::Mutex;
use redb::{Database, ReadTransaction};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::instrument;

use super::config::DurabilityMode;
use super::group_commit::{GroupCommitBuffer, PendingCommit};
use super::iterator::MergingIterator;
use super::{bound_as_ref, compute_range_bounds, KV_TABLE};
use crate::backends::shared::{
    CallbackCounts, CallbackManager, ConflictSnapshot, ConflictTracker, ReadSet,
};
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

    /// Keeps conflict history alive for this write transaction's snapshot.
    pub(crate) _conflict_snapshot: Option<ConflictSnapshot>,

    /// Keeps transaction snapshots aligned with committed conflict versions.
    pub(crate) commit_gate: Arc<tokio::sync::RwLock<()>>,

    /// Version at which this transaction's snapshot was taken
    pub(crate) read_version: u64,

    /// Redb MVCC read transaction for snapshot isolation (O(1) creation)
    pub(crate) read_txn: ReadTransaction,

    /// Pending changes (Some(value) = set, None = delete)
    pub(crate) pending: Mutex<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,

    /// Point keys and ranges read by this transaction.
    pub(crate) read_set: Mutex<ReadSet>,

    /// Whether this is a read-only transaction
    pub(crate) readonly: bool,

    /// Durability mode for write commits
    pub(crate) durability: DurabilityMode,

    /// Whether the transaction has been discarded
    pub(crate) discarded: AtomicBool,

    /// Whether the transaction has been committed
    pub(crate) committed: AtomicBool,

    /// Transaction lifecycle callbacks
    pub(crate) callbacks: CallbackManager,

    /// Group commit buffer for coalescing writes (None = direct commit)
    pub(crate) group_commit: Option<Arc<GroupCommitBuffer>>,
}

impl Drop for RedbTxn {
    fn drop(&mut self) {
        // Always decrement the active transaction count
        self.active_txn_count.fetch_sub(1, Ordering::AcqRel);

        // Log warning if dropped without explicit commit/discard
        let was_committed = self.committed.load(Ordering::Acquire);
        let was_discarded = self.discarded.load(Ordering::Acquire);
        if !was_committed && !was_discarded {
            let has_pending = !self.pending.lock().is_empty();
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
            } else if has_pending {
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
        if !self.readonly {
            let pending = self.pending.lock();
            if let Some(pending_value) = pending.get(key) {
                return Ok(pending_value.clone());
            }
        }

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
        if !self.readonly {
            let pending = self.pending.lock();
            if let Some(pending_value) = pending.get(key) {
                return Ok(pending_value.is_some());
            }
        }

        // Fall back to redb ReadTransaction
        let table = match self.read_txn.open_table(KV_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(false),
            Err(e) => return Err(e.into()),
        };
        Ok(table.get(key)?.is_some())
    }
}

impl crate::corekv::private::Sealed for RedbTxn {}

#[async_trait]
impl Reader for RedbTxn {
    #[instrument(level = "trace", skip(self))]
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.discarded.load(Ordering::Acquire) {
            return Err(Error::DiscardedTxn);
        }

        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        self.read_set.lock().record_key(key);
        self.get_internal(key)
    }

    async fn has(&self, key: &[u8]) -> Result<bool> {
        if self.discarded.load(Ordering::Acquire) {
            return Err(Error::DiscardedTxn);
        }

        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        self.read_set.lock().record_key(key);
        self.has_internal(key)
    }

    async fn get_size(&self, key: &[u8]) -> Result<Option<usize>> {
        if self.discarded.load(Ordering::Acquire) {
            return Err(Error::DiscardedTxn);
        }

        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        self.read_set.lock().record_key(key);
        Ok(self.get_internal(key)?.map(|v| v.len()))
    }

    async fn iterator(&self, opts: IterOptions) -> Result<Box<dyn Iterator>> {
        if self.discarded.load(Ordering::Acquire) {
            return Err(Error::DiscardedTxn);
        }

        // Compute the effective range bounds for efficient range queries
        let (start_bound, end_bound) = compute_range_bounds(&opts);
        let keys_only = opts.keys_only();
        self.read_set.lock().record_iter_options(&opts);

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
                    let key_bytes = key.value();
                    if matches_prefix(key_bytes) {
                        items.push((
                            key_bytes.to_vec(),
                            if keys_only {
                                Vec::new()
                            } else {
                                value.value().to_vec()
                            },
                        ));
                    }
                }
                items
            }
            Err(redb::TableError::TableDoesNotExist(_)) => Vec::new(),
            Err(e) => return Err(e.into()),
        };

        // Extract pending items into Vec (sorted by BTreeMap, with Option for deletions)
        let pending_items = if self.readonly {
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
            snapshot_items,
            pending_items,
            opts,
        )))
    }
}

#[async_trait]
impl Writer for RedbTxn {
    #[instrument(level = "trace", skip(self))]
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
impl Txn for RedbTxn {
    #[instrument(level = "trace", skip(self))]
    async fn commit(mut self: Box<Self>) -> Result<()> {
        // Note: active_txn_count is decremented by Drop impl when self is dropped
        // at the end of this function (on any exit path).

        if self.discarded.load(Ordering::Acquire) {
            tracing::warn!("Attempted to commit a discarded transaction");
            CallbackManager::execute_callbacks(self.callbacks.take_error());
            CallbackManager::execute_async_callbacks(self.callbacks.take_error_async()).await;
            return Err(Error::DiscardedTxn);
        }

        if self.committed.load(Ordering::Acquire) {
            tracing::warn!("Attempted to commit an already committed transaction");
            return Err(Error::Other("Transaction already committed".into()));
        }

        // Take pending changes (avoids clone — commit consumes self via Box<Self>)
        let pending = std::mem::take(&mut *self.pending.lock());
        let read_set = self.read_set.lock().clone();

        // Apply pending changes to the database if there are any
        if !pending.is_empty() {
            // Group commit path: enqueue changes for batched flush.
            // Conflict detection is deferred to the flush loop so that version
            // tracking is atomic with the data write.
            if let Some(ref gc) = self.group_commit {
                let (result_tx, result_rx) = tokio::sync::oneshot::channel();
                let commit = PendingCommit {
                    changes: pending,
                    read_version: self.read_version,
                    read_set,
                    _conflict_snapshot: self
                        ._conflict_snapshot
                        .take()
                        .expect("write transaction has a conflict snapshot"),
                    result_tx,
                    on_success: self.callbacks.take_success(),
                    on_success_async: self.callbacks.take_success_async(),
                    on_error: self.callbacks.take_error(),
                    on_error_async: self.callbacks.take_error_async(),
                };

                if gc.enqueue(commit).is_err() {
                    return Err(Error::Other("group commit channel closed".into()));
                }

                self.committed.store(true, Ordering::Release);

                // Wait for the batch flush to complete
                return result_rx
                    .await
                    .map_err(|_| Error::Other("group commit result channel dropped".into()))?;
            }

            // Move blocking redb operations to a blocking thread so we don't
            // starve tokio worker threads while waiting for the exclusive write lock.
            let db = self.db.clone();
            let conflict_tracker = self.conflict_tracker.clone();
            let commit_gate = self.commit_gate.clone();
            let read_version = self.read_version;
            let durability = self.durability;
            let conflict_snapshot = self
                ._conflict_snapshot
                .take()
                .expect("write transaction has a conflict snapshot");
            // Carried into the commit task so a dropped future does not skip
            // the events for a write that still lands (#1185).
            let callbacks = crate::backends::shared::CommitCallbacks::drain(&self.callbacks);

            let write_result = tokio::task::spawn_blocking(
                move || -> (Result<()>, tokio::task::JoinHandle<()>) {
                    let _conflict_snapshot = conflict_snapshot;
                    let outcome = (|| -> Result<()> {
                        let reservation = conflict_tracker.reserve(
                            read_version,
                            pending.keys(),
                            &read_set,
                        )?;

                        let mut write_txn = db.begin_write().map_err(|e| {
                            tracing::error!(error = %e, pending_changes = pending.len(),
                                "Failed to begin write transaction during commit");
                            Error::from(e)
                        })?;

                        write_txn.set_durability(match durability {
                            DurabilityMode::Immediate => redb::Durability::Immediate,
                            DurabilityMode::Eventual => redb::Durability::Eventual,
                        });

                        {
                            let mut table = write_txn.open_table(KV_TABLE).map_err(|e| {
                                tracing::error!(error = %e, "Failed to open KV table during commit");
                                Error::from(e)
                            })?;

                            for (key, value) in &pending {
                                match value {
                                    Some(v) => {
                                        if let Err(e) = table.insert(key.as_slice(), v.as_slice()) {
                                            tracing::error!(error = %e, key_len = key.len(),
                                                value_len = v.len(), "Failed to insert key during commit");
                                            return Err(e.into());
                                        }
                                    }
                                    None => {
                                        if let Err(e) = table.remove(key.as_slice()) {
                                            tracing::error!(error = %e, key_len = key.len(),
                                                "Failed to delete key during commit");
                                            return Err(e.into());
                                        }
                                    }
                                }
                            }
                        }

                        if let Err(e) = write_txn.commit() {
                            tracing::error!(error = %e, pending_changes = pending.len(),
                                "Failed to finalize commit");
                            return Err(e.into());
                        }

                        let wait_started = std::time::Instant::now();
                        let _publication_guard = commit_gate.blocking_write();
                        conflict_tracker.record_commit_gate_wait(wait_started.elapsed());
                        reservation.publish();
                        Ok(())
                    })();
                    let callbacks = callbacks.spawn(outcome.is_ok());
                    (outcome, callbacks)
                },
            )
            .await;

            match write_result {
                Ok((outcome, callbacks)) => {
                    // Keep the guarantee that callbacks finish before commit()
                    // returns (and that a callback panic still propagates); a
                    // cancelled caller simply never gets here and the spawned
                    // task runs on regardless.
                    crate::backends::shared::join_commit_callbacks(callbacks).await;
                    outcome?;
                }
                Err(join_err) => {
                    let msg = if join_err.is_panic() {
                        let panic = join_err.into_panic();
                        if let Some(s) = panic.downcast_ref::<String>() {
                            format!("spawn_blocking panicked: {}", s)
                        } else if let Some(s) = panic.downcast_ref::<&str>() {
                            format!("spawn_blocking panicked: {}", s)
                        } else {
                            "spawn_blocking panicked with non-string payload".to_string()
                        }
                    } else {
                        format!("spawn_blocking cancelled: {}", join_err)
                    };
                    return Err(Error::Other(msg));
                }
            }

            // Mark as committed AFTER successful database commit
            self.committed.store(true, Ordering::Release);
            return Ok(());
        }

        // Nothing was written, so there is no commit task to carry them.
        self.committed.store(true, Ordering::Release);

        CallbackManager::execute_callbacks(self.callbacks.take_success());
        CallbackManager::execute_async_callbacks(self.callbacks.take_success_async()).await;

        Ok(())
    }

    fn discard(self: Box<Self>) {
        // Note: active_txn_count is decremented by Drop impl when self is dropped
        // at the end of this function.

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
