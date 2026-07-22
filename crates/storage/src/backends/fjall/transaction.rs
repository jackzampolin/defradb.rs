use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use super::compute_range_bounds;
use super::iterator::MergingIterator;
use crate::backends::shared::DurabilityMode;
use crate::backends::shared::{
    CallbackCounts, CallbackManager, ConflictSnapshot, ConflictTracker, ReadSet,
};
use crate::corekv::{
    AsyncTxnCallback, Error, IterOptions, Iterator, Reader, Result, Txn, TxnCallback, Writer,
};

/// Fjall transaction with snapshot isolation and buffered writes.
pub(crate) struct FjallTxn {
    pub(crate) db: fjall::Database,
    pub(crate) keyspace: fjall::Keyspace,
    pub(crate) conflict_tracker: Arc<ConflictTracker>,
    pub(crate) _conflict_snapshot: Option<ConflictSnapshot>,
    /// Keeps conflict versions aligned with physical snapshots and commits.
    pub(crate) commit_gate: Arc<tokio::sync::RwLock<()>>,
    pub(crate) active_txn_count: Arc<AtomicUsize>,
    pub(crate) read_version: u64,
    pub(crate) snapshot: fjall::Snapshot,
    /// Pending changes (Some(value) = set, None = delete)
    pub(crate) pending: Mutex<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
    /// Point keys and ranges read by this transaction.
    pub(crate) read_set: Mutex<ReadSet>,
    pub(crate) readonly: bool,
    pub(crate) discarded: AtomicBool,
    pub(crate) committed: AtomicBool,
    pub(crate) callbacks: CallbackManager,
    pub(crate) durability: DurabilityMode,
}

impl Drop for FjallTxn {
    fn drop(&mut self) {
        self.active_txn_count.fetch_sub(1, Ordering::AcqRel);

        let was_committed = self.committed.load(Ordering::Acquire);
        let was_discarded = self.discarded.load(Ordering::Acquire);
        if !was_committed && !was_discarded {
            let has_pending = !self.pending.lock().is_empty();
            let total_skipped =
                self.callbacks.counts().on_discard + self.callbacks.counts().on_discard_async;

            if total_skipped > 0 {
                tracing::warn!(
                    skipped_callbacks = total_skipped,
                    "Transaction dropped without commit() or discard() - \
                     {} registered discard callback(s) were NOT executed.",
                    total_skipped
                );
            } else if has_pending {
                tracing::warn!(
                    "Transaction dropped without commit() or discard() - \
                     pending changes were lost."
                );
            }
        }
    }
}

impl FjallTxn {
    #[allow(dead_code)]
    pub fn callback_counts(&self) -> CallbackCounts {
        self.callbacks.counts()
    }

    fn get_internal(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check pending changes first
        let pending = self.pending.lock();
        if let Some(pending_value) = pending.get(key) {
            return Ok(pending_value.clone());
        }
        drop(pending);

        // Fall back to fjall snapshot
        use fjall::Readable;
        match self.snapshot.get(&self.keyspace, key)? {
            Some(value) => Ok(Some(value.to_vec())),
            None => Ok(None),
        }
    }

    fn has_internal(&self, key: &[u8]) -> Result<bool> {
        let pending = self.pending.lock();
        if let Some(pending_value) = pending.get(key) {
            return Ok(pending_value.is_some());
        }
        drop(pending);

        use fjall::Readable;
        Ok(self.snapshot.contains_key(&self.keyspace, key)?)
    }
}

impl crate::corekv::private::Sealed for FjallTxn {}

#[async_trait]
impl Reader for FjallTxn {
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

        self.read_set.lock().record_iter_options(&opts);
        let (start_bound, end_bound) = compute_range_bounds(&opts);
        let keys_only = opts.keys_only();

        let matches_prefix =
            |key: &[u8]| -> bool { opts.prefix().is_none_or(|p| key.starts_with(p)) };

        // Read matching items from the fjall snapshot
        use fjall::Readable;
        let snapshot_items: Vec<(Vec<u8>, Vec<u8>)> = {
            let iter = match (&start_bound, &end_bound) {
                (std::ops::Bound::Unbounded, std::ops::Bound::Unbounded) => {
                    self.snapshot.iter(&self.keyspace)
                }
                _ => self.snapshot.range::<&[u8], _>(
                    &self.keyspace,
                    (bound_as_ref(&start_bound), bound_as_ref(&end_bound)),
                ),
            };

            let mut items = Vec::new();
            for guard in iter {
                match guard.into_inner() {
                    Ok((k, v)) => {
                        let key_bytes = k.as_ref();
                        if matches_prefix(key_bytes) {
                            items.push((
                                key_bytes.to_vec(),
                                if keys_only { Vec::new() } else { v.to_vec() },
                            ));
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "Error reading from fjall snapshot iterator");
                        return Err(Error::Backend(format!("fjall iterator error: {}", e)));
                    }
                }
            }
            items
        };

        // Extract pending items in range
        let pending = self.pending.lock();
        let pending_items: Vec<(Vec<u8>, Option<Vec<u8>>)> = pending
            .range((start_bound, end_bound))
            .filter(|(k, _)| matches_prefix(k))
            .map(|(k, v)| {
                (
                    k.clone(),
                    v.as_ref()
                        .map(|value| if keys_only { Vec::new() } else { value.clone() }),
                )
            })
            .collect();

        Ok(Box::new(MergingIterator::new(
            snapshot_items,
            pending_items,
            opts,
        )))
    }
}

/// Convert a `Bound<Vec<u8>>` to `Bound<&[u8]>` for fjall range queries.
fn bound_as_ref(bound: &std::ops::Bound<Vec<u8>>) -> std::ops::Bound<&[u8]> {
    match bound {
        std::ops::Bound::Included(v) => std::ops::Bound::Included(v.as_slice()),
        std::ops::Bound::Excluded(v) => std::ops::Bound::Excluded(v.as_slice()),
        std::ops::Bound::Unbounded => std::ops::Bound::Unbounded,
    }
}

#[async_trait]
impl Writer for FjallTxn {
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
impl Txn for FjallTxn {
    async fn commit(self: Box<Self>) -> Result<()> {
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

        let pending = std::mem::take(&mut *self.pending.lock());
        let read_set = self.read_set.lock().clone();

        if !pending.is_empty() {
            let commit_result = {
                let _commit_guard = self.commit_gate.write().await;
                match self.conflict_tracker.check_and_record(
                    self.read_version,
                    pending.keys(),
                    &read_set,
                ) {
                    Err(error) => Err(error),
                    Ok(()) => {
                        let mut batch = self.db.batch();
                        match self.durability {
                            DurabilityMode::Immediate => {
                                batch = batch.durability(Some(fjall::PersistMode::SyncAll));
                            }
                            DurabilityMode::Eventual => {
                                batch = batch.durability(Some(fjall::PersistMode::Buffer));
                            }
                        }
                        for (key, value) in &pending {
                            match value {
                                Some(v) => {
                                    batch.insert(&self.keyspace, key.as_slice(), v.as_slice())
                                }
                                None => batch.remove(&self.keyspace, key.as_slice()),
                            }
                        }
                        batch.commit().map_err(Error::from)
                    }
                }
            };

            if let Err(e) = commit_result {
                if !matches!(e, Error::TxnConflict) {
                    tracing::error!(
                        error = %e,
                        pending_changes = pending.len(),
                        "Failed to commit fjall batch"
                    );
                }
                CallbackManager::execute_callbacks(self.callbacks.take_error());
                CallbackManager::execute_async_callbacks(self.callbacks.take_error_async()).await;
                return Err(e);
            }
        }

        self.committed.store(true, Ordering::Release);

        CallbackManager::execute_callbacks(self.callbacks.take_success());
        CallbackManager::execute_async_callbacks(self.callbacks.take_success_async()).await;

        Ok(())
    }

    fn discard(self: Box<Self>) {
        self.discarded.store(true, Ordering::Release);

        CallbackManager::execute_callbacks(self.callbacks.take_discard());

        let on_discard_async = self.callbacks.take_discard_async();
        if !on_discard_async.is_empty() {
            let callback_count = on_discard_async.len();
            tracing::warn!(
                count = callback_count,
                "Transaction has async discard callbacks. Spawning in background."
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
