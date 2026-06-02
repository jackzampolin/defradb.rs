use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::iterator::MergingIterator;
use crate::backends::shared::DurabilityMode;
use crate::backends::shared::{CallbackManager, ConflictTracker, ReadSet};
use crate::corekv::{
    AsyncTxnCallback, Error, IterOptions, Iterator, Reader, Result, Txn, TxnCallback, Writer,
};

/// Lark transaction with snapshot isolation and buffered writes.
///
/// Reads go through a `lark_kv::Snapshot` captured at transaction creation.
/// Writes are buffered in a `BTreeMap` and applied atomically on commit
/// via `lark_kv::WriteBatch`.
pub(crate) struct LarkTxn {
    db: Arc<lark_kv::Db>,
    snapshot: lark_kv::Snapshot,
    conflict_tracker: Arc<ConflictTracker>,
    active_txn_count: Arc<AtomicUsize>,
    read_version: u64,
    pending: Mutex<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
    read_set: Mutex<ReadSet>,
    readonly: bool,
    discarded: Mutex<bool>,
    committed: Mutex<bool>,
    callbacks: CallbackManager,
    durability: DurabilityMode,
}

impl Drop for LarkTxn {
    fn drop(&mut self) {
        self.active_txn_count.fetch_sub(1, Ordering::AcqRel);

        let was_committed = *self.committed.lock();
        let was_discarded = *self.discarded.lock();
        if !was_committed && !was_discarded {
            let has_pending = !self.pending.lock().is_empty();
            let total_skipped =
                self.callbacks.counts().on_discard + self.callbacks.counts().on_discard_async;

            if total_skipped > 0 {
                tracing::warn!(
                    skipped_callbacks = total_skipped,
                    "Transaction dropped without commit() or discard()"
                );
            } else if has_pending {
                tracing::warn!(
                    "Transaction dropped without commit() or discard() - pending changes lost"
                );
            }
        }
    }
}

impl LarkTxn {
    pub(crate) fn new(
        db: Arc<lark_kv::Db>,
        conflict_tracker: Arc<ConflictTracker>,
        active_txn_count: Arc<AtomicUsize>,
        readonly: bool,
        durability: DurabilityMode,
    ) -> Self {
        let read_version = conflict_tracker.current_version();
        let snapshot = db.snapshot();

        Self {
            db,
            snapshot,
            conflict_tracker,
            active_txn_count,
            read_version,
            pending: Mutex::new(BTreeMap::new()),
            read_set: Mutex::new(ReadSet::default()),
            readonly,
            discarded: Mutex::new(false),
            committed: Mutex::new(false),
            callbacks: CallbackManager::new(),
            durability,
        }
    }

    fn get_internal(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check pending changes first
        let pending = self.pending.lock();
        if let Some(pending_value) = pending.get(key) {
            return Ok(pending_value.clone());
        }
        drop(pending);

        // Fall back to snapshot
        self.snapshot
            .get(key)
            .map_err(|e| Error::Backend(e.to_string()))
    }

    fn has_internal(&self, key: &[u8]) -> Result<bool> {
        let pending = self.pending.lock();
        if let Some(pending_value) = pending.get(key) {
            return Ok(pending_value.is_some());
        }
        drop(pending);

        self.snapshot
            .get(key)
            .map(|v| v.is_some())
            .map_err(|e| Error::Backend(e.to_string()))
    }
}

impl crate::corekv::private::Sealed for LarkTxn {}

#[async_trait]
impl Reader for LarkTxn {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if *self.discarded.lock() {
            return Err(Error::DiscardedTxn);
        }
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        self.read_set.lock().record_key(key);
        self.get_internal(key)
    }

    async fn has(&self, key: &[u8]) -> Result<bool> {
        if *self.discarded.lock() {
            return Err(Error::DiscardedTxn);
        }
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        self.read_set.lock().record_key(key);
        self.has_internal(key)
    }

    async fn get_size(&self, key: &[u8]) -> Result<Option<usize>> {
        if *self.discarded.lock() {
            return Err(Error::DiscardedTxn);
        }
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        self.read_set.lock().record_key(key);
        Ok(self.get_internal(key)?.map(|v| v.len()))
    }

    async fn iterator(&self, opts: IterOptions) -> Result<Box<dyn Iterator>> {
        if *self.discarded.lock() {
            return Err(Error::DiscardedTxn);
        }

        self.read_set.lock().record_iter_options(&opts);
        let keys_only = opts.keys_only();
        let matches_prefix =
            |key: &[u8]| -> bool { opts.prefix().is_none_or(|p| key.starts_with(p)) };

        let (start_bound, end_bound) = compute_range_bounds(&opts);
        let snapshot_iter = self.snapshot.owned_iter();

        // Extract pending items in range
        let pending = self.pending.lock();
        let pending_items: Vec<(Vec<u8>, Option<Vec<u8>>)> = pending
            .range((start_bound.clone(), end_bound.clone()))
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
            snapshot_iter,
            pending_items,
            opts,
            start_bound,
            end_bound,
        )?))
    }
}

fn compute_range_bounds(opts: &IterOptions) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let start_bound = match (opts.prefix(), opts.start()) {
        (Some(prefix), Some(start)) => {
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

    let end_bound = match (opts.prefix(), opts.end()) {
        (Some(prefix), Some(end)) => {
            let prefix_end = prefix_to_end_bound(prefix);
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
        (Some(prefix), None) => match prefix_to_end_bound(prefix) {
            Some(end) => Bound::Excluded(end),
            None => Bound::Unbounded,
        },
        (None, Some(end)) => Bound::Excluded(end.to_vec()),
        (None, None) => Bound::Unbounded,
    };

    (start_bound, end_bound)
}

fn prefix_to_end_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    if prefix.is_empty() {
        return None;
    }

    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xFF {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

#[async_trait]
impl Writer for LarkTxn {
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
impl Txn for LarkTxn {
    async fn commit(self: Box<Self>) -> Result<()> {
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

        let pending = std::mem::take(&mut *self.pending.lock());
        let read_set = self.read_set.lock().clone();

        if !pending.is_empty() {
            // Check conflicts
            if let Err(e) =
                self.conflict_tracker
                    .check_and_record(self.read_version, pending.keys(), &read_set)
            {
                CallbackManager::execute_callbacks(self.callbacks.take_error());
                CallbackManager::execute_async_callbacks(self.callbacks.take_error_async()).await;
                return Err(e);
            }

            // Apply via lark WriteBatch
            let mut batch = lark_kv::WriteBatch::new();
            for (key, value) in &pending {
                match value {
                    Some(v) => batch.put(key, v),
                    None => batch.delete(key),
                }
            }
            let durability = match self.durability {
                DurabilityMode::Immediate => lark_kv::DurabilityMode::Immediate,
                DurabilityMode::Eventual => lark_kv::DurabilityMode::Eventual,
            };

            if let Err(e) = self.db.write_with_durability(batch, durability) {
                tracing::error!(error = %e, "Failed to commit lark batch");
                CallbackManager::execute_callbacks(self.callbacks.take_error());
                CallbackManager::execute_async_callbacks(self.callbacks.take_error_async()).await;
                return Err(Error::Backend(e.to_string()));
            }
        }

        *self.committed.lock() = true;

        CallbackManager::execute_callbacks(self.callbacks.take_success());
        CallbackManager::execute_async_callbacks(self.callbacks.take_success_async()).await;

        Ok(())
    }

    fn discard(self: Box<Self>) {
        *self.discarded.lock() = true;

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
