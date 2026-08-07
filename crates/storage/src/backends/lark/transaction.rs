use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::iterator::MergingIterator;
use crate::backends::shared::DurabilityMode;
use crate::backends::shared::{
    CallbackManager, ConflictReservation, ConflictSnapshot, ConflictTracker, ReadSet,
};
use crate::corekv::{
    AsyncTxnCallback, Error, IterOptions, Iterator, Reader, Result, Txn, TxnCallback, Writer,
};
use crate::empty_iterator::EmptyIterator;
use crate::range_bounds::compute_range_bounds;

/// Lark transaction with snapshot isolation and buffered writes.
///
/// Reads go through a `lark_kv::Snapshot` captured at transaction creation.
/// Writes are buffered in insertion order and indexed lazily when a read or
/// iterator needs pending-write visibility.
pub(crate) struct LarkTxn {
    db: Arc<lark_kv::Db>,
    snapshot: lark_kv::Snapshot,
    conflict_tracker: Arc<ConflictTracker>,
    _conflict_snapshot: Option<ConflictSnapshot>,
    /// Keeps conflict versions aligned with physical snapshots and commits.
    commit_gate: Arc<tokio::sync::RwLock<()>>,
    active_txn_count: Arc<AtomicUsize>,
    read_version: u64,
    pending: Mutex<PendingWrites>,
    read_set: Mutex<ReadSet>,
    readonly: bool,
    discarded: Mutex<bool>,
    committed: Mutex<bool>,
    callbacks: CallbackManager,
    durability: DurabilityMode,
}

#[derive(Default)]
struct PendingWrites {
    ops: Vec<PendingWrite>,
    index: Option<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
}

enum PendingWrite {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl PendingWrite {
    fn key(&self) -> &Vec<u8> {
        match self {
            Self::Put { key, .. } | Self::Delete { key } => key,
        }
    }

    fn apply_to_batch(self, batch: &mut lark_kv::WriteBatch) {
        match self {
            Self::Put { key, value } => batch.put(&key, &value),
            Self::Delete { key } => batch.delete(&key),
        }
    }
}

impl PendingWrites {
    fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        if let Some(index) = &mut self.index {
            index.insert(key.clone(), Some(value.clone()));
        }
        self.ops.push(PendingWrite::Put { key, value });
    }

    fn delete(&mut self, key: Vec<u8>) {
        if let Some(index) = &mut self.index {
            index.insert(key.clone(), None);
        }
        self.ops.push(PendingWrite::Delete { key });
    }

    fn get(&mut self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        self.index().get(key).cloned()
    }

    fn range_items(
        &mut self,
        start_bound: Bound<Vec<u8>>,
        end_bound: Bound<Vec<u8>>,
        keys_only: bool,
        matches_prefix: impl Fn(&[u8]) -> bool,
    ) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.index()
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
    }

    fn reserve_conflicts(
        &self,
        conflict_tracker: &Arc<ConflictTracker>,
        read_version: u64,
        read_set: &ReadSet,
    ) -> Result<ConflictReservation> {
        if let Some(index) = &self.index {
            conflict_tracker.reserve(read_version, index.keys(), read_set)
        } else {
            conflict_tracker.reserve(
                read_version,
                self.ops.iter().map(PendingWrite::key),
                read_set,
            )
        }
    }

    fn into_write_batch(self) -> lark_kv::WriteBatch {
        let mut batch = lark_kv::WriteBatch::new();
        if let Some(index) = self.index {
            for (key, value) in index {
                match value {
                    Some(value) => batch.put(&key, &value),
                    None => batch.delete(&key),
                }
            }
        } else {
            for op in self.ops {
                op.apply_to_batch(&mut batch);
            }
        }
        batch
    }

    fn index(&mut self) -> &BTreeMap<Vec<u8>, Option<Vec<u8>>> {
        self.index.get_or_insert_with(|| {
            let mut index = BTreeMap::new();
            for op in &self.ops {
                match op {
                    PendingWrite::Put { key, value } => {
                        index.insert(key.clone(), Some(value.clone()));
                    }
                    PendingWrite::Delete { key } => {
                        index.insert(key.clone(), None);
                    }
                }
            }
            index
        })
    }
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
        commit_gate: Arc<tokio::sync::RwLock<()>>,
        active_txn_count: Arc<AtomicUsize>,
        readonly: bool,
        durability: DurabilityMode,
    ) -> Self {
        let conflict_snapshot = (!readonly).then(|| conflict_tracker.begin_snapshot());
        let read_version = conflict_snapshot.as_ref().map_or_else(
            || conflict_tracker.current_version(),
            |snapshot| snapshot.version(),
        );
        let snapshot = db.snapshot();

        Self {
            db,
            snapshot,
            conflict_tracker,
            _conflict_snapshot: conflict_snapshot,
            commit_gate,
            active_txn_count,
            read_version,
            pending: Mutex::new(PendingWrites::default()),
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
        let mut pending = self.pending.lock();
        if let Some(pending_value) = pending.get(key) {
            return Ok(pending_value);
        }
        drop(pending);

        // Fall back to snapshot
        self.snapshot
            .get(key)
            .map_err(|e| Error::Backend(e.to_string()))
    }

    fn snapshot_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.snapshot
            .get(key)
            .map_err(|e| Error::Backend(e.to_string()))
    }

    fn has_internal(&self, key: &[u8]) -> Result<bool> {
        let mut pending = self.pending.lock();
        if let Some(pending_value) = pending.get(key) {
            return Ok(pending_value.is_some());
        }
        drop(pending);

        self.snapshot
            .get(key)
            .map(|v| v.is_some())
            .map_err(|e| Error::Backend(e.to_string()))
    }

    fn snapshot_has(&self, key: &[u8]) -> Result<bool> {
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
        if self.readonly {
            return self.snapshot_get(key);
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
        if self.readonly {
            return self.snapshot_has(key);
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
        if self.readonly {
            return Ok(self.snapshot_get(key)?.map(|v| v.len()));
        }
        self.read_set.lock().record_key(key);
        Ok(self.get_internal(key)?.map(|v| v.len()))
    }

    async fn iterator(&self, opts: IterOptions) -> Result<Box<dyn Iterator>> {
        if *self.discarded.lock() {
            return Err(Error::DiscardedTxn);
        }

        if !self.readonly {
            self.read_set.lock().record_iter_options(&opts);
        }
        let keys_only = opts.keys_only();
        let matches_prefix =
            |key: &[u8]| -> bool { opts.prefix().is_none_or(|p| key.starts_with(p)) };

        let Some((start_bound, end_bound)) = compute_range_bounds(&opts) else {
            return Ok(Box::new(EmptyIterator::new()));
        };
        let snapshot_iter = self.snapshot.owned_iter();

        let pending_items = if self.readonly {
            Vec::new()
        } else {
            self.pending.lock().range_items(
                start_bound.clone(),
                end_bound.clone(),
                keys_only,
                matches_prefix,
            )
        };

        Ok(Box::new(MergingIterator::new(
            snapshot_iter,
            pending_items,
            opts,
            start_bound,
            end_bound,
        )?))
    }
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
        self.pending.lock().put(key.to_vec(), value.to_vec());
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
        self.pending.lock().delete(key.to_vec());
        Ok(())
    }
}

#[async_trait]
impl Txn for LarkTxn {
    async fn commit(mut self: Box<Self>) -> Result<()> {
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
            // The blocking task owns the reservation and callbacks, so
            // cancelling the commit future cannot abandon an in-flight write
            // or its events. Conflicting committers see the reservation before
            // this task writes, while disjoint physical writes can proceed in
            // parallel. The gate only pairs successful version publication
            // with new write-transaction snapshots.
            let db = Arc::clone(&self.db);
            let conflict_tracker = Arc::clone(&self.conflict_tracker);
            let commit_gate = Arc::clone(&self.commit_gate);
            let read_version = self.read_version;
            let durability = match self.durability {
                DurabilityMode::Immediate => lark_kv::DurabilityMode::Immediate,
                DurabilityMode::Eventual => lark_kv::DurabilityMode::Eventual,
            };
            let conflict_snapshot = self._conflict_snapshot.take();
            let callbacks = crate::backends::shared::CommitCallbacks::drain(&self.callbacks);

            let write_result = tokio::task::spawn_blocking(
                move || -> (Result<()>, tokio::task::JoinHandle<()>) {
                    let _conflict_snapshot = conflict_snapshot;
                    let outcome = (|| -> Result<()> {
                        let reservation = pending.reserve_conflicts(
                            &conflict_tracker,
                            read_version,
                            &read_set,
                        )?;

                        let batch = pending.into_write_batch();
                        db.write_with_durability(batch, durability)
                            .map_err(|error| Error::Backend(error.to_string()))?;

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

            let commit_result = match write_result {
                Ok((outcome, callbacks)) => {
                    // Keep the guarantee that callbacks finish before commit()
                    // returns (and that a callback panic still propagates); a
                    // cancelled caller simply never gets here and the spawned
                    // task runs on regardless.
                    crate::backends::shared::join_commit_callbacks(callbacks).await;
                    outcome
                }
                Err(join_err) => Err(Error::Other(if join_err.is_panic() {
                    format!("commit task panicked: {join_err}")
                } else {
                    format!("commit task cancelled: {join_err}")
                })),
            };

            if let Err(e) = commit_result {
                if !matches!(e, Error::TxnConflict) {
                    tracing::error!(error = %e, "Failed to commit lark batch");
                }
                return Err(e);
            }

            *self.committed.lock() = true;
            return Ok(());
        }

        // Nothing was written, so there is no commit task to carry them.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_writes_build_index_lazily_and_keep_last_write() {
        let mut pending = PendingWrites::default();

        pending.put(b"a".to_vec(), b"old".to_vec());
        pending.delete(b"b".to_vec());
        pending.put(b"a".to_vec(), b"new".to_vec());

        assert!(pending.index.is_none());
        assert_eq!(pending.get(b"a"), Some(Some(b"new".to_vec())));
        assert_eq!(pending.get(b"b"), Some(None));
        assert!(pending.index.is_some());
    }
}
