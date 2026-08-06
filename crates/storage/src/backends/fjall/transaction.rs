use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use super::compute_range_bounds;
use crate::backends::shared::DurabilityMode;
use crate::backends::shared::{
    CallbackCounts, CallbackManager, ConflictSnapshot, ConflictTracker, ReadSet,
};
use crate::chunked::{ChunkedSnapshot, DEFAULT_CHUNK_SIZE};
use crate::corekv::{
    AsyncTxnCallback, Error, IterOptions, Iterator, Reader, Result, Txn, TxnCallback, Writer,
};
use crate::merging::MergingIterator;

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

        use fjall::Readable;

        let snapshot = if opts.reverse() {
            // Chunked forward reads cannot be reversed after the fact, and
            // reverse scans are not on the limit hot path: read everything
            // matching eagerly, exactly as before.
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
            items.reverse();
            let chunk_size = items.len().max(1);
            ChunkedSnapshot::new(chunk_size, move |after| {
                // Already fully materialized: the whole (reversed) result is
                // one window, and any later refill is the empty terminator.
                let batch = if after.is_none() {
                    items.clone()
                } else {
                    Vec::new()
                };
                async move { Ok(batch) }
            })
        } else {
            let start_bound_for_chunk = start_bound.clone();
            let end_bound_for_chunk = end_bound.clone();
            let prefix = opts.prefix().map(|p| p.to_vec());
            let snapshot = self.snapshot.clone();
            let keyspace = self.keyspace.clone();
            ChunkedSnapshot::new(DEFAULT_CHUNK_SIZE, move |after: Option<Vec<u8>>| {
                // Resume strictly after the last key yielded: `last_key + 0x00`
                // is its exclusive successor, so nothing sorts between them.
                let lower_bound = match &after {
                    Some(k) => {
                        let mut succ = k.clone();
                        succ.push(0);
                        std::ops::Bound::Included(succ)
                    }
                    None => start_bound_for_chunk.clone(),
                };
                let result = (|| -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
                    let iter = match (&lower_bound, &end_bound_for_chunk) {
                        (std::ops::Bound::Unbounded, std::ops::Bound::Unbounded) => {
                            snapshot.iter(&keyspace)
                        }
                        _ => snapshot.range::<&[u8], _>(
                            &keyspace,
                            (
                                bound_as_ref(&lower_bound),
                                bound_as_ref(&end_bound_for_chunk),
                            ),
                        ),
                    };
                    let mut items = Vec::new();
                    for guard in iter {
                        match guard.into_inner() {
                            Ok((k, v)) => {
                                let key_bytes = k.as_ref();
                                if prefix.as_deref().is_none_or(|p| key_bytes.starts_with(p)) {
                                    items.push((
                                        key_bytes.to_vec(),
                                        if keys_only { Vec::new() } else { v.to_vec() },
                                    ));
                                    if items.len() >= DEFAULT_CHUNK_SIZE {
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                return Err(Error::Backend(format!("fjall iterator error: {}", e)));
                            }
                        }
                    }
                    Ok(items)
                })();
                async move { result }
            })
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
            snapshot,
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
    async fn commit(mut self: Box<Self>) -> Result<()> {
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
            let pending_changes = pending.len();

            // The blocking task owns the reservation and callbacks, so
            // cancelling the commit future cannot abandon an in-flight write
            // or its events. Conflicting committers see the reservation before
            // this task writes, while disjoint physical writes can proceed in
            // parallel. The gate only pairs successful version publication
            // with new write-transaction snapshots.
            let db = self.db.clone();
            let keyspace = self.keyspace.clone();
            let conflict_tracker = Arc::clone(&self.conflict_tracker);
            let commit_gate = Arc::clone(&self.commit_gate);
            let read_version = self.read_version;
            let durability = self.durability;
            let conflict_snapshot = self._conflict_snapshot.take();
            let callbacks = crate::backends::shared::CommitCallbacks::drain(&self.callbacks);

            let write_result = tokio::task::spawn_blocking(
                move || -> (Result<()>, tokio::task::JoinHandle<()>) {
                    let _conflict_snapshot = conflict_snapshot;
                    let outcome = (|| -> Result<()> {
                        let reservation =
                            conflict_tracker.reserve(read_version, pending.keys(), &read_set)?;

                        let mut batch = db.batch();
                        match durability {
                            DurabilityMode::Immediate => {
                                batch = batch.durability(Some(fjall::PersistMode::SyncAll));
                            }
                            DurabilityMode::Eventual => {
                                batch = batch.durability(Some(fjall::PersistMode::Buffer));
                            }
                        }
                        for (key, value) in &pending {
                            match value {
                                Some(v) => batch.insert(&keyspace, key.as_slice(), v.as_slice()),
                                None => batch.remove(&keyspace, key.as_slice()),
                            }
                        }

                        batch.commit()?;

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
                    tracing::error!(
                        error = %e,
                        pending_changes,
                        "Failed to commit fjall batch"
                    );
                }
                return Err(e);
            }

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
