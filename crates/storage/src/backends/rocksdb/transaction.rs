use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use super::metrics::RocksDbTransactionMetrics;
use crate::backends::shared::DurabilityMode;
use crate::backends::shared::{
    CallbackCounts, CallbackManager, ConflictSnapshot, ConflictTracker, ReadSet,
};
use crate::chunked::{ChunkedSnapshot, DEFAULT_CHUNK_SIZE};
use crate::corekv::{
    AsyncTxnCallback, Error, IterOptions, Iterator, Reader, Result, Txn, TxnCallback, Writer,
};
use crate::merging::MergingIterator;

/// Owned snapshot that holds an `Arc<DB>` to ensure the DB outlives the snapshot.
///
/// `SnapshotWithThreadMode` borrows the DB, but we need owned semantics.
/// The transmute is safe because the `Arc<DB>` guarantees the DB lives
/// as long as this struct.
struct OwnedSnapshot {
    // IMPORTANT: _db MUST be declared before snapshot to ensure correct drop order.
    // Rust drops fields in declaration order. If snapshot were dropped after _db,
    // and _db held the last Arc reference, the snapshot would reference freed memory.
    _db: Arc<rocksdb::OptimisticTransactionDB>,
    snapshot: rocksdb::SnapshotWithThreadMode<'static, rocksdb::OptimisticTransactionDB>,
}

// Safety: OwnedSnapshot is safe to Send/Sync because:
// (1) the Arc<DB> ensures the DB outlives the snapshot,
// (2) rocksdb::SnapshotWithThreadMode is internally thread-safe (uses a C pointer),
// (3) no &mut access to the snapshot is possible through &OwnedSnapshot.
unsafe impl Send for OwnedSnapshot {}
unsafe impl Sync for OwnedSnapshot {}

impl OwnedSnapshot {
    fn new(db: Arc<rocksdb::OptimisticTransactionDB>) -> Self {
        // Safety: The Arc<DB> stored in _db ensures the DB outlives the snapshot.
        // We transmute the lifetime from the borrow to 'static.
        let snapshot = unsafe {
            let snap = db.snapshot();
            std::mem::transmute::<
                rocksdb::SnapshotWithThreadMode<'_, rocksdb::OptimisticTransactionDB>,
                rocksdb::SnapshotWithThreadMode<'static, rocksdb::OptimisticTransactionDB>,
            >(snap)
        };
        Self { _db: db, snapshot }
    }

    fn get(&self, key: &[u8]) -> std::result::Result<Option<Vec<u8>>, rocksdb::Error> {
        self.snapshot.get(key)
    }

    fn iterator_opt(
        &self,
        mode: rocksdb::IteratorMode,
        readopts: rocksdb::ReadOptions,
    ) -> rocksdb::DBIteratorWithThreadMode<'_, rocksdb::OptimisticTransactionDB> {
        self.snapshot.iterator_opt(mode, readopts)
    }
}

/// RocksDB transaction with snapshot isolation and buffered writes.
pub(crate) struct RocksDbTxn {
    pub(crate) db: Arc<rocksdb::OptimisticTransactionDB>,
    /// Shared so a chunked scan's per-window refill closure can hold its own
    /// handle without borrowing `&self` past the `iterator()` call.
    snapshot: Arc<OwnedSnapshot>,
    pub(crate) conflict_tracker: Arc<ConflictTracker>,
    pub(crate) _conflict_snapshot: Option<ConflictSnapshot>,
    pub(crate) commit_gate: Arc<tokio::sync::RwLock<()>>,
    pub(crate) active_txn_count: Arc<AtomicUsize>,
    pub(crate) read_version: u64,
    /// Pending changes (Some(value) = set, None = delete)
    pub(crate) pending: Mutex<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
    /// Point keys and ranges read by this transaction.
    pub(crate) read_set: Mutex<ReadSet>,
    pub(crate) readonly: bool,
    pub(crate) discarded: AtomicBool,
    pub(crate) committed: AtomicBool,
    pub(crate) callbacks: CallbackManager,
    pub(crate) durability: DurabilityMode,
    pub(crate) metrics: Arc<RocksDbTransactionMetrics>,
}

impl Drop for RocksDbTxn {
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

impl RocksDbTxn {
    /// Create a new transaction with snapshot isolation.
    pub(crate) fn new(
        db: Arc<rocksdb::OptimisticTransactionDB>,
        conflict_tracker: Arc<ConflictTracker>,
        commit_gate: Arc<tokio::sync::RwLock<()>>,
        active_txn_count: Arc<AtomicUsize>,
        readonly: bool,
        durability: DurabilityMode,
        metrics: Arc<RocksDbTransactionMetrics>,
    ) -> Self {
        let conflict_snapshot = (!readonly).then(|| conflict_tracker.begin_snapshot());
        let read_version = conflict_snapshot.as_ref().map_or_else(
            || conflict_tracker.current_version(),
            |snapshot| snapshot.version(),
        );
        let snapshot = Arc::new(OwnedSnapshot::new(Arc::clone(&db)));

        Self {
            db,
            snapshot,
            conflict_tracker,
            _conflict_snapshot: conflict_snapshot,
            commit_gate,
            active_txn_count,
            read_version,
            pending: Mutex::new(BTreeMap::new()),
            read_set: Mutex::new(ReadSet::default()),
            readonly,
            discarded: AtomicBool::new(false),
            committed: AtomicBool::new(false),
            callbacks: CallbackManager::new(),
            durability,
            metrics,
        }
    }

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

        // Fall back to RocksDB snapshot (point-in-time read)
        Ok(self.snapshot.get(key)?)
    }

    fn has_internal(&self, key: &[u8]) -> Result<bool> {
        let pending = self.pending.lock();
        if let Some(pending_value) = pending.get(key) {
            return Ok(pending_value.is_some());
        }
        drop(pending);

        Ok(self.snapshot.get(key)?.is_some())
    }
}

impl crate::corekv::private::Sealed for RocksDbTxn {}

#[async_trait]
impl Reader for RocksDbTxn {
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
        let keys_only = opts.keys_only();
        let matches_prefix =
            |key: &[u8]| -> bool { opts.prefix().is_none_or(|p| key.starts_with(p)) };

        // Compute range bounds
        let (start_bound, end_bound) = compute_range_bounds(&opts);

        let snapshot = if opts.reverse() {
            // Chunked forward reads cannot be reversed after the fact, and
            // reverse scans are not on the limit hot path: read everything
            // matching eagerly, exactly as before.
            let mut items = Vec::new();
            let mut read_opts = rocksdb::ReadOptions::default();
            if let Bound::Included(ref start) = start_bound {
                read_opts.set_iterate_lower_bound(start.clone());
            }
            set_upper_bound(&mut read_opts, &end_bound);

            let iter = self
                .snapshot
                .iterator_opt(rocksdb::IteratorMode::Start, read_opts);
            for result in iter {
                match result {
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
                        tracing::error!(error = %e, "Error reading from RocksDB snapshot iterator");
                        return Err(Error::Backend(format!("rocksdb iterator error: {}", e)));
                    }
                }
            }
            items.reverse();
            // Already fully materialized: the whole (reversed) result is
            // handed over as the one and only window.
            ChunkedSnapshot::from_window(items)
        } else {
            // `set_iterate_lower_bound` is inclusive, so an `Excluded` start
            // needs its key's successor (`+ 0x00`) — the same adjustment the
            // refill below makes for `after`.
            let initial_lower = match &start_bound {
                Bound::Included(start) => Some(start.clone()),
                Bound::Unbounded => None,
                Bound::Excluded(start) => Some({
                    let mut succ = start.clone();
                    succ.push(0);
                    succ
                }),
            };
            let end_bound_for_chunk = end_bound.clone();
            let prefix = opts.prefix().map(|p| p.to_vec());
            let snapshot = Arc::clone(&self.snapshot);
            ChunkedSnapshot::new(DEFAULT_CHUNK_SIZE, move |after: Option<Vec<u8>>| {
                // Only the lower bound moves between refills, to
                // `after`'s exclusive successor (`after + 0x00`); the upper
                // bound is fixed for the whole scan.
                let lower = match &after {
                    Some(k) => {
                        let mut succ = k.clone();
                        succ.push(0);
                        Some(succ)
                    }
                    None => initial_lower.clone(),
                };
                let result = (|| -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
                    let mut read_opts = rocksdb::ReadOptions::default();
                    if let Some(lower) = &lower {
                        read_opts.set_iterate_lower_bound(lower.clone());
                    }
                    set_upper_bound(&mut read_opts, &end_bound_for_chunk);

                    let iter = snapshot.iterator_opt(rocksdb::IteratorMode::Start, read_opts);
                    let mut items = Vec::new();
                    for r in iter {
                        let (k, v) = r.map_err(|e| {
                            Error::Backend(format!("rocksdb iterator error: {}", e))
                        })?;
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
                    Ok(items)
                })();
                async move { result }
            })
        };

        // Extract pending items into Vec (sorted by BTreeMap, with Option for deletions)
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

/// Apply `end_bound` as `read_opts`'s upper bound.
///
/// `set_iterate_upper_bound` is exclusive, so an `Included` bound needs its
/// key's successor (`+ 0x00`) to still cover the key itself.
fn set_upper_bound(read_opts: &mut rocksdb::ReadOptions, end_bound: &Bound<Vec<u8>>) {
    match end_bound {
        Bound::Excluded(end) => read_opts.set_iterate_upper_bound(end.clone()),
        Bound::Included(end) => {
            let mut upper = end.clone();
            upper.push(0);
            read_opts.set_iterate_upper_bound(upper);
        }
        Bound::Unbounded => {}
    }
}

/// Compute the start and end bounds for a range query from IterOptions.
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
impl Writer for RocksDbTxn {
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
impl Txn for RocksDbTxn {
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
            let db = Arc::clone(&self.db);
            let conflict_tracker = Arc::clone(&self.conflict_tracker);
            let commit_gate = Arc::clone(&self.commit_gate);
            let read_version = self.read_version;
            let durability = self.durability;
            let metrics = Arc::clone(&self.metrics);
            let conflict_snapshot = self._conflict_snapshot.take();
            let callbacks = crate::backends::shared::CommitCallbacks::drain(&self.callbacks);

            let write_result = tokio::task::spawn_blocking(
                move || -> (Result<()>, tokio::task::JoinHandle<()>) {
                    let _conflict_snapshot = conflict_snapshot;
                    let outcome = (|| -> Result<()> {
                        let reservation =
                            conflict_tracker.reserve(read_version, pending.keys(), &read_set)?;

                        let mut batch = rocksdb::WriteBatchWithTransaction::<true>::default();
                        for (key, value) in &pending {
                            match value {
                                Some(v) => batch.put(key, v),
                                None => batch.delete(key),
                            }
                        }

                        let mut write_opts = rocksdb::WriteOptions::default();
                        match durability {
                            DurabilityMode::Immediate => write_opts.set_sync(true),
                            DurabilityMode::Eventual => write_opts.set_sync(false),
                        }

                        db.write_opt(batch, &write_opts)?;

                        let wait_started = std::time::Instant::now();
                        let _publication_guard = commit_gate.blocking_write();
                        let wait = wait_started.elapsed();
                        metrics.record_commit_gate_wait(wait);
                        conflict_tracker.record_commit_gate_wait(wait);
                        reservation.publish();
                        Ok(())
                    })();
                    if matches!(outcome, Err(Error::TxnConflict)) {
                        metrics.record_conflict();
                    }
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
                        "Failed to commit RocksDB batch"
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
