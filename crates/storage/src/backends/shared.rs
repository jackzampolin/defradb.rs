use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
#[cfg(not(target_arch = "wasm32"))]
use std::collections::{BTreeMap, HashMap, HashSet};
#[cfg(not(target_arch = "wasm32"))]
use std::ops::Bound;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
#[cfg(not(target_arch = "wasm32"))]
use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
use crate::corekv::IterOptions;
use crate::corekv::{AsyncTxnCallback, TxnCallback};

/// Controls when data is flushed to disk after a commit.
///
/// Default is `Immediate`, which fsyncs every commit so acknowledged writes
/// survive process and OS crashes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum DurabilityMode {
    /// Flush to disk on every commit. Safe against process and OS crashes.
    #[default]
    Immediate,
    /// Rely on the OS to flush eventually. Faster, but OS crashes may lose
    /// acknowledged writes.
    Eventual,
}

/// Manages transaction lifecycle callbacks (success, error, discard).
///
/// All three backends (memory, redb, leveldb) use identical callback storage,
/// registration, execution, and counting logic. This struct centralizes that.
pub(crate) struct CallbackManager {
    on_success: Mutex<Vec<TxnCallback>>,
    on_success_async: Mutex<Vec<AsyncTxnCallback>>,
    on_error: Mutex<Vec<TxnCallback>>,
    on_error_async: Mutex<Vec<AsyncTxnCallback>>,
    on_discard: Mutex<Vec<TxnCallback>>,
    on_discard_async: Mutex<Vec<AsyncTxnCallback>>,
}

impl CallbackManager {
    pub(crate) fn new() -> Self {
        Self {
            on_success: Mutex::new(Vec::new()),
            on_success_async: Mutex::new(Vec::new()),
            on_error: Mutex::new(Vec::new()),
            on_error_async: Mutex::new(Vec::new()),
            on_discard: Mutex::new(Vec::new()),
            on_discard_async: Mutex::new(Vec::new()),
        }
    }

    // Registration methods

    pub(crate) fn register_success(&self, cb: TxnCallback) {
        self.on_success.lock().push(cb);
    }

    pub(crate) fn register_success_async(&self, cb: AsyncTxnCallback) {
        self.on_success_async.lock().push(cb);
    }

    pub(crate) fn register_error(&self, cb: TxnCallback) {
        self.on_error.lock().push(cb);
    }

    pub(crate) fn register_error_async(&self, cb: AsyncTxnCallback) {
        self.on_error_async.lock().push(cb);
    }

    pub(crate) fn register_discard(&self, cb: TxnCallback) {
        self.on_discard.lock().push(cb);
    }

    pub(crate) fn register_discard_async(&self, cb: AsyncTxnCallback) {
        self.on_discard_async.lock().push(cb);
    }

    // Take methods (drain the vecs for execution)

    pub(crate) fn take_success(&self) -> Vec<TxnCallback> {
        std::mem::take(&mut *self.on_success.lock())
    }

    pub(crate) fn take_success_async(&self) -> Vec<AsyncTxnCallback> {
        std::mem::take(&mut *self.on_success_async.lock())
    }

    pub(crate) fn take_error(&self) -> Vec<TxnCallback> {
        std::mem::take(&mut *self.on_error.lock())
    }

    pub(crate) fn take_error_async(&self) -> Vec<AsyncTxnCallback> {
        std::mem::take(&mut *self.on_error_async.lock())
    }

    pub(crate) fn take_discard(&self) -> Vec<TxnCallback> {
        std::mem::take(&mut *self.on_discard.lock())
    }

    pub(crate) fn take_discard_async(&self) -> Vec<AsyncTxnCallback> {
        std::mem::take(&mut *self.on_discard_async.lock())
    }

    // Counting

    pub(crate) fn count(&self) -> usize {
        self.on_success.lock().len()
            + self.on_success_async.lock().len()
            + self.on_error.lock().len()
            + self.on_error_async.lock().len()
            + self.on_discard.lock().len()
            + self.on_discard_async.lock().len()
    }

    #[allow(dead_code)]
    pub(crate) fn counts(&self) -> CallbackCounts {
        CallbackCounts {
            on_success: self.on_success.lock().len(),
            on_success_async: self.on_success_async.lock().len(),
            on_error: self.on_error.lock().len(),
            on_error_async: self.on_error_async.lock().len(),
            on_discard: self.on_discard.lock().len(),
            on_discard_async: self.on_discard_async.lock().len(),
        }
    }

    // Static execution methods

    /// Execute sync callbacks directly.
    pub(crate) fn execute_callbacks(callbacks: Vec<TxnCallback>) {
        for callback in callbacks {
            callback();
        }
    }

    /// Execute async callbacks directly.
    pub(crate) async fn execute_async_callbacks(callbacks: Vec<AsyncTxnCallback>) {
        for callback in callbacks {
            callback().await;
        }
    }
}

/// Callback counts for monitoring transaction callback accumulation.
#[derive(Debug, Clone, Default)]
pub struct CallbackCounts {
    /// Number of synchronous on_success callbacks registered
    pub on_success: usize,
    /// Number of asynchronous on_success callbacks registered
    pub on_success_async: usize,
    /// Number of synchronous on_error callbacks registered
    pub on_error: usize,
    /// Number of asynchronous on_error callbacks registered
    pub on_error_async: usize,
    /// Number of synchronous on_discard callbacks registered
    pub on_discard: usize,
    /// Number of asynchronous on_discard callbacks registered
    pub on_discard_async: usize,
}

impl CallbackCounts {
    /// Total number of callbacks registered across all types.
    pub fn total(&self) -> usize {
        self.on_success
            + self.on_success_async
            + self.on_error
            + self.on_error_async
            + self.on_discard
            + self.on_discard_async
    }
}

/// Keys and key ranges read by a write transaction.
///
/// Go's badger-backed transactions provide SSI semantics: a transaction that
/// writes data must conflict if another transaction committed after its snapshot
/// and either wrote a key it read, or read a key/range it wrote. Tracking both
/// point reads and iterator ranges gives the Rust backends the same behavior.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone, Default)]
pub(crate) struct ReadSet {
    keys: HashSet<Vec<u8>>,
    ranges: Vec<ReadRange>,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone)]
enum ReadRange {
    Prefix {
        prefix: Vec<u8>,
        commutative_set: bool,
    },
    Range {
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
    },
}

#[cfg(not(target_arch = "wasm32"))]
impl ReadSet {
    pub(crate) fn record_key(&mut self, key: &[u8]) {
        self.keys.insert(key.to_vec());
    }

    pub(crate) fn record_iter_options(&mut self, opts: &IterOptions) {
        if let Some(prefix) = opts.prefix() {
            if is_document_collection_scan_prefix(prefix) {
                return;
            }
            self.ranges.push(ReadRange::Prefix {
                prefix: prefix.to_vec(),
                commutative_set: opts.commutative_set(),
            });
        } else {
            self.ranges.push(ReadRange::Range {
                start: opts.start().map(Vec::from),
                end: opts.end().map(Vec::from),
            });
        }
    }

    fn has_commutative_range(&self, key: &[u8]) -> bool {
        self.ranges
            .iter()
            .any(|range| range.commutative_set() && range.contains(key))
    }

    fn conflicts_key(&self, key: &[u8], other: &Self) -> bool {
        self.keys.contains(key)
            || self.ranges.iter().any(|range| {
                range.contains(key)
                    && (!range.commutative_set() || !other.has_commutative_range(key))
            })
    }
}

/// Content-addressed blockstore data keys: the blockstore namespace byte
/// followed by valid raw CID bytes.
///
/// The key is the hash of the value, so any two writers of the same key write
/// identical bytes — blind write-write overlap on these keys is not a
/// serializability hazard, and Go's badger likewise never conflicts blind
/// writes (it only checks reads against committed writes). Without this
/// carve-out, concurrent updates to DIFFERENT documents that produce
/// byte-identical field deltas (e.g. both rewrite `status: "streaming"`)
/// collide on the shared delta block and spuriously abort (#1194).
///
/// Merge-tracking keys under the blockstore namespace (`b` then `m` then
/// CID, [`ToMergeIndexKey`](crate::keys::blockstore::ToMergeIndexKey)) are
/// explicitly excluded: their presence is mutable state that must stay
/// conflict-checked. Only the write-write check consults this predicate;
/// read-vs-write conflicts apply to block keys like any other key.
#[cfg(not(target_arch = "wasm32"))]
fn is_content_addressed_block_key(key: &[u8]) -> bool {
    let Some((&namespace, cid_bytes)) = key.split_first() else {
        return false;
    };
    namespace == crate::namespace::Namespace::Blockstore.prefix()
        && !crate::keys::blockstore::ToMergeIndexKey::is_merge_key(cid_bytes)
        && cid::Cid::try_from(cid_bytes).is_ok_and(|cid| cid.encoded_len() == cid_bytes.len())
}

#[cfg(not(target_arch = "wasm32"))]
fn is_document_collection_scan_prefix(prefix: &[u8]) -> bool {
    // Namespaced datastore document scans use `d/d/...`; root datastore scans
    // use `/d/...`. Go's SSI conflict in the relation tests comes from FK index
    // range reads, while full document collection scans do not conflict with
    // unrelated inserts into the same collection.
    prefix.starts_with(b"d/d/")
        || prefix.starts_with(b"/d/")
        || prefix.starts_with(b"d/del/")
        || prefix.starts_with(b"/del/")
}

#[cfg(not(target_arch = "wasm32"))]
impl ReadRange {
    fn contains(&self, key: &[u8]) -> bool {
        match self {
            Self::Prefix { prefix, .. } => key.starts_with(prefix),
            Self::Range { start, end } => {
                let after_start = start.as_ref().is_none_or(|start| key >= start.as_slice());
                let before_end = end.as_ref().is_none_or(|end| key < end.as_slice());
                after_start && before_end
            }
        }
    }

    fn commutative_set(&self) -> bool {
        matches!(
            self,
            Self::Prefix {
                commutative_set: true,
                ..
            }
        )
    }
}

/// Tracks committed read/write sets for optimistic conflict detection.
///
/// Each committed transaction's read/write set is recorded along with the
/// version at which it was committed. When a write transaction commits, it
/// checks whether transactions committed after its snapshot either wrote keys it
/// read or read keys/ranges it wrote, matching Go's SSI conflict behavior.
#[cfg(not(target_arch = "wasm32"))]
type CommittedTxnRecord = (u64, HashSet<Vec<u8>>, ReadSet);

#[cfg(not(target_arch = "wasm32"))]
type PendingTxnRecord = (HashSet<Vec<u8>>, ReadSet);

/// Cumulative storage conflicts classified by the dependency that aborted the transaction.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct TransactionConflictStats {
    /// This transaction wrote a key written by a newer transaction.
    pub write_write: u64,
    /// This transaction wrote a key read by a newer transaction.
    pub write_read: u64,
    /// This transaction read a key written by a newer transaction.
    pub read_write: u64,
}

/// Current conflict-tracker state retained for active write snapshots.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct ConflictTrackerStats {
    /// Latest published storage version.
    pub current_version: u64,
    /// Committed transactions retained for active snapshots.
    pub committed_transactions: u64,
    /// Conflict-checked writes awaiting physical commit.
    pub pending_reservations: u64,
    /// Write transaction snapshots currently alive.
    pub active_snapshots: u64,
    /// Distinct retained keys indexed as writes.
    pub indexed_write_keys: u64,
    /// Distinct retained keys indexed as point reads.
    pub indexed_read_keys: u64,
    /// Retained iterator read ranges.
    pub indexed_read_ranges: u64,
}

/// Backend-neutral, process-lifetime transaction diagnostics for one store.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct TransactionStatsSnapshot {
    /// Storage backend that owns these diagnostics.
    pub backend: &'static str,
    /// Cumulative transaction aborts for the store lifetime.
    pub conflicts: TransactionConflictStats,
    /// Number of successful commits that acquired the publication gate.
    pub commit_gate_waits: u64,
    /// Cumulative publication-gate wait time in microseconds.
    pub commit_gate_wait_micros: u64,
    /// Longest publication-gate wait in microseconds.
    pub commit_gate_wait_max_micros: u64,
    /// Current retained conflict-tracker state.
    pub tracker: ConflictTrackerStats,
}

/// Cloneable handle for capturing transaction diagnostics from a live store.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub struct TransactionStatsHandle {
    tracker: Arc<ConflictTracker>,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Copy)]
enum ConflictRule {
    WriteWrite,
    WriteRead,
    ReadWrite,
}

#[cfg(not(target_arch = "wasm32"))]
impl ConflictRule {
    const fn as_str(self) -> &'static str {
        match self {
            Self::WriteWrite => "write_write",
            Self::WriteRead => "write_read",
            Self::ReadWrite => "read_write",
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Default)]
struct TransactionMetrics {
    write_write_conflicts: AtomicU64,
    write_read_conflicts: AtomicU64,
    read_write_conflicts: AtomicU64,
    commit_gate_waits: AtomicU64,
    commit_gate_wait_micros: AtomicU64,
    commit_gate_wait_max_micros: AtomicU64,
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct ConflictTracker {
    backend: &'static str,
    version: AtomicU64,
    state: Mutex<ConflictTrackerState>,
    metrics: TransactionMetrics,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Default)]
struct ConflictTrackerState {
    committed: Vec<CommittedTxnRecord>,
    write_versions: BTreeMap<Vec<u8>, Vec<u64>>,
    read_key_versions: HashMap<Vec<u8>, Vec<u64>>,
    read_ranges: Vec<(u64, ReadRange)>,
    pending: BTreeMap<u64, PendingTxnRecord>,
    next_reservation_id: u64,
    active_snapshots: BTreeMap<u64, usize>,
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct ConflictSnapshot {
    tracker: Arc<ConflictTracker>,
    version: u64,
}

/// A conflict-checked write set waiting for its physical backend commit.
///
/// Pending reservations participate in conflict checks but do not advance the
/// snapshot version. Publishing after the physical write preserves the rule
/// that every published version is visible to subsequently paired snapshots.
/// Dropping a reservation cancels it.
#[cfg(not(target_arch = "wasm32"))]
#[must_use = "publish the reservation after the physical write succeeds"]
pub(crate) struct ConflictReservation {
    tracker: Arc<ConflictTracker>,
    id: Option<u64>,
}

#[cfg(not(target_arch = "wasm32"))]
impl ConflictSnapshot {
    pub(crate) fn version(&self) -> u64 {
        self.version
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for ConflictSnapshot {
    fn drop(&mut self) {
        self.tracker.release_snapshot(self.version);
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl ConflictTrackerState {
    #[cfg(test)]
    fn committed_after(&self, version: u64) -> &[CommittedTxnRecord] {
        let first = self
            .committed
            .partition_point(|(commit_version, _, _)| *commit_version <= version);
        &self.committed[first..]
    }

    fn prune(&mut self, current_version: u64) {
        let oldest_active = self
            .active_snapshots
            .first_key_value()
            .map_or(current_version, |(version, _)| *version);
        let drain_count = self
            .committed
            .partition_point(|(version, _, _)| *version <= oldest_active);
        for (version, writes, reads) in self.committed.drain(..drain_count) {
            for key in writes {
                let remove_key = self.write_versions.get_mut(&key).is_some_and(|versions| {
                    if let Ok(index) = versions.binary_search(&version) {
                        versions.remove(index);
                    }
                    versions.is_empty()
                });
                if remove_key {
                    self.write_versions.remove(&key);
                }
            }
            for key in reads.keys {
                let remove_key = self
                    .read_key_versions
                    .get_mut(&key)
                    .is_some_and(|versions| {
                        if let Ok(index) = versions.binary_search(&version) {
                            versions.remove(index);
                        }
                        versions.is_empty()
                    });
                if remove_key {
                    self.read_key_versions.remove(&key);
                }
            }
        }

        let range_drain_count = self
            .read_ranges
            .partition_point(|(version, _)| *version <= oldest_active);
        self.read_ranges.drain(..range_drain_count);
    }

    fn record(&mut self, version: u64, writes: HashSet<Vec<u8>>, reads: ReadSet) {
        for key in &writes {
            self.write_versions
                .entry(key.clone())
                .or_default()
                .push(version);
        }
        for key in &reads.keys {
            self.read_key_versions
                .entry(key.clone())
                .or_default()
                .push(version);
        }
        self.read_ranges
            .extend(reads.ranges.iter().cloned().map(|range| (version, range)));
        self.committed.push((version, writes, reads));
    }

    fn committed_record(&self, version: u64) -> Option<&CommittedTxnRecord> {
        self.committed
            .binary_search_by_key(&version, |(version, _, _)| *version)
            .ok()
            .map(|index| &self.committed[index])
    }

    fn conflicts_committed(
        &self,
        read_version: u64,
        write_keys: &[&Vec<u8>],
        read_set: &ReadSet,
    ) -> Option<ConflictRule> {
        for write_key in write_keys {
            if !is_content_addressed_block_key(write_key) {
                if let Some(versions) = self.write_versions.get(*write_key) {
                    let first = versions.partition_point(|version| *version <= read_version);
                    for version in &versions[first..] {
                        let Some((_, _, committed_reads)) = self.committed_record(*version) else {
                            continue;
                        };
                        let commutative_overlap = read_set.has_commutative_range(write_key)
                            && committed_reads.has_commutative_range(write_key);
                        if !commutative_overlap {
                            return Some(ConflictRule::WriteWrite);
                        }
                    }
                }
            }

            if self
                .read_key_versions
                .get(*write_key)
                .and_then(|versions| versions.last())
                .is_some_and(|version| *version > read_version)
            {
                return Some(ConflictRule::WriteRead);
            }

            let first = self
                .read_ranges
                .partition_point(|(version, _)| *version <= read_version);
            if self.read_ranges[first..].iter().any(|(_, range)| {
                range.contains(write_key)
                    && (!range.commutative_set() || !read_set.has_commutative_range(write_key))
            }) {
                return Some(ConflictRule::WriteRead);
            }
        }

        for read_key in &read_set.keys {
            if self
                .write_versions
                .get(read_key)
                .and_then(|versions| versions.last())
                .is_some_and(|version| *version > read_version)
            {
                return Some(ConflictRule::ReadWrite);
            }
        }

        if read_set
            .ranges
            .iter()
            .any(|range| self.write_range_conflicts(read_version, range))
        {
            Some(ConflictRule::ReadWrite)
        } else {
            None
        }
    }

    fn write_range_conflicts(&self, read_version: u64, range: &ReadRange) -> bool {
        if let ReadRange::Range {
            start: Some(start),
            end: Some(end),
        } = range
        {
            if start >= end {
                return false;
            }
        }

        let (start, end) = match range {
            ReadRange::Prefix { prefix, .. } => (
                Bound::Included(prefix.clone()),
                prefix_end(prefix).map_or(Bound::Unbounded, Bound::Excluded),
            ),
            ReadRange::Range { start, end } => (
                start.clone().map_or(Bound::Unbounded, Bound::Included),
                end.clone().map_or(Bound::Unbounded, Bound::Excluded),
            ),
        };

        self.write_versions
            .range((start, end))
            .any(|(key, versions)| {
                let first = versions.partition_point(|version| *version <= read_version);
                versions[first..].iter().any(|version| {
                    let Some((_, _, committed_reads)) = self.committed_record(*version) else {
                        return false;
                    };
                    !range.commutative_set() || !committed_reads.has_commutative_range(key)
                })
            })
    }

    fn conflicts_pending(
        &self,
        write_keys: &[&Vec<u8>],
        read_set: &ReadSet,
    ) -> Option<ConflictRule> {
        self.pending
            .values()
            .find_map(|(writes, reads)| transaction_conflict(write_keys, read_set, writes, reads))
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.last_mut() {
        if *last != u8::MAX {
            *last += 1;
            return Some(end);
        }
        end.pop();
    }
    None
}

#[cfg(not(target_arch = "wasm32"))]
fn transaction_conflict(
    write_keys: &[&Vec<u8>],
    read_set: &ReadSet,
    other_writes: &HashSet<Vec<u8>>,
    other_reads: &ReadSet,
) -> Option<ConflictRule> {
    for write_key in write_keys {
        let commutative_overlap = read_set.has_commutative_range(write_key)
            && other_reads.has_commutative_range(write_key);
        if other_writes.contains(*write_key)
            && !is_content_addressed_block_key(write_key)
            && !commutative_overlap
        {
            return Some(ConflictRule::WriteWrite);
        }
        if other_reads.conflicts_key(write_key, read_set) {
            return Some(ConflictRule::WriteRead);
        }
    }

    if other_writes
        .iter()
        .any(|other_write| read_set.conflicts_key(other_write, other_reads))
    {
        Some(ConflictRule::ReadWrite)
    } else {
        None
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl ConflictTracker {
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self::for_backend("test")
    }

    pub(crate) fn for_backend(backend: &'static str) -> Self {
        Self {
            backend,
            version: AtomicU64::new(0),
            state: Mutex::new(ConflictTrackerState::default()),
            metrics: TransactionMetrics::default(),
        }
    }

    pub(crate) fn stats_handle(self: &Arc<Self>) -> TransactionStatsHandle {
        TransactionStatsHandle {
            tracker: Arc::clone(self),
        }
    }

    /// Get the current version for a new transaction's snapshot.
    pub(crate) fn current_version(&self) -> u64 {
        self.version.load(Ordering::SeqCst)
    }

    /// Register a write transaction snapshot until its transaction is finalized.
    pub(crate) fn begin_snapshot(self: &Arc<Self>) -> ConflictSnapshot {
        let (version, sizes) = {
            let mut state = self.state.lock();
            let version = self.current_version();
            *state.active_snapshots.entry(version).or_default() += 1;
            (version, tracker_sizes(&state))
        };
        self.record_tracker_sizes(sizes);
        ConflictSnapshot {
            tracker: Arc::clone(self),
            version,
        }
    }

    /// Reserve a conflict-checked write set before its physical backend write.
    pub(crate) fn reserve<'a>(
        self: &Arc<Self>,
        read_version: u64,
        write_keys: impl Iterator<Item = &'a Vec<u8>>,
        read_set: &ReadSet,
    ) -> std::result::Result<ConflictReservation, crate::corekv::Error> {
        let write_keys: Vec<&Vec<u8>> = write_keys.collect();
        if write_keys.is_empty() {
            return Ok(ConflictReservation {
                tracker: Arc::clone(self),
                id: None,
            });
        }

        let mut state = self.state.lock();
        if let Some(rule) = state
            .conflicts_committed(read_version, &write_keys, read_set)
            .or_else(|| state.conflicts_pending(&write_keys, read_set))
        {
            drop(state);
            self.record_conflict(rule);
            return Err(crate::corekv::Error::TxnConflict);
        }

        state.next_reservation_id = state
            .next_reservation_id
            .checked_add(1)
            .expect("conflict reservation ID overflow");
        let id = state.next_reservation_id;
        state.pending.insert(
            id,
            (write_keys.into_iter().cloned().collect(), read_set.clone()),
        );
        let sizes = tracker_sizes(&state);
        drop(state);
        self.record_tracker_sizes(sizes);
        Ok(ConflictReservation {
            tracker: Arc::clone(self),
            id: Some(id),
        })
    }

    fn release_snapshot(&self, version: u64) {
        let mut state = self.state.lock();
        let count = state
            .active_snapshots
            .get_mut(&version)
            .expect("registered conflict snapshot");
        *count -= 1;
        if *count == 0 {
            state.active_snapshots.remove(&version);
        }
        state.prune(self.current_version());
        let sizes = tracker_sizes(&state);
        drop(state);
        self.record_tracker_sizes(sizes);
    }

    pub(crate) fn record_commit_gate_wait(&self, elapsed: Duration) {
        let micros = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
        self.metrics
            .commit_gate_waits
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .commit_gate_wait_micros
            .fetch_add(micros, Ordering::Relaxed);
        self.metrics
            .commit_gate_wait_max_micros
            .fetch_max(micros, Ordering::Relaxed);
        telemetry::record_commit_gate_wait(self.backend, elapsed.as_secs_f64());
    }

    fn record_conflict(&self, rule: ConflictRule) {
        let counter = match rule {
            ConflictRule::WriteWrite => &self.metrics.write_write_conflicts,
            ConflictRule::WriteRead => &self.metrics.write_read_conflicts,
            ConflictRule::ReadWrite => &self.metrics.read_write_conflicts,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        telemetry::record_storage_conflict(self.backend, rule.as_str());
    }

    fn record_tracker_sizes(&self, sizes: (u64, u64, u64)) {
        telemetry::record_conflict_tracker_size(self.backend, sizes.0, sizes.1, sizes.2);
    }

    fn stats(&self) -> TransactionStatsSnapshot {
        let state = self.state.lock();
        TransactionStatsSnapshot {
            backend: self.backend,
            conflicts: TransactionConflictStats {
                write_write: self.metrics.write_write_conflicts.load(Ordering::Relaxed),
                write_read: self.metrics.write_read_conflicts.load(Ordering::Relaxed),
                read_write: self.metrics.read_write_conflicts.load(Ordering::Relaxed),
            },
            commit_gate_waits: self.metrics.commit_gate_waits.load(Ordering::Relaxed),
            commit_gate_wait_micros: self.metrics.commit_gate_wait_micros.load(Ordering::Relaxed),
            commit_gate_wait_max_micros: self
                .metrics
                .commit_gate_wait_max_micros
                .load(Ordering::Relaxed),
            tracker: ConflictTrackerStats {
                current_version: self.current_version(),
                committed_transactions: state.committed.len() as u64,
                pending_reservations: state.pending.len() as u64,
                active_snapshots: state.active_snapshots.values().sum::<usize>() as u64,
                indexed_write_keys: state.write_versions.len() as u64,
                indexed_read_keys: state.read_key_versions.len() as u64,
                indexed_read_ranges: state.read_ranges.len() as u64,
            },
        }
    }

    /// Reserve and immediately publish a record for conflict-tracker tests.
    #[cfg(test)]
    pub(crate) fn check_and_record<'a>(
        self: &Arc<Self>,
        read_version: u64,
        write_keys: impl Iterator<Item = &'a Vec<u8>>,
        read_set: &ReadSet,
    ) -> std::result::Result<u64, crate::corekv::Error> {
        Ok(self.reserve(read_version, write_keys, read_set)?.publish())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl TransactionStatsHandle {
    pub fn snapshot(&self) -> TransactionStatsSnapshot {
        self.tracker.stats()
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn tracker_sizes(state: &ConflictTrackerState) -> (u64, u64, u64) {
    (
        state.committed.len() as u64,
        state.pending.len() as u64,
        state.active_snapshots.values().sum::<usize>() as u64,
    )
}

#[cfg(not(target_arch = "wasm32"))]
impl ConflictReservation {
    /// Publish the reservation after the physical write succeeds.
    pub(crate) fn publish(mut self) -> u64 {
        let Some(id) = self.id.take() else {
            return 0;
        };
        let mut state = self.tracker.state.lock();
        let (writes, reads) = state
            .pending
            .remove(&id)
            .expect("active conflict reservation");
        let version = self.tracker.version.fetch_add(1, Ordering::SeqCst) + 1;
        state.record(version, writes, reads);
        state.prune(version);
        let sizes = tracker_sizes(&state);
        drop(state);
        self.tracker.record_tracker_sizes(sizes);
        version
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for ConflictReservation {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            let mut state = self.tracker.state.lock();
            state.pending.remove(&id);
            let sizes = tracker_sizes(&state);
            drop(state);
            self.tracker.record_tracker_sizes(sizes);
        }
    }
}

/// Await spawned commit callbacks, re-raising a panic from one of them in the
/// caller.
///
/// Callbacks run in a spawned task so they survive a cancelled commit future,
/// which also isolates their panics. `commit()` is contractually required to
/// propagate a panicking callback (see `test_suite::callbacks`), so the panic
/// is resumed here. A caller that is already gone never reaches this, and the
/// spawned task's panic is reported by the runtime instead.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn join_commit_callbacks(handle: tokio::task::JoinHandle<()>) {
    if let Err(join_error) = handle.await {
        if join_error.is_panic() {
            std::panic::resume_unwind(join_error.into_panic());
        }
    }
}

/// A transaction's callback sets, drained before its commit is handed to the
/// blocking task that performs the write.
///
/// The write runs on a `spawn_blocking` thread, which keeps running after the
/// caller's commit future is dropped. Selecting and starting the callbacks
/// from the async side after that await would skip them for a write that
/// still lands, silently losing the subscription and P2P Update events
/// registered through `on_success_async` (#1185).
#[cfg(all(
    not(target_arch = "wasm32"),
    any(
        feature = "redb",
        feature = "fjall",
        feature = "rocksdb",
        feature = "lark"
    )
))]
pub(crate) struct CommitCallbacks {
    success: Vec<TxnCallback>,
    success_async: Vec<AsyncTxnCallback>,
    error: Vec<TxnCallback>,
    error_async: Vec<AsyncTxnCallback>,
    handle: tokio::runtime::Handle,
}

#[cfg(all(
    not(target_arch = "wasm32"),
    any(
        feature = "redb",
        feature = "fjall",
        feature = "rocksdb",
        feature = "lark"
    )
))]
impl CommitCallbacks {
    /// Drain every set. Called from the async side, which is where the
    /// runtime handle comes from.
    pub(crate) fn drain(callbacks: &CallbackManager) -> Self {
        Self {
            success: callbacks.take_success(),
            success_async: callbacks.take_success_async(),
            error: callbacks.take_error(),
            error_async: callbacks.take_error_async(),
            handle: tokio::runtime::Handle::current(),
        }
    }

    /// Start the success or error set on the runtime, returning its handle.
    ///
    /// Called from the blocking commit task once the write outcome is known.
    /// The work is spawned rather than run inline on that thread because
    /// these callbacks do real async work — P2P broadcasts, `tokio::fs`
    /// reads, arbitrary post-commit hooks — and `tokio::fs` is itself backed
    /// by the blocking pool, so occupying a pool thread while waiting on them
    /// risks exhausting it. Spawning also detaches them from the caller,
    /// which may already be gone.
    ///
    /// Callers await the returned handle on the normal path, preserving the
    /// existing guarantee that callbacks finish before `commit()` returns.
    #[must_use]
    pub(crate) fn spawn(self, succeeded: bool) -> tokio::task::JoinHandle<()> {
        let Self {
            success,
            success_async,
            error,
            error_async,
            handle,
        } = self;
        handle.spawn(async move {
            if succeeded {
                CallbackManager::execute_callbacks(success);
                CallbackManager::execute_async_callbacks(success_async).await;
            } else {
                CallbackManager::execute_callbacks(error);
                CallbackManager::execute_async_callbacks(error_async).await;
            }
        })
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "shared_tests.rs"]
mod tests;
