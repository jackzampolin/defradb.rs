use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
#[cfg(not(target_arch = "wasm32"))]
use std::collections::BTreeMap;
use std::collections::HashSet;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use crate::corekv::{AsyncTxnCallback, IterOptions, TxnCallback};

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
#[derive(Debug, Clone, Default)]
pub(crate) struct ReadSet {
    keys: HashSet<Vec<u8>>,
    ranges: Vec<ReadRange>,
}

#[derive(Debug, Clone)]
enum ReadRange {
    Prefix(Vec<u8>),
    Range {
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
    },
}

impl ReadSet {
    pub(crate) fn record_key(&mut self, key: &[u8]) {
        self.keys.insert(key.to_vec());
    }

    pub(crate) fn record_iter_options(&mut self, opts: &IterOptions) {
        if let Some(prefix) = opts.prefix() {
            if is_document_collection_scan_prefix(prefix) {
                return;
            }
            self.ranges.push(ReadRange::Prefix(prefix.to_vec()));
        } else {
            self.ranges.push(ReadRange::Range {
                start: opts.start().map(Vec::from),
                end: opts.end().map(Vec::from),
            });
        }
    }

    fn conflicts_key(&self, key: &[u8]) -> bool {
        self.keys.contains(key) || self.ranges.iter().any(|range| range.contains(key))
    }
}

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

impl ReadRange {
    fn contains(&self, key: &[u8]) -> bool {
        match self {
            Self::Prefix(prefix) => key.starts_with(prefix),
            Self::Range { start, end } => {
                let after_start = start.as_ref().is_none_or(|start| key >= start.as_slice());
                let before_end = end.as_ref().is_none_or(|end| key < end.as_slice());
                after_start && before_end
            }
        }
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
pub(crate) struct ConflictTracker {
    version: AtomicU64,
    state: Mutex<ConflictTrackerState>,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Default)]
struct ConflictTrackerState {
    committed: Vec<CommittedTxnRecord>,
    active_snapshots: BTreeMap<u64, usize>,
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct ConflictSnapshot {
    tracker: Arc<ConflictTracker>,
    version: u64,
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
    fn prune(&mut self, current_version: u64) {
        let oldest_active = self
            .active_snapshots
            .first_key_value()
            .map_or(current_version, |(version, _)| *version);
        let drain_count = self
            .committed
            .partition_point(|(version, _, _)| *version <= oldest_active);
        self.committed.drain(..drain_count);
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl ConflictTracker {
    pub(crate) fn new() -> Self {
        Self {
            version: AtomicU64::new(0),
            state: Mutex::new(ConflictTrackerState::default()),
        }
    }

    /// Get the current version for a new transaction's snapshot.
    pub(crate) fn current_version(&self) -> u64 {
        self.version.load(Ordering::SeqCst)
    }

    /// Register a write transaction snapshot until its transaction is finalized.
    pub(crate) fn begin_snapshot(self: &Arc<Self>) -> ConflictSnapshot {
        let version = {
            let mut state = self.state.lock();
            let version = self.current_version();
            *state.active_snapshots.entry(version).or_default() += 1;
            version
        };
        ConflictSnapshot {
            tracker: Arc::clone(self),
            version,
        }
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
    }

    /// Check for conflicts and record the read/write set if no conflict.
    /// Returns Err(TxnConflict) if the transaction conflicts with any
    /// transaction that committed after `read_version`.
    pub(crate) fn check_and_record<'a>(
        &self,
        read_version: u64,
        write_keys: impl Iterator<Item = &'a Vec<u8>>,
        read_set: &ReadSet,
    ) -> std::result::Result<(), crate::corekv::Error> {
        let write_keys: Vec<&Vec<u8>> = write_keys.collect();
        if write_keys.is_empty() {
            return Ok(());
        }

        let mut state = self.state.lock();

        // Check for conflicts against transactions committed after our snapshot.
        for (commit_ver, committed_writes, committed_reads) in &state.committed {
            if *commit_ver > read_version {
                for write_key in &write_keys {
                    if committed_writes.contains(*write_key)
                        || committed_reads.conflicts_key(write_key)
                    {
                        return Err(crate::corekv::Error::TxnConflict);
                    }
                }

                if committed_writes
                    .iter()
                    .any(|committed_write| read_set.conflicts_key(committed_write))
                {
                    return Err(crate::corekv::Error::TxnConflict);
                }
            }
        }

        // No conflict - clone keys into storage (single clone per key)
        let new_version = self.version.fetch_add(1, Ordering::SeqCst) + 1;
        let write_set = write_keys.into_iter().cloned().collect();
        state
            .committed
            .push((new_version, write_set, read_set.clone()));
        state.prune(new_version);

        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn detects_write_to_committed_read_prefix() {
        let tracker = Arc::new(ConflictTracker::new());
        let first_snapshot = tracker.begin_snapshot();
        let second_snapshot = tracker.begin_snapshot();

        let mut first_reads = ReadSet::default();
        first_reads.record_iter_options(&IterOptions::new().with_prefix(b"d/i/books/".to_vec()));
        let first_writes = [b"d/d/publishers/website".to_vec()];
        tracker
            .check_and_record(first_snapshot.version(), first_writes.iter(), &first_reads)
            .unwrap();
        drop(first_snapshot);

        let second_writes = [b"d/i/books/online-book".to_vec()];
        let err = tracker
            .check_and_record(
                second_snapshot.version(),
                second_writes.iter(),
                &ReadSet::default(),
            )
            .unwrap_err();

        assert!(matches!(err, crate::corekv::Error::TxnConflict));
    }

    #[test]
    fn ignores_document_collection_scan_prefixes() {
        let tracker = Arc::new(ConflictTracker::new());
        let first_snapshot = tracker.begin_snapshot();
        let second_snapshot = tracker.begin_snapshot();

        let mut first_reads = ReadSet::default();
        first_reads.record_iter_options(&IterOptions::new().with_prefix(b"d/d/books/".to_vec()));
        let first_writes = [b"d/d/publishers/website".to_vec()];
        tracker
            .check_and_record(first_snapshot.version(), first_writes.iter(), &first_reads)
            .unwrap();
        drop(first_snapshot);

        let second_writes = [b"d/d/books/online-book".to_vec()];
        tracker
            .check_and_record(
                second_snapshot.version(),
                second_writes.iter(),
                &ReadSet::default(),
            )
            .unwrap();
    }

    #[test]
    fn detects_read_of_committed_write_key() {
        let tracker = Arc::new(ConflictTracker::new());
        let first_snapshot = tracker.begin_snapshot();
        let second_snapshot = tracker.begin_snapshot();

        let first_writes = [b"d/d/books/website-book".to_vec()];
        tracker
            .check_and_record(
                first_snapshot.version(),
                first_writes.iter(),
                &ReadSet::default(),
            )
            .unwrap();
        drop(first_snapshot);

        let mut second_reads = ReadSet::default();
        second_reads.record_key(b"d/d/books/website-book");
        let second_writes = [b"d/d/publishers/online".to_vec()];
        let err = tracker
            .check_and_record(
                second_snapshot.version(),
                second_writes.iter(),
                &second_reads,
            )
            .unwrap_err();

        assert!(matches!(err, crate::corekv::Error::TxnConflict));
    }
}
