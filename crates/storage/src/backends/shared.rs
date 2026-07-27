use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
#[cfg(not(target_arch = "wasm32"))]
use std::collections::BTreeMap;
#[cfg(not(target_arch = "wasm32"))]
use std::collections::HashSet;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

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
    Prefix(Vec<u8>),
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
    /// transaction that committed after `read_version`; otherwise returns the
    /// recorded commit version (0 when the write set is empty and nothing was
    /// recorded). If the physical write backing this record subsequently
    /// fails, the caller must `unrecord` the returned version while still
    /// holding the store's commit gate.
    pub(crate) fn check_and_record<'a>(
        &self,
        read_version: u64,
        write_keys: impl Iterator<Item = &'a Vec<u8>>,
        read_set: &ReadSet,
    ) -> std::result::Result<u64, crate::corekv::Error> {
        let write_keys: Vec<&Vec<u8>> = write_keys.collect();
        if write_keys.is_empty() {
            return Ok(0);
        }

        let mut state = self.state.lock();

        // Check for conflicts against transactions committed after our snapshot.
        for (_, committed_writes, committed_reads) in state.committed_after(read_version) {
            for write_key in &write_keys {
                if (committed_writes.contains(*write_key)
                    && !is_content_addressed_block_key(write_key))
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

        // No conflict - clone keys into storage (single clone per key)
        let new_version = self.version.fetch_add(1, Ordering::SeqCst) + 1;
        let write_set = write_keys.into_iter().cloned().collect();
        state
            .committed
            .push((new_version, write_set, read_set.clone()));
        state.prune(new_version);

        Ok(new_version)
    }

    /// Remove the record for `version` after its physical write failed.
    ///
    /// Without this, the recorded write-set describes data that never landed
    /// and later writers get phantom `TxnConflict` errors against it. Must be
    /// called while still holding the commit gate that covered the failed
    /// `check_and_record`, so no other committer can observe the phantom
    /// record. A version already pruned (or 0) is a no-op; the version
    /// counter keeps its gap, which `committed_after` tolerates.
    /// Prefer [`RecordGuard`], which also covers unwinds.
    ///
    /// Only compiled for backends with fallible physical writes; the
    /// memory backend's writes cannot fail after the conflict check.
    #[cfg(any(
        test,
        feature = "redb",
        feature = "fjall",
        feature = "rocksdb",
        feature = "lark"
    ))]
    pub(crate) fn unrecord(&self, version: u64) {
        if version == 0 {
            return;
        }
        let mut state = self.state.lock();
        if let Ok(index) = state
            .committed
            .binary_search_by_key(&version, |(v, _, _)| *v)
        {
            state.committed.remove(index);
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

/// Rolls back a `check_and_record` entry unless defused.
///
/// Armed right after a successful `check_and_record` and defused only once
/// the physical write has succeeded, it converts BOTH error returns and
/// panics during the write into an `unrecord`, so no phantom write-set can
/// survive a failed commit. Hold it (and drop/defuse it) while the commit
/// gate is still held.
#[cfg(all(
    not(target_arch = "wasm32"),
    any(
        test,
        feature = "redb",
        feature = "fjall",
        feature = "rocksdb",
        feature = "lark"
    )
))]
pub(crate) struct RecordGuard<'t> {
    tracker: &'t ConflictTracker,
    version: u64,
    defused: bool,
}

#[cfg(all(
    not(target_arch = "wasm32"),
    any(
        test,
        feature = "redb",
        feature = "fjall",
        feature = "rocksdb",
        feature = "lark"
    )
))]
impl<'t> RecordGuard<'t> {
    pub(crate) fn new(tracker: &'t ConflictTracker, version: u64) -> Self {
        Self {
            tracker,
            version,
            defused: false,
        }
    }

    /// The physical write succeeded; keep the record.
    pub(crate) fn defuse(mut self) {
        self.defused = true;
    }
}

#[cfg(all(
    not(target_arch = "wasm32"),
    any(
        test,
        feature = "redb",
        feature = "fjall",
        feature = "rocksdb",
        feature = "lark"
    )
))]
impl Drop for RecordGuard<'_> {
    fn drop(&mut self) {
        if !self.defused {
            self.tracker.unrecord(self.version);
        }
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

    fn cid_bytes(fill: u8) -> Vec<u8> {
        // Valid CIDv1: version 1, dag-cbor codec, sha2-256 multihash.
        let mut bytes = vec![0x01, 0x71, 0x12, 0x20];
        bytes.extend(std::iter::repeat_n(fill, 32));
        assert!(cid::Cid::try_from(bytes.as_slice()).is_ok());
        bytes
    }

    fn block_key(fill: u8) -> Vec<u8> {
        let mut key = vec![b'b'];
        key.extend(cid_bytes(fill));
        key
    }

    #[test]
    fn identical_block_writes_do_not_conflict() {
        let tracker = Arc::new(ConflictTracker::new());
        let first_snapshot = tracker.begin_snapshot();
        let second_snapshot = tracker.begin_snapshot();

        // Two overlapping transactions write the same content-addressed block
        // (same CID => byte-identical value). Both must commit (#1194).
        let writes = [block_key(0xaa)];
        tracker
            .check_and_record(first_snapshot.version(), writes.iter(), &ReadSet::default())
            .unwrap();
        drop(first_snapshot);

        tracker
            .check_and_record(
                second_snapshot.version(),
                writes.iter(),
                &ReadSet::default(),
            )
            .unwrap();
    }

    #[test]
    fn merge_index_writes_still_conflict() {
        let tracker = Arc::new(ConflictTracker::new());
        let first_snapshot = tracker.begin_snapshot();
        let second_snapshot = tracker.begin_snapshot();

        // Blockstore merge-tracking keys ('b' + 'm' + CID) are mutable state,
        // not content-addressed data: write-write stays a conflict even though
        // the payload after the merge prefix is a valid CID.
        let mut merge_key = vec![b'b', b'm'];
        merge_key.extend(cid_bytes(0xaa));
        let writes = [merge_key];
        tracker
            .check_and_record(first_snapshot.version(), writes.iter(), &ReadSet::default())
            .unwrap();
        drop(first_snapshot);

        let err = tracker
            .check_and_record(
                second_snapshot.version(),
                writes.iter(),
                &ReadSet::default(),
            )
            .unwrap_err();
        assert!(matches!(err, crate::corekv::Error::TxnConflict));
    }

    #[test]
    fn non_cid_blockstore_writes_still_conflict() {
        let tracker = Arc::new(ConflictTracker::new());
        let first_snapshot = tracker.begin_snapshot();
        let second_snapshot = tracker.begin_snapshot();

        // A blockstore-namespace key whose payload is not a valid CID is not
        // content-addressed: write-write stays a conflict.
        let mut bogus_key = vec![b'b', 0x01, 0x71];
        bogus_key.extend(std::iter::repeat_n(0xaa, 34));
        assert!(cid::Cid::try_from(&bogus_key[1..]).is_err());
        let writes = [bogus_key];
        tracker
            .check_and_record(first_snapshot.version(), writes.iter(), &ReadSet::default())
            .unwrap();
        drop(first_snapshot);

        let err = tracker
            .check_and_record(
                second_snapshot.version(),
                writes.iter(),
                &ReadSet::default(),
            )
            .unwrap_err();
        assert!(matches!(err, crate::corekv::Error::TxnConflict));
    }

    #[test]
    fn cid_with_trailing_bytes_still_conflicts() {
        let tracker = Arc::new(ConflictTracker::new());
        let first_snapshot = tracker.begin_snapshot();
        let second_snapshot = tracker.begin_snapshot();

        // CID parsing accepts a valid prefix, so require the entire payload to
        // be one CID before treating the key as immutable content-addressed data.
        let mut key = block_key(0xaa);
        key.push(0xff);
        assert!(cid::Cid::try_from(&key[1..]).is_ok());
        let writes = [key];
        tracker
            .check_and_record(first_snapshot.version(), writes.iter(), &ReadSet::default())
            .unwrap();
        drop(first_snapshot);

        let err = tracker
            .check_and_record(
                second_snapshot.version(),
                writes.iter(),
                &ReadSet::default(),
            )
            .unwrap_err();
        assert!(matches!(err, crate::corekv::Error::TxnConflict));
    }

    #[test]
    fn block_write_vs_committed_read_still_conflicts() {
        let tracker = Arc::new(ConflictTracker::new());
        let first_snapshot = tracker.begin_snapshot();
        let second_snapshot = tracker.begin_snapshot();

        // A committed transaction READ the block key; a later write to it is
        // still an anti-dependency and must conflict.
        let block = block_key(0xbb);
        let mut first_reads = ReadSet::default();
        first_reads.record_key(&block);
        let first_writes = [b"d/d/books/other".to_vec()];
        tracker
            .check_and_record(first_snapshot.version(), first_writes.iter(), &first_reads)
            .unwrap();
        drop(first_snapshot);

        let second_writes = [block];
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
    fn unrecord_removes_phantom_record() {
        let tracker = Arc::new(ConflictTracker::new());
        let first_snapshot = tracker.begin_snapshot();
        let second_snapshot = tracker.begin_snapshot();

        let writes = [b"d/i/books/failed-write".to_vec()];
        let version = tracker
            .check_and_record(first_snapshot.version(), writes.iter(), &ReadSet::default())
            .unwrap();
        drop(first_snapshot);

        // Simulate the physical write failing: without unrecord the second
        // transaction would hit a phantom conflict against data that never
        // landed.
        tracker.unrecord(version);

        tracker
            .check_and_record(
                second_snapshot.version(),
                writes.iter(),
                &ReadSet::default(),
            )
            .unwrap();
    }

    #[test]
    fn unrecord_tolerates_pruned_and_empty_versions() {
        let tracker = Arc::new(ConflictTracker::new());

        // Version 0 marks "nothing recorded" (empty write set).
        let empty_version = tracker
            .check_and_record(0, [].iter(), &ReadSet::default())
            .unwrap();
        assert_eq!(empty_version, 0);
        tracker.unrecord(empty_version);

        // A record pruned before unrecord (no active snapshots pin it) is
        // silently gone; unrecord must not panic or disturb later commits.
        let snapshot = tracker.begin_snapshot();
        let writes = [b"d/i/books/pruned".to_vec()];
        let version = tracker
            .check_and_record(snapshot.version(), writes.iter(), &ReadSet::default())
            .unwrap();
        drop(snapshot);
        tracker.unrecord(version);
        tracker.unrecord(version);

        let survivor = tracker.begin_snapshot();
        tracker
            .check_and_record(survivor.version(), writes.iter(), &ReadSet::default())
            .unwrap();
    }

    #[test]
    fn record_guard_unrecords_on_drop() {
        let tracker = Arc::new(ConflictTracker::new());
        let loser_snapshot = tracker.begin_snapshot();
        let writes = [b"d/i/books/guarded".to_vec()];

        let version = tracker
            .check_and_record(
                tracker.current_version(),
                writes.iter(),
                &ReadSet::default(),
            )
            .unwrap();
        drop(RecordGuard::new(&tracker, version));

        // The dropped (armed) guard removed the record: no phantom conflict.
        tracker
            .check_and_record(loser_snapshot.version(), writes.iter(), &ReadSet::default())
            .unwrap();
    }

    #[test]
    fn record_guard_defuse_keeps_record() {
        let tracker = Arc::new(ConflictTracker::new());
        let loser_snapshot = tracker.begin_snapshot();
        let writes = [b"d/i/books/kept".to_vec()];

        let version = tracker
            .check_and_record(
                tracker.current_version(),
                writes.iter(),
                &ReadSet::default(),
            )
            .unwrap();
        RecordGuard::new(&tracker, version).defuse();

        let err = tracker
            .check_and_record(loser_snapshot.version(), writes.iter(), &ReadSet::default())
            .unwrap_err();
        assert!(matches!(err, crate::corekv::Error::TxnConflict));
    }

    #[test]
    fn record_guard_unrecords_on_panic() {
        let tracker = Arc::new(ConflictTracker::new());
        let loser_snapshot = tracker.begin_snapshot();
        let writes = [b"d/i/books/panicked".to_vec()];

        let version = tracker
            .check_and_record(
                tracker.current_version(),
                writes.iter(),
                &ReadSet::default(),
            )
            .unwrap();
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = RecordGuard::new(&tracker, version);
            panic!("physical write panicked");
        }));
        assert!(unwound.is_err());

        tracker
            .check_and_record(loser_snapshot.version(), writes.iter(), &ReadSet::default())
            .unwrap();
    }

    #[test]
    fn skips_retained_prefix_for_recent_snapshots() {
        const RETAINED_PREFIX: usize = 64;

        let tracker = Arc::new(ConflictTracker::new());
        let old_snapshot = tracker.begin_snapshot();
        let no_reads = ReadSet::default();

        for index in 0..RETAINED_PREFIX {
            let snapshot = tracker.begin_snapshot();
            let writes = [format!("history/{index}").into_bytes()];
            tracker
                .check_and_record(snapshot.version(), writes.iter(), &no_reads)
                .unwrap();
        }

        let recent_snapshot = tracker.begin_snapshot();
        let suffix_snapshot = tracker.begin_snapshot();
        let suffix_writes = [b"suffix/conflict".to_vec()];
        tracker
            .check_and_record(suffix_snapshot.version(), suffix_writes.iter(), &no_reads)
            .unwrap();
        drop(suffix_snapshot);

        {
            let state = tracker.state.lock();
            assert_eq!(state.committed.len(), RETAINED_PREFIX + 1);
            assert_eq!(state.committed_after(recent_snapshot.version()).len(), 1);
        }

        let old_writes = [b"history/0".to_vec()];
        let old_err = tracker
            .check_and_record(old_snapshot.version(), old_writes.iter(), &no_reads)
            .unwrap_err();
        assert!(matches!(old_err, crate::corekv::Error::TxnConflict));

        let recent_err = tracker
            .check_and_record(recent_snapshot.version(), suffix_writes.iter(), &no_reads)
            .unwrap_err();
        assert!(matches!(recent_err, crate::corekv::Error::TxnConflict));
    }
}
