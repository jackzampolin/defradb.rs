use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::corekv::{AsyncTxnCallback, TxnCallback};

/// Controls when data is flushed to disk after a commit.
///
/// Default is `Eventual` (`SyncWrites = false`). Process crashes are safe
/// due to WAL; only OS crashes risk data loss.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum DurabilityMode {
    /// Flush to disk on every commit. Safe against process and OS crashes.
    Immediate,
    /// Rely on the OS to flush eventually (default). Process crash is still
    /// safe due to WAL.
    #[default]
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

/// Tracks committed write sets for optimistic conflict detection.
///
/// Each committed transaction's write set is recorded along with the version
/// at which it was committed. When a new transaction commits, it checks whether
/// any of its written keys were also written by transactions that committed
/// after this transaction's snapshot was taken.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct ConflictTracker {
    /// Monotonically increasing version counter.
    version: std::sync::atomic::AtomicU64,
    /// Write sets from committed transactions: (commit_version, keys_written).
    /// Protected by a mutex since we only access it during commit (not hot path).
    committed_writes: Mutex<Vec<(u64, std::collections::HashSet<Vec<u8>>)>>,
}

#[cfg(not(target_arch = "wasm32"))]
impl ConflictTracker {
    pub(crate) fn new() -> Self {
        use std::sync::atomic::AtomicU64;
        Self {
            version: AtomicU64::new(0),
            committed_writes: Mutex::new(Vec::new()),
        }
    }

    /// Get the current version for a new transaction's snapshot.
    pub(crate) fn current_version(&self) -> u64 {
        use std::sync::atomic::Ordering;
        self.version.load(Ordering::SeqCst)
    }

    /// Check for conflicts and record the write set if no conflict.
    /// Returns Err(TxnConflict) if any key in `write_set` was written by a
    /// transaction that committed after `read_version`.
    pub(crate) fn check_and_record<'a>(
        &self,
        read_version: u64,
        write_keys: impl Iterator<Item = &'a Vec<u8>>,
    ) -> std::result::Result<(), crate::corekv::Error> {
        use std::sync::atomic::Ordering;

        let write_keys: Vec<&Vec<u8>> = write_keys.collect();
        if write_keys.is_empty() {
            return Ok(());
        }

        let mut committed = self.committed_writes.lock();

        // Check for conflicts: any key we wrote was also written by a
        // transaction committed after our snapshot
        for (commit_ver, keys) in committed.iter() {
            if *commit_ver > read_version {
                for key in &write_keys {
                    if keys.contains(*key) {
                        return Err(crate::corekv::Error::TxnConflict);
                    }
                }
            }
        }

        // No conflict - clone keys into storage (single clone per key)
        let new_version = self.version.fetch_add(1, Ordering::SeqCst) + 1;
        let write_set = write_keys.into_iter().cloned().collect();
        committed.push((new_version, write_set));

        // Prune old entries that can no longer conflict (optional GC).
        // Keep entries that are newer than the oldest possible active transaction.
        // For simplicity, keep last 1000 entries.
        if committed.len() > 1000 {
            let drain_count = committed.len() - 1000;
            committed.drain(..drain_count);
        }

        Ok(())
    }
}
