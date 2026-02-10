use parking_lot::Mutex;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::corekv::Error;

/// Tracks committed write sets for optimistic conflict detection.
///
/// Each committed transaction's write set is recorded along with the version
/// at which it was committed. When a new transaction commits, it checks whether
/// any of its written keys were also written by transactions that committed
/// after this transaction's snapshot was taken.
pub(crate) struct ConflictTracker {
    /// Monotonically increasing version counter.
    version: AtomicU64,
    /// Write sets from committed transactions: (commit_version, keys_written).
    /// Protected by a mutex since we only access it during commit (not hot path).
    committed_writes: Mutex<Vec<(u64, HashSet<Vec<u8>>)>>,
}

impl ConflictTracker {
    pub(crate) fn new() -> Self {
        Self {
            version: AtomicU64::new(0),
            committed_writes: Mutex::new(Vec::new()),
        }
    }

    /// Get the current version for a new transaction's snapshot.
    pub(crate) fn current_version(&self) -> u64 {
        self.version.load(Ordering::SeqCst)
    }

    /// Check for conflicts and record the write set if no conflict.
    /// Returns Err(TxnConflict) if any key in `write_set` was written by a
    /// transaction that committed after `read_version`.
    pub(crate) fn check_and_record(
        &self,
        read_version: u64,
        write_set: HashSet<Vec<u8>>,
    ) -> std::result::Result<(), Error> {
        if write_set.is_empty() {
            return Ok(());
        }

        let mut committed = self.committed_writes.lock();

        // Check for conflicts: any key we wrote was also written by a
        // transaction committed after our snapshot
        for (commit_ver, keys) in committed.iter() {
            if *commit_ver > read_version {
                for key in &write_set {
                    if keys.contains(key) {
                        return Err(Error::TxnConflict);
                    }
                }
            }
        }

        // No conflict - record our write set
        let new_version = self.version.fetch_add(1, Ordering::SeqCst) + 1;
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
