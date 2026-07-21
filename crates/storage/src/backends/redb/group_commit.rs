use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::Mutex;
use redb::Database;
use tokio::sync::{mpsc, oneshot, RwLock as AsyncRwLock};

use super::config::DurabilityMode;
use super::KV_TABLE;
use crate::backends::shared::{CallbackManager, ConflictSnapshot, ConflictTracker, ReadSet};
use crate::corekv::{AsyncTxnCallback, Error, Result, TxnCallback};

/// Payload for a single transaction's pending commit.
pub(crate) struct PendingCommit {
    pub changes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    pub read_version: u64,
    pub read_set: ReadSet,
    pub _conflict_snapshot: ConflictSnapshot,
    pub result_tx: oneshot::Sender<Result<()>>,
    pub on_success: Vec<TxnCallback>,
    pub on_success_async: Vec<AsyncTxnCallback>,
    pub on_error: Vec<TxnCallback>,
    pub on_error_async: Vec<AsyncTxnCallback>,
}

/// Coalesces multiple transaction commits into single redb write transactions.
///
/// Instead of each transaction acquiring the exclusive redb write lock individually,
/// pending commits are queued and flushed together. This amortizes the B-tree + COW
/// overhead across multiple mutations, dramatically improving throughput for
/// write-heavy workloads (e.g., 852 document creates per Ethereum block).
///
/// Conflict detection is performed inside the flush loop to ensure atomicity
/// between version tracking and data writes.
pub(crate) struct GroupCommitBuffer {
    sender: Mutex<Option<mpsc::UnboundedSender<PendingCommit>>>,
    flush_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl GroupCommitBuffer {
    pub fn new(
        db: Arc<Database>,
        durability: DurabilityMode,
        conflict_tracker: Arc<ConflictTracker>,
        commit_gate: Arc<AsyncRwLock<()>>,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let flush_handle = tokio::spawn(flush_loop(
            rx,
            db,
            durability,
            conflict_tracker,
            commit_gate,
        ));
        Self {
            sender: Mutex::new(Some(tx)),
            flush_handle: Mutex::new(Some(flush_handle)),
        }
    }

    #[allow(clippy::result_large_err)]
    pub fn enqueue(&self, commit: PendingCommit) -> std::result::Result<(), PendingCommit> {
        match self.sender.lock().as_ref() {
            Some(sender) => sender.send(commit).map_err(|e| e.0),
            None => Err(commit),
        }
    }

    /// Shut down the flush loop and wait for it to release the database handle.
    pub async fn shutdown(&self) {
        // Drop sender to signal flush loop to exit
        self.sender.lock().take();
        // Wait for flush loop to complete (releases Arc<Database>)
        let handle = self.flush_handle.lock().take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
    }
}

impl Drop for GroupCommitBuffer {
    fn drop(&mut self) {
        // Abort the flush loop to release Arc<Database> immediately.
        // For graceful shutdown, use shutdown() via store.close() instead.
        if let Some(handle) = self.flush_handle.lock().take() {
            handle.abort();
        }
    }
}

async fn flush_loop(
    mut rx: mpsc::UnboundedReceiver<PendingCommit>,
    db: Arc<Database>,
    durability: DurabilityMode,
    conflict_tracker: Arc<ConflictTracker>,
    commit_gate: Arc<AsyncRwLock<()>>,
) {
    loop {
        // Block until at least one commit arrives
        let first = match rx.recv().await {
            Some(c) => c,
            None => return,
        };

        // Drain any additional commits that arrived concurrently
        let mut batch = vec![first];
        while let Ok(commit) = rx.try_recv() {
            batch.push(commit);
            if batch.len() >= 500 {
                break;
            }
        }

        let (passed, failed, result) = {
            // Keep version publication and the Redb flush indivisible to new snapshots.
            let _commit_guard = commit_gate.write().await;
            let mut passed = Vec::with_capacity(batch.len());
            let mut failed: Vec<(PendingCommit, Error)> = Vec::new();

            for commit in batch {
                match conflict_tracker.check_and_record(
                    commit.read_version,
                    commit.changes.keys(),
                    &commit.read_set,
                ) {
                    Ok(()) => passed.push(commit),
                    Err(e) => failed.push((commit, e)),
                }
            }

            let result = if passed.is_empty() {
                None
            } else {
                Some(flush_batch(&db, &passed, durability))
            };
            (passed, failed, result)
        };

        for (commit, err) in failed {
            CallbackManager::execute_callbacks(commit.on_error);
            CallbackManager::execute_async_callbacks(commit.on_error_async).await;
            let _ = commit.result_tx.send(Err(err));
        }

        let Some(result) = result else {
            continue;
        };

        let batch_size = passed.len();
        let total_changes: usize = passed.iter().map(|c| c.changes.len()).sum();

        if let Err(ref e) = result {
            tracing::error!(
                batch_size,
                total_changes,
                error = %e,
                "Group commit flush failed"
            );
        } else {
            tracing::debug!(batch_size, total_changes, "Group commit flushed");
        }

        // Notify each committer and execute their callbacks
        for commit in passed {
            match &result {
                Ok(()) => {
                    CallbackManager::execute_callbacks(commit.on_success);
                    CallbackManager::execute_async_callbacks(commit.on_success_async).await;
                }
                Err(_) => {
                    CallbackManager::execute_callbacks(commit.on_error);
                    CallbackManager::execute_async_callbacks(commit.on_error_async).await;
                }
            }
            let _ = commit.result_tx.send(result.clone());
        }
    }
}

fn flush_batch(db: &Database, batch: &[PendingCommit], durability: DurabilityMode) -> Result<()> {
    let mut write_txn = db.begin_write().map_err(Error::from)?;

    write_txn.set_durability(match durability {
        DurabilityMode::Immediate => redb::Durability::Immediate,
        DurabilityMode::Eventual => redb::Durability::Eventual,
    });

    {
        let mut table = write_txn.open_table(KV_TABLE).map_err(Error::from)?;
        for commit in batch {
            for (key, value) in &commit.changes {
                match value {
                    Some(v) => {
                        table
                            .insert(key.as_slice(), v.as_slice())
                            .map_err(Error::from)?;
                    }
                    None => {
                        table.remove(key.as_slice()).map_err(Error::from)?;
                    }
                }
            }
        }
    }

    write_txn.commit().map_err(Error::from)?;
    Ok(())
}
