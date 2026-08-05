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
/// Conflict reservations are created inside the flush loop before data writes
/// and published only after a successful batch commit.
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

        // Reserve each accepted write before the flush so conflicts remain
        // deterministic within the batch. The physical flush does not hold
        // the snapshot-pairing gate; only successful version publication does.
        let flush_db = Arc::clone(&db);
        let flush_tracker = Arc::clone(&conflict_tracker);
        let flush_gate = Arc::clone(&commit_gate);
        let flush_result = tokio::task::spawn_blocking(move || {
            let mut passed = Vec::with_capacity(batch.len());
            let mut reservations = Vec::with_capacity(batch.len());
            let mut failed: Vec<(PendingCommit, Error)> = Vec::new();

            for commit in batch {
                match flush_tracker.reserve(
                    commit.read_version,
                    commit.changes.keys(),
                    &commit.read_set,
                ) {
                    Ok(reservation) => {
                        passed.push(commit);
                        reservations.push(reservation);
                    }
                    Err(e) => failed.push((commit, e)),
                }
            }

            let result = if passed.is_empty() {
                None
            } else {
                let result = flush_batch(&flush_db, &passed, durability);
                if result.is_ok() {
                    let wait_started = std::time::Instant::now();
                    let _publication_guard = flush_gate.blocking_write();
                    let gate_wait = wait_started.elapsed();
                    for reservation in reservations {
                        flush_tracker.record_commit_gate_wait(gate_wait);
                        reservation.publish();
                    }
                }
                Some(result)
            };
            (passed, failed, result)
        })
        .await;

        let (passed, failed, result) = match flush_result {
            Ok(output) => output,
            Err(join_err) => {
                // A panicking flush (e.g. storage corruption) would repeat on
                // every batch; shut the loop down so later commits fail fast
                // at enqueue with "group commit channel closed" instead.
                tracing::error!(error = %join_err, "Group commit flush task failed; stopping");
                return;
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn grouped_commits_record_gate_wait_per_transaction() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db = Arc::new(Database::create(temp_dir.path().join("group.redb")).unwrap());
        let tracker = Arc::new(ConflictTracker::for_backend("redb"));
        let stats = tracker.stats_handle();
        let gate = Arc::new(AsyncRwLock::new(()));
        let (sender, receiver) = mpsc::unbounded_channel();
        let mut results = Vec::new();

        for key in [b"first".to_vec(), b"second".to_vec()] {
            let (result_tx, result_rx) = oneshot::channel();
            sender
                .send(PendingCommit {
                    changes: BTreeMap::from([(key, Some(b"value".to_vec()))]),
                    read_version: 0,
                    read_set: ReadSet::default(),
                    _conflict_snapshot: tracker.begin_snapshot(),
                    result_tx,
                    on_success: Vec::new(),
                    on_success_async: Vec::new(),
                    on_error: Vec::new(),
                    on_error_async: Vec::new(),
                })
                .unwrap();
            results.push(result_rx);
        }
        drop(sender);

        flush_loop(receiver, db, DurabilityMode::Eventual, tracker, gate).await;
        for result in results {
            result.await.unwrap().unwrap();
        }
        assert_eq!(stats.snapshot().commit_gate_waits, 2);
    }
}
