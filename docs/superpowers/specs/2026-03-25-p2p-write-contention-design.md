# P2P Write Contention Fix

## Problem

Simultaneous document writes on two P2P-connected nodes deadlock. The pattern:

1. Node A creates a document (mutation in progress)
2. P2P replicates to Node B
3. Node B creates a response document
4. P2P replicates response back to Node A
5. Node A's merge handler and broadcast path compete for resources
6. Mutation times out

**Root causes:**

1. `BroadcastMutator` awaits P2P broadcast inline — the HTTP request stays open during network I/O, creating a window where incoming merges and outgoing broadcasts compete for the same tokio runtime resources.
2. redb's `begin_write()` is a synchronous blocking call inside async `commit()`, which starves tokio worker threads when multiple concurrent commits compete.
3. No per-document merge serialization — concurrent merges for the same document race at the storage level.
4. No conflict retry — a merge that hits a write-write conflict fails permanently instead of retrying.

**Reproduction:** `simultaneous_writes_on_connected_nodes` test in the amygdala repo (`crates/e2e-tests/tests/p2p_write_contention.rs`). Sequential writes pass; simultaneous writes fail with 30s timeout.

## Solution

Four changes matching Go DefraDB's proven architecture:

### 1. Decouple Broadcast from Mutation Path

**Files:** `crates/db/src/broadcast_mutator/mod.rs`, `crates/query/src/mutator.rs`

`BroadcastMutator::create/update/delete` currently awaits P2P operations inline after committing the local transaction. The mutation response is blocked during `push_dag_to_replicators` and `broadcast_with_retry`.

Change: after `self.inner.create().await` commits the transaction, spawn the entire broadcast sequence as a detached `tokio::spawn` task. Return immediately with `BroadcastStatus::Pending`.

The spawned task receives everything by value: `Arc<SyncCoordinator>`, `BlockResult`, `collection_id`, `creator_did`. No references back to the mutation handler. The `broadcast_with_retry` retry logic is unchanged — it runs inside the spawned task.

Add a `Pending` variant to `BroadcastStatus`:

```rust
pub enum BroadcastStatus {
    Success,
    Failed(String),
    Pending,       // broadcast spawned, not yet complete
    #[default]
    NotAttempted,
}
```

All four mutation methods (`create`, `create_many`, `update`, `delete`) get the same treatment. The batch mutator (`broadcast_mutator/batch.rs`) also needs the same decoupling: its `MutationBatchController::commit()` currently awaits `broadcast_pending` sequentially in a loop after the inner commit succeeds. Change this to spawn the entire pending broadcast loop as a single `tokio::spawn` task.

### 2. Per-Document Merge Queue

**Files:** `crates/db/src/merge_handler/queue.rs` (new), `crates/db/src/merge_handler/mod.rs`

A `MergeQueue` that serializes merges per-document (or per-collection for branchable collections), matching Go's `docMergeQueue`/`colMergeQueue`.

```rust
pub struct MergeQueue {
    locks: parking_lot::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl MergeQueue {
    pub async fn acquire(&self, key: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let mutex = {
            let mut map = self.locks.lock();
            // Prune idle entries if map exceeds threshold
            if map.len() > 10_000 {
                map.retain(|_, v| Arc::strong_count(v) > 1);
            }
            map.entry(key.to_string())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        mutex.lock_owned().await
    }
}
```

- Key is `doc_id` for normal collections, `collection_id` for branchable.
- Different keys run in parallel. Same key serializes.
- The outer `parking_lot::Mutex<HashMap>` is held only for lookup/insert (microseconds).
- Idle entries pruned on acquire when map exceeds 10k entries (strong count == 1 means no waiters).

`DbMergeHandler` gets a `merge_queue: Arc<MergeQueue>` field. In `merge_blocks_individually`, each block acquires the queue before processing:

```rust
let queue_key = if is_branchable {
    block.collection_id.clone()
} else {
    block.doc_id.clone()
};
let _guard = self.merge_queue.acquire(&queue_key).await;
// ... process block, commit txn ...
// guard drops, next merge for this key proceeds
```

The merge queue applies only to `merge_blocks_individually`, not to `try_batch_merge`. The batch path already has split-retry logic: if a batch transaction conflicts, it splits in half and recurses, eventually falling back to individual merges where the queue kicks in. Adding queue acquisition to the batch path would risk deadlocks (two batches acquiring overlapping doc sets in different order) for no benefit.

### 3. Conflict Retry Loop

**Files:** `crates/db/src/merge_handler/batch.rs`, `crates/storage/src/corekv.rs`

Wrap the individual merge-and-commit sequence in a retry loop:

```rust
const MAX_MERGE_RETRIES: usize = 5;

for attempt in 0..MAX_MERGE_RETRIES {
    let txn = self.db.new_txn(false).await?;
    // ... process block in txn ...
    match txn.force_commit().await {
        Ok(()) => break,
        Err(e) if e.is_txn_conflict() => {
            tracing::debug!(attempt, doc_id, "Merge conflict, retrying");
            continue;
        }
        Err(e) => return Err(e.into()),
    }
}
```

- `MAX_MERGE_RETRIES = 5` (matches Go).
- Only retries on transaction conflict errors. All other errors propagate immediately.
- Each retry creates a fresh transaction with a new snapshot.
- The per-document merge queue means conflicts only happen between a local mutation and an incoming P2P merge for the same document — rare. The retry is a safety net.

Batch merge interaction: `try_batch_merge` commits all blocks in one transaction. If that conflicts, it already falls back to `try_batch_merge_with_split` and eventually to `merge_blocks_individually`. The retry loop applies to the individual path only.

Expose `is_txn_conflict()` on the storage error type if not already present.

### 4. `spawn_blocking` for redb `begin_write()`

**Files:** `crates/storage/src/backends/redb/transaction.rs`

Wrap the blocking redb commit in `tokio::task::spawn_blocking`:

```rust
let db = self.db.clone();
let pending = std::mem::take(&mut *self.pending.lock());
let durability = self.durability;

tokio::task::spawn_blocking(move || -> Result<()> {
    let mut write_txn = db.begin_write()?;
    write_txn.set_durability(match durability {
        DurabilityMode::Immediate => redb::Durability::Immediate,
        DurabilityMode::Eventual => redb::Durability::Eventual,
    });
    {
        let mut table = write_txn.open_table(KV_TABLE)?;
        for (key, value) in &pending {
            match value {
                Some(v) => { table.insert(key.as_slice(), v.as_slice())?; }
                None => { table.remove(key.as_slice())?; }
            }
        }
    }
    write_txn.commit()?;
    Ok(())
})
.await
.map_err(|e| Error::Other(e.to_string()))??;
```

Two locations in the redb backend:
1. `transaction.rs` direct commit path (line 353)
2. `group_commit.rs` `flush_batch` — already runs on a dedicated thread but should be verified

Other backends (fjall, rocksdb, memory) are not affected.

This matters even with Sections 1-3: two *different* documents committing simultaneously still compete for redb's single write lock. `spawn_blocking` ensures that competition doesn't starve the tokio runtime.

## Testing

1. **Existing integration tests pass** — `cargo test -p integration-test` (all areas).
2. **P2P tests specifically** — `cargo test -p integration-test --test p2p` covers document replication, sync, trust boundaries.
3. **Unit test for MergeQueue** — concurrent acquire/release for same key serializes; different keys run in parallel.
4. **Unit test for conflict retry** — mock a conflict on first attempt, success on second.
5. **Repro test validation** — the `simultaneous_writes_on_connected_nodes` test in amygdala should pass after this fix.

## Non-Goals

- Changing the CRDT merge semantics.
- Adding a global write lock or async mutex.
- Modifying fjall/rocksdb backends (they don't have the single-writer blocking issue).
- Adding broadcast delivery guarantees (eventual delivery via CRDT is sufficient).
- Retry mechanism for failed broadcasts (existing `PushFailure` channel handles this).
