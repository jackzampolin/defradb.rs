# P2P Write Contention Fix — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix deadlock when two P2P-connected nodes write documents simultaneously, by decoupling broadcast from mutations, adding per-document merge serialization, conflict retry, and unblocking redb's `begin_write()`.

**Architecture:** Four independent changes that compose: (1) spawn broadcast as fire-and-forget after local commit, (2) per-document merge queue matching Go's architecture, (3) retry loop for transaction conflicts in merge handler, (4) `spawn_blocking` wrapper around redb's blocking write lock.

**Tech Stack:** Rust, tokio (spawn, spawn_blocking, Mutex, Semaphore), parking_lot, redb, thiserror

**Spec:** `docs/superpowers/specs/2026-03-25-p2p-write-contention-design.md`

---

### Task 1: Add `BroadcastStatus::Pending` Variant

**Files:**
- Modify: `crates/query/src/mutator.rs:19-27` (BroadcastStatus enum)

- [ ] **Step 1: Add `Pending` variant to `BroadcastStatus`**

In `crates/query/src/mutator.rs`, add the `Pending` variant:

```rust
pub enum BroadcastStatus {
    /// Broadcast succeeded
    Success,
    /// Broadcast failed with the given error message
    Failed(String),
    /// Broadcast spawned but not yet complete (fire-and-forget)
    Pending,
    /// Broadcast was not attempted (P2P not enabled or not applicable)
    #[default]
    NotAttempted,
}
```

- [ ] **Step 2: Verify compilation**

Run: `cargo check -p query`
Expected: compiles clean

- [ ] **Step 3: Commit**

```bash
git add crates/query/src/mutator.rs
git commit -m "feat(query): add BroadcastStatus::Pending variant for fire-and-forget broadcast"
```

---

### Task 2: Decouple Broadcast from `BroadcastMutator` Single-Doc Methods

**Files:**
- Modify: `crates/db/src/broadcast_mutator/mod.rs:86-205` (create), `326-443` (update), `445-509` (delete)

The pattern is the same for all three methods. After `self.inner.create().await` returns (transaction committed), spawn the broadcast work as a detached `tokio::spawn` task and return immediately with `BroadcastStatus::Pending`.

- [ ] **Step 1: Refactor `create` method**

Replace the broadcast section of `BroadcastMutator::create()` (everything after `self.inner.create().await` returns successfully, starting at line 103). The new code commits the local transaction (already done by `inner.create()`), then spawns broadcast:

```rust
    async fn create(
        &self,
        collection_name: &str,
        doc: Document,
    ) -> query::error::Result<CreateResult> {
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;
        let version_id = collection.version_id().to_string();
        let collection_id = collection.collection_id().to_string();

        let result = self.inner.create(collection_name, doc).await?;

        // Build the block result for broadcast
        let (cid, block, doc_id_str) = if let (Some(cid), Some(block)) =
            (result.commit_cid, result.commit_block.as_ref())
        {
            (cid, block.clone(), result.doc_id.to_string())
        } else {
            match build_blocks_from_document(&result.document, &version_id, self.sync.blockstore())
                .await
            {
                Ok(br) => (br.cid, br.block, br.doc_id),
                Err(e) => {
                    tracing::error!(
                        doc_id = %result.doc_id,
                        collection = %collection_name,
                        error = %e,
                        "Failed to build blocks for P2P broadcast"
                    );
                    return Ok(CreateResult::with_broadcast(
                        result.doc_id,
                        result.document,
                        BroadcastStatus::Failed(format!("Block build failed: {}", e)),
                    ));
                }
            }
        };

        let block_result = BlockResult {
            cid,
            block,
            doc_id: doc_id_str,
            field_cids: vec![],
        };

        let creator_did = defra_core::signing::get_broadcast_creator_did();

        // Capture broadcast data for branchable collections
        let broadcast_data = result.broadcast_cid.and_then(|col_cid| {
            result
                .broadcast_block
                .as_ref()
                .map(|col_block| (col_cid, col_block.clone()))
        });

        // Spawn broadcast as fire-and-forget
        let sync = self.sync.clone();
        let collection_name_owned = collection_name.to_string();
        tokio::spawn(async move {
            let creator_ref = creator_did.as_deref();

            sync.push_dag_to_replicators_with_creator(
                &block_result.cid,
                &block_result.block,
                &block_result.doc_id,
                &collection_id,
                creator_ref,
            )
            .await;

            let broadcast_status = broadcast_with_retry_with_creator(
                &sync,
                &block_result,
                &collection_id,
                &collection_name_owned,
                creator_ref,
            )
            .await;

            if let BroadcastStatus::Failed(ref e) = broadcast_status {
                tracing::warn!(
                    doc_id = %block_result.doc_id,
                    collection = %collection_name_owned,
                    error = %e,
                    "Background broadcast failed after local commit"
                );
            }

            if let Some((col_cid, col_block)) = broadcast_data {
                let col_block_result = BlockResult {
                    cid: col_cid,
                    block: col_block,
                    doc_id: block_result.doc_id.clone(),
                    field_cids: vec![],
                };
                sync.push_to_replicators_with_creator(
                    &col_block_result.cid,
                    &col_block_result.block,
                    &col_block_result.doc_id,
                    &collection_id,
                    creator_ref,
                )
                .await;
                let _ = broadcast_with_retry_with_creator(
                    &sync,
                    &col_block_result,
                    &collection_id,
                    &collection_name_owned,
                    creator_ref,
                )
                .await;
            }
        });

        Ok(CreateResult::with_commit_and_broadcast(
            result.doc_id,
            result.document,
            block_result.cid,
            block_result.block,
            BroadcastStatus::Pending,
        ))
    }
```

- [ ] **Step 2: Refactor `update` method**

Apply the same fire-and-forget pattern to `BroadcastMutator::update()`. After `self.inner.update()` returns, spawn the push_dag + broadcast + branchable collection broadcast as a detached task. Return `BroadcastStatus::Pending`. Follow the exact same structure as `create` above — read committed block, build `BlockResult`, capture broadcast data, `tokio::spawn`, return immediately.

- [ ] **Step 3: Refactor `delete` method**

Apply the same pattern to `BroadcastMutator::delete()`. After `self.inner.delete()` returns, spawn the push + broadcast as a detached task. Return `BroadcastStatus::Pending`. Note: delete uses `push_to_replicators` (single block), not `push_dag_to_replicators`.

- [ ] **Step 4: Refactor `create_many` method**

Apply the same pattern to `BroadcastMutator::create_many()`. After each doc's block is built in the loop, instead of awaiting broadcast inline, collect broadcast work items and spawn a single detached task after the loop that processes all of them. Each result gets `BroadcastStatus::Pending`.

- [ ] **Step 5: Verify compilation**

Run: `cargo check -p db`
Expected: compiles clean

- [ ] **Step 6: Run existing tests**

Run: `cargo test -p db`
Expected: all pass (broadcast behavior is not directly tested at unit level)

- [ ] **Step 7: Commit**

```bash
git add crates/db/src/broadcast_mutator/mod.rs
git commit -m "feat(db): decouple P2P broadcast from mutation path — fire-and-forget via tokio::spawn"
```

---

### Task 3: Decouple Broadcast from Batch Mutator

**Files:**
- Modify: `crates/db/src/broadcast_mutator/batch.rs:322-334` (MutationBatchController::commit)

- [ ] **Step 1: Refactor batch commit to spawn broadcast**

In `BroadcastBatchMutator`'s `MutationBatchController::commit()`, change the sequential broadcast loop to a spawned task:

```rust
    async fn commit(&self) -> query::error::Result<()> {
        if let Err(err) = self.inner_controller.commit().await {
            self.pending_broadcasts.lock().await.clear();
            return Err(err);
        }

        let pending_broadcasts = std::mem::take(&mut *self.pending_broadcasts.lock().await);
        if !pending_broadcasts.is_empty() {
            let sync = self.sync.clone();
            tokio::spawn(async move {
                for pending in pending_broadcasts {
                    Self::broadcast_pending_static(&sync, pending).await;
                }
            });
        }

        Ok(())
    }
```

This requires extracting `broadcast_pending` into a static method that takes `sync` by reference rather than `&self`:

```rust
    async fn broadcast_pending_static(
        sync: &SyncCoordinator<B, T>,
        pending: PendingBroadcast,
    ) {
        // Same body as current broadcast_pending, but using sync parameter
        // instead of self.sync
        let PendingBroadcast {
            kind, cid, block, doc_id, collection_id,
            collection_name, creator_did, broadcast_cid, broadcast_block,
        } = pending;

        let creator_ref = creator_did.as_deref();

        match kind {
            BroadcastKind::DagPush => {
                sync.push_dag_to_replicators_with_creator(
                    &cid, &block, &doc_id, &collection_id, creator_ref,
                ).await;
            }
            BroadcastKind::SingleBlockPush => {
                sync.push_to_replicators_with_creator(
                    &cid, &block, &doc_id, &collection_id, creator_ref,
                ).await;
            }
        }

        let block_result = BlockResult {
            cid, block, doc_id: doc_id.clone(), field_cids: vec![],
        };
        let broadcast_status = super::broadcast_with_retry_with_creator(
            sync, &block_result, &collection_id, &collection_name, creator_ref,
        ).await;

        if let BroadcastStatus::Failed(error) = &broadcast_status {
            tracing::warn!(
                doc_id = %doc_id,
                collection = %collection_name,
                error = %error,
                "Deferred batch broadcast failed after commit"
            );
        }

        if let (Some(col_cid), Some(col_block)) = (broadcast_cid, broadcast_block) {
            let col_block_result = BlockResult {
                cid: col_cid, block: col_block,
                doc_id: doc_id.clone(), field_cids: vec![],
            };
            sync.push_to_replicators_with_creator(
                &col_block_result.cid, &col_block_result.block,
                &col_block_result.doc_id, &collection_id, creator_ref,
            ).await;
            let col_status = super::broadcast_with_retry_with_creator(
                sync, &col_block_result, &collection_id, &collection_name, creator_ref,
            ).await;
            if let BroadcastStatus::Failed(error) = &col_status {
                tracing::warn!(
                    doc_id = %col_block_result.doc_id,
                    collection = %collection_name,
                    error = %error,
                    "Deferred branchable collection broadcast failed after commit"
                );
            }
        }
    }
```

- [ ] **Step 2: Verify compilation**

Run: `cargo check -p db`
Expected: compiles clean

- [ ] **Step 3: Commit**

```bash
git add crates/db/src/broadcast_mutator/batch.rs
git commit -m "feat(db): decouple batch broadcast from mutation commit path"
```

---

### Task 4: Create `MergeQueue`

**Files:**
- Create: `crates/db/src/merge_handler/queue.rs`
- Modify: `crates/db/src/merge_handler/mod.rs` (add `mod queue; pub use queue::MergeQueue;`)

- [ ] **Step 1: Write MergeQueue tests**

Create `crates/db/src/merge_handler/queue.rs` with the type and tests:

```rust
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::OwnedMutexGuard;

const PRUNE_THRESHOLD: usize = 10_000;

/// Per-key async merge serialization queue.
///
/// Ensures that merges for the same document (or collection, for branchable
/// types) are processed one at a time, while merges for different keys run
/// in parallel. Matches Go DefraDB's `docMergeQueue`/`colMergeQueue`.
pub struct MergeQueue {
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl MergeQueue {
    pub fn new() -> Self {
        Self {
            locks: Mutex::new(HashMap::new()),
        }
    }

    /// Acquire the merge lock for a given key.
    ///
    /// Returns an owned guard that serializes access. Different keys
    /// proceed in parallel; the same key blocks until the previous
    /// holder drops the guard.
    pub async fn acquire(&self, key: &str) -> OwnedMutexGuard<()> {
        let mutex = {
            let mut map = self.locks.lock();
            if map.len() > PRUNE_THRESHOLD {
                map.retain(|_, v| Arc::strong_count(v) > 1);
            }
            map.entry(key.to_string())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        mutex.lock_owned().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn same_key_serializes() {
        let queue = Arc::new(MergeQueue::new());
        let counter = Arc::new(AtomicUsize::new(0));
        let max_concurrent = Arc::new(AtomicUsize::new(0));

        let mut handles = vec![];
        for _ in 0..10 {
            let q = queue.clone();
            let c = counter.clone();
            let m = max_concurrent.clone();
            handles.push(tokio::spawn(async move {
                let _guard = q.acquire("doc-1").await;
                let current = c.fetch_add(1, Ordering::SeqCst) + 1;
                // Update max concurrent
                m.fetch_max(current, Ordering::SeqCst);
                // Simulate work
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                c.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        // Max concurrent should be 1 — all serialized
        assert_eq!(max_concurrent.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn different_keys_run_in_parallel() {
        let queue = Arc::new(MergeQueue::new());
        let counter = Arc::new(AtomicUsize::new(0));
        let max_concurrent = Arc::new(AtomicUsize::new(0));

        let mut handles = vec![];
        for i in 0..10 {
            let q = queue.clone();
            let c = counter.clone();
            let m = max_concurrent.clone();
            let key = format!("doc-{}", i); // All different keys
            handles.push(tokio::spawn(async move {
                let _guard = q.acquire(&key).await;
                let current = c.fetch_add(1, Ordering::SeqCst) + 1;
                m.fetch_max(current, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                c.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        // Max concurrent should be > 1 (parallel)
        assert!(max_concurrent.load(Ordering::SeqCst) > 1);
    }
}
```

- [ ] **Step 2: Add module declaration**

In `crates/db/src/merge_handler/mod.rs`, add near the top with the other module declarations:

```rust
mod queue;
pub use queue::MergeQueue;
```

- [ ] **Step 3: Run the tests**

Run: `cargo test -p db merge_handler::queue`
Expected: 2 tests pass — `same_key_serializes` and `different_keys_run_in_parallel`

- [ ] **Step 4: Commit**

```bash
git add crates/db/src/merge_handler/queue.rs crates/db/src/merge_handler/mod.rs
git commit -m "feat(db): add MergeQueue for per-document merge serialization"
```

---

### Task 5: Wire MergeQueue into DbMergeHandler

**Files:**
- Modify: `crates/db/src/merge_handler/mod.rs:135-162` (struct + constructor)
- Modify: `crates/db/src/merge_handler/batch.rs:18-37` (merge_blocks_individually)

- [ ] **Step 1: Add `merge_queue` field to `DbMergeHandler`**

In `crates/db/src/merge_handler/mod.rs`, add the field to the struct (after line 145):

```rust
pub struct DbMergeHandler<S: Store, B: blockstore::Blockstore> {
    db: Arc<DB<S>>,
    blockstore: Arc<B>,
    composite_merge_hook: std::sync::OnceLock<Arc<dyn CompositeMergeHook>>,
    merged_composites: std::sync::Mutex<HashSet<Cid>>,
    se_enc_key: std::sync::OnceLock<Zeroizing<Vec<u8>>>,
    merge_queue: Arc<MergeQueue>,
}
```

Update the constructor (line 154):

```rust
    pub fn new(db: Arc<DB<S>>, blockstore: Arc<B>) -> Self {
        Self {
            db,
            blockstore,
            composite_merge_hook: std::sync::OnceLock::new(),
            merged_composites: std::sync::Mutex::new(HashSet::new()),
            se_enc_key: std::sync::OnceLock::new(),
            merge_queue: Arc::new(MergeQueue::new()),
        }
    }
```

- [ ] **Step 2: Acquire merge queue in `merge_blocks_individually`**

In `crates/db/src/merge_handler/batch.rs`, modify `merge_blocks_individually` to acquire the queue lock per block. The `is_branchable` flag is not available at this level (it's determined during composite processing), so use `doc_id` as the key for all merges. This is correct — Go also uses `doc_id` for the common case and only switches to `collection_id` for branchable. Since branchable is rare and the queue prevents same-doc races (the primary concern), using doc_id universally is the right first step.

```rust
    pub(crate) async fn merge_blocks_individually(
        &self,
        blocks: &[MergeBlock],
    ) -> Vec<Result<MergeOutcome, MergeError>> {
        let mut results = Vec::with_capacity(blocks.len());
        for block in blocks {
            // Serialize merges for the same document
            let _guard = self.merge_queue.acquire(&block.doc_id).await;

            let metadata = BlockMetadata::normal(
                &block.doc_id,
                &block.collection_id,
                &block.creator,
                block.sender_peer.as_deref(),
                block.is_explicit_replicator,
            )
            .with_explicit_replay_authorization(block.explicit_replay_authorization.clone());
            results.push(
                self.handle_block(&block.cid, &block.block_data, metadata)
                    .await,
            );
        }
        results
    }
```

- [ ] **Step 3: Verify compilation**

Run: `cargo check -p db`
Expected: compiles clean

- [ ] **Step 4: Run tests**

Run: `cargo test -p db`
Expected: all pass

- [ ] **Step 5: Commit**

```bash
git add crates/db/src/merge_handler/mod.rs crates/db/src/merge_handler/batch.rs
git commit -m "feat(db): wire MergeQueue into merge handler for per-document serialization"
```

---

### Task 6: Add `is_txn_conflict()` to `MergeError`

**Files:**
- Modify: `crates/db/src/merge_handler/mod.rs:106-111` (MergeError impl block)

The conflict retry loop needs to detect `TxnConflict` through the error wrapper chain: `MergeError::Database(db::Error::Datastore(datastore::Error::Storage(storage::Error::TxnConflict)))`.

- [ ] **Step 1: Add `is_txn_conflict` method**

In `crates/db/src/merge_handler/mod.rs`, extend the `impl MergeError` block:

```rust
impl MergeError {
    pub(crate) fn depth_exceeded(cid: &Cid, depth: usize) -> Self {
        MergeError::DepthExceeded { cid: *cid, depth }
    }

    /// Check if this error is a transaction conflict that can be retried.
    pub(crate) fn is_txn_conflict(&self) -> bool {
        match self {
            MergeError::Database(db_err) => match db_err {
                crate::error::Error::Datastore(ds_err) => match ds_err {
                    datastore::Error::Storage(storage_err) => storage_err.is_txn_conflict(),
                    _ => false,
                },
                crate::error::Error::Storage(storage_err) => storage_err.is_txn_conflict(),
                _ => false,
            },
            _ => false,
        }
    }
}
```

- [ ] **Step 2: Verify compilation**

Run: `cargo check -p db`
Expected: compiles clean

- [ ] **Step 3: Commit**

```bash
git add crates/db/src/merge_handler/mod.rs
git commit -m "feat(db): add is_txn_conflict() to MergeError for retry detection"
```

---

### Task 7: Add Conflict Retry Loop to Individual Merge Path

**Files:**
- Modify: `crates/db/src/merge_handler/batch.rs:18-37` (merge_blocks_individually)

- [ ] **Step 1: Wrap handle_block in retry loop**

Replace the body of the per-block loop in `merge_blocks_individually` (keeping the merge queue acquisition from Task 5):

```rust
    pub(crate) async fn merge_blocks_individually(
        &self,
        blocks: &[MergeBlock],
    ) -> Vec<Result<MergeOutcome, MergeError>> {
        const MAX_MERGE_RETRIES: usize = 5;

        let mut results = Vec::with_capacity(blocks.len());
        for block in blocks {
            let _guard = self.merge_queue.acquire(&block.doc_id).await;

            let metadata = BlockMetadata::normal(
                &block.doc_id,
                &block.collection_id,
                &block.creator,
                block.sender_peer.as_deref(),
                block.is_explicit_replicator,
            )
            .with_explicit_replay_authorization(block.explicit_replay_authorization.clone());

            let mut last_result = None;
            for attempt in 0..MAX_MERGE_RETRIES {
                let result = self
                    .handle_block(&block.cid, &block.block_data, metadata.clone())
                    .await;
                match &result {
                    Err(e) if e.is_txn_conflict() => {
                        tracing::debug!(
                            attempt,
                            doc_id = %block.doc_id,
                            cid = %block.cid,
                            "Merge conflict, retrying"
                        );
                        last_result = Some(result);
                        continue;
                    }
                    _ => {
                        last_result = Some(result);
                        break;
                    }
                }
            }
            results.push(last_result.unwrap());
        }
        results
    }
```

Note: This requires `BlockMetadata` to implement `Clone`. Check if it already does; if not, derive it.

- [ ] **Step 2: Verify `BlockMetadata` implements `Clone`**

Run: `cargo check -p db`

If compilation fails because `BlockMetadata` doesn't implement `Clone`, add `#[derive(Clone)]` to it (or implement manually if it has non-Clone fields). `BlockMetadata` is defined in `crates/db/src/merge_handler/mod.rs` — find the struct and add the derive.

- [ ] **Step 3: Run tests**

Run: `cargo test -p db`
Expected: all pass

- [ ] **Step 4: Commit**

```bash
git add crates/db/src/merge_handler/batch.rs crates/db/src/merge_handler/mod.rs
git commit -m "feat(db): add conflict retry loop for individual merge path (max 5 retries)"
```

---

### Task 8: Wrap redb `begin_write()` in `spawn_blocking`

**Files:**
- Modify: `crates/storage/src/backends/redb/transaction.rs:295-454` (commit method)

- [ ] **Step 1: Refactor direct commit path to use `spawn_blocking`**

In `crates/storage/src/backends/redb/transaction.rs`, the `Txn::commit()` implementation has a direct commit path starting at line 343. Replace the section from conflict check through `write_txn.commit()` (lines 343-443) with a `spawn_blocking` wrapper. The conflict check stays outside `spawn_blocking` (it's a quick mutex operation), but `begin_write()`, table operations, and `commit()` move inside:

```rust
            // Direct commit path: check conflicts eagerly
            if let Err(e) = self
                .conflict_tracker
                .check_and_record(self.read_version, pending.keys())
            {
                CallbackManager::execute_callbacks(self.callbacks.take_error());
                CallbackManager::execute_async_callbacks(self.callbacks.take_error_async()).await;
                return Err(e);
            }

            // Move blocking redb operations to a blocking thread to avoid
            // starving the tokio runtime (redb's begin_write() acquires an
            // exclusive write lock that blocks the OS thread).
            let db = self.db.clone();
            let durability = self.durability;
            let error_callbacks = self.callbacks.take_error();
            let error_async_callbacks = self.callbacks.take_error_async();

            let write_result = tokio::task::spawn_blocking(move || -> Result<()> {
                let mut write_txn = db.begin_write().map_err(|e| {
                    tracing::error!(
                        error = %e,
                        pending_changes = pending.len(),
                        "Failed to begin write transaction during commit"
                    );
                    Error::from(e)
                })?;

                write_txn.set_durability(match durability {
                    DurabilityMode::Immediate => redb::Durability::Immediate,
                    DurabilityMode::Eventual => redb::Durability::Eventual,
                });

                {
                    let mut table = write_txn.open_table(KV_TABLE).map_err(|e| {
                        tracing::error!(error = %e, "Failed to open KV table during commit");
                        Error::from(e)
                    })?;

                    for (key, value) in pending.iter() {
                        match value {
                            Some(v) => {
                                if let Err(e) = table.insert(key.as_slice(), v.as_slice()) {
                                    tracing::error!(
                                        error = %e,
                                        key_len = key.len(),
                                        value_len = v.len(),
                                        "Failed to insert key during commit"
                                    );
                                    return Err(e.into());
                                }
                            }
                            None => {
                                if let Err(e) = table.remove(key.as_slice()) {
                                    tracing::error!(
                                        error = %e,
                                        key_len = key.len(),
                                        "Failed to delete key during commit"
                                    );
                                    return Err(e.into());
                                }
                            }
                        }
                    }
                }

                if let Err(e) = write_txn.commit() {
                    tracing::error!(
                        error = %e,
                        pending_changes = pending.len(),
                        "Failed to finalize commit"
                    );
                    return Err(e.into());
                }

                Ok(())
            })
            .await;

            match write_result {
                Ok(Ok(())) => {} // Success — fall through to callback execution
                Ok(Err(e)) => {
                    CallbackManager::execute_callbacks(error_callbacks);
                    CallbackManager::execute_async_callbacks(error_async_callbacks).await;
                    return Err(e);
                }
                Err(join_err) => {
                    CallbackManager::execute_callbacks(error_callbacks);
                    CallbackManager::execute_async_callbacks(error_async_callbacks).await;
                    return Err(Error::Other(format!(
                        "spawn_blocking panicked: {}",
                        join_err
                    )));
                }
            }
```

Note: the `pending` variable needs to be moved into the closure. It's already taken from `self.pending` via `std::mem::take` earlier, so this is a move of a `BTreeMap`.

The callbacks taken with `take_error()` / `take_error_async()` are taken BEFORE spawn_blocking (since `self.callbacks` can't move into the closure). On success, the existing success callback execution (lines 449-451) runs as before.

- [ ] **Step 2: Verify compilation**

Run: `cargo check -p storage`
Expected: compiles clean

- [ ] **Step 3: Run storage tests**

Run: `cargo test -p storage`
Expected: all pass

- [ ] **Step 4: Verify `group_commit.rs` `flush_batch` does NOT need `spawn_blocking`**

Read `crates/storage/src/backends/redb/group_commit.rs` and confirm that `flush_batch` is called from a dedicated background thread (not from an async context). If it's called via `std::thread::spawn` or a blocking loop, no change needed. If it's called from an async task, apply the same `spawn_blocking` wrapper.

Expected: `flush_batch` runs on a dedicated thread — no change needed.

- [ ] **Step 5: Commit**

```bash
git add crates/storage/src/backends/redb/transaction.rs
git commit -m "fix(storage): wrap redb begin_write() in spawn_blocking to prevent tokio starvation"
```

---

### Task 9: Integration Test Verification

**Files:** No changes — just verification

- [ ] **Step 1: Run clippy**

Run: `cargo clippy --all -- -D warnings`
Expected: clean

- [ ] **Step 2: Run fmt**

Run: `cargo fmt --all`
Expected: no changes (or apply any formatting fixes)

- [ ] **Step 3: Run unit tests**

Run: `cargo test`
Expected: all pass

- [ ] **Step 4: Run P2P integration tests**

Run: `cargo test -p integration-test --test p2p`
Expected: all pass

- [ ] **Step 5: Run full integration test suite**

Run: `cargo test -p integration-test`
Expected: all pass

- [ ] **Step 6: Commit any formatting fixes**

If `cargo fmt` made changes:

```bash
git add -A
git commit -m "style: apply formatting"
```
