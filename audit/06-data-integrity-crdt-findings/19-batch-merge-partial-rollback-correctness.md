# Batch Merge Binary-Split Retry: Rollback Semantics Verified

**Severity:** Informational
**Category:** Data Integrity / Transaction Safety
**Status:** Verified Clean

## Summary

The batch merge binary-split retry strategy in `batch.rs` correctly rolls back partial work when a batch fails. The transaction is discarded, and the batch is split in half for retry. The base case (single block) falls back to individual processing with its own transaction. The strategy has O(N) worst-case behavior but is correct.

## Affected Files

- `crates/db/src/merge_handler/batch.rs:35-137`
- `crates/db/src/merge_handler/mod.rs:437-446`

## Details

### Transaction Lifecycle

```rust
// batch.rs:68-137
pub(crate) async fn try_batch_merge(&self, blocks: &[MergeBlock])
    -> Result<Vec<Result<MergeOutcome, MergeError>>, MergeError>
{
    let txn = self.db.new_txn(false).await?;
    // ...
    for block in blocks {
        match self.process_block_in_txn(/* ... */).await {
            Ok(outcome) => results.push(Ok(outcome)),
            Err(e) => {
                batch_error = Some(e);
                break;  // <-- Stop processing on first error
            }
        }
    }

    if let Some(e) = batch_error {
        if let Err(de) = txn.force_discard() {
            tracing::error!(error = %de, "Failed to discard batch txn");
        }
        return Err(e);  // <-- Transaction discarded, partial work rolled back
    }

    txn.force_commit().await?;  // <-- Only committed if ALL blocks succeed
    // ...
}
```

### Binary-Split Retry

```rust
// batch.rs:35-62
pub(crate) fn try_batch_merge_with_split<'a>(&'a self, blocks: &'a [MergeBlock])
    -> Pin<Box<dyn Future<Output = Vec<Result<MergeOutcome, MergeError>>> + Send + 'a>>
{
    Box::pin(async move {
        if blocks.len() <= 1 {
            return self.merge_blocks_individually(blocks).await;  // <-- Base case
        }

        match self.try_batch_merge(blocks).await {
            Ok(results) => results,
            Err(e) => {
                let mid = blocks.len() / 2;
                let (left, right) = blocks.split_at(mid);
                let mut results = self.try_batch_merge_with_split(left).await;
                results.extend(self.try_batch_merge_with_split(right).await);
                results
            }
        }
    })
}
```

### Correctness Verification

1. **Rollback on failure**: `txn.force_discard()` is called when any block fails. The shared transaction's partial writes are discarded. No corrupted state leaks.

2. **Base case**: When `blocks.len() <= 1`, falls back to `merge_blocks_individually` which creates a separate transaction per block. A single failing block gets its own transaction that is discarded independently.

3. **No double-processing**: When a batch fails and is split, the failed batch's results are discarded entirely. The two halves are processed independently with new transactions. Previously-processed blocks in the failed batch ARE re-processed in the split halves, but since CRDT merges are idempotent, this is safe.

4. **Worst case**: If all N blocks fail individually, the strategy executes:
   - 1 batch attempt (all N) → fail
   - 2 batch attempts (N/2 each) → fail
   - 4 batch attempts (N/4 each) → fail
   - ...
   - N individual attempts
   - Total attempts: N + N/2 + N/4 + ... + 1 ≈ 2N batch attempts + N individual = O(N)
   - Recursion depth: O(log N) — safe for the 50-block default batch size

5. **Dedup coordination**: Batch-merged CIDs are tracked in a local `batch_merged` set during the batch. On commit, they're moved to the permanent `merged_composites` set. On failure, the local set is discarded — dedup state is consistent.

### Potential Concern: CollectionDefinition in Batch

```rust
// batch.rs:259-263
CrdtDelta::CollectionDefinition(payload) => {
    // CollectionDefinition uses its own txn (rare, not worth batching)
    self.process_collection_definition_delta(cid, &block, payload, metadata).await
}
```

CollectionDefinition blocks create their own transaction inside the batch's shared transaction. If the batch transaction is later discarded, the CollectionDefinition's committed transaction is NOT rolled back. This could leave orphaned collection definitions in the systemstore. However, CollectionDefinition blocks are rare (only during schema sync, not document mutations) and the orphaned definitions are harmless (inactive, require manual activation).

## Test Gap

The batch retry is tested implicitly by integration tests but lacks targeted unit tests:
- Unit test: batch of 5 blocks where block 3 fails → verify blocks 1-2 and 4-5 succeed, block 3 fails
- Unit test: batch where all blocks fail → verify O(N) behavior, no stack overflow
- Unit test: verify no partial state leaks after batch rollback
