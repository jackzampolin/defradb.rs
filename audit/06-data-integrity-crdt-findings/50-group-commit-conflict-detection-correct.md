# Group Commit Conflict Detection Is Correctly Atomic

**Severity:** Informational
**Category:** Transaction Correctness
**Status:** Verified — Correct

## Summary

The redb group commit path (`GroupCommitBuffer`) performs conflict detection inside the flush loop, making the version tracking atomic with the actual data write. This is the correct design — conflicts are checked, the version is advanced, and the storage write all happen under a single serialized flow. Failed commits within a batch are correctly separated from successful ones, and their error callbacks are executed.

## Affected Files

- `crates/storage/src/backends/redb/group_commit.rs:82-160` (flush loop)
- `crates/storage/src/backends/redb/group_commit.rs:162-190` (flush_batch)

## Details

### Flush Loop Architecture

```rust
async fn flush_loop(mut rx: ..., db: ..., conflict_tracker: ...) {
    loop {
        let first = rx.recv().await;  // Block until at least one commit
        let mut batch = vec![first];
        while let Ok(commit) = rx.try_recv() {  // Drain concurrent commits
            batch.push(commit);
            if batch.len() >= 500 { break; }
        }

        // Partition: check conflicts for ALL commits before any storage write
        let mut passed = Vec::new();
        let mut failed = Vec::new();
        for commit in batch {
            match conflict_tracker.check_and_record(commit.read_version, commit.changes.keys()) {
                Ok(()) => passed.push(commit),
                Err(e) => failed.push((commit, e)),
            }
        }

        // Notify failed commits immediately
        for (commit, err) in failed { /* error callbacks + notify */ }

        // Flush all passing commits in ONE storage write
        let result = flush_batch(&db, &passed, durability);

        // Notify each committer
        for commit in passed { /* success/error callbacks + notify */ }
    }
}
```

### Why This Is Better Than Direct Commit

1. **Serialization**: The flush loop is a single async task. Only one batch is processed at a time. No concurrent access to the ConflictTracker during batch processing.

2. **All-or-nothing flush**: `flush_batch` writes all passing commits in a single redb write transaction. Either all succeed or all fail.

3. **Conflict inter-dependencies within batch**: When multiple commits in the same batch conflict with each other, `check_and_record` correctly detects this because each successful check advances the version before the next check.

### Batch Size Limit

The batch is capped at 500 commits per flush cycle. This bounds the transaction size and prevents a single redb write transaction from becoming too large.

### Callback Execution Order

Failed commits' error callbacks execute immediately (before the flush). Successful commits' callbacks execute after the flush completes. This means error callbacks for conflicting commits always run before success callbacks for non-conflicting commits in the same batch.

## Remediation

None needed. The group commit design is sound.

## Test Gap

- No explicit test for inter-batch conflict detection (two commits in the same batch writing the same key)
- No test for the 500-commit batch cap behavior
