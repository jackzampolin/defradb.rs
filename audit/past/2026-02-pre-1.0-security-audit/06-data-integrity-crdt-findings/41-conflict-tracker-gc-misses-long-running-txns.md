# ConflictTracker GC Can Miss Conflicts for Long-Running Transactions

**Severity:** Medium
**Category:** Transaction Integrity / Conflict Detection
**Status:** Confirmed

## Summary

The `ConflictTracker` prunes committed write sets when the list exceeds 1000 entries, keeping only the most recent 1000. If a long-running transaction was started before the pruned entries were committed, its conflict check at commit time will not detect conflicts with those pruned entries. This can silently allow write-write conflicts that should be rejected.

## Affected Files

- `crates/storage/src/backends/shared.rs:248-254` (GC logic)

## Details

### The GC Algorithm

```rust
// shared.rs:248-254
if committed.len() > 1000 {
    let drain_count = committed.len() - 1000;
    committed.drain(..drain_count);
}
```

The tracker keeps a `Vec<(u64, HashSet<Vec<u8>>)>` of committed write sets. When this exceeds 1000 entries, it drains the oldest entries. There is no consideration for whether any active transaction might still need those old entries for conflict detection.

### Exploit Scenario

```
T=0:   TxnA starts, gets read_version=50
T=1:   1001 short transactions commit (versions 51-1051), each writing key "K"
       → GC prunes entries for versions 51-51 (drain oldest to keep 1000)
       → The entry that wrote "K" at version 51 is pruned
T=2:   TxnA writes key "K" and commits
       → check_and_record scans committed_writes for entries > version 50
       → The version-51 entry for "K" was pruned
       → No conflict detected — TxnA commits successfully
       → TxnA's write SILENTLY OVERWRITES the version-51 write
```

### Root Cause

The GC has no awareness of the minimum `read_version` among active transactions. It prunes based on absolute count, not on whether any live transaction could still conflict with the pruned entries.

### Impact Assessment

- **High-throughput scenarios**: The Shinzo indexer (852 document creates per Ethereum block) could produce 1000+ commits in a short window, triggering GC during long-running queries.
- **HTTP transactions**: A user opens a transaction via `POST /api/v0/tx`, waits a while, then commits. If 1000+ other transactions committed during that time, conflict detection is degraded.
- **Batch merge**: The `try_batch_merge` path processes many blocks. If the merge transaction is long-lived while other merges are happening concurrently, conflicts can slip through.

### Not Configurable

The 1000 threshold is hardcoded. There is no option to increase it for high-throughput deployments.

## Remediation

Track the minimum active `read_version` and only prune entries older than that:

```rust
pub(crate) fn check_and_record<'a>(
    &self,
    read_version: u64,
    write_keys: impl Iterator<Item = &'a Vec<u8>>,
    min_active_version: Option<u64>,  // NEW: smallest read_version among live txns
) -> Result<(), Error> {
    // ...existing conflict check...

    // Safe GC: only prune entries that no active transaction can conflict with
    if let Some(min_ver) = min_active_version {
        committed.retain(|(ver, _)| *ver >= min_ver);
    }
    // Fallback: hard cap to prevent unbounded growth
    if committed.len() > 10_000 {
        let drain_count = committed.len() - 10_000;
        committed.drain(..drain_count);
    }
}
```

Each store would need to track the minimum `read_version` of active transactions. This is already partially available via the `active_txn_count` mechanism.

## Test Gap

- No test for conflict detection accuracy after GC threshold is exceeded
- No test with >1000 concurrent committed transactions
- The existing concurrency test suite (`test_suite/concurrency.rs`) tests at most 50 parallel transactions
