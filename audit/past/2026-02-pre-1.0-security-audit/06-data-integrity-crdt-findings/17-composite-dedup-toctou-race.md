# Composite Dedup Check-Then-Insert Has TOCTOU Race

**Severity:** Low
**Category:** Concurrency / Race Condition
**Status:** Confirmed — CRDT Idempotency Mitigates

## Summary

The `merged_composites` dedup guard in the composite merge handler has a Time-Of-Check-to-Time-Of-Use (TOCTOU) race. The check (lock, read, unlock) and the insert (lock, write, unlock) are separated by the entire merge operation. Two concurrent merge tasks processing the same CID can both pass the check and both execute the full merge. CRDT idempotency ensures correctness, but the race causes redundant work and redundant headstore writes.

## Affected Files

- `crates/db/src/merge_handler/composite.rs:104-114` (check, lock released)
- `crates/db/src/merge_handler/composite.rs:662-666` (insert, after merge completes)

## Details

### Race Window

```rust
// composite.rs:104-114 — CHECK (lock acquired, then released)
{
    let merged = self.merged_composites.lock().unwrap();
    if merged.contains(head_cid) {
        continue;  // skip
    }
}  // <-- LOCK RELEASED HERE

// ... entire merge operation happens here (potentially hundreds of ms) ...

// composite.rs:662-666 — INSERT (lock acquired again)
{
    let mut merged = self.merged_composites.lock().unwrap();
    merged.insert(*cid);
}
```

### Race Scenario

```
Task 1: checks merged_composites for CID X → not found
Task 1: releases lock
Task 2: checks merged_composites for CID X → not found (Task 1 hasn't inserted yet)
Task 2: releases lock
Task 1: processes full merge for CID X
Task 2: processes full merge for CID X (redundant)
Task 1: inserts CID X into merged_composites
Task 2: inserts CID X into merged_composites (no-op, already there)
```

### Why CRDT Idempotency Saves Us

- **LWW merge**: Priority-based, idempotent. Applying the same delta twice produces the same result.
- **Counter merge**: Nonce-based, idempotent. Duplicate nonce is rejected.
- **Headstore writes**: Idempotent. Writing the same head CID twice is a no-op.
- **Document save**: Last-write-wins at the storage layer. The final document state is determined by the field values, which are the same regardless of how many times the merge runs.

The double-processing wastes CPU, I/O, and transaction resources but does not corrupt state.

### Batch Path Also Affected

The batch-mode composite merge (`process_composite_delta_in_txn`) has the same pattern with both `merged_composites` and `batch_merged`:

```rust
// composite.rs:753-765
{
    let merged = self.merged_composites.lock().unwrap();
    if merged.contains(head_cid) { continue; }
}
{
    let bm = batch_merged.lock().unwrap();
    if bm.contains(head_cid) { continue; }
}
// ... merge ...
```

This is a double-check pattern with TWO separate locks, creating an even wider TOCTOU window.

## Remediation

Use a try-insert pattern that atomically checks and marks in a single critical section:

```rust
// Atomic check-and-mark
{
    let mut merged = self.merged_composites.lock().unwrap();
    if !merged.insert(*head_cid) {
        // Already present — skip
        continue;
    }
    // Newly inserted — proceed with merge
}
// If merge fails, optionally remove from set
```

This has the trade-off that a failed merge would leave the CID in the set, preventing retry. To handle this, use a three-state tracker (pending/merged/failed) or remove the CID on error.

## Test Gap

No concurrent merge test that sends the same CID from two paths simultaneously and verifies it's processed only once.
