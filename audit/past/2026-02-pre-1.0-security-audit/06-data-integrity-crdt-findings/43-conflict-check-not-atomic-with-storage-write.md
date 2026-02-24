# Conflict Check Not Atomic with Storage Write (Direct Commit Path)

**Severity:** Low
**Category:** Transaction Integrity / TOCTOU
**Status:** Confirmed — Redb Serialization Mitigates

## Summary

In the direct commit path (non-group-commit), `check_and_record()` releases its lock before the actual storage write occurs. If `check_and_record` succeeds but the subsequent storage write fails, a phantom entry remains in the ConflictTracker — the tracker believes a write set was committed when it was not. This causes future transactions to see false conflicts against keys that were never actually written.

The group commit path handles this correctly by performing conflict detection inside the flush loop, atomic with the storage write.

## Affected Files

- `crates/storage/src/backends/redb/transaction.rs:322-421` (direct commit path)
- `crates/storage/src/backends/fjall/transaction.rs:243-279`
- `crates/storage/src/backends/rocksdb/transaction.rs:362-401`
- `crates/storage/src/backends/redb/group_commit.rs:104-115` (correct: atomic)

## Details

### Direct Commit Path (All Three Persistent Backends)

```rust
// Step 1: Check and record in ConflictTracker (mutex acquired then released)
if let Err(e) = self.conflict_tracker.check_and_record(self.read_version, pending.keys()) {
    return Err(e);
}
// <-- ConflictTracker mutex released, write set recorded with new version

// Step 2: Begin storage write (separate operation)
let mut write_txn = self.db.begin_write()?;  // Can fail!
// ...apply pending changes...
write_txn.commit()?;  // Can also fail!
```

### Failure Scenarios

1. **`begin_write()` fails** (e.g., redb internal error): The ConflictTracker has recorded the write set, but no data was written. Future transactions will falsely conflict on those keys.

2. **`write_txn.commit()` fails** (e.g., disk full, I/O error): Same phantom entry problem. The tracker version has advanced, the write set is recorded, but data is not persisted.

3. **Individual key insert fails** (mid-write): The redb write transaction is rolled back (no partial writes), but the tracker entry persists.

### Group Commit Path (Correct)

```rust
// group_commit.rs:104-115 — Both operations under one logical scope
for commit in batch {
    match conflict_tracker.check_and_record(commit.read_version, commit.changes.keys()) {
        Ok(()) => passed.push(commit),
        Err(e) => failed.push((commit, e)),
    }
}
// ...then flush all passed commits in one storage write...
```

The group commit path is better because the conflict check and storage write happen in the same task, serialized. However, even here, if `flush_batch()` fails after `check_and_record()` succeeded for some commits, those commits will have phantom tracker entries.

### Mitigating Factors

- **Redb**: `begin_write()` rarely fails (it's acquiring an internal lock). Commit failures are also rare (I/O errors).
- **False positives, not false negatives**: Phantom entries cause unnecessary `TxnConflict` errors, not data corruption. The affected transactions will retry.
- **Version monotonicity**: The version counter only advances, so phantom entries will eventually be GC'd by the 1000-entry pruning.

## Remediation

Option 1: Make ConflictTracker support rollback — add a method to remove a recorded write set if the storage commit fails:

```rust
pub(crate) fn rollback_version(&self, version: u64) {
    let mut committed = self.committed_writes.lock();
    committed.retain(|(v, _)| *v != version);
}
```

Option 2: Record write sets tentatively and confirm after storage commit (two-phase approach).

Option 3: Accept the false-positive trade-off and document it. The impact is limited to unnecessary retries.

## Test Gap

No test that verifies ConflictTracker state after a failed storage commit. No test that injects storage write failures during commit.
