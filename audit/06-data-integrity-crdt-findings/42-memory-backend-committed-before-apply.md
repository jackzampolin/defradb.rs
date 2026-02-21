# Memory Backend Marks Committed Before Applying Changes

**Severity:** Low
**Category:** Transaction Correctness / Ordering
**Status:** Confirmed

## Summary

The memory backend's `commit()` sets `self.committed = true` before actually applying pending changes to the store. If a panic or task cancellation occurs between these two points, the Drop handler will not warn about lost changes (because it thinks the transaction was committed), and the ConflictTracker will have a phantom write set entry. The redb, fjall, and rocksdb backends correctly set committed AFTER the storage write.

## Affected Files

- `crates/storage/src/backends/memory/transaction.rs:205-221`

## Details

### Memory Backend (Incorrect Ordering)

```rust
// memory/transaction.rs:205-221
// Mark as committed
*self.committed.lock() = true;  // <-- BEFORE apply

// Apply pending changes to store
if !pending.is_empty() {
    let mut store = self.store.write().await;  // <-- await point!
    for (key, value) in pending.iter() {
        match value {
            Some(v) => { store.insert(key.clone(), v.clone()); }
            None => { store.remove(key); }
        }
    }
}
```

### Redb Backend (Correct Ordering)

```rust
// redb/transaction.rs:411-425
if let Err(e) = write_txn.commit() {
    return Err(e.into());
}
// Mark as committed AFTER successful database commit
*self.committed.lock() = true;  // <-- AFTER apply
```

### Consequences

1. **Lost changes without warning**: If a panic occurs after `committed = true` but before the store write, the Drop handler sees the transaction as committed and does not warn about lost pending changes.

2. **Phantom ConflictTracker entry**: The `check_and_record` call (line 195) happens before `committed = true`. It records the write set in the tracker. If the store write never happens, other transactions will see phantom conflicts against keys that were never actually written.

3. **Await point vulnerability**: The `self.store.write().await` is a suspension point. If the tokio task is cancelled (e.g., by a timeout or abort), the committed flag is already set but changes are lost.

### Practical Risk

Low in production because:
- Memory backend is only used for testing
- In-memory BTreeMap inserts cannot fail (only OOM could cause issues)
- The `store.write().await` only waits for the RwLock, which will eventually succeed

However, it violates the invariant that the other three backends maintain, which could mask bugs during testing that would be caught with a persistent backend.

## Remediation

Move `committed = true` after the store write, matching the other backends:

```rust
// Apply pending changes to store
if !pending.is_empty() {
    let mut store = self.store.write().await;
    for (key, value) in pending.iter() {
        match value {
            Some(v) => { store.insert(key.clone(), v.clone()); }
            None => { store.remove(key); }
        }
    }
}

// Mark as committed AFTER applying changes
*self.committed.lock() = true;
```

## Test Gap

No test that verifies committed flag ordering relative to storage write. No test for transaction behavior under tokio task cancellation.
