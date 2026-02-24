# Snapshot Isolation Verified Across All Backends

**Severity:** Informational
**Category:** Transaction Correctness
**Status:** Verified — Correct

## Summary

All four storage backends (redb, fjall, rocksdb, memory) correctly implement snapshot isolation. Snapshots are taken at transaction creation time (`new_txn`), not at first read. Pending writes are correctly overlaid on the snapshot using `BTreeMap<Vec<u8>, Option<Vec<u8>>>` where `None` represents a tombstone (delete). The get/has/iterator operations correctly check pending changes first, then fall back to the snapshot.

## Affected Files

- `crates/storage/src/backends/redb/store.rs:304-308` (snapshot at new_txn)
- `crates/storage/src/backends/fjall/store.rs:172-173`
- `crates/storage/src/backends/rocksdb/transaction.rs:111-112`
- `crates/storage/src/backends/memory/store.rs:53-56`

## Details

### Snapshot Timing

All backends take the snapshot at `new_txn()` time:

| Backend | Mechanism | Cost |
|---------|-----------|------|
| redb | `db.begin_read()` → `ReadTransaction` | O(1) — MVCC COW |
| fjall | `db.snapshot()` | O(1) — LSM snapshot |
| rocksdb | `db.snapshot()` via OwnedSnapshot | O(1) — sequence number |
| memory | `data.read().await.clone()` | O(n) — full BTreeMap clone |

### Tombstone Handling

All backends use `Option<Vec<u8>>` in the pending writes BTreeMap:

```rust
/// Pending changes (Some(value) = set, None = delete)
pub(crate) pending: Mutex<BTreeMap<Vec<u8>, Option<Vec<u8>>>>
```

The `get_internal` correctly handles tombstones:
```rust
fn get_internal(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    let pending = self.pending.lock();
    if let Some(pending_value) = pending.get(key) {
        return Ok(pending_value.clone());  // None for deleted, Some for set
    }
    // Fall back to snapshot...
}
```

### Iterator Merge

Redb, fjall, and rocksdb use a `MergingIterator` that correctly overlays pending changes on snapshot data, respecting tombstones. The memory backend merges the entire snapshot and pending map before iteration.

### Version Assignment

All backends record `read_version` from `conflict_tracker.current_version()` before taking the snapshot. This ensures the version used for conflict detection corresponds exactly to the snapshot point.

### Verified Properties

1. A read transaction sees a consistent view from its creation time
2. Concurrent writes by other transactions are not visible
3. Pending writes within the same transaction ARE visible (read-your-writes)
4. Deleted keys within the transaction return None (tombstones work)
5. The iterator sees a consistent merged view of snapshot + pending changes

## Test Coverage

The test suite at `crates/storage/src/backends/test_suite/concurrency.rs` provides comprehensive coverage:
- `test_snapshot_isolation_concurrent`: 10 readers + 10 writers, verifies no dirty reads
- `test_snapshot_isolation_long_running_reader`: 100 writes while reader is open
- `test_write_write_isolation`: Writers don't see each other's uncommitted data
- `test_snapshot_isolation_iterator`: Iterator consistency under concurrent modification

Additional tests in `crates/db/src/txn_registry_tests.rs`:
- `test_snapshot_isolation_after_external_commit`: Registry-level snapshot isolation
- `test_transaction_does_not_see_uncommitted_writes`: Dirty read protection
- `test_collection_snapshot_isolation_during_deletion`: Schema-level isolation

## Remediation

None needed. Snapshot isolation is correctly implemented.
