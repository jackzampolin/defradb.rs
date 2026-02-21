# Memory Backend: Zero Unsafe, Clean Reference Implementation

**Severity**: Informational
**Category**: Unsafe Code — Reference Implementation Audit
**Status**: Clean

## Summary

The memory storage backend contains zero unsafe code, confirming it serves as a safe reference implementation. Its transaction and iterator patterns match the other backends structurally, making it a reliable baseline for comparing backend behavior.

## Affected Files

- `crates/storage/src/backends/memory/transaction.rs` — 295 lines, no unsafe
- `crates/storage/src/backends/memory/iterator.rs` — 143 lines, no unsafe
- `crates/storage/src/backends/memory/store.rs` — no unsafe

## Details

### Memory Backend Design

```rust
pub(crate) struct MemoryTxn {
    store: Arc<RwLock<BTreeMap<Vec<u8>, Vec<u8>>>>,
    conflict_tracker: Arc<ConflictTracker>,
    read_version: u64,
    snapshot: BTreeMap<Vec<u8>, Vec<u8>>,      // Full clone at txn start
    pending: Mutex<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
    readonly: bool,
    discarded: Mutex<bool>,
    committed: Mutex<bool>,
    callbacks: CallbackManager,
}
```

### Comparison with Other Backends

| Feature | Memory | Redb | Fjall | RocksDB |
|---------|--------|------|-------|---------|
| Unsafe code | None | None | None | Yes (OwnedSnapshot transmute) |
| Snapshot mechanism | BTreeMap clone | ReadTransaction (MVCC) | Snapshot type | OwnedSnapshot (transmute) |
| Iterator type | MemoryIterator (materialized) | MergingIterator (materialized) | MergingIterator (materialized) | RocksDbMergingIterator (materialized) |
| Conflict detection | ConflictTracker | ConflictTracker | ConflictTracker | ConflictTracker |
| Callbacks | CallbackManager | CallbackManager | CallbackManager | CallbackManager |

The memory backend implements the same conflict tracking, callback management, and iterator materialization patterns as the on-disk backends.

### Could the Memory Backend Mask Bugs?

The memory backend differs from on-disk backends in one significant way: it clones the entire store at transaction start (`snapshot: BTreeMap::clone()`), while on-disk backends use their native snapshot mechanisms. This means:

1. **Memory backend can't reveal snapshot bugs** in on-disk backends (e.g., if a RocksDB snapshot doesn't actually isolate correctly).
2. **Memory backend CAN reveal logic bugs** in transaction commit, conflict detection, callback management, and iterator merge logic, since these use the same shared code.

### No Drop impl

Unlike the other backends, MemoryTxn has no custom Drop implementation. When dropped without commit/discard, pending changes are silently lost with no warning logged. This is a minor inconsistency with the on-disk backends (which log warnings) but not a safety issue.

## Remediation

None needed. The memory backend is a clean, safe implementation.

## Test Gap

- The memory backend shares the same test suite macros as on-disk backends, providing good behavioral parity testing.
- Tests run against memory by default (fast), which provides broad coverage of the shared transaction/iterator logic.
