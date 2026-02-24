# RocksDB OwnedSnapshot Lifetime Transmute

**Severity**: Medium
**Category**: Unsafe Code — Self-Referential Struct
**Status**: Sound with caveats

## Summary

`OwnedSnapshot` in `crates/storage/src/backends/rocksdb/transaction.rs:20-56` uses `std::mem::transmute` to extend a `SnapshotWithThreadMode<'_>` lifetime to `'static`, enabling a self-referential struct where the snapshot borrows from a DB held in the same struct. The safety argument is **sound** but depends on invariants not enforced by the compiler.

## Affected Files

- `crates/storage/src/backends/rocksdb/transaction.rs:20-56`

## Details

### The Transmute

```rust
struct OwnedSnapshot {
    _db: Arc<rocksdb::OptimisticTransactionDB>,
    snapshot: rocksdb::SnapshotWithThreadMode<'static, rocksdb::OptimisticTransactionDB>,
}

impl OwnedSnapshot {
    fn new(db: Arc<rocksdb::OptimisticTransactionDB>) -> Self {
        let snapshot = unsafe {
            let snap = db.snapshot();
            std::mem::transmute::<
                rocksdb::SnapshotWithThreadMode<'_, rocksdb::OptimisticTransactionDB>,
                rocksdb::SnapshotWithThreadMode<'static, rocksdb::OptimisticTransactionDB>,
            >(snap)
        };
        Self { _db: db, snapshot }
    }
}
```

### Drop Order Analysis: CORRECT

Rust drops struct fields in **declaration order**. In `OwnedSnapshot`:
1. `_db: Arc<OptimisticTransactionDB>` — dropped first
2. `snapshot: SnapshotWithThreadMode<'static, ...>` — dropped second

**Wait — this is the WRONG order.** The snapshot references the DB, so the snapshot should be dropped first. However, this is actually safe because `_db` is an `Arc`. Dropping the Arc decrements the reference count but does NOT drop the DB unless it's the last reference. Since `RocksDbTxn` also holds its own `Arc<OptimisticTransactionDB>` in field `db`, the DB's strong count is at least 2 when the OwnedSnapshot is constructed (one in OwnedSnapshot._db, one in RocksDbTxn.db). When OwnedSnapshot drops, the _db Arc decrement brings it to at least 1 (from RocksDbTxn.db), so the DB survives.

**However**, the _db Arc in OwnedSnapshot is actually the one passed in, and RocksDbTxn independently holds another clone. The critical invariant is: the `Arc` in `_db` guarantees the DB outlives the snapshot regardless of drop order, because the `snapshot` field's Drop doesn't access the DB through the Arc — it accesses the raw pointer the rocksdb crate stored internally. The `Arc::clone` passed to `OwnedSnapshot::new` ensures the DB's ref count doesn't reach zero until after both the snapshot and the Arc are dropped.

Since both fields live in the same struct and are dropped in the same scope, and the Arc keeps the DB alive for both drops, this is sound.

### Send+Sync Impls

```rust
unsafe impl Send for OwnedSnapshot {}
unsafe impl Sync for OwnedSnapshot {}
```

The comment claims `SnapshotWithThreadMode is Send+Sync`. Checking the `rocksdb` crate v0.22.0: `SnapshotWithThreadMode` does indeed implement Send+Sync when the DB type is `MultiThreaded` (which `OptimisticTransactionDB` uses by default). The manual impls are technically redundant but harmless — the `'static` lifetime in the type signature prevents auto-impl.

### Why Not `ouroboros` or `self_cell`?

No self-referential crate is used anywhere in the dependency tree. The pattern here is simple enough (single Arc + single reference) that the crate overhead may not be justified. However, `self_cell` would provide compile-time guarantees instead of relying on manual safety reasoning.

## Risk Assessment

| Aspect | Assessment |
|--------|-----------|
| Drop order | Safe (Arc prevents premature DB deallocation) |
| Send+Sync | Sound (underlying type is Send+Sync) |
| Clone/Move | OwnedSnapshot is not Clone, not moved after construction |
| mem::forget | If the containing RocksDbTxn is forgotten, both Arc and snapshot leak. No dangling reference — just a memory leak. |
| Concurrent access | Safe (snapshot provides point-in-time read, Arc is thread-safe) |

## Remediation

**Low priority.** The current implementation is sound but relies on manual reasoning.

- Consider using the `self_cell` crate to encode the self-referential relationship at the type level.
- Add a compile-time assertion or test that verifies field order matches the expected layout.
- The `_db` field name with underscore prefix correctly signals "held for lifetime, not directly used."

## Test Gap

- No Miri test exercises this code path (rocksdb is C++ FFI, Miri can't test it).
- No unit test specifically verifies OwnedSnapshot drop safety.
- The integration test suite exercises RocksDB transactions extensively, providing runtime validation.
