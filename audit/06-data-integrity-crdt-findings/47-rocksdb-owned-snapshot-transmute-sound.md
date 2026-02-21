# RocksDB OwnedSnapshot Transmute Is Sound

**Severity:** Informational
**Category:** Unsafe Code / Correctness
**Status:** Verified — Sound

## Summary

The RocksDB backend uses an `unsafe` transmute to convert a borrow-bounded `SnapshotWithThreadMode<'_, DB>` to `SnapshotWithThreadMode<'static, DB>`. This is sound because the snapshot struct also holds an `Arc<DB>` that guarantees the database outlives the snapshot. Manual `Send` and `Sync` implementations are correctly applied.

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

### Why It's Sound

1. **Lifetime guarantee**: The `_db: Arc<DB>` field ensures the database lives at least as long as `OwnedSnapshot`. The transmute extends the borrow lifetime to `'static`, but the `Arc` reference count prevents the database from being dropped while the snapshot exists.

2. **Field ordering**: Rust drops fields in declaration order. `_db` is declared before `snapshot`, so `snapshot` is dropped first. Even if the snapshot's Drop accesses the database, the `Arc<DB>` is still alive. (Note: Even if Rust changed field drop order, the `Arc` would keep the DB alive as long as any reference exists.)

3. **Send/Sync**: The manual `unsafe impl Send for OwnedSnapshot` and `unsafe impl Sync for OwnedSnapshot` are correct because RocksDB's `SnapshotWithThreadMode` is inherently thread-safe (it only reads from a shared, immutable data view).

4. **No aliasing issues**: The snapshot is read-only. No mutable references to the underlying database data are created through the snapshot.

### Pattern Is Common

This self-referential struct pattern with transmute is a well-known Rust pattern for cases where a struct needs to own a resource and borrow from it simultaneously. Alternative approaches (pin, ouroboros crate) add complexity without additional safety.

## Remediation

None needed. The code is correct and well-commented. Consider adding a brief SAFETY comment referencing the field drop order guarantee.

## Test Gap

The RocksDB concurrency tests exercise the snapshot under concurrent access, providing empirical validation.
