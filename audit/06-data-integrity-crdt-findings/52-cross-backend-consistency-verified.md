# Cross-Backend Consistency Verified

**Severity:** Informational
**Category:** Transaction Correctness / Backend Parity
**Status:** Verified — Consistent

## Summary

All four storage backends (redb, fjall, rocksdb, memory) use the shared `ConflictTracker` from `backends/shared.rs` for write-write conflict detection. None rely on their native OCC mechanisms for conflict detection — instead, they all use the same application-level tracker. This ensures identical conflict detection semantics regardless of backend choice.

## Affected Files

- `crates/storage/src/backends/shared.rs:183-258` (shared ConflictTracker)
- `crates/storage/src/backends/redb/store.rs:149` (creates ConflictTracker)
- `crates/storage/src/backends/fjall/store.rs:122`
- `crates/storage/src/backends/rocksdb/store.rs:131`
- `crates/storage/src/backends/memory/store.rs:29`

## Details

### Consistent Architecture

| Component | redb | fjall | rocksdb | memory |
|-----------|------|-------|---------|--------|
| ConflictTracker | Shared | Shared | Shared | Shared |
| Snapshot | ReadTransaction | fjall::Snapshot | OwnedSnapshot | BTreeMap clone |
| Pending Writes | BTreeMap | BTreeMap | BTreeMap | BTreeMap |
| Commit Write | begin_write/commit | WriteBatch | WriteBatchWithTransaction | RwLock write |
| Active Txn Count | AtomicUsize | AtomicUsize | AtomicUsize | (none) |
| Drop Warning | Yes | Yes | Yes | No |
| Group Commit | Optional | No | No | No |

### Backend-Specific Observations

1. **RocksDB uses `OptimisticTransactionDB` but not its native OCC**: Despite using RocksDB's optimistic transaction DB type, the code creates `WriteBatchWithTransaction` instead of `Transaction`. The native RocksDB OCC is not used; the custom ConflictTracker provides conflict detection. This is consistent with the other backends.

2. **Memory backend lacks `active_txn_count`**: The memory store does not track active transactions. This means there's no `close()` wait-for-transactions logic. Acceptable for a testing-only backend.

3. **Redb's group commit is unique**: Only redb has the group commit optimization. Fjall and rocksdb commit directly. The group commit provides better atomicity between conflict detection and storage write (see finding 50).

### Shared Test Suite

The `backends/test_suite/` module provides backend-agnostic tests that are run against all backends:
- `concurrency.rs`: Concurrent write tests, snapshot isolation tests
- `basic_ops.rs`: CRUD operations
- `callbacks.rs`: Callback execution
- `dropable.rs`: Drop-all semantics

This ensures behavioral parity across backends.

## Remediation

None needed. Backend consistency is well-maintained through shared infrastructure and shared test suites.

## Test Gap

None significant. The shared test suite provides good coverage of cross-backend behavior.
