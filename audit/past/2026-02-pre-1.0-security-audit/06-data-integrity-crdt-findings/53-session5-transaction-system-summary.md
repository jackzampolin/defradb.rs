# Session 5: Transaction System Audit Summary

**Stream:** Data Integrity & CRDT Correctness
**Session:** 5 of 6
**Focus:** MVCC snapshot isolation, write-write conflict detection, transaction lifecycle safety
**Date:** 2026-02-21

## Scope

Audited the transaction layer across all four storage backends (redb, fjall, rocksdb, memory), the ConflictTracker conflict detection system, the group commit optimization, the DB-layer transaction lifecycle (DbTxn, DbTransactionRegistry), and the merge handler's transactional behavior.

## Files Audited

### Storage Layer
- `crates/storage/src/backends/shared.rs` — CallbackManager, ConflictTracker
- `crates/storage/src/backends/redb/transaction.rs` — Redb transaction (default backend)
- `crates/storage/src/backends/redb/store.rs` — Redb store, new_txn, close
- `crates/storage/src/backends/redb/group_commit.rs` — Group commit flush loop
- `crates/storage/src/backends/fjall/transaction.rs` — Fjall transaction
- `crates/storage/src/backends/fjall/store.rs` — Fjall store
- `crates/storage/src/backends/rocksdb/transaction.rs` — RocksDB transaction + OwnedSnapshot
- `crates/storage/src/backends/rocksdb/store.rs` — RocksDB store
- `crates/storage/src/backends/memory/transaction.rs` — Memory transaction
- `crates/storage/src/backends/memory/store.rs` — Memory store
- `crates/storage/src/corekv/traits.rs` — Txn trait, callback types
- `crates/storage/src/backends/test_suite/concurrency.rs` — Concurrency test suite

### DB Layer
- `crates/db/src/txn.rs` — DbTxn wrapper
- `crates/db/src/txn_context.rs` — Transaction context for queries
- `crates/db/src/txn_registry.rs` — Transaction registry for HTTP API
- `crates/db/src/txn_registry_tests.rs` — Transaction registry tests
- `crates/db/src/database.rs` — DB struct, new_txn, with_txn
- `crates/db/src/merge_handler/mod.rs` — Merge handler (transactional)
- `crates/db/src/merge_handler/batch.rs` — Batch merge (shared transaction)
- `crates/db/src/auto_commit_mutator/create.rs` — Document create flow

### Integration Tests
- `tools/integration-test/tests/transactions.rs` — Transaction integration tests

## Findings Summary

| # | Finding | Severity | Status |
|---|---------|----------|--------|
| 41 | ConflictTracker GC misses conflicts for long-running transactions | **Medium** | Confirmed |
| 42 | Memory backend marks committed before applying changes | Low | Confirmed |
| 43 | Conflict check not atomic with storage write (direct commit) | Low | Confirmed |
| 44 | No transaction timeout or concurrent transaction limit | **Medium** | Confirmed |
| 45 | Drop does not execute discard callbacks | Low | Confirmed — By Design |
| 46 | Write skew possible — documented trade-off | Informational | Accepted |
| 47 | RocksDB OwnedSnapshot transmute is sound | Informational | Verified |
| 48 | Snapshot isolation verified across all backends | Informational | Verified |
| 49 | Index-document atomicity verified | Informational | Verified |
| 50 | Group commit conflict detection correctly atomic | Informational | Verified |
| 51 | Callback panic safety verified | Informational | Verified |
| 52 | Cross-backend consistency verified | Informational | Verified |

## Key Architectural Strengths

1. **Uniform ConflictTracker**: All backends share the same conflict detection code, eliminating backend-specific concurrency bugs.

2. **Group commit optimization**: The redb group commit coalesces multiple transactions into single storage writes, with conflict detection correctly serialized in the flush loop.

3. **Comprehensive test suite**: The shared `test_suite/concurrency.rs` provides snapshot isolation, write-write conflict, and stress tests that run against all backends.

4. **Callback panic safety**: All callback execution is wrapped in catch_unwind, preventing callback panics from corrupting transaction state.

5. **Atomic document+index operations**: All mutation flows (create, update, delete, merge) keep document writes and index updates within the same transaction.

## Key Risks

1. **ConflictTracker GC (#41)**: The most significant finding. Under sustained high throughput (>1000 committed transactions while a long-running transaction is open), conflict detection accuracy degrades. The threshold is not configurable. This could theoretically allow a write-write conflict to go undetected in the Shinzo indexer's high-throughput path.

2. **No transaction limits (#44)**: A DoS vector exists via the HTTP API where a client can open unbounded transactions. While `cleanup_stale_transactions()` exists, it's not automatically scheduled.

## Checklist Coverage

| Checklist Item | Result |
|----------------|--------|
| 1. Snapshot isolation correctness | Verified — all backends correct (#48) |
| 2. Buffered writes ordering | Verified — tombstones correct (#48) |
| 3. Write-write conflict detection | Confirmed — GC issue (#41) |
| 4. ConflictTracker GC | **Medium risk** — can miss conflicts (#41) |
| 5. Write skew | Accepted trade-off (#46) |
| 6. Drop safety | Verified — by design (#45) |
| 7. Transaction timeout | **Missing** — no timeout or limit (#44) |
| 8. Cross-backend consistency | Verified (#52) |
| 9. Index-document atomicity | Verified (#49) |
| 10. Callback execution | Verified — panic-safe (#51) |
| 11. Concurrent stress | Verified — test suite exists |
