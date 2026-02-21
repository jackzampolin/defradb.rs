# Index-Document Atomicity Verified

**Severity:** Informational
**Category:** Transaction Correctness / Data Integrity
**Status:** Verified — Correct

## Summary

Document mutations (create, update, delete) and their corresponding index updates happen within the same transaction. Block writes to the blockstore and headstore also occur in the same transaction. The `commit()` at the end atomically persists all changes. A crash between individual writes within the transaction cannot leave index-document inconsistency because uncommitted writes are lost atomically.

## Affected Files

- `crates/db/src/auto_commit_mutator/create.rs:43-77` (create + index in same txn)
- `crates/db/src/collection/crud.rs` (CRUD operations)
- `crates/db/src/collection/index_ops.rs` (index operations)
- `crates/db/src/merge_handler/batch.rs:68-137` (batch merge: shared txn)

## Details

### Auto-Commit Create Flow

```rust
// auto_commit_mutator/create.rs
let txn = self.db.new_txn(false).await?;  // Single transaction

// Step 1: Create document + update indexes (same txn)
collection.create_with_indexes(&datastore, &doc, &index_manager, id_was_generated).await?;

// Step 2: Write blocks to blockstore/headstore (same txn)
write_document_blocks(&blockstore, &headstore, &doc, ...)?;

// Step 3: Commit everything atomically
txn.commit().await?;
```

### Batch Merge Flow

```rust
// merge_handler/batch.rs
let txn = self.db.new_txn(false).await?;
// All blocks processed using NamespaceViews from the same txn
for block in blocks {
    self.process_block_in_txn(&datastore, &headstore, ...).await?;
}
// Single atomic commit
txn.force_commit().await?;
```

### Error Handling

If any step fails, the transaction is discarded:
```rust
Err(e) => {
    if let Err(discard_err) = txn.discard() { /* log */ }
    Err(e)
}
```

The `DB::with_txn()` and `DB::with_txn_async()` helpers also ensure proper discard on error.

### Crash Safety

- **Before commit**: All pending changes are in the BTreeMap. A crash loses everything atomically. No partial state.
- **During commit (redb)**: Redb uses WAL (write-ahead log). Either all changes are persisted or none are.
- **During commit (fjall)**: Fjall uses WAL. Atomic commit via WriteBatch.
- **During commit (rocksdb)**: WriteBatch provides atomic commit.
- **After commit**: All changes are durable (modulo the DurabilityMode setting for Eventual mode, which risks OS-crash data loss but not process-crash).

## Remediation

None needed. Index-document atomicity is correctly maintained through single-transaction semantics.

## Test Gap

- The integration tests exercise create/update/delete operations and verify results, implicitly testing atomicity
- No explicit crash-recovery test that verifies index-document consistency after simulated crashes
