# Counter Nonces Survive Deletion — Resurrection Semantics Correct

**Severity:** Informational (Verified Clean)
**Category:** Data Integrity / CRDT Correctness
**Status:** Verified
**Session:** 6 of 6

## Summary

Counter nonces are NOT cleared when a document is deleted. This is the correct behavior — it prevents duplicate counter increments from being re-applied if the document is later resurrected via a higher-priority create operation.

## Affected Files

- `crates/crdt/src/counter.rs` lines 290-308 (nonce persistence)
- `crates/db/src/merge_handler/composite.rs` lines 401-447 (deletion path)
- `crates/db/src/collection/index_ops.rs` lines 111-148 (`delete_with_indexes`)
- `crates/db/src/block_builder/write.rs` lines 382-472 (delete block construction)

## Details

### Deletion Mechanics

Document deletion is a **soft delete** implemented via:

1. A deletion marker key `/del/{collection_id}/{doc_id}` with value `[0x01]`
2. A composite block with `status: 2` in the DAG
3. Index entries removed via `index_manager.on_document_delete()`

What is NOT cleared on deletion:
- Document data at `/d/{collection_id}/{doc_id}` (persists)
- Counter CRDT value at `/data/{schema_version}/{doc_id}/{field}` (persists)
- Counter nonces at `/data/{schema_version}/{doc_id}/{field}/nonces/*` (persists)
- LWW priority at `/data/{schema_version}/{doc_id}/{field}/priority` (persists)

### Resurrection Path

When a deleted document receives a new composite block with `status: 1` and higher priority:

1. The merge handler stores the document content (line 498: `collection.save_with_datastore`)
2. The deletion marker is NOT explicitly removed — but the document becomes visible because the highest-priority composite determines final status
3. Counter nonces from pre-deletion operations remain, preventing double-counting

### Why Nonce Persistence Is Correct

Consider this scenario:
1. Node A creates document with counter field, increment +5 (nonce=100)
2. Node A deletes the document
3. Node B (which received the +5 increment before the delete) sends the same +5 increment
4. If nonces were cleared on delete, the +5 would be applied again → counter = 10 instead of 5

By preserving nonces through deletion, the counter value remains correct after resurrection.

## Security Assessment

The nonce persistence design is sound for CRDT correctness. The trade-off (nonce storage never freed) is documented in Finding 06.

## Test Gap

No integration test exercises the full delete → receive-old-increment → resurrect → verify-counter-value sequence. The LWW resurrection test (`test_lww_deletion_resurrection_with_priority`) exists but no counter-specific resurrection test exists.
