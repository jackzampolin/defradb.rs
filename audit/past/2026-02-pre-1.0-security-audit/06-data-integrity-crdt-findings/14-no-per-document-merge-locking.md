# No Per-Document Merge Locking

**Severity:** Medium
**Category:** Concurrency / Data Integrity
**Status:** Confirmed

## Summary

The merge handler has no per-document locking. Multiple concurrent merge operations for the same document can interleave. The `run_parallel` replication mode allows up to 32 concurrent merge tasks (configurable via `max_workers`). Two concurrent merges for the same document can both read the existing document, compute field updates independently, and the second commit can overwrite the first's document state, losing updates.

## Affected Files

- `crates/db/src/merge_handler/composite.rs:164-565` (document read-modify-write without lock)
- `crates/p2p/src/sync/replication/loop_runner.rs:136-179` (`run_parallel` spawns concurrent tasks)
- `crates/p2p/src/sync/replication/config.rs:13` (`max_workers: 32`)

## Details

### Concurrent Merge Race

The composite merge follows a read-modify-write pattern on the document:

```rust
// composite.rs:455-498
// Step 1: Read existing document
let (mut doc, old_doc) = match collection
    .get_with_datastore(&datastore, &doc_id).await
{
    Ok(Some(existing)) => (existing.clone(), Some(existing)),
    _ => (Document::new(), None),
};

// Step 2: Overlay new field values
for (field_name, value) in &field_values {
    doc.set(field_name, value.clone());
}

// Step 3: Save
collection.save_with_datastore(&datastore, &doc).await
```

If two concurrent merges (M1, M2) run for the same document:

```
M1: reads doc = {name: "Alice", age: 30}
M2: reads doc = {name: "Alice", age: 30}
M1: sets name = "Bob", saves {name: "Bob", age: 30}
M2: sets age = 31, saves {name: "Alice", age: 31}  ← M1's "Bob" is lost
```

### Mitigating Factors

1. **CRDT field merges ARE safe**: The LWW and Counter merges at the CRDT layer use their own storage keys per field and handle concurrent writes correctly via priority-based conflict resolution. The CRDT state is always correct.

2. **Document state is a denormalized view**: The document saved at `save_with_datastore` is a convenience cache for query results, not the authoritative CRDT state. Re-reading from CRDT storage would reconstruct the correct document. The risk is that queries return stale data until the next merge or query reconstruction.

3. **Sequential batch mode avoids this**: The `ReplicationLoop::run` method processes events sequentially (one batch at a time), not concurrently. Only `run_parallel` exposes this race.

4. **Index updates also race**: The `on_document_update` / `on_document_create` index operations use the same read-modify-write pattern, so index entries can also be inconsistent.

### Go Comparison

Go serializes merge events per-collection using a channel-based event queue (`mergeQueue`). Each collection's merges are processed sequentially, preventing this race. Rust's `run_parallel` mode does not have this per-collection serialization.

## Remediation

Add per-document locking using a sharded lock map:

```rust
use dashmap::DashMap;
use tokio::sync::Mutex;

struct MergeLockManager {
    locks: DashMap<String, Arc<Mutex<()>>>,
}

impl MergeLockManager {
    fn lock_for_doc(&self, doc_id: &str) -> Arc<Mutex<()>> {
        self.locks
            .entry(doc_id.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
}
```

Alternatively, match Go's approach: serialize merge events per-collection using a per-collection channel.

## Test Gap

No test exercises concurrent merges for the same document:
- Unit test: two concurrent composite merges for same document, verify both field changes are reflected
- Integration test: parallel P2P merge of conflicting updates, verify convergence
