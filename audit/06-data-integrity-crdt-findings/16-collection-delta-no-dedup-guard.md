# Collection Delta Recursive Processing Has No Dedup Guard

**Severity:** Low
**Category:** Resource Exhaustion / Redundant Work
**Status:** Confirmed

## Summary

Unlike `process_composite_delta` which uses `merged_composites` to prevent re-processing duplicate CIDs from dual broadcast, `process_collection_delta` has NO dedup guard. If the same collection block CID arrives via multiple paths (doc topic + collection topic, or multiple GossipSub propagation paths), the collection handler processes it fully each time. Each processing triggers recursive parent traversal and linked composite merging. The composites have their own dedup, so the CRDT state is correct, but the redundant collection-level processing wastes resources.

## Affected Files

- `crates/db/src/merge_handler/collection.rs:13-244` (no dedup check)
- `crates/db/src/merge_handler/composite.rs:101-114` (has dedup check for comparison)

## Details

### Missing Dedup in Collection Handler

```rust
// collection.rs:13-73 — NO dedup check before recursive processing
pub(crate) async fn process_collection_delta(
    &self,
    cid: &Cid,
    block: &Block,
    payload: &CollectionDeltaPayload,
    metadata: &BlockMetadata<'_>,
) -> Result<MergeOutcome, MergeError> {
    // Immediately recurses into parent blocks — no check if this CID was already processed
    if let Some(heads) = &block.heads {
        for head_cid in heads {
            // ... load and recursively process parent collection blocks ...
        }
    }
    // ... process linked composites ...
}
```

### Composite Handler Comparison

```rust
// composite.rs:101-114 — HAS dedup check
if let Some(heads) = &block.heads {
    for head_cid in heads {
        {
            let merged = self.merged_composites.lock().unwrap();
            if merged.contains(head_cid) {
                continue;  // <-- DEDUP: skip already-processed parent
            }
        }
        // ... process parent ...
    }
}
```

### Impact

- Redundant blockstore reads for parent collection blocks
- Redundant composite merge attempts (mitigated by composite dedup)
- Redundant headstore writes for collection heads (idempotent, no data corruption)
- Redundant transaction creation and commit overhead
- Under adversarial conditions, sending the same collection block CID N times triggers N full processing passes

### Wasted Work Estimation

For a collection block with M parent blocks and L linked composites:
- Without dedup: each arrival triggers M blockstore reads + L composite merge attempts + 1 headstore transaction
- With dedup: second arrival would skip entirely

## Remediation

Add a `merged_collections` dedup set analogous to `merged_composites`:

```rust
// In DbMergeHandler:
merged_collections: std::sync::Mutex<LruCache<Cid, ()>>,

// In process_collection_delta:
{
    let merged = self.merged_collections.lock().unwrap();
    if merged.contains(cid) {
        return Ok(MergeOutcome::skipped("collection already merged"));
    }
}
```

## Test Gap

No test sends the same collection block via dual broadcast and verifies dedup behavior.
