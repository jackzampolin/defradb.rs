# merged_composites HashSet Grows Unboundedly

**Severity:** Medium
**Category:** Resource Exhaustion / Memory Leak
**Status:** Confirmed

## Summary

The `merged_composites: Mutex<HashSet<Cid>>` field in `DbMergeHandler` is never cleared. It accumulates every composite CID ever merged for the entire lifetime of the node process. Over long-running operation with millions of documents, this set grows without bound, consuming memory proportional to total document mutation count.

## Affected Files

- `crates/db/src/merge_handler/mod.rs:114` (field declaration)
- `crates/db/src/merge_handler/mod.rs:124` (initialization, never cleared)
- `crates/db/src/merge_handler/composite.rs:664` (insert on success)
- `crates/db/src/merge_handler/batch.rs:124` (batch insert)

## Details

### Growth Pattern

```rust
// mod.rs:114 — lives for the duration of the node
merged_composites: std::sync::Mutex<HashSet<Cid>>,

// composite.rs:664 — insert after every successful composite merge, never removed
{
    let mut merged = self.merged_composites.lock().unwrap();
    merged.insert(*cid);
}

// batch.rs:124 — batch insert, never removed
{
    let batch = batch_merged.lock().unwrap();
    let mut merged = self.merged_composites.lock().unwrap();
    merged.extend(batch.iter());
}
```

### Memory Analysis

Each `Cid` in a `HashSet` consumes approximately:
- CID itself: ~36 bytes (CIDv1 with SHA2-256)
- HashSet bucket overhead: ~48 bytes (hash, pointer, metadata)
- Total: ~84 bytes per entry

For a moderately active node:
- 1,000 documents x 100 mutations each = 100,000 CIDs = ~8.4 MB
- 100,000 documents x 100 mutations = 10M CIDs = ~840 MB
- Active indexer processing all SourceHub transactions: potentially millions of CIDs/day

### Comparison with Blockstore Caches

The blockstore uses bounded LRU caches (`DEFAULT_BLOCK_CACHE_SIZE = 1_000_000`, `DEFAULT_MERGED_CACHE_SIZE = 100_000`). The `merged_composites` set has no such bound.

### Go Comparison

Go's `loadComposites` dedup is per-processLog-call scoped (it's a local map created in `db.handleMergeEvent`), not a node-lifetime set. The Rust implementation inadvertently changed the scope from per-merge to per-node.

## Remediation

Replace `HashSet` with a bounded LRU cache, matching the blockstore's pattern:

```rust
use lru::LruCache;

merged_composites: parking_lot::Mutex<LruCache<Cid, ()>>,

// In new():
merged_composites: parking_lot::Mutex::new(LruCache::new(
    NonZeroUsize::new(100_000).unwrap()
)),
```

Alternatively, match Go's scoping: create `merged_composites` per top-level merge invocation rather than as a node-lifetime field. This would require passing it as a parameter through the recursive calls.

## Test Gap

No long-running test that monitors memory growth over thousands of merges.
