# Field Iteration Order: Deterministic via DAGLink Ordering

**Severity:** Informational
**Category:** Convergence
**Status:** Verified Clean

## Summary

Field merges within a composite block iterate over `block.links` (a `Vec<DAGLink>`, not a HashMap). The links are sorted lexicographically by CID at block creation time. This ordering is deterministic and consistent across all nodes. Additionally, LWW and Counter CRDT merges are order-independent (commutative), so the iteration order does not affect the final merged state.

## Affected Files

- `crates/db/src/merge_handler/composite.rs:216-383` (field iteration via `block.links`)
- `crates/defra-core/src/block.rs:67-93` (links sorted at creation)

## Details

### Link Ordering

```rust
// block.rs:67-93 — links are sorted at block creation
pub fn new(delta: CrdtDelta, heads: Vec<Cid>, links: Vec<DAGLink>) -> Self {
    let mut sorted_links = links;
    sorted_links.sort();  // <-- Ord impl sorts by CID bytes
    // ...
}
```

### Field Iteration

```rust
// composite.rs:216-383 — iterates Vec<DAGLink>, not HashMap
if let Some(links) = &block.links {
    for dag_link in links {
        // Process each linked field block in deterministic CID order
    }
}
```

### Why Order Doesn't Matter

Even if the iteration order were non-deterministic:

1. **LWW merges are independent**: Each LWW merge reads/writes to its own field-specific storage key. Field A's merge does not depend on field B's state.

2. **Counter merges are independent**: Same as LWW — each counter field has its own accumulation storage.

3. **Document reconstruction is order-independent**: `field_values.insert(field_name, value)` uses a `HashMap`, and the final document overlay applies all field values. The insertion order doesn't matter for the final document state.

### One Exception: `HashMap<String, NormalValue>` for field_values

```rust
// composite.rs:198
let mut field_values: HashMap<String, NormalValue> = HashMap::new();
```

This is a `HashMap` whose iteration order is non-deterministic. However, it's only used for `doc.set(field_name, value)` calls, which are field-name-keyed and independent. There is no case where two different field names could map to the same document field.

## Conclusion

Field merge ordering is deterministic (sorted by CID) and order-independent (CRDT commutativity). No convergence risk.
