# Schema Evolution: Unknown Fields Silently Discarded During Cross-Version Merge

**Severity:** Low
**Category:** Data Integrity / Schema Evolution
**Status:** By Design (document)
**Session:** 6 of 6

## Summary

When a node with schema v2 (e.g., fields: name, age, email) sends a document to a node with schema v1 (fields: name, age), the merge handler strips unknown fields before storing the document. The "email" field data is silently discarded. If the v1 node later upgrades to v2, the previously-received "email" data is gone — it must be re-replicated.

## Affected Files

- `crates/db/src/merge_handler/composite.rs` lines 480-495 (standard merge)
- `crates/db/src/merge_handler/composite.rs` lines 1013-1025 (batch merge)

## Details

### Unknown Field Stripping

```rust
// Only store fields that the local collection knows about,
// so cross-version syncs don't leak unknown fields into
// query results.
let known_fields: std::collections::HashSet<&str> = collection
    .schema()
    .fields
    .iter()
    .map(|f| f.name.as_str())
    .collect();
let all_field_names: Vec<String> =
    doc.field_names().map(|s| s.to_string()).collect();
for fname in &all_field_names {
    if !known_fields.contains(fname.as_str()) {
        doc.remove(fname);
    }
}
```

### What Is Preserved

The CRDT-level field blocks (LWW/Counter) are stored in the blockstore and replicated via Bitswap. Even though the document layer discards unknown fields, the underlying DAG blocks remain available. If the node later upgrades its schema, the lensed fetcher system can reconstruct documents from the DAG.

### Asymmetry: Counter vs LWW

- **LWW field for unknown field**: The LWW merge (`process_lww_delta_in_txn`) succeeds at the CRDT layer (bytes stored). Only the document reconstruction discards the field value.
- **Counter field for unknown field**: The counter merge (`process_counter_delta_in_txn`) calls `collection.schema().field_by_name()` — if the field doesn't exist locally, it returns `MergeError::MissingMetadata` (hard error). This is a more severe asymmetry.

### Security Consideration

The field stripping is a correctness feature, not a bug. Storing unknown fields would pollute query results with data the schema doesn't describe, leading to undefined behavior in the query engine.

## Remediation

No code change needed. This matches Go DefraDB's behavior. Document the interaction:
- Unknown LWW fields: CRDT merge succeeds, document layer discards, DAG blocks preserved
- Unknown Counter fields: CRDT merge fails (hard error), entire composite merge may fail

Consider making the counter path softer (skip instead of fail) for better cross-version compatibility.

## Test Gap

No integration test exercises cross-version replication where the sender has a newer schema with additional fields.
