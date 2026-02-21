# No Document Size Limit — Single Field Can Be Multi-GB

**Severity:** Low
**Category:** Resource Exhaustion / Input Validation
**Status:** Confirmed
**Session:** 6 of 6

## Summary

There is no limit on individual document size or field value size at the CRDT or document layer. The only size constraint is at the storage backend level (redb: 3 GiB max value, datastore: 256 MB chunking limit). A malicious peer or client could create documents with arbitrarily large field values, consuming excessive memory during merge operations.

## Affected Files

- `crates/crdt/src/lww.rs` — no size validation on `data` field
- `crates/db/src/merge_handler/lww.rs` — no size check before processing
- `crates/db/src/merge_handler/composite.rs` — loads full field values into memory
- `crates/storage/src/stores/datastore.rs` line 64 ("max 256MB")
- `crates/storage/src/backends/redb/errors.rs` line 84 ("3 GiB")

## Details

### No Size Validation in CRDT Layer

```rust
// lww.rs — LwwDelta accepts arbitrary data
pub fn new(doc_id: Vec<u8>, field_name: String, priority: u64,
           schema_version_id: String, data: Vec<u8>) -> Result<Self> {
    // Validates doc_id, field_name, schema_version_id are non-empty
    // NO validation on data.len()
    Ok(Self { doc_id, field_name, priority, schema_version_id, data })
}
```

### Memory Impact During Merge

The composite merge handler loads all field values into a `HashMap<String, NormalValue>`:

```rust
// composite.rs:198
let mut field_values: HashMap<String, NormalValue> = HashMap::new();
// ...
field_values.insert(lww_payload.field_name.clone(), value);
```

A document with 100 fields each containing 10 MB of data would require ~1 GB of memory during merge.

### Storage Backend Limits

| Backend | Max Single Value | Mechanism |
|---------|-----------------|-----------|
| redb | 3 GiB | Hard error from redb |
| fjall | No explicit limit | Writes to LSM tree |
| rocksdb | No explicit limit | Writes to LSM tree |
| memory | No limit | HashMap |
| datastore | 256 MB (chunked) | 1 MB chunks, max 256 |

### LWW Test Confirms No Limit

`test_lww_large_payload` in `lww_tests.rs` successfully stores a 10 MB payload. No test exists for 100 MB+ payloads.

## Remediation

Add a configurable max field value size at the merge handler level:

```rust
const MAX_FIELD_VALUE_SIZE: usize = 16 * 1024 * 1024; // 16 MB
if payload.data.len() > MAX_FIELD_VALUE_SIZE {
    return Err(MergeError::MergeFailed("field value exceeds size limit"));
}
```

This is defense-in-depth — the P2P message size limit (16 MB, `MAX_MESSAGE_SIZE`) provides partial protection, but a composite block with many linked field blocks can exceed this per-message limit.

## Test Gap

No test exercises merge behavior with very large documents (100+ MB). No test verifies graceful degradation when storage backend size limits are hit.
