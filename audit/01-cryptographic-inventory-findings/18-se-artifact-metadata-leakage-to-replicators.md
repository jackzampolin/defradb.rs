# Finding: SE Artifact Metadata Leakage to Replicators

**Stream**: 01 - Cryptographic Inventory
**Session**: 5 - Searchable Encryption & Merkle Proof
**Severity**: MEDIUM (by design, but under-documented privacy implications)
**Category**: Searchable Encryption / Information Leakage
**Status**: NEW

## Summary

The SE artifact structure transmits collection IDs, field names, document IDs, and search tags in plaintext to replicator nodes. While the search tag itself is opaque (HMAC output), the surrounding metadata reveals the database schema structure, document identifiers, and which fields are encrypted-indexed. A replicator can build a detailed structural map of the data without knowing any field values.

## Evidence

### Artifact Structure — All Metadata in Plaintext

`crates/crypto/src/se/artifact.rs:69-87`:

```rust
pub struct Artifact {
    pub collection_id: String,    // PLAINTEXT
    pub doc_id: String,           // PLAINTEXT
    pub index_id: String,         // PLAINTEXT (= field name)
    pub search_tag: Vec<u8>,      // OPAQUE (16-byte HMAC)
}
```

### Storage Key Format — Metadata in Key Path

`crates/db/src/se/storage.rs:4`:

```
/se/<collectionID>/<indexID>/<searchTagHex>/<docID>
```

Every component of the storage key except the search tag is plaintext. The search tag is hex-encoded but still just a deterministic HMAC tag (see Finding 17).

### Field Name = Index ID

`crates/db/src/se/artifact_gen.rs:58`:

```rust
Ok(Artifact::new(
    collection_id,
    doc_id,
    &enc_idx.field_name, // IndexID is the field name
    tag.to_vec(),
))
```

The `index_id` is set directly to the field name. A replicator receiving artifacts for a "users" collection would see index IDs like `"email"`, `"salary"`, `"ssn"`, revealing exactly which sensitive fields are being encrypted-indexed.

### Replicator Can Correlate Queries to Documents

`crates/db/src/se/storage.rs:77-136` — The `fetch_doc_ids` function performs a prefix scan using the search tag. A replicator serving this query sees:
1. Which collection is being queried
2. Which field (via index_id)
3. The exact search tag (which maps to a specific value)
4. Which documents match

Over multiple queries, the replicator can build a complete query log.

### What a Replicator Learns Without Any Keys

| Information | Source | Risk |
|---|---|---|
| Schema field names | `index_id` in artifacts | Reveals database structure |
| Document existence | `doc_id` in artifacts | Tracks document lifecycle |
| Document-field associations | Artifact presence | Maps which docs have which fields |
| Value equality across documents | Same `search_tag` | Knows when two docs share a value |
| Query patterns | `fetch_doc_ids` calls | Tracks what values are searched |
| Value change history | New/deleted artifacts | Tracks when field values change |

### Go Has the Same Design

This metadata leakage pattern is identical in Go's `internal/se/core/artifact.go` and `internal/se/se.go`. The replicator trust model is shared.

## Impact

### Sensitive Field Name Exposure

If a collection has encrypted indexes on fields named `"diagnosis"`, `"income"`, or `"criminal_record"`, a replicator learns that these sensitive fields exist and can track access patterns to them.

### Complete Document-Value Graph

By correlating search tags across documents and fields, a replicator can build a graph of which documents share values for which fields — without knowing the actual values. Combined with frequency analysis (Finding 17), this can reveal significant information.

### Query Pattern Analysis

A replicator serving SE queries can track what values are being searched for, how often, and by whom (if identity is visible). This is a significant traffic analysis surface.

## Affected Code

- `crates/crypto/src/se/artifact.rs:69-87` — `Artifact` struct with plaintext fields
- `crates/db/src/se/artifact_gen.rs:55-60` — field name used as index_id
- `crates/db/src/se/storage.rs:38-61` — artifact storage with plaintext keys
- `crates/db/src/se/storage.rs:77-136` — query serving reveals access patterns

## Remediation

This is largely inherent to the SE design and shared with Go. Potential mitigations:

1. **Hash the index_id**: Use `HMAC(enc_key, field_name)` as the index_id instead of the plaintext field name. The replicator can still correlate artifacts by index but cannot see the actual field name.

2. **Encrypt the doc_id**: Use a deterministic encryption of the doc_id that can be reversed by the querying client.

3. **Document the trust model**: Clearly document what information replicators can observe, so users can make informed decisions about which nodes to replicate to.

**Note**: Options 1 and 2 would require coordinated Go changes for compatibility.
