# Finding: SE Artifact Storage Key Reveals Document-Tag Associations in Plaintext

**Stream**: 06 - Data Integrity & CRDT Correctness
**Session**: 4 - Searchable Encryption Deep-Dive
**Severity**: MEDIUM (inherent to design, but full document-field-value graph exposed to storage-level adversary)
**Category**: Searchable Encryption / Storage Leakage
**Status**: ACKNOWLEDGED (shared with Go)

## Summary

SE artifacts are stored with keys of the form `/se/{collectionID}/{indexID}/{searchTagHex}/{docID}`. A storage-level adversary (with read access to the raw database) can enumerate all SE keys and reconstruct the complete document-tag association graph: which documents share values, how many unique values each field has, and which documents match any given tag. The tag itself is opaque, but all surrounding metadata is plaintext.

## Evidence

### Storage Key Structure

`crates/storage/src/keys/datastore/misc.rs:48-56`:

```rust
impl Key for DatastoreSE {
    fn bytes(&self) -> Vec<u8> {
        let search_tag_hex = hex::encode(&self.search_tag);
        let s = format!(
            "/se/{}/{}/{}/{}",
            self.collection_id, self.index_id, search_tag_hex, self.doc_id
        );
        s.into_bytes()
    }
}
```

### Values Are Empty — Key IS the Data

`crates/db/src/se/storage.rs:56-57`:

```rust
// Value is empty - presence of key indicates match
store.set(&key.bytes(), &[]).await?;
```

### What a Storage-Level Adversary Learns

By iterating the `/se/` prefix:

| Enumeration | Query | Information Gained |
|---|---|---|
| All tags for a doc | Prefix `/se/{col}/{idx}/` → scan for `{docID}` | All encrypted-indexed field values for a document |
| All docs for a tag | Prefix `/se/{col}/{idx}/{tag}/` | All documents sharing a specific value |
| All unique tags per field | Prefix `/se/{col}/{idx}/` → distinct tags | Cardinality of unique values per field |
| Tag distribution | Count docs per tag | Frequency distribution (combined with Finding 01-17) |

### Query Evaluation Reveals Access Patterns

`crates/db/src/se/storage.rs:90-101`:

```rust
let prefix_key = DatastoreSE::new(
    collection_id,
    &query.index_id,
    query.search_tag.clone(),
    "", // Empty doc_id for prefix scan
);
```

The prefix scan reveals which tag is being searched (and therefore which value is being queried).

### No Tag Comparison Timing Protection

`crates/db/src/se/storage.rs:98-113` — Tag matching is done via key prefix scan, not tag comparison. The storage engine's B-tree/LSM lookup is not constant-time, but this is acceptable since the tags themselves are not secret (they're HMAC outputs, not plaintext values). Timing of prefix scans doesn't reveal the HMAC key.

## Impact

### Document-Field-Value Graph Fully Visible

A replicator node (the primary consumer of SE artifacts) has complete visibility into:
- Schema structure (field names visible as `index_id`)
- Document identity (doc_id in plaintext)
- Value equality relationships (same tag = same value)
- Query patterns (which tags are looked up)

This is a known and documented design trade-off (see Stream 1 Finding 18). The SE scheme provides query functionality to replicators while keeping actual values hidden, but reveals structural metadata.

### This Is Go-Compatible Behavior

Go's `internal/se/se.go` uses an identical storage key format. This is a shared design decision, not a Rust-specific issue.

## Affected Code

- `crates/storage/src/keys/datastore/misc.rs:48-56` — key format
- `crates/db/src/se/storage.rs:47-61` — artifact storage
- `crates/db/src/se/storage.rs:77-136` — artifact query

## Remediation

No code change needed for 1.0 — this is inherent to the SE design. See Stream 1 Finding 18 for longer-term mitigations (hashed index_id, encrypted doc_id).

## Cross-References

- Finding 01-17: Deterministic tags enable frequency analysis (INFORMATIONAL)
- Finding 01-18: SE artifact metadata leakage to replicators (MEDIUM)
