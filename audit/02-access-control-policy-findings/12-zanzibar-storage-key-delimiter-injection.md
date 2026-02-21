# Finding: Zanzibar Storage Key Lacks Delimiter Sanitization

**Stream**: 02 - Access Control Policy
**Severity**: LOW
**Category**: Input Validation
**Status**: CONFIRMED
**Session**: S2 - NAC and Zanzibar Evaluation

## Summary

Zanzibar relationship storage keys use unsanitized `/`-delimited format: `/rel/{resource}/{object_id}/{relation}/{subject_hash}`. Neither the `Relationship` struct nor the `PersistentZanzibarStore` validate that resource names, object IDs, or relation names are free of the `/` delimiter character. A crafted resource name containing `/` could cause key prefix collisions, potentially making unrelated relationship lookups return incorrect results.

## Affected Files

| File | Line | Issue |
|------|------|-------|
| `crates/zanzibar/src/types/relationship.rs` | 39-55 | `storage_key()`, `object_prefix()`, `relation_prefix()` — no sanitization |
| `crates/acp/src/zanzibar/store/persistent.rs` | 44-62 | Store methods build keys with unsanitized components |

## Details

### The Key Format

```rust
// crates/zanzibar/src/types/relationship.rs:39-55
pub fn storage_key(&self) -> String {
    format!("/rel/{}/{}/{}/{}", self.resource, self.object_id, self.relation, self.subject.storage_hash())
}

pub fn object_prefix(resource: &str, object_id: &str) -> String {
    format!("/rel/{}/{}/", resource, object_id)
}

pub fn relation_prefix(resource: &str, object_id: &str, relation: &str) -> String {
    format!("/rel/{}/{}/{}/", resource, object_id, relation)
}
```

### Collision Example

Consider two relationships:
- Resource `foo`, object `bar`, relation `reader` → key: `/rel/foo/bar/reader/{hash}`
- Resource `foo/bar`, object `reader`, relation `x` → key: `/rel/foo/bar/reader/x/{hash}`

A prefix scan for `/rel/foo/bar/reader/` would match both, even though they belong to different resources.

### Why This Matters for DAC

In the `ZanzibarDocumentACP`, the `add_actor_relationship` method passes `collection_id` as both the policy ID and resource name:

```rust
// crates/acp/src/zanzibar/acp/document_acp.rs:178
self.ensure_policy(collection_id, collection_id).await?;
```

Collection names come from SDL schema definitions and are user-controlled. While collection names are typically validated at the schema layer (alphanumeric), the Zanzibar layer itself has no validation.

### Practical Exploitability

Low. Collection/resource names pass through schema validation which restricts them to alphanumeric identifiers. But:
1. There's no defense-in-depth at the storage layer
2. Direct Zanzibar store usage (NAC, programmatic API) may not go through schema validation
3. The `_disabled` sentinel relation is stored the same way and contains no special characters, but demonstrates that internal code assumes safe names

### Subject Hash Mitigation

The subject field uses `storage_hash()` which produces a 16-hex-char hash, avoiding delimiter issues for the subject component. But resource, object_id, and relation are stored raw.

### Severity Rationale

LOW because:
1. Schema validation at higher layers typically prevents dangerous names
2. No known path to inject `/` through the HTTP API
3. But the lack of defense-in-depth means future code changes could introduce vulnerabilities
4. The fix is straightforward — validate or encode components

## Remediation

Add validation in `Relationship::new()` or `storage_key()`:

```rust
fn validate_component(s: &str, name: &str) -> Result<(), Error> {
    if s.contains('/') || s.contains('\0') {
        return Err(Error::InvalidInput(format!("{} must not contain '/' or null: '{}'", name, s)));
    }
    Ok(())
}
```

Alternatively, encode components using percent-encoding or base64 before building keys.
