# Schema Migration — No Field Drop or Type Change Guard

**Severity**: LOW
**Category**: Input Validation — Schema Integrity
**Status**: Confirmed

## Summary

Schema versioning uses content-addressed CIDs (not monotonic version numbers), and the migration system tracks `previous_version` links but does not validate backward compatibility. There is no guard preventing a schema update that drops fields, changes field types, or removes indexes — which could corrupt existing documents or break queries.

## Affected Files

- `crates/schema/src/collection.rs` — `CollectionVersion` struct, version ID as content hash
- `crates/schema/src/source.rs` — `CollectionSource` migration tracking
- `crates/schema/src/validation.rs` — cross-collection validation (no migration validation)

## Details

### Version ID Is Content-Addressed

```rust
// collection.rs
pub struct CollectionVersion {
    pub version_id: String,      // Content hash — not a sequential number
    pub collection_id: String,   // Stable across versions
    pub previous_version: Option<CollectionSource>,
}
```

The `version_id` is computed from the collection name, ID, and field IDs. This means:
- Same schema content → same version_id (deterministic)
- No monotonic ordering — you can't tell if version A came before version B by looking at the IDs
- Re-submitting an old schema produces the same old version_id

### No Migration Compatibility Check

The `SchemaValidator` validates cross-collection invariants (unique names, relation consistency) but does **not** validate:
- That fields from a previous version still exist
- That field types haven't changed incompatibly
- That CRDT types are compatible across versions
- That removed fields don't break existing documents

### What Happens on Field Drop

If a schema update removes a field:
1. New `CollectionVersion` is created without the field
2. `previous_version` links to the old version
3. Existing documents still have the field data in storage
4. Queries against the new version silently skip the removed field
5. Old data becomes unreachable but not deleted

### Security Assessment

**Risk is LOW** because:
1. Schema mutations require `CollectionPatch` permission (NAC protected)
2. This is a data integrity issue, not a remote exploit
3. Go DefraDB has the same behavior (no migration guards)
4. Content-addressed versioning prevents undetectable corruption

## Remediation

Add a backward-compatibility check when a schema has a `previous_version`:

```rust
fn validate_migration(old: &CollectionVersion, new: &CollectionVersion) -> Result<()> {
    // Ensure no fields were removed
    for old_field in &old.fields {
        if !new.fields.iter().any(|f| f.name == old_field.name) {
            return Err(SchemaError::FieldDropped(old_field.name.clone()));
        }
    }
    // Ensure field types are compatible
    // ...
}
```

## Test Gap

No test verifies:
- That dropping a field from a schema update is rejected
- That changing a field type (e.g., Int → String) is rejected
- That existing documents are accessible after schema migration
- That re-submitting an old schema version is handled correctly
