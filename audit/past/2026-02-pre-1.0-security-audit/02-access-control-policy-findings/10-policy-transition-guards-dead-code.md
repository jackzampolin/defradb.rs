# Finding: Policy Transition Safety Guards Are Dead Code

**Stream**: 02 - Access Control Policy
**Severity**: MEDIUM
**Category**: Missing Safety Check
**Status**: CONFIRMED
**Session**: S2 - NAC and Zanzibar Evaluation

## Summary

`block_unsafe_policy_transition()` and `warn_on_unsafe_policy_transition()` are defined in `collection_acp.rs`, exported from the `db` crate, and thoroughly unit-tested — but never called from any production code path. Schema updates that change or remove a collection's ACP policy proceed silently without any warning or blocking, even when the transition would expose previously protected documents.

## Affected Files

| File | Line | Issue |
|------|------|-------|
| `crates/db/src/collection_acp.rs` | 289-305 | `warn_on_unsafe_policy_transition()` — defined, never called |
| `crates/db/src/collection_acp.rs` | 334-369 | `block_unsafe_policy_transition()` — defined, never called |
| `crates/db/src/lib.rs` | 106-107 | Both functions exported but unused |
| `crates/http/src/handlers/schema.rs` | 24-39 | `add_schema()` — no transition check |
| `crates/http/src/handlers/collections.rs` | 115 | `patch_collection()` — no transition check |

## Details

### The Dead Code

```rust
// crates/db/src/collection_acp.rs:334-369
pub fn block_unsafe_policy_transition(
    collection_name: &str,
    old_policy: Option<&schema::PolicyDescription>,
    new_policy: Option<&schema::PolicyDescription>,
    force: bool,
) -> crate::Result<()> {
    // ... well-implemented logic that is never reached
}
```

Grep for all callers outside of test files:

```
crates/db/src/collection_acp.rs:334  — definition
crates/db/src/lib.rs:106             — export
crates/db/tests/...                  — tests only
```

No production call site exists.

### What's Supposed to Be Guarded

The function correctly identifies three dangerous transitions:

1. **Protected → Open** (removing a policy): All previously protected documents become public
2. **Resource Name Change**: Orphans existing ACP tuples, documents become effectively public
3. **Policy ID Change** (same resource name): May affect permission evaluation

### The Gap

When an admin patches a schema to remove a collection's ACP policy:

```bash
# Remove policy from Users collection
curl -X PATCH http://node:9181/api/v0/schema -d '
type Users { name: String }'
# Previously: type Users @policy(id: "...", resource: "users") { name: String }
```

This removes ACP protection from all existing documents — silently, with no warning, no confirmation, and no audit log entry.

### Severity Rationale

MEDIUM because:
1. The safety functions exist and are correct — they just need to be wired in
2. Only admin-level operations (schema patches) trigger this — requires `CollectionPatch` NAC permission
3. But the impact is significant: all documents in the collection lose protection
4. An admin may not realize that a schema change has security implications

## Remediation

Wire `block_unsafe_policy_transition()` into the schema update path. Before applying a schema change that modifies a collection's policy, call:

```rust
block_unsafe_policy_transition(
    &collection_name,
    old_collection.policy.as_ref(),
    new_collection.policy.as_ref(),
    force_flag, // from request parameter
)?;
```

This should be integrated at the point where schema SDL is processed and collections are created/updated — likely in the `SchemaOperations::add_schema()` implementation.
