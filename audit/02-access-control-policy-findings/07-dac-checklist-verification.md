# Finding: DAC Implementation Checklist Verification

**Stream**: 02 - Access Control Policy
**Severity**: INFORMATIONAL
**Category**: Audit Verification
**Status**: VERIFIED
**Session**: S1 - DAC Implementation Review

## Summary

This finding documents the systematic verification of the DAC implementation checklist from the audit plan. Each item is traced through the code with a pass/fail determination and supporting evidence.

## Checklist Results

### 1. `check_doc_permission()`: returns true when collection has no policy (line 48-51)

**PASS** — Correct behavior.

```rust
// crates/db/src/collection_acp.rs:48-51
let policy = match &collection.policy {
    Some(p) => p,
    None => return Ok(true),  // No policy = no enforcement
};
```

When a collection has no policy, ACP is not enforced, and all operations are allowed. This is the intended "open by default" behavior.

### 2. `register_doc_if_needed()`: only registers if BOTH policy exists AND identity provided (line 78-80)

**PASS** — Correct behavior.

```rust
// crates/db/src/collection_acp.rs:78-80
let (policy, did) = match (&collection.policy, identity) {
    (Some(p), Some(id)) => (p, id),
    _ => return Ok(()),  // No policy or no identity = public document
};
```

Documents created without identity on ACP-protected collections remain public (unregistered). This matches Go DefraDB behavior.

### 3. Unregistered documents pass through both read and write paths

**PASS** — Correct behavior.

In `check_doc_access()` (local.rs:138-143):
```rust
if !self.store.is_doc_registered(resource_name, doc_id).await? {
    return Ok(true);  // Unregistered = public, allow all
}
```

### 4. PermissionFilterNode: fail-closed on error (line 103-112)

**PASS** — Correct behavior.

```rust
// crates/query/src/plan/permission_filter.rs:103-112
.unwrap_or_else(|e| {
    tracing::warn!(..., "Permission check failed, denying access to document");
    false  // Error → deny access (fail-closed)
})
```

### 5. DAC bypass flag: who can set it? Is it test-only?

**PASS** — Correctly gated, not test-only.

The flag is production code, gated behind:
1. NAC must be enabled (`NacStatus::Enabled`)
2. Identity must have `NodePermission::DacBypass` permission
3. Only activated via `should_bypass_dac()` in `crates/acp/src/nac/dac_bypass.rs`

Set from two entry points:
- HTTP: `crates/http/src/query_context.rs:40` (via `resolve_dac_bypass()`)
- FFI: `crates/ffi/src/query/mod.rs:49` (via `check_and_set_dac_bypass()`)

See finding 05 for thread-local safety concerns.

### 6. PermissionFilterNode wraps the FULL SelectNode (including filters, joins, limits)

**PASS** — Correct positioning.

In `plan_with_index_info()` (builder/mod.rs:517-519):
```rust
// Position: after Select/joins/similarity but before GroupBy/Aggregates/OrderBy/Limit.
plan = self.maybe_wrap_with_acp_filter(plan, &collection);
```

ACP filter wraps the full plan (ScanNode → Joins → SelectNode → Similarity), and aggregates/ordering/limit operate on ACP-filtered results. This is correct — aggregates should count only permitted documents.

In `build_plan()` (runner/plan.rs:479-489), the simple path also inserts ACP after Select but before OrderBy/Limit/Aggregates.

### 7. `maybe_wrap_with_acp_filter()`: called for ALL query types?

**PARTIAL PASS** — Called for regular queries and joins, but NOT for:
- `_commits` queries (finding 02 — CRITICAL)
- CID time-travel queries (finding 03 — MEDIUM)
- Encrypted search queries (finding 04 — MEDIUM)
- View collection's own policy (finding 06 — LOW)

For joins, child plans ARE wrapped: `joins/mod.rs:954`, `aggregate_joins.rs:457`, `multi_level.rs:105,229`, `filter_relation.rs:88`.

### 8. No direct blockstore/datastore reads bypass PermissionFilterNode

**PARTIAL PASS** — The standard query path is protected. But:
- `dump.rs` reads all namespaces directly (finding 01)
- CID queries use `fetcher.get_documents_at_cid()` directly (finding 03)
- Recovery mode reads blockstore directly (finding 00)

### 9. Create/update/delete mutations all check permission BEFORE executing

**PASS** — Correct behavior.

In `mutation.rs:182-277`, the mutation handler performs a two-phase ACP check BEFORE building/executing the mutation plan:
- Phase 1: Check Read permission (invisible docs silently removed)
- Phase 2: Check Update/Delete permission (visible but unauthorized returns error)
- CREATE: No pre-check needed; registration happens after creation (lines 426-473)

### 10. _commits bypass: confirm caller_identity is discarded

**CONFIRMED** — See finding 02. The `caller_identity` parameter is available in `execute_select_internal()` but the `_commits` early-return at line 28-29 discards it. `execute_commits_query()` accepts no identity parameter.

### 11. Error messages: do permission denials leak document existence?

**PASS** — Correct behavior.

Read path: `PermissionFilterNode` silently skips unauthorized documents (returns false from `has_read_permission()`), producing empty results — no error message reveals document existence.

Write path: Mutation handler (mutation.rs:390-407) wraps `DocumentNotFound` errors with generic message when ACP is active:
```rust
QueryError::document_not_found("document not found or not authorized to access")
```

Block verify: Returns "missing permission" (block_verify.rs:83) — this DOES leak that the block exists. However, the caller already has the CID (must provide it), so existence is already known.

## Overall Assessment

The core DAC implementation is **sound and well-designed**:
- Fail-closed on errors
- Correct permission hierarchy (owner > relation > anonymous)
- Atomic document registration with TOCTOU protection
- Proper error message masking on the write path
- Join child plans get independent ACP filtering

The gaps are in **bypass paths** (findings 02, 03, 04) where code takes early-return paths before reaching the ACP enforcement layer, and in **defense-in-depth** (findings 00, 01, 05, 06) where secondary code paths lack the same ACP rigor.
