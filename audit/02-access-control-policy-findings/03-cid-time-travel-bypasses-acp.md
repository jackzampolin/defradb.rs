# Finding: CID Time-Travel Queries Bypass ACP

**Stream**: 02 - Access Control Policy
**Severity**: MEDIUM
**Category**: Access Control Bypass
**Status**: CONFIRMED
**Session**: S1 - DAC Implementation Review

## Summary

CID-based time-travel queries (`query { Users(cid: "bafy...") { ... } }`) bypass ACP entirely. The `execute_cid_query_with_version()` function deliberately ignores the `_caller_identity` parameter (underscore-prefixed, Rust convention for "intentionally unused"). Any user can reconstruct any document at any historical state by providing its CID.

## Affected Files

| File | Line | Issue |
|------|------|-------|
| `crates/query/src/runner/version.rs` | 27-31 | `_caller_identity` parameter unused |
| `crates/query/src/runner/version.rs` | 44-58 | `get_documents_at_cid()` called without ACP |
| `crates/query/src/runner/version.rs` | 121-159 | Nested relation lookups also bypass ACP |
| `crates/query/src/runner/query/select.rs` | 44-48 | CID queries route to this bypass path |

## Details

### The Bypass

```rust
// crates/query/src/runner/version.rs:27-31
pub(crate) async fn execute_cid_query_with_version(
    &self,
    select: &Select,
    fetcher: &dyn DocFetcher,
    _caller_identity: Option<Did>,  // DELIBERATELY UNUSED
    version_selection: Option<&Select>,
) -> Result<JsonValue> {
```

The function:
1. Calls `fetcher.get_documents_at_cid()` to reconstruct the document — no ACP check
2. Renders document fields directly — no `PermissionFilterNode` in the path
3. Resolves nested relations via `fetcher.get_by_ids()` — also no ACP check

### Contrast with Regular Queries

Regular queries flow through `execute_simple_select()` or `execute_nested_select_with_planner()`, both of which insert ACP filtering. CID queries take a completely separate code path that reconstructs documents from the Merkle DAG without any permission checks.

### What's Exposed

A CID query returns the full document as it existed at that commit:
- **All field values** at that point in time (not just metadata)
- **Nested relation data** (also without ACP)
- **Historical states** that may have been intentionally superseded

### Attack Scenario

```graphql
# Attacker discovers a CID from _commits query (finding 02) or DAG traversal
query {
  Users(cid: "bafy2bzaced...") {
    name
    email
    ssn
  }
}
```

This chains with finding 02: `_commits` reveals CIDs, then CID queries reveal document content.

### Severity Rationale

MEDIUM (not CRITICAL) because:
1. Attacker needs a valid CID — these are content-addressed hashes, not guessable
2. But CIDs can be obtained via finding 02 (`_commits` bypass) or P2P DAG traversal
3. Combined with finding 02, this escalates to full document content disclosure

## Remediation

### Option A: Check document-level Read permission before rendering

Before returning CID query results, look up the document's collection and check `DocumentPermission::Read` against the caller's identity. Return empty results if denied.

### Option B: Route CID queries through the planner

Modify the CID path to use the standard query planner, which inserts `PermissionFilterNode` automatically. This ensures CID queries get the same ACP treatment as regular queries.

## Test Gap

No integration test verifies that CID queries respect ACP boundaries. Should add a test where:
1. Create a document with ACP policy granting read to Alice only
2. Get a CID from the document's commit history
3. Query via CID as Bob (unauthorized)
4. Verify empty results
