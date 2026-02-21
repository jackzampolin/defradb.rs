# Finding: _commits Queries Bypass ACP Entirely

**Stream**: 02 - Access Control Policy
**Severity**: CRITICAL
**Category**: Access Control Bypass
**Status**: CONFIRMED

## Summary

The `_commits` system collection query bypasses ACP entirely. When a GraphQL query targets `_commits`, the execution path early-returns before the caller's identity is checked or any `PermissionFilterNode` is applied. Any user can query the full commit history of any ACP-protected document.

## Affected Files

| File | Line | Issue |
|------|------|-------|
| `crates/query/src/runner/query/select.rs` | 27-30 | Early return for `_commits` discards `caller_identity` |
| `crates/query/src/runner/commits.rs` | ~388 | `execute_commits_query()` accepts no identity parameter |
| `crates/query/src/runner/explain/execute.rs` | ~129 | Explain queries for `_commits` also skip identity |

## Details

### The Bypass

In `select.rs`, when the query targets `_commits`, it immediately routes to a separate execution path:

```rust
// crates/query/src/runner/query/select.rs:27-30
if select.collection_name == "_commits" {
    return self.execute_commits_query(select).await;
}
```

The `caller_identity: Option<Did>` parameter available in the function signature is **completely ignored** for this path. The `execute_commits_query()` function has no identity parameter and performs no ACP checks.

### Contrast with Regular Queries

Regular collection queries go through the planner, which calls `maybe_wrap_with_acp_filter()` (planner/builder/mod.rs:129-146). This wraps the query plan in a `PermissionFilterNode` that checks `DocumentPermission::Read` for each document. The commits query path skips all of this.

### What's Exposed

A `_commits` query returns:
- **Commit CIDs** for every mutation on the document
- **Field names** that were modified (reveals document schema structure)
- **Commit heights** (reveals temporal mutation history)
- **Document IDs** (confirms document existence)

### Attack Scenario

```graphql
# Unauthorized user can query commit history of any ACP-protected document
query {
  _commits(docID: "bae-restricted-document-id") {
    cid
    docID
    fieldName
    height
  }
}
```

This returns full commit history regardless of the caller's identity or ACP policy.

## Remediation

### Option A: Pass identity through to commits query path

Add `caller_identity: Option<Did>` to `execute_commits_query()` and filter results based on the caller's read permission on the referenced document.

### Option B: Check document-level permission before executing commits query

Before fetching commits, resolve the document's collection and check `DocumentPermission::Read` via the ACP subsystem. Reject with empty results if unauthorized.

## Test Gap

No integration test queries `_commits` on an ACP-protected document with an unauthorized identity. Should add a test where:
1. Create a document with ACP policy granting read to Alice only
2. Query `_commits(docID: ...)` as Bob (unauthorized)
3. Verify empty results or access denied error
