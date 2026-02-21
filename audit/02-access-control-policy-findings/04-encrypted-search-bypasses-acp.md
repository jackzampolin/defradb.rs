# Finding: Encrypted Search Queries Bypass ACP

**Stream**: 02 - Access Control Policy
**Severity**: MEDIUM
**Category**: Access Control Bypass
**Status**: CONFIRMED
**Session**: S1 - DAC Implementation Review

## Summary

Encrypted search queries (`encrypted_<Collection>`) bypass ACP entirely. The `execute_encrypted_select()` function has no identity parameter and performs no ACP checks. It fetches all documents, applies filter matching, and returns matching document IDs regardless of the caller's permissions.

## Affected Files

| File | Line | Issue |
|------|------|-------|
| `crates/query/src/runner/query/select.rs` | 23-25 | `is_encrypted` check early-returns before ACP |
| `crates/query/src/runner/query/select.rs` | 301-355 | `execute_encrypted_select()` has no identity parameter |
| `crates/query/src/runner/query/select.rs` | 331 | `fetcher.get_all()` fetches all docs without ACP |

## Details

### The Bypass

```rust
// crates/query/src/runner/query/select.rs:23-25
if select.is_encrypted {
    return self.execute_encrypted_select(select, fetcher).await;
}
```

This early-returns before the `caller_identity` parameter is used. The `execute_encrypted_select()` function signature:

```rust
async fn execute_encrypted_select(
    &self,
    select: &Select,
    fetcher: &dyn DocFetcher,
    // NO identity parameter
) -> Result<JsonValue> {
```

### What's Exposed

The function returns `[{"docIDs": [...]}]` — a list of document IDs matching the encrypted filter criteria. While the actual encrypted field values are not returned, the document IDs of ACP-protected documents are exposed to unauthorized callers.

### Attack Scenario

```graphql
# Unauthorized user can discover which ACP-protected documents
# have specific encrypted field values
query {
  encrypted_Users(filter: {ssn: {_eq: "encrypted_token_123"}}) {
    docIDs
  }
}
```

This reveals which document IDs match a given encrypted value, even if the caller has no read permission on the collection.

### Severity Rationale

MEDIUM because:
1. Only document IDs are returned (not field values)
2. Requires knowledge of encrypted index tokens to construct useful queries
3. But confirms document existence and allows correlation attacks

## Remediation

### Option A: Add identity and ACP check to encrypted search

Pass `caller_identity` to `execute_encrypted_select()`, filter the matching document IDs through ACP before returning.

### Option B: Filter results through PermissionFilterNode

After collecting matching document IDs, check `DocumentPermission::Read` for each and exclude unauthorized ones.

## Test Gap

No integration test verifies that encrypted search respects ACP boundaries. Should add a test where:
1. Create documents with ACP policy and encrypted indexes
2. Query encrypted search as unauthorized identity
3. Verify ACP-protected document IDs are not returned
