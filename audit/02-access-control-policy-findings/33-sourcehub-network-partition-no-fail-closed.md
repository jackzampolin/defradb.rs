# Finding: SourceHub Network Partition — Permission Checks Fail but Without Explicit Fail-Closed Policy

**Stream**: 02 - Access Control Policy
**Severity**: MEDIUM
**Category**: Availability / Fail-Closed Analysis
**Status**: CONFIRMED

## Summary

When SourceHub is unreachable, all `check_doc_access()` calls fail with HTTP errors that propagate as `acp::Error::Storage`. The query-level permission filter catches these errors and denies access (fail-closed). However, this is an **emergent behavior** — there is no explicit fail-closed policy or circuit breaker. The fail-closed behavior depends entirely on the `unwrap_or_else(|e| { ... false })` pattern in `permission_filter.rs:103`. Different call sites handle errors differently, and not all paths are fail-closed.

## Affected Files

| File | Line | Issue |
|------|------|-------|
| `crates/sourcehub/src/client.rs:33,72,111` | HTTP calls | `reqwest` errors propagate as `ClientError::Http` |
| `crates/sourcehub/src/dac.rs:132-136` | `check_doc_access()` | `provider.query_object_owner()` can fail |
| `crates/sourcehub/src/dac.rs:165-169` | `check_doc_access()` | `provider.verify_access()` can fail |
| `crates/query/src/plan/permission_filter.rs:103` | Query filter | Catches errors → deny (fail-closed) |
| `crates/db/src/acp_merge_handler.rs:156-163` | Merge handler | Errors propagate as `AcpMergeError::AcpError` |
| `crates/db/src/collection_acp.rs:40-61` | Write path | Errors propagate to caller |

## Details

### Error Propagation Chain

When SourceHub is down:

```
reqwest::Client::get().send().await
    ↓ reqwest::Error
SourceHubClient::query_object_owner()
    ↓ ClientError::Http(reqwest::Error)
CosmosProvider::query_object_owner()
    ↓ ProviderError::Query(String)
SourceHubDocumentACP::check_doc_access()
    ↓ acp::Error::Storage(String)
check_doc_permission()
    ↓ acp::Error::Storage(String)
```

### Path-by-Path Analysis

**Read queries** (via `PermissionFilterNode`):
```rust
// permission_filter.rs:93-113
Ok(self.acp.check_doc_access(...).await.unwrap_or_else(|e| {
    tracing::warn!(..., "Permission check failed, denying access to document");
    false  // ← FAIL-CLOSED ✓
}))
```
Result: Documents are filtered out. User sees empty results. **Fail-closed.**

**Write mutations** (via `check_doc_permission` in `collection_acp.rs`):
```rust
// collection_acp.rs:49-60
acp.check_doc_access(...).await  // ← Error propagates to caller
```
Result: Error propagates to the HTTP handler, which returns an error response. **Fail-closed.**

**P2P merge** (via `AcpMergeHandler`):
```rust
// acp_merge_handler.rs:156-163
let permitted = check_doc_permission(...).await?;  // ← ? operator
```
Result: Error propagates as `AcpMergeError::AcpError`. The merge handler returns an error. **The merge is rejected, but the error handling at the sync layer determines whether this is retried or permanently dropped.**

**Document registration** (`register_doc_object`):
```rust
// sourcehub/dac.rs:101-107
let bearer_token = self.create_bearer_token(identity.as_str())?;
self.provider.register_object(&bearer_token, ...).await.map_err(provider_err)
```
Result: Error propagates. Document creation fails. **Fail-closed.**

### The Missing Circuit Breaker

There is no:
1. Health check for SourceHub connectivity
2. Circuit breaker to stop hammering a down SourceHub
3. Graceful degradation mode (e.g., use cached permissions temporarily)
4. Status endpoint showing SourceHub connectivity

During a SourceHub outage, **every read query** will:
1. Attempt an HTTP request to SourceHub (for `query_object_owner`)
2. Wait for the HTTP timeout
3. Get an error
4. Filter out the document

For a collection with N documents, every query triggers N HTTP requests to a dead endpoint, each blocking until timeout. This turns a SourceHub outage into a severe **performance degradation** for the DefraDB node.

### Contrast with Local ACP

In local ACP mode, permission checks are in-process HashMap lookups — sub-microsecond, never fail with network errors. The SourceHub path converts every permission check into a blocking HTTP call, fundamentally changing the performance and reliability characteristics.

## Mitigating Factors

1. All code paths are fail-closed — no unauthorized access during outages
2. `reqwest` has default timeouts that prevent infinite hangs
3. In practice, SourceHub outages are rare in production deployments
4. The P2P merge path correctly rejects rather than accepts on error

## Remediation

1. Add a SourceHub health check and circuit breaker
2. Consider caching recent permission check results with short TTL for resilience
3. Add a `/api/v0/status` field showing SourceHub connectivity
4. Configure aggressive HTTP timeouts for permission checks (sub-second)

## Test Coverage

No test verifies behavior when SourceHub is unreachable. All SourceHub integration tests require a running SourceHub devnet.
