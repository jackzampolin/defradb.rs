# Finding: SourceHub verify_access Fails Open on ABCI Error Codes

**Stream**: 02 - Access Control Policy
**Severity**: HIGH
**Category**: Fail-Open / Authentication Bypass
**Status**: CONFIRMED

## Summary

When the SourceHub on-chain `verify_access` ABCI query returns a non-zero error code, the client silently returns `false` (access denied) — which appears safe. However, the **query filter's `unwrap_or_else` handler** converts any `Err(...)` from `check_doc_access` into `false`. The issue is that the SourceHub client itself converts **network-level HTTP failures** into `ProviderError::Query`, which propagates as `acp::Error::Storage`, which the permission filter catches and logs as a warning — correctly denying access. This chain is sound **for read queries**.

The critical problem is in the ABCI layer: a non-zero ABCI code (line 121-127 of `client.rs`) returns `Ok(false)` — meaning the caller **cannot distinguish between "user definitely has no access" and "SourceHub returned an error we don't understand."** If SourceHub returns a bug-induced error code for a valid access check, the user silently loses access with no retry or escalation.

More critically, the `unwrap_or` default in protobuf decoding (line 140) treats **any malformed response** — including truncated, corrupted, or man-in-the-middle altered responses — as `Ok(false)`. This conflates "access denied" with "response parsing failed."

## Affected Files

| File | Line | Issue |
|------|------|-------|
| `crates/sourcehub/src/client.rs:120-127` | ABCI error code | Non-zero ABCI code → `Ok(false)` instead of `Err(...)` |
| `crates/sourcehub/src/client.rs:130-142` | Protobuf decode | Empty/malformed base64 → `Ok(false)` |
| `crates/sourcehub/src/client.rs:140` | Byte check | Hardcoded `0x08, 0x01` pattern — brittle protobuf parsing |
| `crates/query/src/plan/permission_filter.rs:103` | Error handler | `unwrap_or_else(... false)` — fail-closed at query level |

## Details

### ABCI Error Masking

```rust
// crates/sourcehub/src/client.rs:119-127
let abci_code = body["result"]["response"]["code"].as_u64().unwrap_or(0);
if abci_code != 0 {
    let log = body["result"]["response"]["log"]
        .as_str()
        .unwrap_or("unknown");
    tracing::debug!(abci_code, log, "verify_access ABCI error");
    return Ok(false);  // ← ERROR masqueraded as denial
}
```

This converts **every ABCI error** into a successful `Ok(false)` return. The caller (`SourceHubDocumentACP::check_doc_access`) sees `Ok(false)` and interprets it as "user does not have access." The query filter sees `Ok(false)` and filters out the document.

In read-path queries this is fail-closed (correct) — the user sees fewer documents. But for **write-path permission checks** (e.g., `check_doc_permission` for Update in the merge handler), `Ok(false)` means "merge denied" — which may silently drop legitimate P2P updates during transient SourceHub issues.

### Brittle Protobuf Parsing

```rust
// crates/sourcehub/src/client.rs:138-142
// QueryVerifyAccessRequestResponse: field 1 (bool) valid
// Protobuf: tag 0x08 (field 1, varint), value 0x01 (true)
let valid = result_bytes.len() >= 2 && result_bytes[0] == 0x08 && result_bytes[1] == 0x01;
```

This hand-rolled protobuf decoding only checks for the exact two-byte pattern `[0x08, 0x01]`. Any valid protobuf response with additional fields (e.g., a future protocol version adding metadata) would produce `false` even if `valid=true` is present. The default-false behavior means **protocol evolution silently breaks access**.

### Impact

1. **Transient SourceHub errors → silent document loss**: During SourceHub maintenance or network issues, all permission checks return `Ok(false)`, making all ACP-protected documents invisible to users
2. **P2P merge drops**: Legitimate peer updates to protected documents are silently rejected during SourceHub outages
3. **No observability**: ABCI errors are logged at `debug` level only — operators won't see them without debug logging enabled
4. **Protocol brittleness**: Any change to the protobuf response format breaks access checks with no error signal

### Mitigating Factors

1. The query-level permission filter is fail-closed, so this doesn't grant unauthorized access
2. HTTP-level connection failures DO propagate as errors (reqwest errors → `ClientError::Http`)
3. The ABCI error code IS logged (at debug level)

## Remediation

1. Return `Err(ProviderError::Query(...))` for non-zero ABCI codes instead of `Ok(false)`
2. Use proper protobuf decoding (prost) instead of byte-level pattern matching
3. Elevate ABCI error logging from `debug` to `warn`
4. Add a health check or circuit breaker for SourceHub connectivity

## Test Coverage

No test verifies behavior when SourceHub returns ABCI errors or malformed responses.
