# Empty Bearer Token Treated as Anonymous

- **Severity**: Medium
- **Category**: HTTP Authentication
- **Status**: Confirmed — Intentional for Go Compatibility

## Summary

`Authorization: Bearer ` (with trailing space, empty token) and `Authorization: Bearer` followed by only whitespace are treated as anonymous (`Ok(None)`) rather than as an authentication error. This means an attacker who can inject or modify headers can downgrade an authenticated request to anonymous by sending an empty bearer value, bypassing any ACP protections the caller intended.

## Affected Files

- `crates/http/src/identity_extractor.rs:160-163`

## Details

```rust
// Lines 160-163
// Empty token after stripping prefix = anonymous
if token.is_empty() {
    return Ok(None);
}
```

After `strip_prefix("Bearer ")` and `.trim()`, if the token is empty, the request is treated identically to one with no `Authorization` header at all.

**Attack scenario**: A reverse proxy or middleware that adds `Authorization: Bearer ` without a valid token (misconfiguration) would silently downgrade all requests to anonymous instead of failing loudly with 403.

**Mitigating factor**: This behavior matches Go DefraDB exactly. The Go code uses `strings.TrimPrefix` + empty check → treats as anonymous. Changing this would break Go compatibility.

Additionally, `Authorization: Bearer   ` (multiple spaces, no token) also hits this path because `.trim()` produces an empty string.

## Remediation

**Accept for Go compatibility**, but document that empty bearer != no auth header. Consider adding a `tracing::warn!` when an empty bearer is received:

```rust
if token.is_empty() {
    tracing::warn!("Empty Bearer token received - treating as anonymous");
    return Ok(None);
}
```

## Test Gap

`test_empty_bearer_returns_anonymous` covers this. Missing test: `"Bearer    "` (spaces-only token) → should also be anonymous.
