# 403 Forbidden Used Instead of 401 Unauthorized for Invalid Credentials

- **Severity**: Info
- **Category**: HTTP Semantics
- **Status**: Confirmed — Intentional Go Compatibility

## Summary

The identity extractor returns `403 Forbidden` for invalid/expired tokens. Per RFC 7235, `401 Unauthorized` should be used for "missing or invalid credentials" and `403 Forbidden` for "valid credentials, insufficient permissions." The current code conflates these two cases.

Additionally, no `WWW-Authenticate` header is included in the response, which RFC 7235 Section 3.1 requires for 401 responses.

## Affected Files

- `crates/http/src/identity_extractor.rs:104-123` (all errors → 403)
- `crates/http/src/error.rs:21-30` (separate `Unauthorized` and `Forbidden` variants exist)

## Details

The HTTP crate actually has distinct error types for the correct semantics:

```rust
// error.rs:21-30
/// 401 Unauthorized - Used for NAC permission denials.
Unauthorized(String),
/// 403 Forbidden - Used for invalid/expired tokens.
Forbidden(String),
```

The identity extractor uses 403 for all failures. The NAC guard uses 401 for permission denials. This is actually a reasonable separation:
- **403**: You provided credentials but they're bad (wrong token)
- **401**: Your credentials are fine but you lack permission (NAC)

This is the reverse of standard HTTP semantics but matches Go DefraDB's behavior:
- Go `AuthMiddleware` → 403 for bad tokens
- Go `CollectionMiddleware` → 401 for NAC denials

## Remediation

**Accept as-is for Go compatibility.** This is well-documented in the code comments and matches Go DefraDB exactly. The behavior is internally consistent between Rust and Go implementations.

## Test Gap

None — the current behavior is tested and documented.
