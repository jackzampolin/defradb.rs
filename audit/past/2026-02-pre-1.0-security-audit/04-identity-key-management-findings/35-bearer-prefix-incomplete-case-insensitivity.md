# Bearer Prefix Incomplete Case-Insensitivity

- **Severity**: Low
- **Category**: HTTP Authentication
- **Status**: Confirmed

## Summary

The Bearer token prefix matching only handles `"Bearer "` and `"bearer "` — two specific casings. RFC 6750 Section 2.1 specifies that the `Authorization` header token type `Bearer` is case-insensitive. Mixed-case variants like `"BEARER "`, `"beArer "`, or `"BeArEr "` are rejected with a 403 rather than parsed.

## Affected Files

- `crates/http/src/identity_extractor.rs:148-158` (extract_identity_from_auth_header)
- `crates/http/src/identity_extractor.rs:316-324` (extract_token_identity_from_auth_header)

## Details

```rust
// Lines 148-158 — only two cases handled
let token = if let Some(token) = auth_value.strip_prefix("Bearer ") {
    token.trim()
} else if let Some(token) = auth_value.strip_prefix("bearer ") {
    token.trim()
} else {
    return Err(IdentityExtractionError::InvalidToken(
        "unsupported authorization scheme (expected Bearer)".to_string(),
    ));
};
```

The test at line 289-296 explicitly confirms `"BEARER"` is rejected. This is an intentional deviation to match Go DefraDB behavior (Go's `strings.TrimPrefix` is also case-sensitive), but it deviates from RFC 6750.

**Practical impact**: Most HTTP clients use `"Bearer "` (capital B). The `"bearer "` variant covers common alternate tooling. Other variants are exceedingly rare in practice. The deviation from RFC is intentional for Go compatibility.

## Remediation

**Accept as-is for Go compatibility.** If strict RFC compliance is desired later, replace with:

```rust
let lower = auth_value.to_ascii_lowercase();
let token = if let Some(token) = lower.strip_prefix("bearer ") {
    // Use offset into original string to preserve token casing
    &auth_value[7..].trim()
} else { ... };
```

## Test Gap

The existing test `test_uppercase_bearer_returns_error` documents current behavior. No additional tests needed unless behavior changes.
