# Finding 63: Error Path Identity Handling Is Clean (Green)

**Severity**: GREEN
**Category**: Error Handling / Identity Safety
**Status**: Verified sound

## Summary

When JWT verification fails at any stage, the identity is `None` (anonymous) — there is no partial identity state. The `from_token()` function either returns a fully-validated `TokenIdentity` or an error. The HTTP identity extractor maps errors to 403 responses and never passes a partially-validated identity to handlers.

## Affected Files

- `crates/identity/src/token/mod.rs:204-274` — `from_token()` returns `Result<TokenIdentity>`
- `crates/http/src/identity_extractor.rs:138-181` — Maps errors to 403
- `crates/identity/src/token/identity.rs` — `TokenIdentity` struct (no partial state)

## Details

### `from_token()` failure modes

All failure paths return `Err(...)`, never a partial `TokenIdentity`:

| Failure | Return |
|---------|--------|
| Invalid UTF-8 | `Err(TokenDecoding)` |
| Bad base64 | `Err(TokenDecoding)` |
| Malformed JSON | `Err(TokenDecoding)` |
| Unsupported algorithm | `Err(TokenDecoding)` |
| Invalid signature | `Err(TokenDecoding)` |
| Algorithm mismatch | `Err(TokenDecoding)` |
| Bad public key hex | `Err(InvalidClaimValue)` |
| DID derivation failure | `Err(InvalidClaimValue)` |
| Issuer mismatch | `Err(InvalidClaimValue)` |

### Identity extractor error handling

```rust
let token_identity = from_token(token.as_bytes())
    .map_err(|e| IdentityExtractionError::InvalidToken(e.to_string()))?;
verify_auth_token(&token_identity, expected_audience)
    .map_err(|e| IdentityExtractionError::TokenVerificationFailed(e.to_string()))?;
```

The `?` operator ensures that on any error, the handler never receives a `TokenIdentity`. The extractor returns `IdentityExtractionError`, which Axum converts to a 403 response.

### No sensitive data in error logs

The error messages contain:
- Generic failure descriptions ("signature verification failed", "invalid UTF-8")
- Claim names ("sub", "iss") but NOT claim values
- No private key material, token content, or bearer token strings

The `TokenIdentity::Debug` impl redacts sensitive fields:
```rust
fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    // Shows: TokenIdentity { did:key:..., key_type: Ed25519 }
    // Does NOT show: bearer_token, public_key bytes
}
```

## Remediation

None required. Error paths are clean and identity-safe.
