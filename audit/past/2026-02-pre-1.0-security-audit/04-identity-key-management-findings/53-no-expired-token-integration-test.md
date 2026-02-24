# Finding 53: No Expired Token Integration Test

**Severity**: MEDIUM
**Category**: Test Coverage / Authentication
**Status**: Confirmed

## Summary

No integration test sends an expired JWT to the HTTP API and verifies that it is rejected with 403. The unit tests in `crates/identity/tests/token_tests.rs` verify expiration by manipulating the `TokenIdentity.claims.exp` field *after* parsing, but this bypasses the real expiration check path (which happens in the identity extractor middleware). There is no end-to-end test that creates a token with `exp` in the past and sends it over HTTP.

## Affected Files

- `tools/integration-test/tests/identity_lifecycle.rs` — Tests key CRUD only, no token expiry tests
- `tools/integration-test/tests/identity_types.rs` — Tests cross-key-type ACP, no expired token
- `crates/identity/tests/token_tests.rs:232-252` — Unit test manipulates claims post-parsing

## Details

### What the unit test does

```rust
fn test_expired_token() {
    // Create valid token
    let token = new_token(&identity, Duration::from_secs(3600), ...);
    let mut token_identity = from_token(&token).unwrap();
    // Manipulate claims AFTER signature verification succeeded
    token_identity.claims.exp = 0;
    let result = verify_auth_token(&token_identity, "audience");
    assert!(result.is_err());
}
```

This tests that `verify_auth_token()` correctly rejects expired tokens, but:
1. The token was *originally* valid — signature verification passed
2. The claims were manipulated in-memory, not in the actual JWT
3. A real expired token would have `exp` set at creation time
4. The HTTP identity extractor calls `from_token()` then `verify_auth_token()` — the full path is untested

### What's missing

- An integration test creating a token with `Duration::from_secs(0)` and sending it after a short delay
- A test creating a backdated token (requires custom claims encoding, not using `new_token()`)
- A test at the clock skew boundary (61 seconds past expiry)

### Risk

If the identity extractor or HTTP middleware changes and the ordering of `from_token()` → `verify_auth_token()` is disrupted, expired tokens could be accepted. The unit test would not catch this because it tests the verification function in isolation.

## Remediation

Add an integration test that:
1. Generates an identity
2. Creates a token with a 1-second expiry
3. Sleeps 2+ seconds (past the 1s expiry + beyond the 60s clock skew window is impractical, so test with `verify_auth_token_with_skew(_, _, 0)` in a unit test)
4. Sends an HTTP request with the token
5. Asserts 403 response

For practical testing without long sleeps, add a unit test to the identity crate:
```rust
fn test_token_expired_at_skew_boundary() {
    let mut token_identity = from_token(&token).unwrap();
    token_identity.claims.exp = now - 61;  // 61 seconds ago
    assert!(verify_auth_token(&token_identity, "aud").is_err());
    token_identity.claims.exp = now - 59;  // 59 seconds ago (within skew)
    assert!(verify_auth_token(&token_identity, "aud").is_ok());
}
```

## Test Gap

- No integration test for expired token rejection over HTTP
- No boundary test for 60-second clock skew tolerance
- No test for `exp: 0`, `exp: u64::MAX`, or `nbf > exp`
