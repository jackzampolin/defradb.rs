# Finding 61: Missing Integration Test for Wrong-Key-Type Token

**Severity**: LOW
**Category**: Test Coverage / Authentication
**Status**: Confirmed

## Summary

No integration test sends a JWT signed with one key type (e.g., Ed25519) but claiming to be another (e.g., secp256k1). The unit test `test_algorithm_mismatch_rejected` in `token_tests.rs` covers this at the unit level by modifying the `key_type` claim, but there is no end-to-end test through the HTTP stack.

## Affected Files

- `crates/identity/tests/token_tests.rs:488-512` — Unit test for algorithm mismatch
- `tools/integration-test/tests/` — No integration test for key-type mismatch

## Details

### Unit test coverage

```rust
fn test_algorithm_mismatch_rejected() {
    // Creates Ed25519 token, changes key_type claim to secp256k1
    claims["key_type"] = serde_json::json!("secp256k1");
    // Signature verification fails (payload modified)
    let result = from_token(modified_token.as_bytes());
    assert!(result.is_err());
}
```

This test works because modifying the payload invalidates the signature. The actual algorithm check in `from_token()` (line 225-235) is a secondary defense that catches cases where the attacker signs with a different algorithm but the signature happens to validate.

### What's missing at the integration level

An integration test that:
1. Generates an Ed25519 identity
2. Manually constructs a JWT with `"alg": "ES256K"` header but Ed25519 signature
3. Sends it to the HTTP API
4. Verifies rejection

This is complex to test via the CLI (which always creates correctly-typed tokens), so it would need to either use a custom HTTP client or be a unit test of the HTTP layer.

### Risk assessment

Low risk because:
1. The unit test covers the code path
2. The `from_token()` function checks both signature AND algorithm consistency
3. A real attacker would need to forge a valid signature to exploit this

## Remediation

Consider adding to the HTTP layer unit tests (`crates/http/tests/`) a test that sends a manually-constructed mismatched-algorithm token.

## Test Gap

- No integration test for algorithm mismatch through HTTP stack
- Covered at unit level in `token_tests.rs`
