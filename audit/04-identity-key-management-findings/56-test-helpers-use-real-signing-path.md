# Finding 56: Test Helpers Use Real Signing Path (Green)

**Severity**: GREEN
**Category**: Test Realism
**Status**: Verified sound

## Summary

The integration test helpers generate identities and create JWT tokens using the same code paths as production. Test tokens are created by the CLI binary itself (`defra identity new` and `defra client -i <key>`), not by test-only shortcuts that could mask bugs.

## Affected Files

- `tools/integration-test/src/identity.rs` — `generate_identity()`, `generate_ed25519_identity()`, `generate_secp256r1_identity()`
- `tools/integration-test/src/client/mod.rs:56-66` — `exec_with_identity()` passes `-i hex_key` to CLI

## Details

### Identity generation

The test helpers invoke the actual DefraDB binary, which generates real keys using the same `crypto::generate_secp256k1()` function used in production.

### Token creation

The CLI's `-i` flag triggers `new_token()` which creates a real JWT with proper signing, audience binding, and expiry. The HTTP client then attaches this as a `Bearer` token in the `Authorization` header.

### Assessment

This is the ideal pattern. Test tokens go through the full path:
1. Key generation via real crypto
2. Token creation via real `new_token()` with signing
3. HTTP transmission via real `Authorization: Bearer` header
4. Token verification via real `from_token()` + `verify_auth_token()`

No test-only token minting or signature bypasses exist.

## Remediation

None required. This is correct and should be preserved.
