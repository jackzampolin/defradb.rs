# Finding 59: JWT Claim Validation Ordered Correctly After Signature (Green)

**Severity**: GREEN
**Category**: Authentication / Timing Resistance
**Status**: Verified sound

## Summary

JWT processing follows the correct security order: signature verification happens *before* any claims (exp, nbf, aud) are trusted. This prevents timing oracles where an attacker could distinguish "valid signature + expired" from "invalid signature + expired" based on response time differences.

## Affected Files

- `crates/identity/src/token/mod.rs:204-274` — `from_token()` verifies signature first
- `crates/identity/src/token/mod.rs:120-185` — `verify_auth_token()` checks claims after
- `crates/http/src/identity_extractor.rs:138-181` — Extractor calls `from_token()` then `verify_auth_token()`

## Details

### Processing order

1. **`from_token()`** (line 204): Parse JWT, extract claims, verify signature
   - Parses algorithm from header
   - Decodes claims from payload
   - Reconstructs public key from `sub` claim
   - **Verifies signature** against the signing input
   - Validates `iss` matches derived DID
   - Returns `TokenIdentity` only if signature is valid

2. **`verify_auth_token()`** (line 120): Validate time and audience claims
   - Checks `nbf` with clock skew tolerance
   - Checks `exp` with clock skew tolerance
   - Checks `aud` against expected audience

3. **Identity extractor** (line 167):
   ```rust
   let token_identity = from_token(token.as_bytes())?;  // signature first
   verify_auth_token(&token_identity, expected_audience)?;  // claims second
   ```

### Why this ordering matters

If claims were checked *before* signature verification, an attacker could:
- Send tokens with different `exp` values and the same invalid signature
- Observe that "expired" returns faster than "valid claims but bad signature"
- Use this timing difference to probe whether the server's clock matches their expectations

With signature-first ordering, all invalid-signature tokens are rejected at the same point regardless of claim values.

### Note on `from_token()` internal ordering

Within `from_token()`, claims are *parsed* (deserialized) before signature verification because the public key needed for verification is extracted from the `sub` claim. This is acceptable — parsing is not the same as *trusting*. The claims are not acted upon until after signature verification succeeds.

## Remediation

None required. The ordering is correct.
