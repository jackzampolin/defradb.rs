# Finding 05: JWT Algorithm Dispatch From Header — Verified Correct

**Severity**: GREEN
**Category**: Authentication / JWT Security
**Status**: Verified safe

## Summary

JWT token verification in `from_token()` dispatches based on the `alg` header field, which is a known attack vector (e.g., the classic `"alg": "none"` attack). This implementation handles it correctly: only three specific algorithms are accepted (`EdDSA`, `ES256K`, `ES256`), signatures are cryptographically verified before claims are trusted, and the header algorithm is cross-checked against the body's `key_type` claim.

## Affected Files

- `crates/identity/src/token/mod.rs:204-274` — `from_token()`
- `crates/identity/src/token/decoding.rs:117-134` — `parse_algorithm()`

## Details

### Verification flow

1. Parse header `alg` → only `EdDSA`, `ES256K`, `ES256` accepted (line 211-220)
2. Decode claims from payload (unsigned, not yet trusted)
3. Reconstruct public key from `claims.sub` hex bytes using the algorithm's key type
4. **Verify signature** against the public key (cryptographic proof)
5. Cross-check: `header_alg == expected_alg` derived from `claims.key_type` (line 230-235)
6. Derive DID from public key and verify `claims.iss` matches (line 256-264)

### Why this is safe

- **"alg: none" attack**: Rejected at step 1 (unknown algorithm error)
- **Algorithm confusion**: If attacker changes `alg` from `ES256K` to `EdDSA`, the verification at step 4 will fail because the public key bytes (from `claims.sub`) will be interpreted as Ed25519 instead of secp256k1, producing an invalid key or failed verification.
- **Key type cross-check**: Even if signature somehow passed, step 5 catches the mismatch.
- **Issuer binding**: Step 6 ensures the DID in `iss` matches the public key in `sub`, preventing substitution attacks.

### One observation

The signature verification happens before the `key_type` consistency check (step 4 before step 5). This means a crafted token with mismatched `alg` and `key_type` will fail at signature verification rather than at the consistency check. This is functionally correct — the order doesn't affect security — but it means error messages for this scenario will say "verification failed" rather than "algorithm mismatch."

## Remediation

None required. The implementation is sound.
