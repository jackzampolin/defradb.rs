# Finding 17: Signature Verified Before Claims Trusted

**Severity**: GREEN
**Category**: Authentication / Verification Ordering
**Status**: Verified sound

## Summary

All three JWT decode functions (`decode_ed25519`, `decode_secp256k1`, `decode_secp256r1`) verify the cryptographic signature before returning claims. Claims are decoded early (required to extract the public key from `sub`), but they are never returned to the caller without passing signature verification. The post-signature checks in `from_token()` (algorithm cross-check, issuer verification) provide defense-in-depth.

## Affected Files

- `crates/identity/src/token/decoding.rs:78-115` — all three decode functions
- `crates/identity/src/token/mod.rs:204-274` — `from_token()`

## Details

### Verification ordering in each decode function

```
1. parse_jwt(token)                    ← Split into 3 parts (reject if != 3)
2. decode_claims(jwt.payload)          ← Decode claims (UNTRUSTED at this point)
3. decode_public_key_from_claims(...)  ← Extract key from untrusted sub claim
4. decode_signature(jwt.signature)     ← Decode signature bytes
5. verify_signature(key, input, sig)   ← CRYPTOGRAPHIC VERIFICATION
6. Ok(claims)                          ← Return ONLY if step 5 passes
```

Claims at step 2 are untrusted raw data. The public key at step 3 is constructed from untrusted hex bytes. However, step 5 proves that whoever constructed the token possessed the private key corresponding to the public key in `sub`. Only then are claims returned.

### Post-signature checks in `from_token()`

After the decode function returns verified claims:

```
7. claims.key_type.parse()             ← Parse key type string
8. header_alg == expected_alg          ← Cross-check algorithm consistency
9. public_key_from_bytes(...)          ← Reconstruct key (redundant but validates)
10. public_key.did() == claims.iss     ← DID-issuer binding
11. Ok(TokenIdentity { ... })          ← Return fully validated identity
```

### Edge case: empty signature

`parse_jwt()` accepts `"header.payload."` (empty signature string). `decode_signature("")` base64-decodes to empty bytes. For Ed25519, `verify()` checks `signature.len() != 64` → `Ok(false)` → error. For ECDSA, `raw_to_der()` checks `raw.len() != 64` → error. Both paths correctly reject empty signatures.

### Edge case: extra parts

`parse_jwt()` checks `parts.len() != 3` and rejects tokens with more than 3 dot-separated parts. `"header.payload.sig.extra"` → `parts.len() == 4` → error.

### No TOCTOU between parse_algorithm and decode

`from_token()` calls `parse_algorithm(token_str)` then the decode function, both on the same immutable `&str`. No possibility of the token changing between the two operations.

## Remediation

None required. The verification ordering is correct and handles all edge cases safely.
