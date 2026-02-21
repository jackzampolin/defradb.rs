# Finding 16: Self-Authenticating Token Design Sound

**Severity**: GREEN
**Category**: Authentication Architecture
**Status**: Verified sound

## Summary

The JWT design uses self-authenticating tokens: the public key is embedded in the `sub` claim, and the token proves the signer possesses the corresponding private key. The DID in `iss` is derived from the public key and cross-checked. This is the correct pattern for DID-based authentication where there is no pre-registered key directory.

## Affected Files

- `crates/identity/src/token/mod.rs:204-274` — `from_token()`
- `crates/identity/src/token/decoding.rs:40-53` — `decode_public_key_from_claims()`
- `crates/crypto/src/keys/generation.rs:210-230` — `public_key_from_bytes()`

## Details

### Self-authentication flow

```
Token claims:
  sub = hex(public_key_bytes)   ← "I am this key"
  iss = did:key:z...            ← "My DID is this"

Signature = Sign(private_key, header.payload)

Verification:
  1. Extract public_key from sub    ← Trust nothing yet
  2. Verify(public_key, header.payload, signature)  ← Cryptographic proof
  3. Assert DID(public_key) == iss  ← Binding check
```

### Why this is safe despite "attacker chooses their own key"

The concern with self-authenticating tokens is that an attacker can sign with any key and the signature will verify. However:

1. **Identity binding**: The DID is deterministically derived from the public key. An attacker can only authenticate as their own DID — they cannot impersonate someone else's DID.
2. **Authorization is separate**: Whether the authenticated DID has permission to perform an action is checked by the ACP layer, not the JWT layer. The JWT only establishes "this request comes from DID X."
3. **Audience binding**: Tokens are scoped to a specific host via the `aud` claim.

### Public key validation

Extracted public keys are validated by the crypto library:
- **Ed25519**: `VerifyingKey::from_bytes()` checks the key is a valid curve point (32 bytes required)
- **secp256k1**: `k256::PublicKey` validates the point is on the curve (33 or 65 bytes)
- **secp256r1**: `p256::PublicKey` validates the point is on the curve (33 or 65 bytes)

Invalid key bytes (wrong length, not on curve, identity point) are rejected before signature verification, preventing key-related panics or UB.

### Cross-algorithm key size barrier

| Algorithm | Public key `raw()` size | Header `alg` |
|-----------|------------------------|--------------|
| Ed25519 | 32 bytes | EdDSA |
| secp256k1 | 33 bytes (compressed) | ES256K |
| secp256r1 | 33 bytes (compressed) | ES256 |

If an attacker swaps the header `alg` (e.g., EdDSA → ES256K), the 32-byte Ed25519 key bytes would be rejected by `Secp256k1PublicKey::from_bytes()` (expects 33 or 65 bytes). Even for secp256k1 ↔ secp256r1 (both 33 bytes), the key might parse but signature verification would fail because the curves are different.

## Remediation

None required. The design is cryptographically sound for DID-based authentication.
