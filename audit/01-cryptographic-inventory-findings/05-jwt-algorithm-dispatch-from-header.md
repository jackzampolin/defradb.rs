# Finding: JWT Algorithm Dispatch Based on Attacker-Controlled Header

**Stream**: 01 - Cryptographic Inventory
**Severity**: LOW
**Category**: JWT / Defense-in-Depth
**Status**: NEW

## Summary

The JWT `from_token()` function selects its decode/verify path based on the `alg` field in the JWT header, which is attacker-controlled. The signature is verified using the header-specified algorithm, and only AFTER successful verification does the code check that the header algorithm matches the `key_type` claim in the payload. While not exploitable in practice (the self-signed JWT design requires the attacker to hold the private key), the ordering violates defense-in-depth: cryptographic work runs against attacker-chosen algorithm before input validation completes.

## Affected Code

### Algorithm Dispatch — `crates/identity/src/token/mod.rs:209-221`

```rust
let header_alg = parse_algorithm(token_str)?;  // (1) Algorithm from header

let claims: IdentityClaims = match header_alg.as_str() {
    "EdDSA" => decode_ed25519(token_str)?,    // (2) Signature verified as Ed25519
    "ES256K" => decode_secp256k1(token_str)?,  // (2) Signature verified as secp256k1
    "ES256" => decode_secp256r1(token_str)?,   // (2) Signature verified as secp256r1
    alg => { return Err(...) }
};

let key_type: IdentityKeyType = claims.key_type.parse()?;
let expected_alg = match key_type { ... };
if header_alg != expected_alg {          // (3) Consistency check AFTER verification
    return Err(...)
}
```

### Decode Functions Hardcode Key Type — `crates/identity/src/token/decoding.rs:78-115`

Each decode function uses a hardcoded `KeyType`, not the claims `key_type`:

```rust
pub(crate) fn decode_secp256k1(token: &str) -> Result<IdentityClaims> {
    // ...
    let public_key = decode_public_key_from_claims(&claims, KeyType::Secp256k1)?;  // Hardcoded
    // ... verify signature ...
}
```

## Why This Is Not Exploitable

The JWTs in DefraDB are self-signed: the token's `sub` claim contains the public key, and the signature proves the sender holds the corresponding private key. For an algorithm confusion attack to succeed, the attacker would need:

1. A public key that is valid on multiple curves simultaneously
2. Knowledge of the corresponding private key on the "wrong" curve
3. The key type mismatch to survive the post-verification check

This fails at step 3 — the `header_alg != expected_alg` check after verification catches any mismatch between the header algorithm and the claims `key_type`.

Even without step 3, cross-curve key reuse is impractical:
- Ed25519 public keys (32 bytes) vs ECDSA public keys (33 bytes) — different lengths
- secp256k1 and secp256r1 are different curves — same point encoding doesn't mean same private key

## Why It's Still Worth Noting

1. **Unnecessary crypto work**: An attacker can force the server to attempt Ed25519 verification for a secp256k1 token (or any other combination). The verification will fail, but the server performs hash computation and curve operations before rejecting.

2. **Defense-in-depth violation**: Best practice is to validate all input constraints before performing expensive/sensitive operations. The algorithm consistency check is cheap and should run first.

3. **Future risk**: If new key types are added with overlapping key lengths, the hardcoded `KeyType` in each decode function becomes the only barrier. Moving the consistency check earlier adds an explicit guard.

## Remediation

Move the algorithm consistency check before selecting the decode function:

```rust
let header_alg = parse_algorithm(token_str)?;

// Decode claims without verifying signature first
let jwt = parse_jwt(token_str)?;
let claims = decode_claims(jwt.payload)?;

// Validate algorithm matches claims BEFORE signature verification
let key_type: IdentityKeyType = claims.key_type.parse()?;
let expected_alg = match key_type { ... };
if header_alg != expected_alg {
    return Err(...)
}

// Now verify signature with the validated algorithm
let claims = match header_alg.as_str() {
    "EdDSA" => decode_ed25519(token_str)?,
    // ...
};
```

This eliminates unnecessary cryptographic operations for algorithm-mismatched tokens.
