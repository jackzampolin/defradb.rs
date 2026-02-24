# Finding 18: Signature Verification Uses Constant-Time Crypto Libraries

**Severity**: GREEN
**Category**: Timing Side Channels
**Status**: Verified sound

## Summary

All three signature verification paths delegate to well-audited Rust crypto libraries that use constant-time operations internally. No custom comparison or branching on secret data exists in the verification path.

## Affected Files

- `crates/crypto/src/keys/ed25519.rs:225-240` — Ed25519 verify via `ed25519_dalek`
- `crates/crypto/src/keys/secp256k1.rs:191-214` — secp256k1 verify via `k256`
- `crates/crypto/src/keys/secp256r1.rs:180-202` — secp256r1 verify via `p256`

## Details

### Ed25519 (`ed25519_dalek`)

```rust
let signature = ed25519_dalek::Signature::from_bytes(&sig_bytes);
match self.key.verify(data, &signature) {
    Ok(_) => Ok(true),
    Err(_) => Ok(false),
}
```

`ed25519_dalek` uses `curve25519-dalek` which provides constant-time field arithmetic. The `verify()` method performs constant-time point multiplication and comparison. The `Ok`/`Err` result branch is not timing-sensitive since the result is public (accept/reject).

### secp256k1 (`k256`)

```rust
let sig = sig.normalize_s().unwrap_or(sig);
match self.key.verify_digest(hasher, &sig) {
    Ok(_) => Ok(true),
    Err(_) => Ok(false),
}
```

`k256` (from RustCrypto) uses constant-time field operations. The `normalize_s()` call operates on the signature value (not secret), and `verify_digest()` uses constant-time scalar multiplication.

### secp256r1 (`p256`)

```rust
let sig = sig.normalize_s().unwrap_or(sig);
match self.key.verify_digest(digest, &sig) {
    Ok(_) => Ok(true),
    Err(_) => Ok(false),
}
```

Same pattern as secp256k1, using `p256` (RustCrypto). Constant-time by construction.

### Non-constant-time paths (acceptable)

The DER parsing in `Signature::from_der()` is NOT constant-time — it returns early on malformed input. However, the DER bytes are derived from the JWT signature field (public data, not secret), so timing variations in DER parsing do not leak any sensitive information.

## Remediation

None required. The underlying crypto libraries provide constant-time guarantees.
