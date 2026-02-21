# Key Type Ambiguity for 32-Byte Keys (secp256k1 vs Ed25519 Seed)

- **Severity**: Medium
- **Category**: Key Management
- **Status**: Confirmed — Known Design Limitation

## Summary

Both the CLI auth token generation and keyring-based auth use key length to infer the key type: 32 bytes → secp256k1, 64 bytes → Ed25519. However, an Ed25519 seed is also 32 bytes, and a secp256r1 key is also 32 bytes. The 32-byte disambiguation defaults to secp256k1 for Go CLI compatibility, but this means Ed25519 seeds and secp256r1 keys stored directly (not as 64-byte expanded form) will be misidentified.

## Affected Files

- `crates/cli/src/commands/client/mod.rs:188-199` (generate_auth_token)
- `crates/cli/src/commands/client/mod.rs:233-242` (generate_auth_token_from_keyring)
- `crates/cli/src/commands/identity.rs:277-284` (detect_key_type)

## Details

```rust
// client/mod.rs:190-199
let key_type = match key_bytes.len() {
    32 => KeyType::Secp256k1, // Default to secp256k1 for 32-byte keys (Go CLI compat)
    64 => KeyType::Ed25519,
    len => { return Err(...) }
};
```

```rust
// identity.rs:277-284 — same issue
fn detect_key_type(bytes: &[u8]) -> Result<identity::IdentityKeyType> {
    match bytes.len() {
        64 => Ok(identity::IdentityKeyType::Ed25519),
        32 => Ok(identity::IdentityKeyType::Secp256k1),
        n => Err(...)
    }
}
```

**Problems**:
1. secp256r1 keys (32 bytes) will be misidentified as secp256k1
2. Ed25519 seeds (32 bytes from JWK `d` field) will be misidentified as secp256k1 if re-imported outside the identity command flow
3. No way to distinguish without metadata

The `keyring generate` command stores Ed25519 as 64 bytes (seed + pubkey) which avoids the ambiguity for that key type. But `identity import` from JWK reconstructs the 64-byte form explicitly:
```rust
// identity.rs:397-401 — correct: 32-byte seed → 64-byte key
let full_key = crypto::ed25519_key_from_seed(&d_bytes)?;
Ok((identity::IdentityKeyType::Ed25519, full_key))
```

## Remediation

Consider storing key type metadata alongside the raw bytes in the keyring, or use a tagged format:

```
[1 byte key_type tag] [N bytes raw key]
```

This would eliminate length-based ambiguity. However, this breaks Go keyring compatibility where raw bytes are stored without metadata.

**Accept as-is for Go compatibility** — the 32-byte → secp256k1 default matches Go CLI behavior.

## Test Gap

No test for misidentification scenario (e.g., importing a secp256r1 key then trying to use it for auth).
