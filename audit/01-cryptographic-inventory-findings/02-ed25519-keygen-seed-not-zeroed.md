# Finding: Ed25519 Key Generation Leaves Seed Material Unzeroed

**Stream**: 01 - Cryptographic Inventory
**Severity**: MEDIUM
**Category**: Key Lifecycle / Memory Safety
**Status**: NEW

## Summary

The `generate_ed25519()` function and `ed25519_key_from_seed()` helper leave seed material and intermediate key representations on the stack and heap without zeroization. Combined with the missing `"zeroize"` feature flag on `ed25519-dalek` (Finding 00), this means the full chain from entropy to key construction leaks sensitive material.

## Affected Code

### `generate_ed25519()` — `crates/crypto/src/keys/generation.rs:86-103`

```rust
pub fn generate_ed25519() -> Result<Ed25519PrivateKey> {
    let mut seed = [0u8; 32];                              // (1) 32-byte seed on stack
    OsRng.try_fill_bytes(&mut seed)?;

    let signing_key = Ed25519SigningKey::from_bytes(&seed); // (2) SigningKey on stack

    let public = signing_key.verifying_key().to_bytes();
    let mut key_bytes = Vec::with_capacity(64);             // (3) 64-byte Vec on heap
    key_bytes.extend_from_slice(&seed);                     //     contains raw seed
    key_bytes.extend_from_slice(&public);

    Ed25519PrivateKey::from_bytes(&key_bytes)               // key_bytes dropped here, unzeroed
}
```

Three unzeroed secrets:

| Variable | Location | Size | Concern |
|----------|----------|------|---------|
| `seed` | Stack | 32 bytes | Raw CSPRNG entropy — the actual private scalar |
| `signing_key` | Stack | ~64 bytes | Inner `SigningKey` NOT zeroed on drop (feature disabled) |
| `key_bytes` | Heap | 64 bytes | `Vec<u8>` containing seed + public key, not zeroed on drop |

### `ed25519_key_from_seed()` — `crates/crypto/src/keys/ed25519.rs:252-262`

```rust
pub fn ed25519_key_from_seed(seed: &[u8]) -> Result<Vec<u8>> {
    let seed_array: [u8; 32] = seed.try_into()?;           // (1) seed copy on stack
    let signing_key = SigningKey::from_bytes(&seed_array);  // (2) SigningKey, NOT zeroed
    let verifying_key = signing_key.verifying_key();
    let mut full_key = Vec::with_capacity(64);              // (3) 64-byte Vec on heap
    full_key.extend_from_slice(seed);
    full_key.extend_from_slice(verifying_key.as_bytes());
    Ok(full_key)                                            // returns unzeroed Vec
}
```

Called from JWK import path (`crates/cli/src/commands/identity.rs:398`). The returned `Vec<u8>` flows through identity construction and keyring storage without zeroization.

## Why MEDIUM

- The `seed` is the entire Ed25519 private key (the scalar). Anyone who recovers the seed can reconstruct the full signing key.
- `generate_ed25519()` is called once per node initialization and once per identity creation — the seed persists on the stack/heap until overwritten by unrelated operations.
- The heap-allocated `key_bytes` is especially concerning because heap memory is less likely to be overwritten quickly than stack memory, and can survive across allocator reuse boundaries.
- This compounds with Finding 00 (the `SigningKey` itself also not being zeroed).

## Contrast

The secp256k1 and secp256r1 generation functions (`generation.rs:65-74`) have a narrower exposure because:

1. They use `SigningKey::random(&mut OsRng)` — entropy stays inside the library
2. The temporary `SigningKey` IS zeroed on drop (k256/p256 do this unconditionally)
3. Only the `.to_bytes()` return value (a `FieldBytes` on the stack) lingers briefly

## Remediation

```rust
use zeroize::Zeroize;

pub fn generate_ed25519() -> Result<Ed25519PrivateKey> {
    let mut seed = [0u8; 32];
    OsRng.try_fill_bytes(&mut seed)?;

    let signing_key = Ed25519SigningKey::from_bytes(&seed);

    let public = signing_key.verifying_key().to_bytes();
    let mut key_bytes = Vec::with_capacity(64);
    key_bytes.extend_from_slice(&seed);
    key_bytes.extend_from_slice(&public);

    seed.zeroize();  // Zero the raw entropy

    let result = Ed25519PrivateKey::from_bytes(&key_bytes);
    key_bytes.zeroize();  // Zero the intermediate representation
    result
}
```

Also enable the `"zeroize"` feature on `ed25519-dalek` (see Finding 00) so `signing_key` is zeroed on drop automatically.
