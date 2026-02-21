# Finding: Private Key Types Lack Zeroize

**Stream**: 01 - Cryptographic Inventory
**Severity**: MEDIUM
**Category**: Key Lifecycle / Memory Safety
**Status**: CONFIRMED

## Summary

None of the three private key types (`Ed25519PrivateKey`, `Secp256k1PrivateKey`, `Secp256r1PrivateKey`) implement `Zeroize` or `ZeroizeOnDrop`. Private key material remains in memory after the key struct is dropped.

## Affected Files

| File | Struct | Inner Type |
|------|--------|-----------|
| `crates/crypto/src/keys/ed25519.rs:38-41` | `Ed25519PrivateKey` | `ed25519_dalek::SigningKey` |
| `crates/crypto/src/keys/secp256k1.rs:23-26` | `Secp256k1PrivateKey` | `k256::ecdsa::SigningKey` |
| `crates/crypto/src/keys/secp256r1.rs` | `Secp256r1PrivateKey` | `p256::ecdsa::SigningKey` |

## Details

### Problem 1: No Drop-time zeroing

The structs are defined as simple wrappers:

```rust
#[derive(Clone)]
pub struct Ed25519PrivateKey {
    key: SigningKey,
}
```

No `Zeroize` derive, no `ZeroizeOnDrop` derive, no manual `Drop` impl. When the struct goes out of scope, the key material stays in the process's memory until the page is reused.

### Problem 2: `raw()` returns unprotected Vec

The `Key::raw()` trait method returns `Vec<u8>` containing raw key bytes:

```rust
fn raw(&self) -> Vec<u8> {
    let seed = self.key.to_bytes();
    let public = self.key.verifying_key().to_bytes();
    let mut result = Vec::with_capacity(64);
    result.extend_from_slice(&seed);
    result.extend_from_slice(&public);
    result
}
```

This `Vec<u8>` is not wrapped in `Zeroizing<Vec<u8>>`, so callers of `raw()` also leave key material in memory.

### Problem 3: `Clone` without `Zeroize`

All three types derive `Clone`, creating additional copies of key material that won't be zeroed.

## Impact

In a long-running node process, private key material may persist in memory indefinitely. If the process memory is swapped to disk, dumped via core dump, or read by a privileged process, key material could be extracted.

For a database node that manages document signing keys and peer identity keys, this is a meaningful risk.

## Contrast with Keyring

The keyring crate correctly uses `Zeroizing<Vec<u8>>` for passwords (`crates/keyring/src/file.rs:36`). The same pattern should be applied to private key types.

## Remediation

1. Add `zeroize` dependency to crypto crate (already a workspace dep)
2. Implement `ZeroizeOnDrop` for all private key types
3. Change `raw()` return type to `Zeroizing<Vec<u8>>` (breaking change to `Key` trait)
4. Audit all call sites of `raw()` to ensure returned bytes are handled securely

### Minimal Fix (non-breaking)

Add manual `Drop` impl that zeroes the inner key:

```rust
impl Drop for Ed25519PrivateKey {
    fn drop(&mut self) {
        // ed25519_dalek::SigningKey stores a 32-byte seed internally
        // We can't directly zero it without unsafe, but we can overwrite via from_bytes
        self.key = SigningKey::from_bytes(&[0u8; 32]);
    }
}
```

### Better Fix

The underlying libraries (`ed25519-dalek`, `k256`, `p256`) all support `Zeroize` on their signing key types. Derive or delegate:

```rust
impl Zeroize for Ed25519PrivateKey {
    fn zeroize(&mut self) {
        self.key.zeroize(); // ed25519-dalek::SigningKey implements Zeroize
    }
}

impl Drop for Ed25519PrivateKey {
    fn drop(&mut self) {
        self.zeroize();
    }
}
```

Note: `ed25519-dalek::SigningKey` implements `Zeroize` when the `zeroize` feature is enabled. Check if it's enabled in `Cargo.toml`.
