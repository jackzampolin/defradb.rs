# Finding: Private Key Types Lack Zeroize

**Stream**: 01 - Cryptographic Inventory
**Severity**: MEDIUM-HIGH (Ed25519), LOW (secp256k1, secp256r1)
**Category**: Key Lifecycle / Memory Safety
**Status**: CONFIRMED — deep-dive completed, severity split by key type

## Summary

The three private key wrapper types (`Ed25519PrivateKey`, `Secp256k1PrivateKey`, `Secp256r1PrivateKey`) do not implement `Zeroize` or `ZeroizeOnDrop`. However, the severity differs sharply between Ed25519 and the other two due to underlying library behavior.

## Deep-Dive Results

### Ed25519: MEDIUM-HIGH — Inner key NOT zeroed on drop

The workspace `Cargo.toml` (line 67) specifies:

```toml
ed25519-dalek = { version = "2.1", features = ["serde"] }
```

**The `"zeroize"` feature is NOT enabled.** The `ed25519-dalek 2.2.0` `SigningKey` implements `ZeroizeOnDrop` only when the `"zeroize"` feature flag is active. Without it, neither the wrapper `Ed25519PrivateKey` nor its inner `ed25519_dalek::SigningKey` zeroes key material on drop.

This means the 32-byte private seed persists in memory indefinitely after the struct is dropped.

**Affected code**: `crates/crypto/src/keys/ed25519.rs:38-41`

```rust
#[derive(Clone)]
pub struct Ed25519PrivateKey {
    key: SigningKey,  // NOT zeroed on drop — feature flag missing
}
```

Ed25519 keys are used as **node peer identity keys** (long-lived, one per node) and document signing keys. These are the highest-value keys in the system.

### secp256k1: LOW — Inner key IS zeroed on drop

The `k256 0.13.4` crate's `ecdsa::SigningKey` (backed by `ecdsa 0.16.9`) implements `ZeroizeOnDrop` **unconditionally** — no feature flag required. When the wrapper `Secp256k1PrivateKey` drops, Rust drops its inner `SigningKey` field, which triggers zeroization.

**Affected code**: `crates/crypto/src/keys/secp256k1.rs:23-26`

The remaining concern is `.raw()` returning unprotected `Vec<u8>` (see Finding 03).

### secp256r1: LOW — Inner key IS zeroed on drop

Same as secp256k1. The `p256 0.13.2` crate's `ecdsa::SigningKey` implements `ZeroizeOnDrop` unconditionally.

**Affected code**: `crates/crypto/src/keys/secp256r1.rs:22-26`

## Evidence: Library Versions and Zeroize Support

| Crate | Version (Cargo.lock) | `ZeroizeOnDrop` | Feature Required | Feature Enabled? |
|-------|---------------------|-----------------|------------------|-----------------|
| `ed25519-dalek` | 2.2.0 | Yes | `"zeroize"` | **NO** — only `"serde"` |
| `k256` (via `ecdsa`) | 0.13.4 (ecdsa 0.16.9) | Yes | None | N/A |
| `p256` (via `ecdsa`) | 0.13.2 (ecdsa 0.16.9) | Yes | None | N/A |

## Clone Behavior

All three types derive `Clone`. For k256/p256, cloned copies independently zeroize on drop (each clone contains its own `SigningKey` with `ZeroizeOnDrop`). For Ed25519, cloned copies are also NOT zeroed — compounding the issue.

## Affected Call Sites

The `raw()` concern (returning unprotected `Vec<u8>`) affects all three types and is tracked separately in Finding 03.

## Remediation

### Critical (Ed25519)

Add `"zeroize"` to ed25519-dalek features in workspace `Cargo.toml`:

```toml
ed25519-dalek = { version = "2.1", features = ["serde", "zeroize"] }
```

This single change enables `ZeroizeOnDrop` on the inner `SigningKey`, which Rust's drop semantics will invoke automatically when `Ed25519PrivateKey` drops. No wrapper code changes needed for the basic fix.

### Defense-in-Depth (All Three Types)

For explicit zeroization control, implement `Zeroize`/`ZeroizeOnDrop` on the wrapper types. Note that `ed25519-dalek::SigningKey` implements `ZeroizeOnDrop` but NOT `Zeroize` (cannot call `.zeroize()` manually). For k256/p256, `ecdsa::SigningKey` also implements only `ZeroizeOnDrop`, not `Zeroize`.

The manual `Drop` + overwrite approach from the original finding is still valid as defense-in-depth.
