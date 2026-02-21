# Finding: ECIES Ciphertext Validation Gaps vs Go

**Stream**: 01 - Cryptographic Inventory
**Session**: 3 - Encryption & ECIES Correctness
**Severity**: LOW
**Category**: ECIES / Input Validation / Go Compatibility
**Status**: NEW

## Summary

Two input validation differences between Rust and Go ECIES implementations cause different error behavior for malformed or edge-case inputs. Neither is exploitable, but both create behavioral divergence in a mixed Go/Rust network.

## Gap 1: Minimum Ciphertext Length Check

### Go — Strict Upfront Validation

`crypto/ecies.go:216-223`:

```go
minLength := X25519PublicKeySize + AESNonceSize + HMACSize + minCipherTextSize
// = 32 + 12 + 32 + 16 = 92 (prepended key mode)
// = 60 (non-prepended mode)
if len(cipherText) < minLength {
    return nil, ErrCipherTextTooShort
}
```

Go validates that the ciphertext is large enough for ALL components: ephemeral key (32) + AES nonce (12) + minimum GCM ciphertext (16, the auth tag alone) + HMAC (32). This rejects malformed input immediately, before any cryptographic computation.

### Rust — Partial Check, Defers to AES Layer

`crates/crypto/src/encryption/ecies.rs:193`:

```rust
if ciphertext.len() < X25519_PUBLIC_KEY_SIZE + HMAC_SIZE {  // 32 + 32 = 64
    return Err(...)
}
```

Rust only checks for ephemeral key + HMAC. It does not account for the AES nonce (12 bytes) or minimum ciphertext (16 bytes, the GCM auth tag).

### Behavioral Difference

For ciphertexts between 64 and 91 bytes (prepended key mode):

| Step | Go | Rust |
|------|-----|------|
| Length check | Rejects: "ciphertext too short" | Passes |
| ECDH | — | Computes shared secret |
| HKDF | — | Derives AES + HMAC keys |
| HMAC verify | — | Likely fails (wrong key/data) |
| Error returned | Immediate, cheap | After ECDH + HKDF + HMAC |
| Error message | "ciphertext too short" | "HMAC verification failed" |

The Rust code is still safe — the AES layer correctly rejects short input. But:
1. Unnecessary cryptographic work (ECDH + HKDF + HMAC) is performed before rejection
2. Different error messages make cross-implementation debugging harder
3. In the edge case where an attacker uses a low-order key (Finding 07) with a short ciphertext, HMAC verification could succeed on empty/minimal data, pushing the error to the AES layer

## Gap 2: Default `prepend_public_key` Behavior Inverted

### Go Default: Prepend

```go
type eciesOptions struct {
    noPubKeyPrepended bool  // Default: false → prepend IS the default
}
```

### Rust Default: Don't Prepend

```rust
#[derive(Default)]
pub struct EciesOptionsBuilder {
    prepend_public_key: bool,  // Default: false → don't prepend
}
```

The semantics are inverted. Go uses a double-negative (`noPubKeyPrepended = false` → prepend), while Rust uses a direct boolean (`prepend_public_key = false` → don't prepend).

### Current Impact: None

All callers in the codebase use the builder and explicitly set `prepend_public_key(true)` or `prepend_public_key(false)`. `EciesOptions::default()` is never called directly. This is a latent issue — it would cause interop failure if any code path used default options.

## Affected Code

- **Length check**: `crates/crypto/src/encryption/ecies.rs:193` — missing AES nonce + GCM tag minimum
- **Default options**: `crates/crypto/src/encryption/ecies.rs:44` — `EciesOptionsBuilder` default

## Remediation

### Length Check

Match Go's minimum length:

```rust
let min_overhead = AES_NONCE_SIZE + 16 + HMAC_SIZE;  // 12 + 16 + 32 = 60
let min_length = if has_prepended_key {
    X25519_PUBLIC_KEY_SIZE + min_overhead  // 32 + 60 = 92
} else {
    min_overhead  // 60
};

if ciphertext.len() < min_length {
    return Err(crypto_error("ciphertext too short"));
}
```

### Default Options

Change the builder default to match Go:

```rust
#[derive(Default)]
pub struct EciesOptionsBuilder {
    prepend_public_key: bool,  // Change default to true
    // ...
}

impl Default for EciesOptionsBuilder {
    fn default() -> Self {
        Self {
            prepend_public_key: true,  // Match Go default
            // ...
        }
    }
}
```

Or explicitly remove the `Default` derive and require callers to always specify `prepend_public_key`. Since all current callers set it explicitly, this is a non-breaking change.
