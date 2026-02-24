# Finding: ECIES Derived Key Material Not Zeroed

**Stream**: 01 - Cryptographic Inventory
**Severity**: LOW (downgraded from LOW-MEDIUM)
**Category**: Key Lifecycle / Memory Safety
**Status**: CONFIRMED — deep-dive narrows scope, downgrades severity

## Summary

The ECIES encrypt and decrypt functions leave HKDF-derived key material (`keys`, `aes_key`, `hmac_key`) unzeroed on the stack after use. The X25519 shared secret and ephemeral private key ARE zeroed on drop by the underlying library.

## Deep-Dive Results: What IS Zeroed

### SharedSecret — ZEROED on drop

`x25519-dalek 2.0.1` implements `Zeroize` and `ZeroizeOnDrop` on `SharedSecret` via the `"zeroize"` feature, which is a **default feature**. The crypto crate's `Cargo.toml` specifies:

```toml
x25519-dalek = { version = "2.0", features = ["static_secrets"] }
```

No `default-features = false`, so defaults (including `"zeroize"`) are active. The `SharedSecret` at lines 122 and 227 IS zeroed when it drops at end of scope.

### StaticSecret (ephemeral private key) — ZEROED on drop

Same logic applies. The ephemeral `StaticSecret` generated at line 116-118 (encrypt path) or passed via `private_key` parameter (decrypt path) implements `ZeroizeOnDrop` with the default `"zeroize"` feature.

## What Is NOT Zeroed

### Encrypt Path (`crates/crypto/src/encryption/ecies.rs:130-135`)

```rust
let mut keys = [0u8; AES_KEY_SIZE + AES_KEY_SIZE];       // 64 bytes on stack — NOT zeroed
hkdf.expand(&[], &mut keys)?;

let aes_key: [u8; AES_KEY_SIZE] = keys[..AES_KEY_SIZE].try_into().unwrap();   // 32 bytes — NOT zeroed
let hmac_key: [u8; AES_KEY_SIZE] = keys[AES_KEY_SIZE..].try_into().unwrap();  // 32 bytes — NOT zeroed
```

After `encrypt_ecies` returns, these three stack arrays (128 bytes total of derived key material) remain until the stack frame is reused.

### Decrypt Path (`crates/crypto/src/encryption/ecies.rs:234-239`)

Identical pattern — `keys`, `aes_key`, `hmac_key` left unzeroed on the stack.

## Why LOW (Downgraded from LOW-MEDIUM)

The original assessment overestimated severity by assuming the shared secret and ephemeral private key were also unzeroed. With the deep-dive confirming those ARE zeroed:

1. **Only derived keys remain** — these are one-time-use symmetric keys derived from already-zeroed secrets
2. **Stack-allocated** — shorter lifetime than heap, overwritten by subsequent function calls
3. **Ephemeral by design** — new keys derived per encrypt/decrypt operation
4. **Cannot recover the shared secret** — even with the derived keys, the X25519 private key is zeroed
5. **Attack requires live memory access** — core dump, swap, or memory forensics during the function call

The residual risk is that an attacker with memory access during or shortly after an ECIES operation could extract the symmetric keys for that specific operation. This is a defense-in-depth concern, not a primary vulnerability.

## Affected File

`crates/crypto/src/encryption/ecies.rs` — lines 130-135 (encrypt) and 234-239 (decrypt)

## Remediation

Use `zeroize` crate to clear derived key arrays before function return:

```rust
use zeroize::Zeroize;

// After encryption/decryption is complete:
keys.zeroize();
aes_key.zeroize();
hmac_key.zeroize();
```

The `zeroize` crate uses `volatile_set_memory` to prevent compiler optimization from eliding the zeroing. Stack arrays implement `Zeroize` via blanket impl on `[u8; N]`.
