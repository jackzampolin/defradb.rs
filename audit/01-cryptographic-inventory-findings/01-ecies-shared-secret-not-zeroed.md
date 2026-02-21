# Finding: ECIES Shared Secrets and Derived Keys Not Zeroed

**Stream**: 01 - Cryptographic Inventory
**Severity**: LOW-MEDIUM
**Category**: Key Lifecycle / Memory Safety
**Status**: CONFIRMED

## Summary

The ECIES encrypt and decrypt functions leave the X25519 shared secret, HKDF-derived AES key, and HMAC key on the stack after use. These sensitive values persist in memory until the stack frame is reused.

## Affected File

`crates/crypto/src/encryption/ecies.rs`

## Details

### Encrypt Path (lines 122-135)

```rust
let shared_secret = ephemeral_private.diffie_hellman(public_key);  // line 122
// ...
let mut keys = [0u8; AES_KEY_SIZE + AES_KEY_SIZE];                // line 130
hkdf.expand(&[], &mut keys)?;                                      // line 131

let aes_key: [u8; AES_KEY_SIZE] = keys[..AES_KEY_SIZE].try_into().unwrap();   // line 134
let hmac_key: [u8; AES_KEY_SIZE] = keys[AES_KEY_SIZE..].try_into().unwrap();  // line 135
```

After the function returns, `shared_secret` (32 bytes), `keys` (64 bytes), `aes_key` (32 bytes), and `hmac_key` (32 bytes) all remain on the stack.

### Decrypt Path (lines 227-239)

Same pattern - shared secret and derived keys left on stack after decryption completes.

### Why This Matters

ECIES is used for document field encryption. The shared secrets and derived keys could decrypt any message encrypted with them. If the process is dumped (core dump, swap, memory forensics), these keys could be extracted.

### Why LOW-MEDIUM (not HIGH)

- These are ephemeral keys (new ones generated per encryption operation)
- They're stack-allocated (shorter lifetime than heap)
- The attack requires memory access to the running process
- The risk is lower than the private key zeroization issue (finding 00) because these are per-operation, not long-lived

## Remediation

Use `zeroize` crate to clear sensitive stack variables before function return:

```rust
use zeroize::Zeroize;

// After encryption/decryption is complete:
keys.zeroize();
// For SharedSecret, need to access inner bytes or use Zeroize if x25519-dalek supports it
```

Note: The compiler may optimize away zeroization of stack variables. Use `zeroize`'s `Zeroize` trait which uses `volatile_set_memory` to prevent this.

Also consider wrapping `aes_key` and `hmac_key` in `Zeroizing<[u8; N]>` types.
