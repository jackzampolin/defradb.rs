# Finding: SE HMAC Key Accepts Any Length Without Validation

**Stream**: 01 - Cryptographic Inventory
**Session**: 5 - Searchable Encryption & Merkle Proof
**Severity**: LOW-MEDIUM (defense in depth — callers currently pass 32-byte keys)
**Category**: Searchable Encryption / Input Validation
**Status**: NEW

## Summary

The `generate_equality_tag` function accepts HMAC keys of any length, including zero-length keys. While HMAC-SHA256 technically operates on any key size, the function's contract specifies a "32-byte AES-256 key" yet enforces nothing. This creates a defense-in-depth gap where a caller could accidentally pass incorrect key material without detection.

## Evidence

### No Key Length Check

`crates/crypto/src/se/tag.rs:88`:

```rust
let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
```

The `.expect()` message explicitly acknowledges that any key size is accepted. The HMAC RFC (RFC 2104) does specify that HMAC works with any key length — keys shorter than the hash block size are zero-padded, keys longer are hashed first.

### API Contract Says 32 Bytes

`crates/crypto/src/se/tag.rs:57`:

```rust
/// * `key` - 32-byte AES-256 key for HMAC computation
```

The documentation says "32-byte AES-256 key" but the code enforces nothing. A 1-byte key, empty key, or 128-byte key would all silently produce tags.

### Insecure Key Lengths Possible

| Key Length | HMAC Behavior | Security |
|---|---|---|
| 0 bytes | Zero-padded to block size | **BROKEN** — no secret |
| 1-15 bytes | Zero-padded | **WEAK** — low entropy |
| 16-31 bytes | Zero-padded | Reduced security margin |
| 32 bytes | Used directly | **CORRECT** |
| 33+ bytes | Hashed first (HMAC spec) | Works but unexpected |

### Caller Default is All-Zeros

`crates/db/src/se/coordinator.rs:68`:

```rust
enc_key: vec![0u8; 32],
```

The default coordinator config uses a 32-byte all-zeros key. While this is "correct" length, it has zero entropy (see Finding 16).

## Impact

If a bug in the key provisioning pipeline passes a truncated, empty, or wrong-size key to the tag generator, the tags would still be computed without error. The resulting tags would:
- Not match Go's tags (if Go validates key length)
- Provide reduced or zero security
- Be undetectable without comparing against expected values

## Affected Code

- `crates/crypto/src/se/tag.rs:88` — `HmacSha256::new_from_slice(key)` with no length check
- `crates/db/src/se/coordinator.rs:68` — default all-zeros key

## Remediation

Add key length validation at the tag generation level:

```rust
assert_eq!(key.len(), 32, "SE HMAC key must be exactly 32 bytes, got {}", key.len());
```

Or return a `Result` instead of panicking:

```rust
if key.len() != 32 {
    return Err(Error::Crypto(format!("SE key must be 32 bytes, got {}", key.len())));
}
```
