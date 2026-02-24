# 01: `from_raw_parts` with Uncapped Length — Buffer Over-Read

| Field | Value |
|-------|-------|
| **Severity** | MEDIUM |
| **Category** | Memory Safety / Buffer Over-Read |
| **Status** | Open |

## Summary

Five call sites use `std::slice::from_raw_parts(ptr, len)` where `len` comes directly from the C caller with no upper-bound validation. If the Go side passes a length larger than the actual allocation, this creates a slice that extends beyond allocated memory — instant undefined behavior and a potential information disclosure vector.

## Affected Files

- `crates/ffi/src/node.rs:110-113` — `signing_private_key` + `signing_private_key_len`
- `crates/ffi/src/node.rs:218-220` — `sourcehub_signer_key` + `sourcehub_signer_key_len`
- `crates/ffi/src/p2p/node.rs:73-76` — `signing_private_key` (P2P variant)
- `crates/ffi/src/p2p/node.rs:474` — `sourcehub_signer_key` (P2P variant)
- `crates/ffi/src/se_key.rs:27` — `key_ptr` + `key_len`

## Details

### node.rs (lines 106-113)

```rust
let raw_identity = if !options.signing_private_key.is_null()
    && options.signing_private_key_len > 0
{
    let key_bytes = unsafe {
        std::slice::from_raw_parts(
            options.signing_private_key,
            options.signing_private_key_len,  // No cap!
        )
    };
```

The only validation is `!is_null() && len > 0`. There is no check for a reasonable maximum length. Cryptographic keys have well-known sizes:
- secp256k1 private key: 32 bytes
- ed25519 private key: 32 bytes (or 64 for expanded)
- AES-256 key: 32 bytes

### se_key.rs (lines 23-27)

```rust
if key_ptr.is_null() || key_len == 0 {
    return FfiResult::error("se encryption key is null or empty");
}
let key = std::slice::from_raw_parts(key_ptr, key_len).to_vec();
// key.len() == 32 check happens AFTER from_raw_parts
```

Here the length is validated to be exactly 32 **after** the slice is created and copied. But the UB from `from_raw_parts` with a bogus length occurs before the validation. If `key_len` is `usize::MAX`, the `from_raw_parts` itself is UB, and `.to_vec()` would attempt to allocate `usize::MAX` bytes.

### Exploitation scenario

If Go has a bug where it passes an incorrect length (e.g., uses `len(slice)` on a nil slice that defaults to a large number, or an integer overflow in CGO), the Rust side will read beyond the buffer. Since the key material is then used to construct cryptographic identities and stored in memory, this could leak adjacent heap data into identity material.

## Remediation

Add reasonable maximum length checks before `from_raw_parts`:

```rust
const MAX_PRIVATE_KEY_LEN: usize = 128; // generous upper bound

if options.signing_private_key_len > MAX_PRIVATE_KEY_LEN {
    return Err(format!(
        "signing_private_key_len {} exceeds maximum {}",
        options.signing_private_key_len, MAX_PRIVATE_KEY_LEN
    ));
}

let key_bytes = unsafe {
    std::slice::from_raw_parts(
        options.signing_private_key,
        options.signing_private_key_len,
    )
};
```

Apply the same pattern to all five call sites. For `se_key.rs`, move the `key_len == 32` check before `from_raw_parts`:

```rust
if key_len != 32 {
    return FfiResult::error(format!("se encryption key must be 32 bytes, got {}", key_len));
}
let key = unsafe { std::slice::from_raw_parts(key_ptr, key_len) }.to_vec();
```

## Test Gap

- No test passes an oversized `signing_private_key_len` (e.g., `usize::MAX` or `1_000_000`)
- No test verifies that mismatched pointer/length combinations are caught
- Add a test that sets `signing_private_key_len = 1000` with a 32-byte buffer to verify the cap
