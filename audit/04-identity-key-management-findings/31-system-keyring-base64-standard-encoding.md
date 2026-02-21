# SystemKeyring: base64 STANDARD Encoding Choice

- **Severity**: Informational (green)
- **Category**: Encoding
- **Status**: Verified acceptable

## Summary

SystemKeyring uses `base64::engine::general_purpose::STANDARD` (with `+`, `/`, and `=` padding) for encoding keys before storing in the OS keyring. This is intentional for Go DefraDB compatibility and is safe because OS keyrings store arbitrary strings — the `+`, `/`, and `=` characters are not problematic in this context.

## Affected Files

- `crates/keyring/src/system.rs:39` — STANDARD encode on set
- `crates/keyring/src/system.rs:62` — STANDARD decode on get

## Details

```rust
// system.rs:38-39
use base64::Engine;
let encoded = base64::engine::general_purpose::STANDARD.encode(key);
```

```rust
// system.rs:62-64
base64::engine::general_purpose::STANDARD
    .decode(&encoded)
    .map_err(|e| Error::Decryption(format!("invalid base64: {}", e)))
```

**Why STANDARD is fine here**:
1. OS keyrings (macOS Keychain, Linux Secret Service, Windows Credential Manager) store passwords as strings. Base64 STANDARD produces valid UTF-8 strings.
2. The `+` and `/` characters are not URL-special in this context (not used in URLs).
3. `=` padding is handled correctly by the `STANDARD` engine.
4. Go DefraDB uses standard base64 for the same purpose.

**Decode is strict**: `STANDARD.decode()` will reject non-base64 characters, returning an error. Corrupted data will not silently produce wrong keys — it will return `Error::Decryption`.

**Null byte concern**: Base64 STANDARD output never contains null bytes (output alphabet is `A-Za-z0-9+/=`), so OS keyrings that use C strings internally will not truncate the data.

## Verified Clean

- Encoding choice is correct and intentional
- Decode rejects invalid base64
- No null byte issues
- Compatible with Go DefraDB
