# Finding 01: DID Validation Only Checks Prefix, Not Structure

**Severity**: LOW
**Category**: Input Validation
**Status**: Confirmed (by design for Go compatibility)

## Summary

`Did::new()` validates only that the input starts with `"did:key:"`. It does not verify the multibase prefix (`z` for base58btc), the base58 encoding validity, the multicodec key type header, or whether the decoded key material has the correct length. This means syntactically invalid DIDs like `"did:key:"`, `"did:key:\x00"`, and `"did:key:NOT_BASE58"` all pass validation.

## Affected Files

- `crates/identity/src/did.rs:38-47`
- `crates/zanzibar/src/did.rs:22-31`

## Details

```rust
// crates/identity/src/did.rs:38
pub fn new(s: impl Into<String>) -> Result<Self, Error> {
    let s = s.into();
    if !s.starts_with(DID_KEY_PREFIX) {
        return Err(Error::InvalidDid(/* ... */));
    }
    Ok(Self(s)) // No further validation
}
```

### Accepted but invalid DIDs

| Input | Valid did:key? | `Did::new()` result |
|-------|---------------|---------------------|
| `"did:key:"` | No (empty key) | Ok |
| `"did:key:z"` | No (no key data) | Ok |
| `"did:key:INVALID"` | No (not multibase) | Ok |
| `"did:key:\x00\x00"` | No (null bytes) | Ok |
| `"did:key:z6Mk..."` (valid) | Yes | Ok |

### Security impact

Low. DIDs are used as string identifiers in ACP tuple lookups (exact string match). A malformed DID will simply never match a legitimately derived DID, so it can't be used to gain access to documents owned by valid identities. However:

1. **Late error detection**: Invalid DIDs propagate through the system until they're used in a context that expects real key material (e.g., signature verification).
2. **Storage waste**: Invalid DIDs can be stored in ACP relationship tuples, consuming storage without ever being matchable.

### Why this exists

Go DefraDB also uses prefix-only validation for DID strings. The Rust implementation matches Go's behavior for compatibility. Full did:key validation would require multibase decoding and multicodec parsing, adding dependencies and complexity.

## Remediation

Consider adding optional strict validation for DIDs arriving from external boundaries (HTTP API, P2P messages) while keeping the lightweight validation for internal use:

```rust
pub fn new_strict(s: impl Into<String>) -> Result<Self, Error> {
    let s = s.into();
    if !s.starts_with(DID_KEY_PREFIX) {
        return Err(Error::InvalidDid(/* ... */));
    }
    let key_portion = &s[DID_KEY_PREFIX.len()..];
    if !key_portion.starts_with('z') {
        return Err(Error::InvalidDid("missing multibase prefix 'z'".into()));
    }
    // Optionally: validate base58btc decoding
    Ok(Self(s))
}
```

## Test Gap

- No test for `Did::new("did:key:")` (empty key portion)
- No test for null bytes or non-UTF8-safe content in DID strings
- No test that invalid DIDs fail ACP lookups gracefully
