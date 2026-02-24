# Finding: SE Tag Domain Separator UTF-8 Lossy Conversion Diverges from Go

**Stream**: 01 - Cryptographic Inventory
**Session**: 4 - Go Compatibility Cross-Verification
**Severity**: HIGH (1.0 blocker — SE tags will not match between Go and Rust nodes)
**Category**: Go Compatibility / Searchable Encryption
**Status**: NEW

## Summary

The Rust `generate_equality_tag()` function uses `String::from_utf8_lossy()` to convert identity bytes to a string for the domain separator, replacing invalid UTF-8 sequences with the Unicode replacement character (U+FFFD). Go's equivalent uses `string(pubKey.Raw())`, which preserves raw bytes verbatim. Since public keys are raw binary data (not valid UTF-8), the domain separators differ for nearly all identities, producing incompatible search tags.

## Evidence

### Go: Raw Bytes Preserved

`internal/se/se.go:215-217`:

```go
if pubKey := ident.PublicKey(); pubKey != nil {
    identityStr = string(pubKey.Raw())  // Raw binary bytes, no encoding
}
```

`internal/se/core/tag.go:41`:

```go
domainSeparator := fmt.Sprintf("eq:%s:%s:%s", identityID, collectionID, fieldName)
```

Go's `string([]byte{...})` and `fmt.Sprintf("%s", ...)` pass raw bytes through without transformation.

### Rust: Invalid UTF-8 Replaced

`crates/crypto/src/se/tag.rs:84`:

```rust
let identity_str = String::from_utf8_lossy(identity_id);
```

`String::from_utf8_lossy` replaces any byte sequence that is not valid UTF-8 with U+FFFD (3 bytes: `0xEF 0xBF 0xBD`).

### Concrete Example

For the Ed25519 public key `[0xd7, 0x5a, 0x98, ...]`:

| Implementation | Domain separator bytes (identity portion) |
|---|---|
| Go | `[0xd7, 0x5a, 0x98, ...]` (raw bytes) |
| Rust | `[0xEF, 0xBF, 0xBD, 0x5a, 0xEF, 0xBF, 0xBD, ...]` (replacement chars) |

The byte `0xd7` is a UTF-8 lead byte (110xxxxx) requiring a continuation byte (10xxxxxx). `0x5a` ('Z') is not a valid continuation, so `0xd7` is replaced with U+FFFD (3 bytes), and `0x5a` is kept. `0x98` is also an invalid start byte, replaced with U+FFFD.

These completely different domain separators produce completely different HMAC tags.

### Production Code Path Confirmed

`crates/db/src/se/artifact_gen.rs:42-52`:

```rust
let identity_bytes = identity_pubkey.unwrap_or(&[]);

let tag = match enc_idx.index_type {
    EncryptedIndexType::Equality => generate_equality_tag(
        enc_key,
        identity_bytes,  // Raw public key bytes, NOT hex-encoded
        collection_id,
        &enc_idx.field_name,
        &value_bytes,
    ),
};
```

The production code passes raw public key bytes directly, triggering the UTF-8 lossy conversion.

## Impact

### Searchable Encryption Broken in Mixed Networks

In a mixed Go/Rust network with searchable encryption enabled:

1. **Go node** creates document, generates SE tag with raw-bytes domain separator
2. **Rust node** tries to search for that document, generates SE tag with UTF-8-lossy domain separator
3. Tags don't match — search returns no results
4. Same failure in reverse: Rust creates, Go can't find

This affects ALL encrypted equality queries where identity is present (which is the default — `identity_pubkey` is provided whenever an authenticated user creates or queries documents).

### Not Caught by Existing Tests

The `go_compat_se.rs` test file contains NO Go-generated test vectors. All tests verify the implementation against a local `compute_expected_tag()` function that uses the same `from_utf8_lossy` conversion — circular validation. See details in Finding 11.

## Affected Code

- **Primary**: `crates/crypto/src/se/tag.rs:84` — `String::from_utf8_lossy(identity_id)`
- **Caller**: `crates/db/src/se/artifact_gen.rs:42-52` — passes raw public key bytes

## Remediation

Match Go's behavior by passing raw identity bytes directly to HMAC without UTF-8 conversion:

```rust
pub fn generate_equality_tag(
    key: &[u8],
    identity_id: &[u8],
    collection_id: &str,
    field_name: &str,
    value: &[u8],
) -> [u8; SEARCH_TAG_SIZE] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");

    // Build domain separator as raw bytes, matching Go's string() cast
    mac.update(b"eq:");
    mac.update(identity_id);  // Raw bytes, NOT UTF-8 converted
    mac.update(b":");
    mac.update(collection_id.as_bytes());
    mac.update(b":");
    mac.update(field_name.as_bytes());
    mac.update(value);

    let result = mac.finalize();
    let full_tag = result.into_bytes();

    let mut tag = [0u8; SEARCH_TAG_SIZE];
    tag.copy_from_slice(&full_tag[..SEARCH_TAG_SIZE]);
    tag
}
```

This feeds the identity bytes directly into HMAC without string conversion, matching Go's behavior exactly. The `fmt.Sprintf("eq:%s:%s:%s", ...)` in Go concatenates raw bytes — this approach does the same thing through incremental HMAC updates.

After fixing, add a Go-generated test vector with a known public key to confirm byte-for-byte tag equality (see Finding 11).
