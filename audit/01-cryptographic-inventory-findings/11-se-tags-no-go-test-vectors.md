# Finding: SE Tag Tests Contain No Go-Generated Test Vectors

**Stream**: 01 - Cryptographic Inventory
**Session**: 4 - Go Compatibility Cross-Verification
**Severity**: MEDIUM-HIGH (masks Finding 10, the HIGH severity UTF-8 divergence)
**Category**: Test Coverage / Go Compatibility
**Status**: NEW

## Summary

The SE Go compatibility test file (`crates/crypto/tests/go_compat_se.rs`) contains zero hardcoded test vectors from Go. All 14 tests verify the Rust implementation against a local `compute_expected_tag()` function that reimplements the same algorithm — this is circular validation. The file claims Go compatibility but proves only internal consistency.

## Evidence

### All Other Go Compat Files Use Hardcoded Vectors

| Test File | Hardcoded Go Vectors | Byte-Equality Tests |
|---|---|---|
| `go_compat_keys.rs` | 15+ signature/key constants | Ed25519, secp256k1 byte-equal |
| `go_compat_encryption.rs` | X25519 keys, shared secret, HKDF keys, ciphertexts | Full chain byte-equal |
| `go_compat_serialization.rs` | DAG-CBOR hex strings from Go | Exact byte match |
| **`go_compat_se.rs`** | **NONE** | **NONE** |

### Circular Validation Pattern

`go_compat_se.rs:18-35`:

```rust
fn compute_expected_tag(
    key: &[u8], identity: &str, collection: &str, field: &str, value: &[u8],
) -> [u8; 16] {
    let domain_separator = format!("eq:{}:{}:{}", identity, collection, field);
    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(domain_separator.as_bytes());
    mac.update(value);
    let result = mac.finalize().into_bytes();
    let mut tag = [0u8; 16];
    tag.copy_from_slice(&result[..16]);
    tag
}
```

This is functionally identical to the implementation in `tag.rs`. Every test that calls `compute_expected_tag()` and compares to `generate_equality_tag_str()` is comparing the implementation to itself.

### The "Known Vector" Test Isn't Known

`go_compat_se.rs:247-268`:

```rust
#[test]
fn test_known_vector_simple() {
    // ...
    let expected = compute_expected_tag(&key, identity, collection, field, value);
    let actual = generate_equality_tag_str(&key, identity, collection, field, value);
    assert_eq!(hex::encode(actual), hex::encode(expected), "Known test vector mismatch");
}
```

The "known test vector" is computed by the test itself, not hardcoded from Go output. If the algorithm had a subtle bug (like the UTF-8 lossy issue in Finding 10), this test would still pass because both sides have the same bug.

### What a Real Go Vector Would Look Like

A proper test would hardcode a hex string produced by Go:

```rust
#[test]
fn test_go_vector_with_binary_identity() {
    let key = [0u8; 32];
    let identity = &[0xd7, 0x5a, 0x98, 0x01]; // Raw public key bytes
    let collection = "users";
    let field = "email";
    let value = b"alice@example.com";

    // This hex was produced by Go: secore.GenerateEqualityTag(key, string(identity), ...)
    let expected_hex = "abcd1234..."; // HARDCODED FROM GO OUTPUT
    let actual = generate_equality_tag(&key, identity, collection, field, value);
    assert_eq!(hex::encode(actual), expected_hex);
}
```

No such test exists.

## Impact

Finding 10 (UTF-8 lossy divergence) went undetected because the SE test suite has no cross-implementation validation. Any algorithmic difference between Go and Rust SE tag generation — domain separator format, identity encoding, truncation behavior — would also be masked.

## Remediation

Generate test vectors from Go by running a simple Go program:

```go
package main

import (
    "encoding/hex"
    "fmt"
    secore "github.com/sourcenetwork/defradb/internal/se/core"
)

func main() {
    key := make([]byte, 32) // zero key
    identity := string([]byte{0xd7, 0x5a, 0x98, 0x01})
    collection := "users"
    field := "email"
    value := []byte("alice@example.com")

    tag := secore.GenerateEqualityTag(key, identity, collection, field, value)
    fmt.Printf("Tag: %s\n", hex.EncodeToString(tag))
}
```

Add the resulting hex as a hardcoded constant in `go_compat_se.rs` and assert byte equality. This would have immediately caught the UTF-8 lossy divergence.
