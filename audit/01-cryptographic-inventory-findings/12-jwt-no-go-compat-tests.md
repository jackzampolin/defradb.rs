# Finding: JWT Token Format Has No Go Compatibility Tests

**Stream**: 01 - Cryptographic Inventory
**Session**: 4 - Go Compatibility Cross-Verification
**Severity**: MEDIUM
**Category**: Test Coverage / Go Compatibility
**Status**: NEW

## Summary

The identity crate has extensive JWT roundtrip tests (Rust -> Rust) but zero cross-implementation tests. No test parses a Go-generated JWT, and no test verifies that a Rust-generated JWT matches Go's output. The JWT wire format involves multiple components (header JSON serialization, claims field ordering, DER-to-raw signature conversion, base64url encoding) where subtle differences could cause interop failures.

## Evidence

### Existing JWT Tests Are All Self-Roundtrips

`crates/identity/tests/token_tests.rs` contains 24 tests. All follow this pattern:

```rust
let token = new_token(&identity, ...);   // Rust creates
let parsed = from_token(&token);         // Rust parses its own output
assert_eq!(parsed.did(), original_did);  // Works because same implementation
```

No test does:

```rust
const GO_JWT_ED25519: &[u8] = b"eyJhbGciOiJFZERTQSIs..."; // FROM GO
let parsed = from_token(GO_JWT_ED25519);  // Rust parses Go's output
```

### Go's Identity Compat Tests Also Lack JWTs

`crates/identity/tests/go_compat.rs` tests signatures and DIDs against Go vectors but does NOT test JWT encoding or decoding. All signature tests use raw `identity.sign()` — they never exercise the JWT code path (header construction, DER-to-raw conversion, base64url encoding).

### Potential Divergence Points

| Component | Rust Implementation | Potential Go Difference |
|---|---|---|
| Header JSON | `serde_json::json!({"alg": alg, "typ": "JWT"}).to_string()` | Go's `lestrrat-go/jwx` uses internal serialization |
| Claims JSON | `serde_json::to_string(claims)` — field order depends on struct | Go's jwx builds claims differently |
| Custom claims | `key_type`, `authorized_account` as flat fields | Go sets via `token.Set(KeyTypeClaim, ...)` |
| Signature format | Ed25519: raw 64 bytes; ECDSA: DER → raw R\|\|S 64 bytes | Go's jwx handles internally |
| Base64url | `URL_SAFE_NO_PAD` from `base64` crate | Go's jwx uses its own base64url |

### Most Likely Interop Failure: Claims JSON Field Ordering

Rust's `IdentityClaims` struct serialization order:

```rust
#[derive(Serialize, Deserialize)]
pub struct IdentityClaims {
    pub sub: String,      // Subject (hex public key)
    pub iss: String,      // Issuer (DID)
    pub exp: u64,         // Expiration
    pub nbf: u64,         // Not before
    pub iat: u64,         // Issued at
    pub aud: Option<Vec<String>>,
    pub key_type: String,
    pub authorized_account: Option<String>,
}
```

Go's JWT library serializes standard claims (`sub`, `iss`, `exp`, etc.) separately from custom claims, potentially in a different order. Since JWT signing covers the raw base64url-encoded payload, field ordering only matters when **comparing tokens**, not when **verifying** them (verification uses the raw string from the token, not re-serialized JSON).

### Why MEDIUM (Not Higher)

JWT verification is ordering-agnostic: both Go and Rust split the token on dots and verify the signature against `header_b64.payload_b64` exactly as received. So a Rust-generated JWT can be verified by Go, and vice versa, because neither side re-serializes the header or payload for verification.

The risk is more subtle:

1. **Claims parsing**: If Go's JWT library expects claims in a specific format that Rust doesn't produce (e.g., `aud` as string vs array), parsing could fail
2. **DER-to-raw conversion edge cases**: The Rust `der_to_raw()` function handles leading zeros and high-bit padding, but hasn't been tested against Go's conversion for edge case signatures
3. **secp256r1 JWT**: Go currently only supports Ed25519 and secp256k1 for JWTs (line 149 of Go's `identity_impl.go`), but Rust supports secp256r1 — creating a compatibility gap if Rust nodes issue ES256 JWTs that Go nodes can't verify

## Affected Code

- **JWT encoding**: `crates/identity/src/token/encoding.rs` — `build_signing_input()`, `encode_*()` functions
- **JWT decoding**: `crates/identity/src/token/decoding.rs` — `decode_*()` functions
- **DER conversion**: `crates/identity/src/token/der.rs` — `der_to_raw()`, `raw_to_der()`
- **Missing tests**: `crates/identity/tests/` — no Go-generated JWT vectors

## Remediation

1. Generate JWT test vectors from Go:

```go
token, _ := identity.NewToken(time.Hour, someAudience, someAccount)
fmt.Printf("JWT: %s\n", string(token))
```

2. Add to Rust tests:

```rust
const GO_ED25519_JWT: &[u8] = b"eyJ...";  // From Go
const GO_SECP256K1_JWT: &[u8] = b"eyJ..."; // From Go

#[test]
fn test_parse_go_ed25519_jwt() {
    let identity = from_token(GO_ED25519_JWT).unwrap();
    assert_eq!(identity.key_type(), IdentityKeyType::Ed25519);
    assert_eq!(identity.did().unwrap().as_str(), ED25519_DID);
}
```

3. Also test Rust -> Go direction by comparing Rust-generated JWT structure (header, payload format) against Go's expectations.
