# Finding 15: Base64 URL_SAFE_NO_PAD Used Consistently

**Severity**: GREEN
**Category**: Encoding / JWT Compliance
**Status**: Verified sound

## Summary

All JWT base64 encode and decode operations consistently use `base64::engine::general_purpose::URL_SAFE_NO_PAD`. No mixing of standard base64 or URL-safe-with-padding variants. The decode side accepts both padded and unpadded input (padding-indifferent), which is more permissive than strict JWT spec but does not introduce any vulnerability.

## Affected Files

- `crates/identity/src/token/encoding.rs:3,20,24,39,54,70` — all encode sites
- `crates/identity/src/token/decoding.rs:3,32,56,123` — all decode sites
- `crates/identity/tests/token_tests.rs:8,119,123,382,...` — test code

## Details

### Encode sites (all use `URL_SAFE_NO_PAD.encode()`)

| File | Line | What's encoded |
|------|------|---------------|
| encoding.rs | 20 | JWT header JSON |
| encoding.rs | 24 | Claims JSON |
| encoding.rs | 39 | Ed25519 signature (raw 64 bytes) |
| encoding.rs | 54 | secp256k1 signature (raw R\|\|S, 64 bytes) |
| encoding.rs | 70 | secp256r1 signature (raw R\|\|S, 64 bytes) |

### Decode sites (all use `URL_SAFE_NO_PAD.decode()`)

| File | Line | What's decoded |
|------|------|---------------|
| decoding.rs | 32 | Claims payload |
| decoding.rs | 56 | Signature bytes |
| decoding.rs | 123 | Header for algorithm extraction |

### Padding-indifferent decode

The `URL_SAFE_NO_PAD` engine uses `DecodePaddingMode::Indifferent`, meaning it accepts input with or without `=` padding. A token created with padded base64 (non-standard but possible from other implementations) would still decode correctly. The decoded bytes are identical regardless of padding presence.

This is not a security issue — it's a laxness that improves interoperability. The JWT spec (RFC 7515 §2) specifies URL-safe base64 without padding, but accepting padding is harmless.

### No timing leaks from base64

Base64 content in JWTs is not secret (tokens are transmitted in HTTP headers). Timing variations in base64 decoding do not leak any sensitive information.

## Remediation

None required. Consistent usage verified across all sites.
