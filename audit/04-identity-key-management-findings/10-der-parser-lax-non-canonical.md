# Finding 10: DER Parser Accepts Non-Canonical Encodings

**Severity**: LOW
**Category**: Cryptographic Parsing
**Status**: Confirmed (latent)

## Summary

`der_to_raw()` in `crates/identity/src/token/der.rs` does not validate the outer SEQUENCE length against actual content, does not verify trailing bytes, and skips (but never reads) multi-byte length fields. This makes it a **lax DER parser** that accepts non-canonical encodings.

## Affected Files

- `crates/identity/src/token/der.rs:7-115` — `der_to_raw()`

## Details

### 1. Sequence length ignored

```rust
// der.rs:18-25
let mut pos: usize = 2;

if der[1] & 0x80 != 0 {
    let len_bytes = (der[1] & 0x7f) as usize;
    pos = pos
        .checked_add(len_bytes)
        .ok_or_else(|| Error::TokenEncoding("DER length field overflow".to_string()))?;
}
```

The outer SEQUENCE length byte (or multi-byte length) is skipped but never read or compared against the actual R + S content length. A DER encoding with `0x30 0x10` (sequence of 16 bytes) containing 70 bytes of R+S data would be accepted.

### 2. Trailing bytes silently ignored

After parsing S, there is no check that `s_end == der.len()` or that the SEQUENCE length accounts for all consumed bytes. Extra bytes appended after S are silently discarded.

### 3. Multi-byte INTEGER lengths not handled

```rust
// der.rs:45
let r_len = der[pos] as usize;
```

R and S lengths are read as single bytes. A crafted DER with multi-byte length encoding for R/S (e.g., `0x82 0x00 0x20` for 32 bytes) would be parsed incorrectly — the first length byte would be interpreted as the full length value.

### Why this is LOW severity

`der_to_raw()` is only called during **token encoding** (`encoding.rs:53,69`) on DER output from the Rust crypto library's `sign()` function. Crypto libraries always produce well-formed, canonical DER. The user never provides DER directly — they provide raw R||S via the JWT signature, which goes through `raw_to_der()` (the reverse path) during decoding.

If `der_to_raw()` were ever called on untrusted input, these issues would become exploitable. The current call-site isolation prevents that.

## Remediation

Add validation for defense-in-depth:

```rust
// After parsing S, verify no trailing bytes
if s_end != der.len() {
    return Err(Error::TokenEncoding("DER signature has trailing bytes".to_string()));
}

// Validate SEQUENCE length against actual content
let expected_seq_len = (r_end - 2) + (s_end - r_end); // approximate
```

Or replace hand-rolled DER parsing with `der` crate or `k256::ecdsa::Signature::from_der()` and extract R/S from there.

## Test Gap

No tests for:
- DER with wrong SEQUENCE length but valid R/S components
- DER with trailing bytes after S
- DER with multi-byte length encoding
- Adversarial DER inputs designed to produce different R/S values
