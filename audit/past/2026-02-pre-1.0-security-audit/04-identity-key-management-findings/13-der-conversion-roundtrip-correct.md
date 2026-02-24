# Finding 13: DER Conversion Roundtrip Mathematically Correct

**Severity**: GREEN
**Category**: Cryptographic Encoding
**Status**: Verified sound

## Summary

The `raw_to_der()` and `der_to_raw()` functions in `crates/identity/src/token/der.rs` correctly handle all valid ECDSA signature encodings for 256-bit curves (secp256k1 and secp256r1). Leading-zero padding, high-bit handling, and short-value right-alignment are all correct.

## Affected Files

- `crates/identity/src/token/der.rs:7-168`

## Details

### `raw_to_der()` correctness (lines 118-168)

Verified all edge cases for `encode_der_integer()`:

| Input R (32 bytes) | Trim | High bit? | DER output | Correct? |
|-------------------|------|-----------|------------|----------|
| `[0x12, ..., 0x34]` | No leading zeros | No | `0x02 0x20 <32 bytes>` | YES |
| `[0xFF, ..., 0x00]` | No leading zeros | Yes | `0x02 0x21 0x00 <32 bytes>` | YES |
| `[0x00, 0x80, ...]` | Strip 1 zero → 31 bytes | Yes | `0x02 0x20 0x00 <31 bytes>` | YES |
| `[0x00, ..., 0x00, 0x01]` | Strip 31 zeros → `[0x01]` | No | `0x02 0x01 0x01` | YES |
| `[0x00, ..., 0x00]` (all zeros) | Strip 31 zeros → `[0x00]` | No | `0x02 0x01 0x00` | YES |

The zero-stripping loop `while start < bytes.len() - 1 && bytes[start] == 0` always preserves at least one byte. The `bytes.len() - 1` guard prevents underflow since input is always exactly 32 bytes (from the 64-byte length check at line 119).

### `der_to_raw()` correctness (lines 7-115)

Verified padding removal and right-alignment:

| DER R component | Strip leading 0x00? | Raw R (32 bytes) | Correct? |
|----------------|---------------------|-------------------|----------|
| 33 bytes: `0x00 \|\| <32 bytes with high bit>` | Yes → 32 bytes | Direct copy | YES |
| 32 bytes: `<no high bit>` | No | Direct copy | YES |
| 1 byte: `0x01` | No | `[0x00, ..., 0x00, 0x01]` | YES |
| 1 byte: `0x00` | No | `[0x00, ..., 0x00]` | YES |

The right-alignment uses `r_offset = 32 - r.len().min(32)` with zero-initialized buffer, correctly left-padding short values with zeros.

### SEQUENCE length cap

`raw_to_der()` checks `content_len > 127` and rejects, ensuring single-byte DER length encoding. Maximum theoretical content length for 256-bit curves is 70 bytes (2 × 35), well under 127.

## Remediation

None required. The implementation is correct for all valid inputs.
