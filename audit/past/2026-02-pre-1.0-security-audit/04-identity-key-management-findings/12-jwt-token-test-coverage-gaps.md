# Finding 12: JWT Token Test Coverage Gaps

**Severity**: LOW
**Category**: Test Coverage
**Status**: Confirmed

## Summary

The JWT token tests (`crates/identity/tests/token_tests.rs`) cover the happy path and basic tamper detection for all three algorithms, but miss several security-relevant edge cases. The DER conversion tests (`crates/identity/src/token/der.rs`) only test roundtrip and basic error cases, not adversarial inputs.

## Affected Files

- `crates/identity/tests/token_tests.rs` — 578 lines, 19 test functions
- `crates/identity/src/token/der.rs:170-256` — 7 test functions

## Details

### Missing token-level tests

| Missing Test | Risk | Description |
|-------------|------|-------------|
| Empty signature part | LOW | Token like `"header.payload."` — should fail, but not tested |
| Truncated signature | LOW | Signature shorter than expected (e.g., 32 bytes instead of 64) |
| Cross-algorithm key confusion | MEDIUM | Ed25519 header with secp256k1 key bytes in `sub` claim |
| Cross-curve confusion | MEDIUM | ES256K header with secp256r1 key bytes in `sub` (same length: 33 bytes compressed) |
| `exp = u64::MAX` | LOW | Token that "never expires" due to `saturating_add` |
| `nbf = 0, exp = u64::MAX` | LOW | Maximum validity window token |
| Missing required claims | LOW | Payload JSON missing `sub`, `iss`, `key_type`, etc. |
| Extra/unknown claims fields | LOW | Payload with unexpected fields (serde should ignore) |
| `aud = []` (empty array) | LOW | Audience present but empty — should fail audience check |
| Multiple audiences | LOW | `aud = ["host1", "host2"]` — should accept either |

### Missing DER-level tests

| Missing Test | Risk | Description |
|-------------|------|-------------|
| All-zero R value | LOW | `R = [0x00; 32]` — DER encodes as integer 0 |
| Short R/S values | LOW | R with many leading zeros (e.g., R = 1, encoded as `[0x00, ..., 0x01]`) |
| Maximum DER length | LOW | Both R and S with high bits set (each 33 bytes with padding) |
| Trailing bytes after S | MEDIUM | Should be rejected but currently silently ignored |
| Non-canonical DER | MEDIUM | Wrong SEQUENCE length, multi-byte length encoding |
| Adversarial DER with embedded extra integers | MEDIUM | `0x30 <len> 0x02 <r> 0x02 <s> 0x02 <extra>` |

### What IS well-tested

- All three algorithm roundtrips (encode → decode → verify)
- Tampered signatures for Ed25519, secp256k1, secp256r1
- Tampered payload detection
- Wrong signer detection
- Algorithm mismatch (header vs key_type claim)
- Issuer mismatch
- Unsupported algorithm
- Invalid UTF-8 / base64 inputs
- Audience validation (valid, wrong, missing)
- Expiration / not-yet-valid checks

## Remediation

Add the missing test cases, prioritizing:
1. Cross-algorithm key confusion (secp256k1 key with ES256 header)
2. Empty/truncated signatures
3. DER trailing bytes and non-canonical encodings
4. Edge case claim values (`exp = u64::MAX`, empty audience array)

## Test Gap

This finding IS the test gap analysis.
