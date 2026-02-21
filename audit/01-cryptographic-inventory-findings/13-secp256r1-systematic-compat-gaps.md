# Finding: secp256r1 Has Systematic Go Compatibility Test Gaps

**Stream**: 01 - Cryptographic Inventory
**Session**: 4 - Go Compatibility Cross-Verification
**Severity**: MEDIUM (supplements Finding 04 — S-normalization divergence)
**Category**: Test Coverage / Go Compatibility
**Status**: NEW

## Summary

secp256r1 (P-256) has significantly weaker Go compatibility test coverage than Ed25519 and secp256k1. While the other two key types have bidirectional byte-equality tests (Rust signs, output matches Go vector), secp256r1 only has one-way verification tests (Rust can verify Go's signatures). This asymmetry masks the S-normalization divergence identified in Finding 04 and leaves other potential differences untested.

## Coverage Comparison

| Test Category | Ed25519 | secp256k1 | secp256r1 |
|---|---|---|---|
| Private key from Go bytes | **YES** (line 204) | **YES** (line 548) | **NO** (`SECP256R1_PRIVATE_KEY` is `#[allow(dead_code)]`) |
| Rust signature = Go signature (byte-equal) | **YES** (5 messages) | **YES** (5 messages) | **NO** (not attempted) |
| Rust verifies Go signature | YES | YES | YES |
| Low-S normalization test | N/A (Ed25519) | **YES** (explicit test) | **NO** |
| Signature block (DAG-CBOR) byte match | **YES** | **YES** | **NO** (not in `go_compat_serialization.rs`) |
| DID matches Go | YES | YES | YES |
| DID roundtrip (parse Go DID) | YES | YES | YES |
| Identity compat (signs through identity layer) | YES | YES | **NO** |

### Key Gaps

#### 1. `SECP256R1_PRIVATE_KEY` Is Dead Code

`crates/crypto/tests/go_compat_keys.rs:122`:

```rust
#[allow(dead_code)]
const SECP256R1_PRIVATE_KEY: [u8; 32] = [
    0x01, 0x02, 0x03, ..., 0x20,
];
```

This constant exists but is never used in any test. There is no `test_secp256r1_private_key_from_go_bytes()` function. Compare with Ed25519 and secp256k1, which both have this test.

#### 2. Incorrect Comment Masks the Real Issue

`crates/crypto/tests/go_compat_keys.rs:141`:

```rust
// Note: secp256r1 signatures are NOT deterministic (unlike secp256k1 with RFC 6979)
```

This is incorrect. Both Go (since 1.20) and Rust's `p256` crate use RFC 6979 for deterministic nonce generation. The comment incorrectly attributes the byte difference to non-determinism, when the actual cause is S-normalization (Finding 04). This misleading comment likely discouraged adding byte-equality tests.

#### 3. No Low-S Normalization Verification

secp256k1 has explicit tests verifying both Rust-generated and Go-generated signatures are low-S:

```rust
// EXISTS for secp256k1:
fn test_secp256k1_signature_is_low_s_normalized() { ... }
fn test_secp256k1_go_signatures_are_low_s() { ... }
```

No equivalent exists for secp256r1. This is relevant because Finding 04 showed Go's secp256r1 signatures use high-S while Rust uses low-S.

#### 4. No Signature Block Format Test

`go_compat_serialization.rs` tests DAG-CBOR signature block byte equality for Ed25519 and secp256k1, verifying the complete pipeline (signing + identity encoding + CBOR serialization). No secp256r1 signature block exists. If secp256r1 is used for document signing, the DAG-CBOR format compatibility is unverified.

#### 5. secp256r1 Missing from Identity Compat Tests

`crates/identity/tests/go_compat.rs` tests DID and signature compatibility through the `RawIdentity` layer for Ed25519 and secp256k1, but not secp256r1. There is no `test_secp256r1_signature_matches_go()` in the identity tests.

## Why This Matters Beyond Finding 04

Finding 04 identified the S-normalization divergence for verification compatibility. This finding identifies that the **test infrastructure** is insufficient to catch secp256r1 issues in general:

1. If secp256r1 is used for document signing (IPLD blocks), the CID divergence from S-normalization would cause replication failures — but no test would catch this before deployment
2. If Go changes its secp256r1 behavior (e.g., adds low-S normalization), the Rust tests wouldn't detect the change because they only test one direction
3. Any future secp256r1 code changes in Rust would lack cross-implementation regression tests

## Remediation

1. **Use `SECP256R1_PRIVATE_KEY`**: Create `test_secp256r1_private_key_from_go_bytes()` that verifies public key derivation

2. **Fix the incorrect comment**: Replace "NOT deterministic" with the actual reason (S-normalization difference)

3. **Add S-normalization awareness test**: Explicitly document which Go secp256r1 signatures are high-S and verify Rust's normalization handles them

4. **Add to identity compat tests**: Create `test_secp256r1_signature_matches_go()` and `test_secp256r1_can_verify_go_signature()` in the identity test file

5. **Add signature block test**: Create a Go-generated secp256r1 DAG-CBOR signature block and test decoding/verification (note: byte-equality will fail due to S-normalization, so test verification only, and document why)
