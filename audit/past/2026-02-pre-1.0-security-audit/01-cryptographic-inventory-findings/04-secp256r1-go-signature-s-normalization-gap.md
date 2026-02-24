# Finding: secp256r1 Go/Rust Signature Byte Mismatch Due to S-Normalization

**Stream**: 01 - Cryptographic Inventory
**Severity**: MEDIUM
**Category**: Go Compatibility / Signing Correctness
**Status**: NEW

## Summary

Go's standard `crypto/ecdsa` package produces secp256r1 (P-256) signatures without normalizing the S component to low-S form. Rust's `p256` crate always normalizes S to low form. Given the same private key and message, both are deterministic (RFC 6979) but produce **different signature bytes** when Go's S > N/2. The Go compat tests only verify that Rust can verify Go-generated signatures — they never check byte-for-byte signature equality for secp256r1, and the test comments incorrectly attribute the difference to non-determinism.

## Evidence

### Go Signature Contains High-S

The Go compat test vector at `crates/crypto/tests/go_compat_keys.rs:143-149`:

```rust
const SECP256R1_SIGNATURE: &[u8] = &[
    0x30, 0x45, 0x02, 0x20, 0x3f, 0x7a, 0xdd, 0x48, ...  // R (32 bytes, positive)
    0x02, 0x21, 0x00, 0xe7, 0x2e, 0x19, 0xdc, ...          // S (33 bytes, leading zero padding)
];
```

The S value starts with `0xe7` (after DER zero padding). For the P-256 curve:
- Curve order N = `0xFFFFFFFF00000000FFFFFFFFFFFFFFFFBCE6FAADA7179E84F3B9CAC2FC632551`
- N/2 ≈ `0x7FFFFFFF800000007FFFFFFFFFFFFFFFDE73...`
- S starts with `0xe7 > 0x7f` → **S > N/2 → high-S**

Rust's `p256` crate would produce `S' = N - S` (low-S form), resulting in different signature bytes for the same key and message.

### Incorrect Test Comment

`crates/crypto/tests/go_compat_keys.rs:141`:

```rust
// Note: secp256r1 signatures are NOT deterministic (unlike secp256k1 with RFC 6979)
```

This is incorrect. Both Go (since 1.20) and Rust use RFC 6979 for deterministic nonce generation on P-256. The actual reason signatures differ is **S-normalization**, not non-determinism.

### secp256k1 Tests Verify Byte Equality, secp256r1 Does Not

For secp256k1, the Go compat tests include byte-equality checks:

```rust
fn test_secp256k1_signature_verification_from_go() {
    // ...
    assert_eq!(rust_signature, go_signature,
        "Rust secp256k1 signature should match Go (both use RFC 6979)");
}
```

No equivalent test exists for secp256r1. The secp256r1 tests only verify that Rust can verify Go-generated signatures — one-way compatibility.

### Why secp256k1 Works But secp256r1 Doesn't

| Property | secp256k1 | secp256r1 |
|----------|-----------|-----------|
| Go library | btcd/btcec | crypto/ecdsa |
| RFC 6979 | Yes | Yes (Go 1.20+) |
| S-normalization | Yes (BIP-62) | **No** |
| Rust library | k256 | p256 |
| Rust S-normalization | Yes | Yes |
| Byte-equal signatures | ✅ Yes | ❌ No |

Go's `btcd/btcec` normalizes S for secp256k1 (Bitcoin convention, BIP-62/BIP-146). Go's standard `crypto/ecdsa` does not normalize S for P-256 since there's no equivalent convention in the NIST world.

## Impact

### Verification: No Issue

Both directions work correctly:
- Rust verifies Go high-S signatures → `normalize_s().unwrap_or(sig)` handles this
- Go verifies Rust low-S signatures → Go's `ecdsa.Verify` accepts any valid (R, S) pair

### Content-Addressed Storage: Potential CID Divergence

If secp256r1 signatures are embedded in IPLD blocks (used for document signing, batch signing), the different signature bytes produce different block CIDs. This would cause:
- Replication divergence between Go and Rust nodes using secp256r1 identities
- Different document state hashes across implementations
- P2P sync failures for secp256r1-signed content

### Practical Scope

Currently, secp256r1 is primarily used for browser-based identities (Web Crypto API). If these identities sign documents stored in IPLD blocks, the CID divergence applies. If secp256r1 is only used for JWT authentication (where byte equality doesn't matter), the impact is lower.

## Affected Code

- **Signing**: `crates/crypto/src/keys/secp256r1.rs:92-100` — always produces low-S
- **Verification**: `crates/crypto/src/keys/secp256r1.rs:179-202` — normalizes S (handles both)
- **Test gap**: `crates/crypto/tests/go_compat_keys.rs:673-808` — no byte-equality test

## Remediation

### Option A: Accept Low-S Only (Both Sides Normalize)

Add S-normalization to Go's secp256r1 signing path. This ensures byte-for-byte identical signatures. Requires a Go-side change.

### Option B: Test and Document the Gap

If secp256r1 signatures never appear in content-addressed blocks (only JWTs), document that byte equality is not guaranteed and fix the misleading test comment. Add a test proving that Rust DOES verify Go high-S signatures (already exists) AND that Go verifies Rust low-S signatures (needs Go-side test).

### Option C: Rust Matches Go's Behavior

Sign without S-normalization in Rust when the destination is a content-addressed block. This is complex and reduces malleability protection.
