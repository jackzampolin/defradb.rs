# Finding: ECIES & AES-GCM Correctness Audit — Session 3 Results

**Stream**: 01 - Cryptographic Inventory
**Session**: 3 - Encryption & ECIES Correctness
**Severity**: INFORMATIONAL
**Category**: Audit Summary
**Status**: COMPLETE

## Summary

Line-by-line audit of the ECIES hybrid encryption scheme and AES-256-GCM implementation. The core cryptographic construction is correct and matches the Go implementation. Two issues found (Findings 07 and 08). Finding 01 from Session 1 confirmed with no severity change.

## Audit Checklist Results

### X25519 ECDH

| Check | Result | Evidence |
|-------|--------|----------|
| Ephemeral key uses OsRng | PASS | `StaticSecret::random_from_rng(rand::rngs::OsRng)` at ecies.rs:118 |
| `diffie_hellman()` produces valid shared secret | PASS (with caveat) | Correct for valid keys. Does not reject low-order keys — see Finding 07 |
| SharedSecret zeroed on drop | PASS | `x25519-dalek` default features include `"zeroize"` — `ZeroizeOnDrop` active |
| Ephemeral `StaticSecret` zeroed on drop | PASS | Same — `ZeroizeOnDrop` active via default features |

### HKDF-SHA256

| Check | Result | Evidence |
|-------|--------|----------|
| `Hkdf::<Sha256>::new(None, ...)` — no salt intentional? | PASS | Matches Go (`nil` salt). RFC 5869 allows this when IKM is uniformly random (X25519 output is) |
| Expand produces 64 bytes (32 AES + 32 HMAC) | PASS | `keys = [0u8; AES_KEY_SIZE + AES_KEY_SIZE]` = `[0u8; 64]` at ecies.rs:130 |
| Key split is correct | PASS | `keys[..32]` = AES, `keys[32..]` = HMAC. Matches Go's sequential `kdf.Read()` |
| HKDF output matches Go test vectors | PASS | `test_hkdf_key_derivation_matches_go()` verifies byte-for-byte equality |
| Empty info parameter matches Go | PASS | Rust: `&[]`, Go: `nil` — both are zero-length, semantically identical in RFC 5869 |
| Derived keys NOT zeroed (Finding 01) | CONFIRMED | `keys`, `aes_key`, `hmac_key` on stack, not zeroed. Severity LOW still appropriate |

### AAD Construction

| Check | Result | Evidence |
|-------|--------|----------|
| Ephemeral public key included | PASS | `ephemeral_public.as_bytes().to_vec()` at ecies.rs:138 |
| Optional extra AAD appended correctly | PASS | `aad.extend_from_slice(&extra_aad)` at ecies.rs:140 |
| AAD reconstruction matches on decrypt | PASS | Same construction at ecies.rs:249-252 |
| Go compatibility | PASS | Go's `makeAAD()` uses identical `publicKeyBytes || associatedData` concatenation |

### AES-256-GCM

| Check | Result | Evidence |
|-------|--------|----------|
| Key must be 32 bytes | PASS | `key.len() != AES_KEY_SIZE` check at aes.rs:47 |
| Nonce is 12 bytes (AES-GCM standard) | PASS | `AES_NONCE_SIZE = 12` at types.rs:31 |
| Nonce prepended to ciphertext | PASS | `[nonce (12) \| ciphertext \| GCM tag (16)]` at aes.rs:72-79 |
| Cipher construction correct | PASS | `Aes256Gcm::new_from_slice(key)` at aes.rs:57 |
| AAD passed to AEAD payload | PASS | `Payload { msg, aad }` at aes.rs:61-64 |
| Decrypt extracts nonce correctly | PASS | `ciphertext.split_at(AES_NONCE_SIZE)` at aes.rs:125 |

### HMAC-SHA256

| Check | Result | Evidence |
|-------|--------|----------|
| Computed over ciphertext (encrypt-then-MAC) | PASS | `mac.update(&encrypted_data)` at ecies.rs:149 — after AES-GCM output |
| `verify_slice` used for constant-time comparison | PASS | `mac.verify_slice(received_mac)` at ecies.rs:245 — uses `subtle::ConstantTimeEq` |
| HMAC covers AES nonce + ciphertext + GCM tag | PASS | `encrypted_data` = full AES-GCM output including prepended nonce |
| Go uses equivalent constant-time comparison | PASS | Go: `hmac.Equal()` uses `subtle.ConstantTimeCompare` |

### Decrypt Ordering

| Check | Result | Evidence |
|-------|--------|----------|
| HMAC verified BEFORE decryption | PASS | HMAC at ecies.rs:242-246, AES decrypt at ecies.rs:255 |
| Ephemeral pubkey extraction handles malformed input | PASS | Length checks at ecies.rs:187-188, 193, 217 |
| Short ciphertext rejected | PASS (with caveat) | Rejected, but minimum length is weaker than Go — see Finding 08 |

### Nonce Generation

| Check | Result | Evidence |
|-------|--------|----------|
| Random mode uses OsRng | PASS | `OsRng.try_fill_bytes(&mut nonce)` at nonce.rs:36 |
| Deterministic mode is test-only | PASS | `USE_DETERMINISTIC_NONCE` is `AtomicBool`, default `false`, only set in `#[test]` functions |
| Deterministic mode clearly gated | PASS | `#[doc(hidden)]`, WARNING comments at nonce.rs:45-46, 61 |
| Test mode matches Go test nonce | PASS | Both use `"deterministic nonce for testing"[:12]` |
| Rust approach safer than Go | PASS | Rust: explicit opt-in. Go: auto-detects via binary name heuristic |

### Nonce Reuse Prevention

| Check | Result | Evidence |
|-------|--------|----------|
| No nonce reuse possible | PASS | Each encryption uses fresh ephemeral key → unique shared secret → unique HKDF output → unique AES key. Even if nonce collided, the key differs |

### Go Compatibility Test Vectors

| Check | Result | Evidence |
|-------|--------|----------|
| X25519 public key derivation matches | PASS | `test_x25519_public_key_derivation_matches_go()` |
| X25519 shared secret matches | PASS | `test_x25519_shared_secret_matches_go()` |
| HKDF key derivation matches | PASS | `test_hkdf_key_derivation_matches_go()` |
| ECIES decrypt Go ciphertext | PASS | `test_ecies_decrypt_go_ciphertext()` |
| ECIES encrypt matches Go (deterministic nonce) | PASS | `test_ecies_encrypt_matches_go_with_deterministic_nonce()` |
| AES decrypt Go ciphertext | PASS | `test_aes_decrypt_go_ciphertext()` |
| AES encrypt matches Go (deterministic nonce) | PASS | `test_aes_encrypt_matches_go_with_deterministic_nonce()` |
| Ciphertext format matches | PASS | `test_ecies_ciphertext_format_matches_go()` — structure verified |

## Finding 01 Status Update

**Finding 01 (ECIES Derived Key Material Not Zeroed) — CONFIRMED, no change.**

Deep-dive confirms:
- `SharedSecret` IS zeroed on drop (`ZeroizeOnDrop` from default `"zeroize"` feature) — verified in `x25519-dalek` Cargo.toml
- `StaticSecret` (ephemeral private key) IS zeroed on drop — same mechanism
- `keys`, `aes_key`, `hmac_key` arrays are NOT zeroed — confirmed at ecies.rs:130-135 (encrypt) and :234-239 (decrypt)
- Severity LOW remains appropriate — these are one-time-use symmetric keys derived from already-zeroed secrets, stack-allocated with short lifetimes

## Architectural Observations

### Dual Authentication (AES-GCM + HMAC)

The scheme uses two authentication layers: AES-GCM's built-in GHASH tag AND an outer HMAC-SHA256. This is encrypt-then-MAC applied over an AEAD cipher. Benefits:

1. HMAC verification (decrypt step 5) rejects tampered data before AES-GCM decryption (step 7)
2. The outer MAC provides a fast rejection path for malformed/tampered ciphertext
3. Matches Go implementation — both layers are required for interop

This is defense-in-depth, not redundancy. The HMAC uses a separate key from AES-GCM, so compromise of one layer doesn't compromise the other.

### ECIES Not Currently Used in Production Paths

ECIES functions (`encrypt_ecies`/`decrypt_ecies`) are exported from the crypto crate but currently only called from test files. The database's actual encryption (block builder, merge handler) uses `encrypt_aes`/`decrypt_aes` directly with pre-shared keys. ECIES will become critical when P2P encryption key negotiation is implemented.

## Files Audited

| File | Lines | Status |
|------|-------|--------|
| `crates/crypto/src/encryption/ecies.rs` | 1-258 (all) | Audited line-by-line |
| `crates/crypto/src/encryption/aes.rs` | 1-148 (all) | Audited line-by-line |
| `crates/crypto/src/encryption/nonce.rs` | 1-64 (all) | Audited line-by-line |
| `crates/crypto/tests/go_compat_encryption.rs` | 1-451 (all) | Audited, all vectors verified |
| `crates/crypto/tests/ecies_tests.rs` | 1-604 (all) | Reviewed for coverage gaps |
| `crates/crypto/src/types.rs` | Constants | Verified correct values |
| Go `crypto/ecies.go` | 1-281 (all) | Cross-referenced for compatibility |
| Go `crypto/aes.go` | 1-107 (all) | Cross-referenced for compatibility |
| Go `crypto/nonce.go` | 1-52 (all) | Cross-referenced for compatibility |
