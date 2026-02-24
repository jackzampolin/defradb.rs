# Stream 01: Cryptographic Inventory — Verification Re-Audit

**Date**: 2026-02-23
**Auditor**: Claude Opus 4.6 (re-audit)
**Scope**: All findings from Stream 01 that were marked "Must Fix" or "Should Fix" in `REMEDIATION_ROADMAP.md`

---

## Must Fix (Phase 1.4 + Phase 4)

### 01-00: Ed25519 Private Key Not Zeroized

- **Status**: FIXED
- **Code location**: `Cargo.toml:67`
- **Evidence**: The workspace `Cargo.toml` now reads:
  ```toml
  ed25519-dalek = { version = "2.1", features = ["serde", "zeroize"] }
  ```
  The `"zeroize"` feature flag is present, which enables `ZeroizeOnDrop` on `ed25519_dalek::SigningKey`. When `Ed25519PrivateKey` drops, Rust drops its inner `SigningKey` field, triggering zeroization of the 32-byte private seed.
- **Test coverage**: No dedicated test exercises zeroization behavior (this is expected — zeroize-on-drop is a library guarantee, not easily unit-testable without unsafe memory inspection).
- **Regression risk**: LOW. The fix is a Cargo.toml feature flag. It would only regress if someone explicitly removed the `"zeroize"` feature, which would be visible in code review. The `use zeroize::Zeroize` import in `crates/crypto/src/keys/ed25519.rs:11` also serves as a compile-time canary — removing the feature would break this import.
- **Notes**: Fix is correct and complete for the Ed25519 case. The finding also noted secp256k1 and secp256r1 inner keys are already zeroed unconditionally by their respective crates (k256, p256), so no action was needed there.

---

### 01-10: SE Tag UTF-8 Lossy Go Divergence

- **Status**: FIXED
- **Code location**: `crates/crypto/src/se/tag.rs:79-119`
- **Evidence**: The `generate_equality_tag()` function now builds the domain separator as raw bytes fed incrementally into HMAC, without any `String::from_utf8_lossy()` call:
  ```rust
  let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
  mac.update(b"eq:");
  mac.update(identity_id);  // Raw bytes, NOT UTF-8 converted
  mac.update(b":");
  mac.update(collection_id.as_bytes());
  mac.update(b":");
  mac.update(field_name.as_bytes());
  mac.update(value);
  ```
  A comment on line 95-96 explicitly documents the rationale: "Go uses string(pubKey.Raw()) which preserves raw bytes; using from_utf8_lossy would replace invalid UTF-8 with U+FFFD, producing different HMAC outputs."

  Grep confirms no `from_utf8_lossy` usage remains in the SE module (the only hit is the comment explaining why it is NOT used).
- **Test coverage**: `crates/crypto/tests/go_compat_se.rs` — tests `test_go_compat_vector_1_zeros_key_empty_identity`, `test_go_compat_vector_2_user_identity`, `test_go_compat_vector_3_short_inputs` use hardcoded hex values pinning the byte output. HOWEVER, these vectors all use ASCII-only or empty string identities, which would not have triggered the original UTF-8 lossy bug. There is no test with raw binary (non-UTF-8) identity bytes and a hardcoded Go-generated vector. See 01-11 assessment below.
- **Regression risk**: LOW. The fix removed a conversion step (simpler code). Future developers would have to actively add a lossy conversion to break this. The inline comment warns against it.
- **Notes**: The fix is correct and matches Go's behavior. The known limitation about delimiter collisions (shared with Go) is now documented in comments on lines 98-102.

---

### 01-16: SE Encryption Key Not Zeroized

- **Status**: FIXED
- **Code location**: `crates/db/src/se/coordinator.rs:14,59,69,95,103`
- **Evidence**: The `enc_key` field type was changed from `Vec<u8>` to `Zeroizing<Vec<u8>>`:
  ```rust
  use zeroize::Zeroizing;

  pub struct SECoordinatorConfig {
      /// SE encryption key (32 bytes). Zeroized on drop.
      pub enc_key: Zeroizing<Vec<u8>>,
      ...
  }
  ```
  All construction sites wrap the key in `Zeroizing::new()`:
  - `Default::default()`: `enc_key: Zeroizing::new(vec![0u8; 32])`
  - `with_key()`: `enc_key: Zeroizing::new(enc_key)`
  - `with_key_and_identity()`: `enc_key: Zeroizing::new(enc_key)`

  `Zeroizing<T>` from the `zeroize` crate zeroes the inner `Vec<u8>` on drop via its `Drop` implementation.
- **Test coverage**: `crates/db/src/se/coordinator.rs` tests at lines 189-259 exercise coordinator creation with the `Zeroizing` wrapper. No explicit zeroization test (same as 01-00 — library guarantee).
- **Regression risk**: LOW. `Zeroizing<Vec<u8>>` is a type-level guarantee. Reverting to `Vec<u8>` would be a visible API change that breaks callers.
- **Notes**: The fix is correct. The all-zeros default on line 69 still exists (`Zeroizing::new(vec![0u8; 32])`), but this is acceptable for the `Default` impl since the SE system validates keys at usage time. The finding recommended removing the default, but keeping it with `Zeroizing` is a reasonable trade-off.

---

### 01-19: SE HMAC Key Length Validation

- **Status**: FIXED
- **Code location**: `crates/crypto/src/se/tag.rs:14-15,86-92`
- **Evidence**: A constant `SE_KEY_LEN` (32) is defined, and key length is validated at the top of `generate_equality_tag()`:
  ```rust
  pub const SE_KEY_LEN: usize = 32;

  pub fn generate_equality_tag(...) -> Result<[u8; SEARCH_TAG_SIZE], Error> {
      if key.len() != SE_KEY_LEN {
          return Err(Error::Crypto(format!(
              "SE HMAC key must be {} bytes, got {}",
              SE_KEY_LEN, key.len()
          )));
      }
      ...
  }
  ```
  The function signature was also changed to return `Result<[u8; SEARCH_TAG_SIZE], Error>` instead of a bare array, propagating the error properly.
- **Test coverage**: `crates/crypto/src/se/tag.rs` — tests at lines 225-243:
  - `test_key_length_validation_rejects_short_key` (16-byte key)
  - `test_key_length_validation_rejects_long_key` (64-byte key)
  - `test_key_length_validation_accepts_32_bytes`

  Additionally, `crates/crypto/tests/go_compat_se.rs` tests `test_rejects_short_key` (16 bytes) and `test_rejects_long_key` (64 bytes).
- **Regression risk**: VERY LOW. The validation is at the function entry point with explicit tests for both boundaries. The `Result` return type forces callers to handle the error.
- **Notes**: Fix is correct and thorough. Both unit tests and integration-style tests cover the validation.

---

## Should Fix (Phase 4.4 + Phase 5.4 + Phase 6.4)

### 01-02: Ed25519 Keygen Seed Not Zeroed

- **Status**: PARTIALLY FIXED
- **Code location**:
  - `crates/crypto/src/keys/ed25519.rs:90-107` (from_bytes — FIXED)
  - `crates/crypto/src/keys/ed25519.rs:256-265` (ed25519_key_from_seed — FIXED)
  - `crates/crypto/src/keys/generation.rs:86-103` (generate_ed25519 — NOT FIXED)
- **Evidence**:
  - In `Ed25519PrivateKey::from_bytes()` (ed25519.rs:90-106): The `seed` array is zeroized on both the error path (line 100) and the success path (line 106): `seed.zeroize();`
  - In `ed25519_key_from_seed()` (ed25519.rs:256-264): The `seed_array` is zeroized before return (line 264): `seed_array.zeroize();`
  - In `generate_ed25519()` (generation.rs:86-103): The `seed` array on line 89 and `key_bytes` Vec on line 98 are **NOT** zeroized. The function returns without calling `.zeroize()` on either. The `seed` is a 32-byte stack array containing the raw CSPRNG entropy. The `key_bytes` is a 64-byte heap `Vec` containing seed + public key. Both leak when the function returns.

  Note that because `ed25519-dalek` now has the `"zeroize"` feature enabled (Finding 00 fix), the intermediate `signing_key` on the stack IS zeroed when it drops. However, `seed` and `key_bytes` are Rust primitives (`[u8; 32]` and `Vec<u8>`) that do not implement `ZeroizeOnDrop`.
- **Test coverage**: NONE for zeroization behavior.
- **Regression risk**: N/A — the fix was not fully applied.
- **Notes**: The `from_bytes()` and `ed25519_key_from_seed()` paths are fixed, but the primary key generation path `generate_ed25519()` still leaks the seed and key bytes. This is the function called during node initialization and identity creation. Recommend adding `seed.zeroize()` after line 100 and `key_bytes.zeroize()` after the `from_bytes` call on line 102 in `generation.rs`.

---

### 01-11: SE Tags Go Test Vectors

- **Status**: PARTIALLY FIXED
- **Code location**: `crates/crypto/tests/go_compat_se.rs:317-362`
- **Evidence**: Three hardcoded test vectors have been added (lines 317-362):
  - Vector 1: zeros key, empty identity, `collection="test"`, `field="value"`, `value=b"hello"` -> `"0ad7f64f62f08513e7932113c857fccc"`
  - Vector 2: `0x42` key, `identity="user123"`, `collection="users_v1"`, `field="age"`, `value=b"21"` -> `"4039c6b6aafb7d570da6b24f81d27f5f"`
  - Vector 3: `0x01` key, empty identity, `collection="col"`, `field="f"`, `value=b"val"` -> `"1f5f9dd6227480f147431ac8eeb80505"`

  These are compared against `generate_equality_tag_str()` output, which is the correct direction (implementation vs hardcoded expected value).

  **However**, the critical gap remains: all three vectors use **ASCII-only string identities** (empty string or `"user123"`). None tests with raw binary identity bytes containing non-UTF-8 sequences. The original UTF-8 lossy bug (Finding 10) would NOT have been caught by these vectors, because ASCII strings pass through both `from_utf8_lossy()` and raw byte treatment identically.

  The older circular `compute_expected_tag()` tests (lines 18-35) still exist alongside the new vectors.
- **Test coverage**: The vectors exist but are insufficient to guard against regression of Finding 10's core issue.
- **Regression risk**: MEDIUM. If someone reintroduced `from_utf8_lossy()`, the current vectors would not catch it (they'd still pass with ASCII identities). A vector using binary identity bytes (e.g., `&[0xd7, 0x5a, 0x98, ...]`) with a Go-generated expected tag would be the definitive guard.
- **Notes**: The fix addresses the "no hardcoded vectors at all" portion of the finding but does not address the critical sub-requirement of testing with binary (non-UTF-8) identity bytes. Recommend adding at least one vector where the identity contains bytes like `[0xd7, 0x5a, 0x98, 0x01]` (a real Ed25519 public key prefix) with the expected tag generated from Go's `string(pubKey.Raw())` path.

---

### 01-12: JWT Go Compat Tests

- **Status**: FIXED
- **Code location**: `crates/identity/tests/go_compat_jwt.rs`
- **Evidence**: A complete Go JWT compatibility test file was added with:
  - Three hardcoded JWT tokens: `EDDSA_JWT`, `ES256K_JWT`, `ES256_JWT` (lines 17-39)
  - Three expected DID strings for cross-verification (lines 41-45)
  - Parse tests for all three key types (lines 47-75)
  - Claims verification tests for all three (lines 77-115)
  - Deterministic signature re-computation test for Ed25519 (lines 117-142)
  - Tamper rejection tests for all three key types plus payload tampering (lines 144-197)

  The JWT tokens contain fixed timestamps (`exp=9999999999`, `nbf=0`, `iat=0`) and use the same test keys as `go_compat_keys.rs`. The tests verify:
  - Rust can parse Go-generated JWTs for all three algorithms
  - Claims fields match expected values
  - DID derivation matches expected Go DIDs
  - Tampered signatures and payloads are rejected
  - Ed25519 deterministic signature byte-equality (lines 117-142)
- **Test coverage**: 11 tests total covering all three key types, positive and negative cases.
- **Regression risk**: VERY LOW. Hardcoded JWT strings serve as pinned reference values. Any change to JWT parsing or signature verification that breaks Go compatibility would be caught.
- **Notes**: Fix is thorough and well-structured. The test coverage matches or exceeds what the finding recommended.

---

### 01-13: secp256r1 Systematic Go Compat Gaps

- **Status**: FIXED
- **Code location**: `crates/crypto/tests/go_compat_p256.rs` (new file, 254 lines)
- **Evidence**: A dedicated P-256 compatibility test file was created addressing all five gaps identified in the finding:

  1. **Private key usage**: The `SECP256R1_PRIVATE_KEY` constant is used to create `Secp256r1PrivateKey` instances for signing (e.g., line 98, 141, 162, 237). The `#[allow(dead_code)]` annotation from `go_compat_keys.rs` is still present there, but the key is actively used in the new file.

  2. **Determinism documentation**: The incorrect comment "secp256r1 signatures are NOT deterministic" is contradicted by the new file's header (lines 8-9): "Rust p256 crate uses RFC 6979 -> deterministic signing / Go crypto/ecdsa uses random k -> non-deterministic". The test `test_rust_secp256r1_signing_is_deterministic` (lines 96-106) explicitly verifies this.

  3. **Low-S normalization**: Comprehensive S-normalization tests:
     - `test_go_sig_has_high_s_value` (line 127) — documents that the Go test vector has high-S
     - `test_rust_produces_low_s_for_same_message` (line 138) — verifies Rust always produces low-S
     - `test_all_rust_signatures_have_low_s` (line 236) — verifies across multiple messages
     - The `is_low_s_p256()` helper (line 82) correctly checks against the P-256 curve order

  4. **S-normalization in verification**: `crates/crypto/src/keys/secp256r1.rs:193-194` contains `normalize_s()` in the verify path, and `test_rust_verifier_accepts_go_high_s_signature` (line 111) exercises this.

  5. **Go signature verification tests**: Four Go-generated DER signatures are hardcoded (lines 31-63) and verified (lines 173-210): high-S, empty message, binary message, 1KB message.

  **Still missing**: The `go_compat_keys.rs` file still has `#[allow(dead_code)]` on `SECP256R1_PRIVATE_KEY` (line 122), and there is no `test_secp256r1_private_key_from_go_bytes()` test in that file. The identity compat tests (`crates/identity/tests/go_compat.rs`) still lack secp256r1. The DAG-CBOR signature block test for secp256r1 was not added. However, the new `go_compat_p256.rs` file covers the critical gaps.
- **Test coverage**: 10 tests in `go_compat_p256.rs` plus the existing verification tests in `go_compat_keys.rs`.
- **Regression risk**: LOW. The new test file covers signing determinism, S-normalization for both Rust-generated and Go-generated signatures, and cross-verification. The secp256r1 verification code's `normalize_s()` call is now exercised by the high-S test.
- **Notes**: The fix addresses the core finding well. The `go_compat_keys.rs` dead code annotation is cosmetic since the key is used in the new file. The missing identity-layer and DAG-CBOR tests are lower priority since the finding confirmed secp256r1 is not used for IPLD block signing (line 11 of go_compat_p256.rs).

---

### 01-15: SE Domain Separator Delimiter Collision

- **Status**: FIXED (documented as accepted limitation)
- **Code location**: `crates/crypto/src/se/tag.rs:98-102`
- **Evidence**: The delimiter collision vulnerability is now documented inline:
  ```rust
  // Known limitation (shared with Go): the `:` delimiter is not escaped, so an
  // identity containing `:` can collide with a different identity+collection pair.
  // Example: identity="a:b", collection="", field="c" produces the same separator
  // as identity="a", collection="b", field="c" -> both yield "eq:a:b:c".
  // This is intentionally kept as-is for byte-for-byte Go compatibility.
  ```
- **Test coverage**: `crates/crypto/tests/go_compat_se.rs:90-112` — `test_domain_separator_format` verifies that different identity/collection/field splits produce different tags for specific ASCII inputs, but this test actually documents the **non-collision** cases. The collision itself is acknowledged as a known limitation.
- **Regression risk**: N/A — this is an accepted design trade-off, not a code fix.
- **Notes**: The remediation roadmap specified "Document as shared Go limitation; no solo fix." The inline documentation fulfills this. A protocol-level fix (length-prefixed components) would require coordinated Go changes and is deferred to post-1.0.

---

## Summary

| Finding | Severity | Roadmap Action | Verification Status | Quality |
|---------|----------|----------------|---------------------|---------|
| 01-00 | MEDIUM-HIGH | Must Fix | FIXED | Correct, minimal, complete |
| 01-10 | HIGH | Must Fix | FIXED | Correct fix, good documentation |
| 01-16 | MEDIUM | Must Fix | FIXED | Type-level guarantee via `Zeroizing<Vec<u8>>` |
| 01-19 | LOW-MEDIUM | Must Fix | FIXED | Validation + error return + tests |
| 01-02 | MEDIUM | Should Fix | PARTIALLY FIXED | `from_bytes` and `ed25519_key_from_seed` fixed; `generate_ed25519()` still leaks |
| 01-11 | MEDIUM-HIGH | Should Fix | PARTIALLY FIXED | Vectors added but none with binary identity bytes |
| 01-12 | MEDIUM | Should Fix | FIXED | 11 tests, all three key types, tamper rejection |
| 01-13 | MEDIUM | Should Fix | FIXED | Comprehensive P-256 test file with S-normalization coverage |
| 01-15 | LOW-MEDIUM | Should Fix (doc) | FIXED | Inline documentation of accepted limitation |

### Remaining Action Items

1. **01-02** (`generate_ed25519()` in `crates/crypto/src/keys/generation.rs:86-103`): Add `seed.zeroize()` after line 100 and wrap `key_bytes` in zeroize-on-scope-exit or call `key_bytes.zeroize()` after the `from_bytes` call. Estimated effort: 3 lines of code.

2. **01-11** (SE binary identity test vector): Add one test in `go_compat_se.rs` using raw binary identity bytes (e.g., `&[0xd7, 0x5a, 0x98, 0x01]`) with a hex tag generated from Go's `secore.GenerateEqualityTag(key, string(identityBytes), ...)`. This is the definitive regression guard for the UTF-8 lossy fix. Estimated effort: run a 10-line Go program, add one test.
