# Audit Stream 1: Cryptographic Inventory & Review

## Scope

Every use of cryptographic primitives across the codebase:
- Hashing (SHA-256, Blake2/3, etc.)
- Signing (Ed25519, secp256k1, secp256r1, etc.)
- Encryption (AES-GCM, ChaCha20, searchable encryption)
- Key derivation (HKDF, PBKDF2, etc.)
- Randomness sources (OsRng, thread_rng, etc.)
- Key lifecycle (generation, storage, serialization, destruction)
- CID/multihash computation

## Key Questions

- Are algorithm choices appropriate and consistent with Go implementation?
- Are parameters (key sizes, nonce sizes, iteration counts) correct?
- Is randomness sourced from CSPRNG everywhere it needs to be?
- Are keys zeroed on drop? Are there memory safety concerns?
- Are there any deprecated or weak algorithms in use?
- Is the searchable encryption scheme sound?

## Crates of Interest

- `crypto/`
- `keyring/`
- `identity/`
- `blockstore/` (CID computation)
- `document/` (document ID hashing)
- `crdt/` (encryption of field values)
- `storage/` (encryption at rest?)

## Recon Findings

### Surface Area
- **crypto crate**: 3,500 LOC across 30+ files, 4 key types (Ed25519, secp256k1, secp256r1, BLS12-381)
- **identity crate**: 1,739 LOC (JWT tokens, DID handling, key type dispatch)
- **keyring crate**: 905 LOC (file/system/systemd backends, JWE encryption)
- **89 files** with crypto functions, **150+ hashing sites**, **2,180 signing occurrences**, **1,338 encryption occurrences**

### Dependencies (all modern, audited)
- `ed25519-dalek 2.1`, `k256 0.13`, `p256 0.13`, `blst 0.3`
- `aes-gcm 0.10`, `x25519-dalek 2.0`, `sha2 0.10`, `hkdf 0.12`
- `rand 0.8` (OsRng only - no thread_rng detected), `subtle 2.5`, `zeroize 1.8`

### Key Findings
- **Algorithm choices**: All standard, no weak algorithms detected
- **Randomness**: OsRng (CSPRNG) used exclusively - no thread_rng
- **Nonce generation**: 12-byte random (AES-GCM standard)
- **Key validation**: Rejects all-zeros and all-0xFF keys

### Red Flags
- **MEDIUM: Private key types lack Zeroize impls** - Ed25519, secp256k1, secp256r1 keys not zeroed on drop
- **MEDIUM: ECIES shared secrets not explicitly cleared** - Temporary secrets from X25519 ECDH linger
- **LOW: Ed25519 seed material** may persist in temp vecs before expansion
- **LOW: Searchable encryption** uses deterministic HMAC tags (frequency analysis possible by design)

### Sub-areas for Deep Dive
1. Key lifecycle & zeroization (all 4 key types)
2. Signing & verification paths (all algorithms)
3. ECIES hybrid encryption correctness
4. Searchable encryption scheme analysis
5. Merkle proof & batch signing
6. Go compatibility cross-verification

## Estimated Scope

**LARGE: 5-6 sessions** (~8-12 hours each)

### Session 1: Key Management & Zeroization (CRITICAL)

| File | Lines | Focus |
|------|-------|-------|
| `crates/crypto/src/keys/ed25519.rs` | 38-111 | Ed25519PrivateKey - no Zeroize, seed in `raw()` |
| `crates/crypto/src/keys/secp256k1.rs` | 22-77 | Secp256k1PrivateKey - no Zeroize, `to_bytes()` temp alloc |
| `crates/crypto/src/keys/secp256r1.rs` | 22-68 | Secp256r1PrivateKey - no Zeroize |
| `crates/crypto/src/keys/generation.rs` | 65-136 | Key generation entropy, seed zeroization |
| `crates/keyring/src/file.rs` | 26-60, 140-150 | Password protection, file permissions, secure deletion |

**Checklist**: Verify Zeroize impls, OsRng for all generation, weak key detection, file permissions 0o600/0o700

### Session 2: Signing & Verification Paths (CRITICAL)

| File | Lines | Focus |
|------|-------|-------|
| `crates/crypto/src/keys/ed25519.rs` | 143-154, 225-240 | Ed25519 sign/verify, constant-time |
| `crates/crypto/src/keys/secp256k1.rs` | 102-125, 190-214 | ECDSA sign/verify, DER encoding, low-S normalization |
| `crates/crypto/src/keys/secp256r1.rs` | 92-107, 179-202 | ECDSA sign/verify, DER encoding |
| `crates/identity/src/token/encoding.rs` | 29-72 | JWT encoding per key type |
| `crates/identity/src/token/decoding.rs` | 78-100+ | JWT decoding, raw-to-DER conversion |
| `crates/identity/src/token/der.rs` | 6-115, 129-168 | DER<->raw signature conversion |
| `crates/crypto/src/batch.rs` | 32-93 | Merkle root computation + batch signing |

**Checklist**: DER off-by-one errors, signature normalization, Go compatibility, algorithm confusion prevention

### Session 3: Encryption & ECIES Correctness (HIGH)

| File | Lines | Focus |
|------|-------|-------|
| `crates/crypto/src/encryption/ecies.rs` | 116-150 (encrypt), 178-258 (decrypt) | X25519 ECDH, HKDF-SHA256, AES-GCM, HMAC |
| `crates/crypto/src/encryption/aes.rs` | 40-82 | AES-256-GCM key size check, nonce, AAD |
| `crates/crypto/src/encryption/nonce.rs` | 24-56 | Nonce generation (random vs deterministic test mode) |
| `crates/crypto/tests/go_compat_encryption.rs` | 1-100+ | Go cross-verification |

**Checklist**: Shared secret zeroization, HKDF output zeroing, nonce generation, AAD construction, Go parity

### Session 4: Go Compatibility Cross-Verification (HIGH)

| File | Focus |
|------|-------|
| `crates/crypto/tests/go_compat_keys.rs` | Ed25519 64-byte format, compressed key formats |
| `crates/crypto/tests/go_compat_serialization.rs` | Key serialization parity |
| `crates/crypto/tests/go_compat_encryption.rs` | ECIES/X25519 output format |
| `crates/crypto/tests/go_compat_se.rs` | Searchable encryption HMAC tags |
| `crates/identity/tests/go_compat.rs` | JWT token format |

**Checklist**: Signature format parity, key encoding parity, CID computation parity

### Session 5: Searchable Encryption & Merkle Proof (MEDIUM)

| File | Lines | Focus |
|------|-------|-------|
| `crates/crypto/src/se/tag.rs` | 75-101 | HMAC-SHA256 tag generation, domain separator, 16-byte truncation |
| `crates/crypto/src/se/artifact.rs` | all | Artifact structure |
| `crates/crypto/src/merkle_proof/mod.rs` | all | Proof generation & verification |
| `crates/crypto/tests/merkle_proof_tests.rs` | all | Proof roundtrip tests |

**Checklist**: Frequency analysis implications, tag isolation, proof extraction, CID determinism

## Completion Status

**Stream 1: Cryptographic Inventory — COMPLETE** (5 sessions, 21 findings)

### Session Status

| Session | Focus | Status | Findings |
|---------|-------|--------|----------|
| 1 | Key Management & Zeroization | COMPLETE | 00, 01, 02, 03 |
| 2 | Signing & Verification Paths | COMPLETE | 04, 05, 06 |
| 3 | Encryption & ECIES Correctness | COMPLETE | 07, 08, 09 |
| 4 | Go Compatibility Cross-Verification | COMPLETE | 10, 11, 12, 13, 14 |
| 5 | Searchable Encryption & Merkle Proof | COMPLETE | 15, 16, 17, 18, 19, 20, 21 |

### All Findings Summary

| # | Finding | Severity | 1.0 Blocker? |
|---|---|---|---|
| 00 | Private key types lack Zeroize impls | MEDIUM | No |
| 01 | ECIES shared secret not zeroed | MEDIUM | No |
| 02 | Ed25519 keygen seed not zeroed | LOW | No |
| 03 | Key `raw()` trait returns unprotected Vec | LOW | No |
| 04 | secp256r1 Go signature S-normalization gap | HIGH | Investigate |
| 05 | JWT algorithm dispatch from header | MEDIUM | No |
| 06 | Batch signing missing secp256r1 | MEDIUM | No |
| 07 | ECIES X25519 low-order key acceptance | MEDIUM | No |
| 08 | ECIES ciphertext validation gaps | MEDIUM | No |
| 09 | ECIES AES-GCM correctness audit | INFORMATIONAL | No |
| 10 | SE tag UTF-8 lossy domain separator diverges from Go | HIGH | **YES** |
| 11 | SE tag tests contain no Go-generated test vectors | MEDIUM-HIGH | Risk amplifier |
| 12 | JWT token format has no Go compatibility tests | MEDIUM | Unverified risk |
| 13 | secp256r1 systematic Go compat test gaps | MEDIUM | Compounds #04 |
| 14 | Session 4 Go compatibility summary | INFORMATIONAL | Summary |
| 15 | SE domain separator delimiter collision vulnerability | LOW-MEDIUM | No (shared with Go) |
| 16 | SE enc_key not zeroized and default all-zeros | MEDIUM | No |
| 17 | SE deterministic tags enable frequency analysis | INFORMATIONAL | By design |
| 18 | SE artifact metadata leakage to replicators | MEDIUM | No (shared with Go) |
| 19 | SE HMAC key accepts any length without validation | LOW-MEDIUM | No |
| 20 | Merkle proof verification sound; trust boundary | LOW | No |
| 21 | Session 5 SE & Merkle proof summary | INFORMATIONAL | Summary |

### 1.0 Blockers

| Finding | Issue | Required Action |
|---------|-------|-----------------|
| **10** | SE tag UTF-8 lossy divergence from Go | **MUST FIX** — feed raw identity bytes to HMAC, add Go test vectors |
| **04** | secp256r1 S-normalization CID divergence | **INVESTIGATE** — determine if secp256r1 signatures appear in IPLD blocks |

### Cross-Stream Themes

1. **Zeroization is systematically absent**: Findings 00, 01, 02, 16 all identify key/secret material that is never zeroed on drop. The `zeroize` crate is a dependency but not used on any key type or the SE encryption key.

2. **Go compatibility has asymmetric test coverage**: Ed25519 and secp256k1 have excellent Go test vectors. secp256r1, JWT, and SE tags have significant gaps (Findings 04, 11, 12, 13).

3. **Defense-in-depth input validation is weak**: HMAC key length not checked (Finding 19), ECIES low-order keys accepted (Finding 07), ciphertext validation has gaps (Finding 08). The underlying crypto libraries are correct but the application layer doesn't validate inputs.

4. **Metadata leakage is by design but under-documented**: The SE scheme leaks field names, document IDs, and value frequencies to replicators (Findings 17, 18). This is inherent to the SSE design but users need to understand the trust model.

5. **Merkle proof system is sound**: The proof generation, verification, and signing code is well-implemented with good test coverage (Finding 20). The only concern is caller-side trust verification of embedded keys.
