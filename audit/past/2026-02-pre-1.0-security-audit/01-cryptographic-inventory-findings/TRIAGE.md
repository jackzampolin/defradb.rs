# Cryptographic Inventory Findings — Triage Summary

**Stream**: 01 - Cryptographic Inventory
**Date**: 2026-02-21
**Findings**: 20 individual findings (excluding session summaries #14 and #21)

---

## 1. Findings Table

| # | Severity | Title | Status | One-Line Summary |
|---|----------|-------|--------|------------------|
| 10 | HIGH | SE Tag UTF-8 Lossy Go Divergence | NEW | `String::from_utf8_lossy()` on identity bytes produces different domain separators than Go's raw-byte `string()` cast, breaking all SE tag interop. |
| 11 | MEDIUM-HIGH | SE Tags No Go Test Vectors | NEW | All 14 SE compat tests use a local `compute_expected_tag()` mirror — circular validation that masked Finding 10. |
| 00 | MEDIUM-HIGH (Ed25519) / LOW (others) | Private Key Zeroization | CONFIRMED | Ed25519 `SigningKey` missing the `"zeroize"` feature flag; key material persists in memory after drop. secp256k1/secp256r1 inner keys ARE zeroed unconditionally. |
| 02 | MEDIUM | Ed25519 Keygen Seed Not Zeroed | NEW | `generate_ed25519()` leaves the 32-byte seed, intermediate `SigningKey`, and a 64-byte heap `Vec` unzeroed through the full generation path. |
| 04 | MEDIUM | secp256r1 Go Signature S-Normalization Gap | NEW | Rust always normalizes S to low form; Go `crypto/ecdsa` does not. Different signature bytes for the same key and message; potential CID divergence if secp256r1 signs IPLD blocks. |
| 12 | MEDIUM | JWT No Go Compat Tests | NEW | 24 JWT roundtrip tests are all Rust-to-Rust; no Go-generated JWT token is ever parsed or compared. |
| 13 | MEDIUM | secp256r1 Systematic Compat Gaps | NEW | secp256r1 has no byte-equality signing test, no low-S normalization test, dead `SECP256R1_PRIVATE_KEY` constant, and no DAG-CBOR signature block test — far weaker coverage than Ed25519/secp256k1. |
| 16 | MEDIUM | SE Enc Key Not Zeroized and Default Zeros | NEW | `SECoordinatorConfig.enc_key` is a plain `Vec<u8>` never zeroed on drop; `Default` impl initializes an all-zeros key that would make all tags globally predictable. |
| 18 | MEDIUM | SE Artifact Metadata Leakage to Replicators | NEW | Collection IDs, field names (as `index_id`), and document IDs are plaintext in artifacts; replicators can map schema structure and track query patterns. |
| 03 | LOW-MEDIUM | Key::raw() Returns Unprotected Vec | NEW | Trait method `fn raw() -> Vec<u8>` returns unprotected private key bytes across 10+ call sites in 5 crates; no caller uses `Zeroizing` wrappers. |
| 07 | LOW-MEDIUM | ECIES X25519 Low-Order Key Acceptance | NEW | Rust ECIES accepts all-zeros (and other small-subgroup) X25519 public keys producing predictable shared secrets; Go rejects these with an error. |
| 15 | LOW-MEDIUM | SE Domain Separator Delimiter Collision | NEW | Colon-delimited `"eq:{identity}:{collection}:{field}"` without escaping allows input tuples to collide when any component contains `:`. Shared with Go. |
| 19 | LOW-MEDIUM | SE HMAC Key No Length Validation | NEW | `generate_equality_tag` accepts any key length (including zero) despite documenting a 32-byte requirement; silently computes insecure tags on wrong-size keys. |
| 01 | LOW | ECIES Shared Secret Not Zeroed | CONFIRMED | HKDF-derived `keys`, `aes_key`, `hmac_key` arrays left unzeroed on the stack after encrypt/decrypt. Shared secret and ephemeral key ARE zeroed. |
| 05 | LOW | JWT Algorithm Dispatch from Header | NEW | Algorithm selection uses attacker-controlled JWT `alg` header before validating consistency with payload `key_type`. Not exploitable (self-signed JWTs) but wastes crypto work. |
| 06 | LOW | Batch Signing Missing secp256r1 | NEW | `sign_batch()` and `verify_batch_signature()` only support Ed25519 and secp256k1; secp256r1 identities cannot batch-sign. |
| 08 | LOW | ECIES Ciphertext Validation Gaps | NEW | Minimum ciphertext length check is weaker than Go (misses AES nonce + GCM tag); default `prepend_public_key` is inverted vs Go. Neither is exploitable. |
| 20 | LOW | Merkle Proof Verification Sound; Trust Boundary | NEW | Implementation is cryptographically sound; `verify_with_embedded_key()` proves self-consistency but not signer trust — callers must check identity independently. |
| 09 | GREEN | ECIES & AES-GCM Correctness Audit | COMPLETE | Full line-by-line audit: X25519 ECDH, HKDF-SHA256, AES-256-GCM, HMAC-SHA256, nonce generation, encrypt-then-MAC ordering — all correct and Go-compatible. |
| 17 | INFORMATIONAL | SE Deterministic Tags Frequency Analysis | ACKNOWLEDGED | Deterministic HMAC tags inherently leak frequency distribution to replicators. This is a known SSE trade-off, documented in code comments. |

---

## 2. Themes

### Theme A: Key Material Zeroization Gaps (Findings 00, 01, 02, 03, 16)

Five findings share a single root cause: cryptographic key material is not zeroed when it leaves scope. The severity varies by key type and lifetime:

- **Ed25519 private keys** (Finding 00) are the highest-value — node identity keys, long-lived, and the `"zeroize"` feature flag is a one-line fix.
- **Ed25519 seed during generation** (Finding 02) compounds Finding 00 by leaking the raw entropy on both stack and heap.
- **ECIES derived keys** (Finding 01) are ephemeral symmetric keys — lower risk because they are per-operation and stack-allocated.
- **Key::raw() trait** (Finding 03) is an architectural issue — every call site across 5 crates receives unprotected `Vec<u8>` with no cleanup path.
- **SE encryption key** (Finding 16) extends the pattern to searchable encryption, with the added concern of an all-zeros default.

All five are defense-in-depth concerns requiring live memory access to exploit. The Ed25519 feature flag fix (Finding 00) is the highest-impact single change.

### Theme B: Go Compatibility Divergences (Findings 04, 07, 08, 10, 15)

Five findings identify behavioral differences between Rust and Go implementations that could cause interop failures in mixed networks:

- **SE tag UTF-8 lossy** (Finding 10) is the most severe — every SE tag for every identity diverges, breaking encrypted equality search entirely.
- **secp256r1 S-normalization** (Finding 04) produces different signature bytes, causing potential CID divergence if secp256r1 signs content-addressed blocks.
- **ECIES low-order key acceptance** (Finding 07) — Rust accepts degenerate ECDH results that Go rejects.
- **ECIES ciphertext validation** (Finding 08) — different minimum-length checks and inverted default options.
- **SE delimiter collision** (Finding 15) — shared with Go, so not a divergence, but a shared design weakness.

### Theme C: Test Coverage Gaps for Go Compatibility (Findings 11, 12, 13)

Three findings identify missing cross-implementation test vectors:

- **SE tags** (Finding 11) have zero hardcoded Go vectors — circular validation masked the HIGH-severity Finding 10.
- **JWT** (Finding 12) has 24 tests but none parse a Go-generated token.
- **secp256r1** (Finding 13) has systematically weaker coverage than Ed25519 and secp256k1 across every test category.

These are "test debt" findings — they do not represent bugs themselves but create blind spots where bugs hide.

### Theme D: Searchable Encryption Privacy Limitations (Findings 15, 17, 18)

Three findings describe inherent limitations of the deterministic SSE design:

- **Frequency analysis** (Finding 17) is inherent to deterministic tags and documented in code.
- **Metadata leakage** (Finding 18) exposes field names, document IDs, and query patterns to replicators.
- **Delimiter collision** (Finding 15) is a theoretical domain separation weakness, shared with Go.

These are design trade-offs rather than implementation bugs. Finding 18 could be partially mitigated by hashing field names.

### Theme E: Feature Completeness (Findings 05, 06)

Two findings identify functional gaps:

- **Batch signing missing secp256r1** (Finding 06) — a dispatch table oversight.
- **JWT algorithm dispatch ordering** (Finding 05) — defense-in-depth improvement, not exploitable.

---

## 3. Actionable vs Informational

### Must Fix (1.0 Blockers)

| # | Finding | Rationale |
|---|---------|-----------|
| 10 | SE Tag UTF-8 Lossy Go Divergence | **Breaks all searchable encryption interop.** Every SE equality query will fail in a mixed Go/Rust network. One-function fix. |
| 00 | Ed25519 Private Key Not Zeroized | **One-line Cargo.toml change** enables `ZeroizeOnDrop` for the highest-value keys in the system. Trivial fix, high impact. |

### Should Fix (Pre-1.0)

| # | Finding | Rationale |
|---|---------|-----------|
| 11 | SE Tags No Go Test Vectors | **Add Go-generated vectors** to prevent regression of Finding 10 and catch future divergences. Effort: generate vectors from Go, add to test file. |
| 02 | Ed25519 Keygen Seed Not Zeroed | Compounds Finding 00. Add `seed.zeroize()` and `key_bytes.zeroize()` — three lines of code. |
| 04 | secp256r1 S-Normalization Gap | **Investigate whether secp256r1 signs IPLD blocks.** If yes, CID divergence is a 1.0 blocker. If only used for JWT auth, document and defer. |
| 07 | ECIES X25519 Low-Order Key Acceptance | Add a post-ECDH all-zeros check in both encrypt and decrypt paths. Two lines of code, matches Go behavior. |
| 16 | SE Enc Key Not Zeroized / Default Zeros | Add `Zeroize` + `ZeroizeOnDrop` to `SECoordinatorConfig`. Remove or guard the all-zeros default. |
| 12 | JWT No Go Compat Tests | Generate JWT test vectors from Go and add parsing tests. Medium effort but closes a significant blind spot. |
| 13 | secp256r1 Systematic Compat Gaps | Enable the dead `SECP256R1_PRIVATE_KEY` constant, add signing tests, fix misleading comments. Medium effort. |
| 19 | SE HMAC Key No Length Validation | Add a 32-byte length assertion. One line of code. |

### Accept Risk / Backlog

| # | Finding | Rationale |
|---|---------|-----------|
| 03 | Key::raw() Returns Unprotected Vec | Architectural change (trait return type) with 10+ call sites. Zeroization at call sites is incremental. Accept for 1.0, plan for post-1.0. |
| 15 | SE Domain Separator Delimiter Collision | Shared with Go — fixing requires coordinated protocol change. Practical exploitability is low (key bytes containing `:` at collision-enabling positions). |
| 18 | SE Artifact Metadata Leakage | Shared with Go, inherent to SSE design. Mitigations (hash field names, encrypt doc IDs) require Go coordination. Document the trust model for users. |
| 01 | ECIES Derived Keys Not Zeroed | Ephemeral stack-allocated symmetric keys. Low risk. Fix if touching ECIES code, otherwise defer. |
| 05 | JWT Algorithm Dispatch Ordering | Not exploitable. Defense-in-depth improvement. Low priority. |
| 06 | Batch Signing Missing secp256r1 | Feature gap. Fix when secp256r1 batch signing is needed. Currently no production use case. |
| 08 | ECIES Ciphertext Validation Gaps | Not exploitable. Tighten the length check and fix the default when touching ECIES code. |

### No Action (GREEN)

| # | Finding | Rationale |
|---|---------|-----------|
| 09 | ECIES & AES-GCM Correctness Audit | Full audit passed. Core crypto construction is correct and Go-compatible. |
| 17 | SE Deterministic Tags Frequency Analysis | Inherent to deterministic SSE. Documented in code. No code change needed. |
| 20 | Merkle Proof Verification Sound | Implementation is correct. Trust boundary is a usage concern, not a bug. Consider doc comment improvement. |

---

## 4. Recommended Fix Order

### Phase 1: Immediate (hours, maximum impact per effort)

**1. Finding 10 — Fix SE tag UTF-8 lossy divergence**
- Why first: HIGH severity, 1.0 blocker, breaks all SE interop.
- Effort: Change one function in `crates/crypto/src/se/tag.rs` to feed raw identity bytes into HMAC instead of UTF-8-converting them.
- Risk: Low — the fix simplifies the code (removes a conversion step).

**2. Finding 00 — Enable ed25519-dalek `"zeroize"` feature**
- Why second: One-line change in workspace `Cargo.toml` enables `ZeroizeOnDrop` for all Ed25519 private keys.
- Effort: Add `"zeroize"` to the ed25519-dalek features list.
- Risk: None — the feature only adds drop behavior.

**3. Finding 02 — Zeroize ed25519 keygen intermediates**
- Why third: Compounds Finding 00. Three lines of `seed.zeroize()` and `key_bytes.zeroize()`.
- Effort: Trivial once Finding 00 is done.

### Phase 2: Same sprint (days, close test gaps that masked Phase 1 bugs)

**4. Finding 11 — Add Go-generated SE tag test vectors**
- Why now: Prevents regression of Finding 10 fix. Also catches any remaining SE divergences.
- Effort: Run a small Go program, hardcode the output, add one test.

**5. Finding 07 — Reject ECIES low-order keys**
- Why now: Simple two-line check (`if shared_secret.as_bytes().iter().all(|&b| b == 0)`) aligns Rust with Go.
- Effort: Trivial.

**6. Finding 19 — Add SE HMAC key length validation**
- Why now: One-line assertion. Prevents silent insecure tag generation.
- Effort: Trivial.

**7. Finding 16 — Zeroize SE encryption key**
- Why now: Follows the zeroization pattern established in Phase 1. Derive `Zeroize` + `ZeroizeOnDrop` on `SECoordinatorConfig`.
- Effort: Small.

### Phase 3: Pre-1.0 (next sprint, investigation + test infrastructure)

**8. Finding 04 — Investigate secp256r1 in IPLD blocks**
- Why now: Need to determine if S-normalization causes CID divergence in practice. If secp256r1 only appears in JWTs, document and close. If it appears in blocks, coordinate with Go on normalization.
- Effort: Investigation, then either documentation or a Go-side change.

**9. Finding 12 — Add Go JWT test vectors**
- Why now: Closes the JWT blind spot. Generate tokens from Go, add parsing tests.
- Effort: Medium (requires running Go code, adding multiple test vectors).

**10. Finding 13 — Complete secp256r1 test coverage**
- Why now: Builds on Finding 04 investigation. Enable dead code, add signing tests, fix comments.
- Effort: Medium.

### Phase 4: Post-1.0 (backlog)

**11-16. Findings 03, 01, 05, 06, 08, 15, 18** — Address as part of ongoing hardening. Finding 03 (Key::raw() trait) is the largest item — plan the trait change for a focused refactoring sprint.
