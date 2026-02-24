# Session 5: Searchable Encryption & Merkle Proof — Complete Results

**Stream**: 01 - Cryptographic Inventory
**Session**: 5 - Searchable Encryption & Merkle Proof (FINAL SESSION)
**Severity**: INFORMATIONAL (summary)
**Category**: Audit Summary
**Status**: COMPLETE

## New Findings This Session

| # | Finding | Severity | 1.0 Blocker? |
|---|---|---|---|
| 15 | SE domain separator delimiter collision vulnerability | LOW-MEDIUM | No (shared with Go) |
| 16 | SE enc_key not zeroized and default all-zeros | MEDIUM | No (pattern from Finding 00) |
| 17 | SE deterministic tags enable frequency analysis | INFORMATIONAL | No (by design) |
| 18 | SE artifact metadata leakage to replicators | MEDIUM | No (shared with Go) |
| 19 | SE HMAC key accepts any length without validation | LOW-MEDIUM | No (defense in depth) |
| 20 | Merkle proof verification sound; embedded key trust boundary | LOW | No (correct design) |

## Session 5 Checklist Results

| Check | Result | Evidence |
|---|---|---|
| SE tags: HMAC-SHA256 correctly computed | **PASS** | `tag.rs:87-100` — standard HMAC-SHA256 with truncation |
| Domain separator prevents cross-field/cross-collection correlation | **PASS** (with caveat) | Domain separator includes identity + collection + field, but delimiter collision possible (Finding 15) |
| SE tag truncation: 16 bytes sufficient? | **PASS** | Birthday bound at 2^64 — more than adequate |
| SE frequency analysis documented? | **PASS** | Code comments at `tag.rs:26-31` document the trade-off (Finding 17) |
| SE tag isolation: different identity → different tag | **PASS** | Verified via unit test `test_different_identities_different_tags` in both `tag.rs` and `artifact_gen.rs` |
| SE enc_key zeroized on coordinator drop? | **FAIL** | No Zeroize impl on `SECoordinatorConfig` (Finding 16) |
| SE artifact generation: randomized or deterministic? | **DETERMINISTIC** | No randomness in `generate_field_artifact` — pure HMAC function (Finding 17) |
| SE field name leakage | **YES — LEAKS** | `index_id` = field name in plaintext (Finding 18) |
| SE replicator leakage | **DOCUMENTED** | Replicator sees collection, field, doc_id, search tag (Finding 18) |
| Merkle proof: extraction includes all intermediate nodes | **PASS** | BFS traversal collects all path nodes (`extraction.rs:78-129`) |
| Merkle proof: verification recomputes hashes correctly | **PASS** | `verify_cid()` recomputes SHA-256, enforces SHA2-256 + DAG-CBOR (Finding 20) |
| Merkle proof: CID computation deterministic | **PASS** | `compute_cid()` is pure SHA-256 → multihash → CID v1 |
| Merkle proof: signed proofs match batch signing | **PARTIAL** | Signed proofs support 4 key types; batch signing only 2 (Ed25519, secp256k1) |

## Files Audited

| File | Lines | Status |
|---|---|---|
| `crates/crypto/src/se/tag.rs` | 205 | Line-by-line audit |
| `crates/crypto/src/se/artifact.rs` | 222 | Line-by-line audit |
| `crates/db/src/se/coordinator.rs` | 241 | Line-by-line audit |
| `crates/db/src/se/artifact_gen.rs` | 272 | Line-by-line audit |
| `crates/db/src/se/storage.rs` | 178 | Line-by-line audit |
| `crates/crypto/src/merkle_proof/mod.rs` | 27 | Reviewed |
| `crates/crypto/src/merkle_proof/extraction.rs` | 176 | Line-by-line audit |
| `crates/crypto/src/merkle_proof/proof.rs` | 131 | Line-by-line audit |
| `crates/crypto/src/merkle_proof/proof_node.rs` | 75 | Line-by-line audit |
| `crates/crypto/src/merkle_proof/signed_proof.rs` | 142 | Line-by-line audit |
| `crates/crypto/tests/merkle_proof_tests.rs` | 684 | Line-by-line audit |
| `crates/crypto/src/batch.rs` | 197 | Reviewed for consistency |
| `crates/db/src/push_docs.rs` | Relevant sections | Cross-referenced for SE usage |

## Overall SE Assessment

### The Good

- HMAC-SHA256 tag generation is correctly implemented
- Domain separator provides identity + collection + field isolation
- Tag truncation to 128 bits is cryptographically adequate
- Deterministic nature is acknowledged and documented
- Unit tests verify isolation properties (different keys/identities/collections/fields produce different tags)

### The Concerning

- **Delimiter collision** (Finding 15): Colon-delimited domain separator without escaping creates theoretical collision risk
- **Key hygiene** (Finding 16): Encryption key not zeroized, default all-zeros key, no length validation
- **Metadata leakage** (Finding 18): Field names, document IDs visible to replicators in plaintext
- **Push docs path** (`push_docs.rs:212`): Coordinator created without identity pubkey, removing identity-based tag isolation

### Design Trade-Offs (Acknowledged)

The SE scheme makes deliberate trade-offs between functionality and privacy:
- Deterministic tags enable equality search but leak frequency (Finding 17)
- Plaintext metadata enables efficient storage/retrieval but leaks structure (Finding 18)
- These trade-offs are shared with Go and are inherent to the SSE design choice

## Overall Merkle Proof Assessment

### The Good

- Proof verification is cryptographically sound — all 5 verification steps are correct
- CID computation is deterministic and matches Go
- Algorithm/codec validation prevents confusion attacks
- DoS protections are in place (node and path limits)
- Signed proofs correctly validate key type matching and identity consistency
- Test coverage is comprehensive (20+ test cases covering all paths)

### Minor Observations

- `verify_with_embedded_key()` is self-signed (trust boundary concern, not a bug)
- Signed proofs support 4 key types but batch signing only supports 2 (Finding 06 from Session 2)
