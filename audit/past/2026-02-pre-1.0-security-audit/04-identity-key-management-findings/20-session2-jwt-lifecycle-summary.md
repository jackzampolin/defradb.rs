# Session 2 Summary: JWT Token Lifecycle

**Scope**: Custom JWT encoder/decoder, DER↔raw conversion, signature verification flow, claim validation, base64 handling, HTTP identity extraction, timing analysis

**Files audited**:
- `crates/identity/src/token/encoding.rs` (73 lines)
- `crates/identity/src/token/decoding.rs` (135 lines)
- `crates/identity/src/token/der.rs` (256 lines)
- `crates/identity/src/token/claims.rs` (36 lines)
- `crates/identity/src/token/mod.rs` (275 lines)
- `crates/identity/src/token/identity.rs` (66 lines)
- `crates/identity/src/key_type.rs` (203 lines)
- `crates/identity/tests/token_tests.rs` (578 lines)
- `crates/http/src/identity_extractor.rs` (408 lines)
- `crates/crypto/src/keys/ed25519.rs` (lines 225-240 — verify)
- `crates/crypto/src/keys/secp256k1.rs` (lines 191-214 — verify)
- `crates/crypto/src/keys/secp256r1.rs` (lines 180-202 — verify)
- `crates/crypto/src/keys/mod.rs` (lines 20-44 — Key/PublicKey traits)
- `crates/crypto/src/keys/generation.rs` (lines 210-230 — public_key_from_bytes)

**Greps performed**:
- `raw_to_der|der_to_raw` — all DER conversion call sites
- `fn verify` — all signature verification implementations
- `clock_skew|CLOCK_SKEW|skew` — time tolerance handling
- `URL_SAFE` — base64 variant consistency across all files
- `from_bytes` — public key construction from raw bytes
- `fn raw` — public key serialization format per key type
- `to_hex_string` — key-to-claims encoding

## Findings Summary

| # | Finding | Severity | Status |
|---|---------|----------|--------|
| 10 | DER parser accepts non-canonical encodings (lax) | LOW | Confirmed (latent) |
| 11 | No token replay protection (no jti claim) | INFO | Confirmed (by design) |
| 12 | JWT token test coverage gaps | LOW | Confirmed |
| 13 | DER conversion roundtrip mathematically correct | GREEN | Verified sound |
| 14 | Clock skew and time validation correct | GREEN | Verified sound |
| 15 | Base64 URL_SAFE_NO_PAD used consistently | GREEN | Verified sound |
| 16 | Self-authenticating token design sound | GREEN | Verified sound |
| 17 | Signature verified before claims trusted | GREEN | Verified sound |
| 18 | Crypto verification uses constant-time libraries | GREEN | Verified sound |
| 19 | HTTP identity extraction and audience verification correct | GREEN | Verified sound |

## Security Checklist Results

| Check | Result |
|-------|--------|
| Algorithm confusion (alg header vs key_type claim)? | **Safe.** Header determines decode path; key_type cross-checked after signature. Different key sizes (32 vs 33 bytes) prevent cross-algorithm reuse. Already verified in Session 1 Finding 05. |
| DER conversion correctness? | **Sound.** `raw_to_der()` and `der_to_raw()` handle all valid edge cases (leading zeros, high bits, short values, all-zeros). Lax parsing in `der_to_raw()` is a latent issue (Finding 10) but only called on trusted crypto library output. |
| Signature verified before claims trusted? | **Yes.** Each decode function verifies signature before returning claims. Claims are decoded early (unavoidable for self-authenticating design) but never trusted without verification. |
| Empty/missing/extra signature? | **Safe.** Empty → rejected (wrong length). Extra parts → rejected (parts.len() != 3). |
| exp/nbf/aud validation? | **Correct.** 60s clock skew via `saturating_add` (overflow-safe). Missing audience → rejected. All required claims enforced by serde deserialization. |
| Base64 consistent? | **Yes.** `URL_SAFE_NO_PAD` everywhere. Decode is padding-indifferent (harmless laxness). |
| Public key from sub validated? | **Yes.** Each crypto library validates key bytes (correct length, valid curve point). Invalid keys rejected before signature verification. |
| Timing attacks? | **Not exploitable.** `ed25519_dalek`, `k256`, `p256` all use constant-time arithmetic. Non-constant-time DER parsing operates on public data only. |
| Token replay? | **By design.** No jti/nonce. Mitigated by audience binding, short expiry, TLS. Matches Go behavior. |
| Host header bypass? | **Prevented.** Authenticated requests require valid Host header. Missing/invalid Host → 403. |
| `iss` must match DID from `sub`? | **Yes.** `from_token()` derives DID from public key and compares to `iss` claim. Mismatch → error. |

## Key Architecture Insight

The JWT implementation is hand-rolled to avoid PKCS#8 DER format mismatches between the `jsonwebtoken` crate and the crypto library's raw key formats. The hand-rolled implementation is surprisingly clean — the highest-risk component (DER conversion) is correct for all valid inputs, and the overall verification flow is well-ordered with appropriate defense-in-depth checks.

## Overall Assessment

The custom JWT implementation is **sound**. No signature bypass, algorithm confusion, claim validation bypass, or timing attack vectors were found. The two actionable findings are low severity: the DER parser is slightly lax (latent, not currently exploitable) and test coverage could be improved for adversarial edge cases. The design choices (self-authenticating tokens, no replay protection) are intentional and appropriate for DID-based authentication.

## Remaining Sessions

- **Session 3**: HTTP identity extraction deep-dive — bearer token parsing edge cases, identity propagation through HTTP handlers, CORS/preflight interaction
- **Session 4**: Keyring integration — key storage, key loading, identity generation CLI
- **Session 5**: Cross-crate identity usage — how identity flows through ACP, P2P, query execution
