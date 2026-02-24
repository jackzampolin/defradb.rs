# Identity & Key Management: Triage Report

**Stream**: 04 — Identity & Key Management
**Total Findings**: 56 (excluding 5 session summaries and STREAM-SUMMARY)
**Date**: 2026-02-21

---

## 1. Findings Table

Sorted by severity (HIGH first, GREEN last), then by finding number.

| # | Severity | Title | Status | One-Line Summary |
|---|----------|-------|--------|------------------|
| 37 | HIGH | Debug dump endpoint has no identity or NAC check | Confirmed | `/api/v0/debug/dump` returns all DB content to any unauthenticated client with network access |
| 00 | MEDIUM | Wildcard DID key_portion() panics on out-of-bounds slice | Confirmed (latent) | Calling `key_portion()` on a wildcard DID panics because `"*"` is shorter than the `did:key:` prefix offset |
| 21 | MEDIUM | PBKDF2 iteration count weak (10,000) | Confirmed | FileKeyring uses 10k PBKDF2-SHA512 iterations vs OWASP 2023 recommendation of 210k; enables offline brute-force of stolen key files |
| 23 | MEDIUM | Keyring secret from environment not zeroized | Confirmed | `load_secret_from_env()` returns plain `Vec<u8>` instead of `Zeroizing<Vec<u8>>`; password persists in heap |
| 24 | MEDIUM | Keyring get() returns plain Vec, not Zeroizing | Confirmed | Decrypted private key material returned as `Vec<u8>` without zeroization guarantee across all backends |
| 27 | MEDIUM | Private key material printed to stdout | Confirmed | `identity new` (unnamed) and `keyring export` print raw private key hex to stdout, captured in scrollback/logs |
| 36 | MEDIUM | Empty Bearer token treated as anonymous | Confirmed (Go compat) | `Authorization: Bearer ` (empty token) silently downgrades to anonymous instead of returning 403 |
| 40 | MEDIUM | CORS allows wildcard origin with auth header | Confirmed (Go compat) | Wildcard CORS with Authorization in allow_headers; safe because browsers block credentials with `*` origin |
| 41 | MEDIUM | No X-Forwarded-Host support for audience validation | Confirmed | Reverse proxy deployments break JWT audience validation because only `Host` header is used |
| 42 | MEDIUM | Private key passed as CLI argument visible in process table | Confirmed | `--identity` / `-i` flag exposes hex private key in `ps`, `/proc`, shell history, and audit logs |
| 43 | MEDIUM | identity new prints private key to stdout (additional context) | Confirmed | Extends finding 27 with details on JSON output mode and `keyring export` / `identity export` paths |
| 45 | MEDIUM | Identity extraction is per-handler, not global middleware | Confirmed | Each handler must opt-in to `ExtractIdentity`; omitting it creates an unauthenticated endpoint (see finding 37) |
| 47 | MEDIUM | keyring import accepts key on CLI argument | Confirmed | `keyring import <name> <hex>` exposes key in process table; `--stdin` alternative exists |
| 51 | MEDIUM | Key type ambiguity for 32-byte keys | Confirmed (Go compat) | 32-byte keys default to secp256k1; secp256r1 and Ed25519 seeds (also 32 bytes) are misidentified |
| 53 | MEDIUM | No expired token integration test | Confirmed | No end-to-end HTTP test verifies that expired JWTs are rejected; only unit-level manipulation tests exist |
| 58 | MEDIUM | No identity confusion/substitution integration test | Confirmed | No test verifies that Alice's token cannot yield Bob's permissions through the full HTTP + ACP pipeline |
| 01 | LOW | DID validation only checks prefix, not structure | Confirmed (by design) | `Did::new()` accepts any string starting with `did:key:` without validating multibase, base58, or key material |
| 02 | LOW | zanzibar::Did::new_unchecked() is pub not pub(crate) | Confirmed | Public unchecked constructor allows any crate to create unvalidated zanzibar DIDs; panic chain via `from_zdid()` |
| 03 | LOW | Wildcard DID cannot survive serde roundtrip | Confirmed | `Did::wildcard()` creates `Did("*")` but deserialization through `TryFrom<String>` rejects `"*"` |
| 10 | LOW | DER parser accepts non-canonical encodings | Confirmed (latent) | `der_to_raw()` ignores SEQUENCE length, accepts trailing bytes, doesn't handle multi-byte INTEGER lengths |
| 12 | LOW | JWT token test coverage gaps | Confirmed | Missing tests for empty/truncated signatures, cross-algorithm key confusion, edge-case claim values, adversarial DER |
| 22 | LOW | File delete: no fsync before unlink | Confirmed | FileKeyring zero-fills before delete but skips `sync_all()`, so zeros may not reach disk before unlink |
| 25 | LOW | SystemdCreds PATH-based lookup, no subprocess timeout | Confirmed | `Command::new("systemd-creds")` resolves via PATH; `wait_with_output()` blocks indefinitely if process hangs |
| 26 | LOW | SystemdCreds: no secure deletion of .cred files | Confirmed | `delete()` uses bare `remove_file()` without zero-fill; mitigated by TPM-bound encryption |
| 28 | LOW | Directory permission TOCTOU on create | Confirmed (accepted) | `create_dir_all()` then `set_permissions(0o700)` has brief window at default umask permissions |
| 29 | LOW | No file locking for concurrent access | Confirmed | Concurrent multi-process writes to FileKeyring can corrupt key files; single-process use is safe |
| 33 | LOW | FileKeyring set() missing fsync | Confirmed | No `sync_all()` after writing key file; crash may produce zero-length or partial file |
| 35 | LOW | Bearer prefix incomplete case-insensitivity | Confirmed (Go compat) | Only `"Bearer "` and `"bearer "` accepted; `"BEARER "` and other mixed-case variants rejected per Go behavior |
| 38 | LOW | 403 error response leaks failure reason | Confirmed | Distinct error messages for malformed token, expired token, and audience mismatch help attacker enumeration |
| 44 | LOW | WebSocket endpoint registered without auth | Confirmed | `/api/v0/graphql/ws` returns 501 Not Implemented; no auth needed since no data is processed |
| 46 | LOW | Host header audience exact match, no port normalization | Confirmed (Green) | Exact string comparison for audience; no subdomain or default-port normalization; correct for DefraDB |
| 48 | LOW | keyring export prints raw key hex to stdout | Confirmed (expected) | Standard export behavior; no `--file` option with restricted permissions |
| 50 | LOW | Multiple Authorization headers: first wins | Confirmed (framework) | `HeaderMap::get()` returns first value only; standard `http` crate behavior matching Go |
| 54 | LOW | No anonymous access test in acp_basic.rs | Partially covered | Primary ACP test lacks anonymous query; covered in `encrypted_acp.rs`, `acp_p2p.rs`, `nac_document_acp.rs` |
| 55 | LOW | Node identity integration test is minimal | Confirmed | Only checks non-empty response; no format validation, persistence, or cryptographic binding tests |
| 61 | LOW | No wrong-key-type token integration test | Confirmed | Algorithm mismatch not tested through HTTP stack; covered at unit level in `token_tests.rs` |
| 04 | INFO | IdentityContext has no public-key-only state | Confirmed | `has_identity()` and `has_full_identity()` are always equivalent; no `PublicOnly` variant exists |
| 11 | INFO | No token replay protection | Confirmed (by design) | No `jti` claim or nonce; replay limited by audience binding, expiry, and TLS; matches Go behavior |
| 39 | INFO | 403 not 401 for invalid credentials | Confirmed (Go compat) | Reversed HTTP semantics (403 for bad token, 401 for insufficient NAC permission); matches Go |
| 62 | INFO | No key rotation test | Confirmed (by design) | Key rotation not supported; DIDs permanently bound to keys; no migration tooling needed for 1.0 |
| 05 | GREEN | JWT algorithm dispatch from header verified correct | Verified safe | Only EdDSA, ES256K, ES256 accepted; `alg:none` rejected; algorithm cross-checked against key_type claim |
| 06 | GREEN | RawIdentity DID-PublicKey binding sound | Verified safe | DID derived from public key at call time, never stored separately; no mismatch possible |
| 07 | GREEN | Wildcard DID cannot be impersonated | Verified safe | All identity construction paths require valid cryptographic key material; `"*"` unreachable via JWT/serde/CLI |
| 08 | GREEN | Key type conversions bijective, BLS12-381 rejected | Verified safe | Exhaustive match on `KeyType`; compiler enforces new variant handling; no type confusion path |
| 13 | GREEN | DER conversion roundtrip mathematically correct | Verified sound | Leading-zero padding, high-bit handling, short-value right-alignment all correct for 256-bit curves |
| 14 | GREEN | Clock skew and time validation correct | Verified sound | 60s tolerance via `saturating_add`; missing audience rejected; all required claims enforced by serde |
| 15 | GREEN | Base64 URL_SAFE_NO_PAD used consistently | Verified sound | All JWT encode/decode operations use the same base64 variant; padding-indifferent decode is harmless |
| 16 | GREEN | Self-authenticating token design sound | Verified sound | Public key in `sub`, DID in `iss` cross-checked, signature proves possession; correct for DID-based auth |
| 17 | GREEN | Signature verified before claims trusted | Verified sound | All three decode functions verify cryptographic signature before returning claims to caller |
| 18 | GREEN | Signature verification uses constant-time crypto | Verified sound | ed25519-dalek, k256, p256 all provide constant-time operations; no custom comparison on secrets |
| 19 | GREEN | HTTP identity extraction and audience verification correct | Verified sound | Missing Host + token = reject; case normalization; empty token = anonymous; matches Go behavior |
| 30 | GREEN | JWE construction sound | Verified clean | josekit library handles PBES2-HS512-A256KW + A256GCM; unique salt per encryption; Go-compatible format |
| 31 | GREEN | SystemKeyring base64 STANDARD encoding correct | Verified clean | Intentional for Go compatibility; OS keyrings handle `+`, `/`, `=` safely; decode rejects invalid input |
| 32 | GREEN | Key name validation prevents path traversal | Verified clean | Rejects `/`, `\`, `\0`, `.`, `..`, empty; consistently applied across all file-based backends |
| 49 | GREEN | Identity extraction before body read | Verified sound | Axum `FromRequestParts` guarantees auth checked before body consumption; prevents pre-auth DoS |
| 56 | GREEN | Test helpers use real signing path | Verified sound | Integration tests use real CLI binary and production signing code; no test-only shortcuts |
| 57 | GREEN | P2P peer identity has cryptographic binding | Verified sound | `verify_message()` checks PeerId matches public key and signature is valid over message content |
| 59 | GREEN | JWT claim validation ordered correctly after signature | Verified sound | Signature verification precedes claim trust; prevents timing oracles on claim values |
| 60 | GREEN | Identity propagation through query pipeline correct | Verified sound | Identity passed by value (cloned `Did`) through function parameters; no thread-local or global state |
| 63 | GREEN | Error path identity handling clean | Verified sound | All failures result in `None` identity or 403; no partial identity state; error messages don't leak secrets |

---

## 2. Themes

### A. JWT Token Security (Findings 05, 10, 11, 12, 13, 14, 15, 16, 17, 18, 59)
The custom JWT implementation is architecturally sound. Signature verification is constant-time, ordered before claim trust, and uses well-audited Rust crypto libraries. The DER parser is slightly lax (10) and test coverage could improve (12), but no bypass vectors exist. Token replay (11) is an accepted design trade-off matching Go.

### B. DID Type Safety (Findings 00, 01, 02, 03, 06, 07, 08)
The wildcard DID (`"*"`) creates a special case that bypasses normal `did:key:` invariants: `key_portion()` panics (00), serde roundtrip fails (03), and `zanzibar::Did::new_unchecked()` is too broadly visible (02). DID validation is prefix-only (01) by design for Go compatibility. The core binding of DIDs to public keys is cryptographically sound (06, 07, 08).

### C. Key Material Zeroization (Findings 23, 24, 27, 43, 48)
The keyring password is correctly zeroized, but the much more valuable private key material flowing through `Keyring::get()` is returned as plain `Vec<u8>` (24). The environment variable secret path also lacks zeroization (23). CLI commands print private keys to stdout (27, 43) and export paths leave key material in non-zeroizing buffers.

### D. Keyring Backend Hardening (Findings 21, 22, 25, 26, 28, 29, 30, 31, 32, 33)
FileKeyring uses weak PBKDF2 iterations (21), lacks fsync on write and delete (22, 33), and has no file locking for concurrent writes (29). SystemdCreds has no subprocess timeout (25) and no secure deletion (26). Directory creation has a minor TOCTOU (28). JWE construction (30), system keyring encoding (31), and key name validation (32) are all sound.

### E. HTTP Authentication Layer (Findings 19, 35, 36, 37, 38, 39, 40, 41, 44, 45, 46, 49, 50)
The most critical finding is the unauthenticated dump endpoint (37). The per-handler identity extraction model (45) is the root cause that allows such omissions. Empty bearer tokens silently downgrade to anonymous (36). Error messages leak failure reasons (38). No reverse proxy header support (41). CORS configuration is safe in practice (40). Many details are Go-compatible trade-offs (35, 39, 46, 50).

### F. CLI Credential Exposure (Findings 27, 42, 43, 47, 48)
Private keys are exposed through CLI arguments visible in process tables (42, 47), printed to stdout (27, 43, 48), and stored in shell history. The `--identity-name` and `--stdin` alternatives exist but are not the default path. All of these match Go CLI behavior.

### G. Integration Test Coverage Gaps (Findings 53, 54, 55, 58, 61, 62)
Integration tests cover happy-path identity flows but lack negative cases: no expired token test through HTTP (53), no identity substitution test (58), no wrong-key-type test at HTTP level (61), minimal node identity test (55), and incomplete anonymous access testing in core ACP suite (54). Unit tests cover most of these scenarios in isolation.

### H. Identity Architecture (Findings 04, 49, 56, 57, 60, 63)
The identity architecture is sound. Identity propagation uses function parameters (60), not global state. Identity extraction runs before body parsing (49). P2P peer identity has cryptographic binding (57). Test helpers use production signing code (56). Error paths produce clean `None` identity states (63).

---

## 3. Actionable vs Informational

### Must Fix (1.0 Blockers)

| # | Finding | Rationale |
|---|---------|-----------|
| 37 | Debug dump endpoint has no identity or NAC check | Any unauthenticated client can dump the entire database including ACP-protected documents; direct data exfiltration path |
| 45 | Identity extraction is per-handler, not global middleware | Root cause of finding 37; without a deny-by-default auth layer, any new endpoint risks being unauthenticated |

### Should Fix (Pre-1.0)

| # | Finding | Rationale |
|---|---------|-----------|
| 21 | PBKDF2 iteration count weak (10k) | Offline brute-force feasible for weak passwords against stolen key files; coordinate with Go on upgrade |
| 24 | Keyring get() returns plain Vec, not Zeroizing | Private key material persists in heap across all backends; single trait change fixes all callers |
| 23 | Keyring secret from env not zeroized | Password bytes persist in heap; straightforward fix to wrap in `Zeroizing` |
| 00 | Wildcard DID key_portion() panics | Latent panic reachable via `pub` API; becomes exploitable if wildcard DID flows into new code paths |
| 36 | Empty Bearer treated as anonymous | Silent auth downgrade via misconfigured proxy; add warning log at minimum |
| 41 | No X-Forwarded-Host support | Breaks JWT audience validation behind reverse proxies; blocking for production deployments |
| 53 | No expired token integration test | Critical negative test missing from HTTP pipeline; regression risk |
| 58 | No identity confusion integration test | Core identity isolation property never tested end-to-end |
| 38 | 403 error leaks failure reason | Audience mismatch message reveals expected Host value; aids targeted token crafting |

### Accept Risk / Backlog

| # | Finding | Rationale |
|---|---------|-----------|
| 01 | DID prefix-only validation | By design for Go compatibility; invalid DIDs never match valid ones in ACP |
| 02 | zanzibar::Did::new_unchecked() is pub | Current call sites are safe; minor API surface issue |
| 03 | Wildcard DID serde asymmetry | Wildcards are never serialized in practice; correctness issue, not security |
| 04 | IdentityContext no public-only state | Design observation; no privilege escalation possible |
| 10 | DER parser lax | Only called on trusted crypto library output; latent, not currently exploitable |
| 11 | No token replay protection | Intentional stateless JWT design; mitigated by audience binding and expiry |
| 12 | JWT test coverage gaps | Useful improvements but no known exploit gaps |
| 22 | File delete no fsync before unlink | Defense-in-depth improvement; CoW filesystems limit effectiveness anyway |
| 25 | SystemdCreds PATH lookup, no timeout | Requires local attacker with PATH write access; low practical risk |
| 26 | SystemdCreds no secure delete | TPM-bound encryption makes ciphertext recovery useless without TPM |
| 27/43 | Private key printed to stdout | Go-compatible behavior; `--name` flag avoids it |
| 28 | Directory permission TOCTOU | Key files themselves are atomically protected; directory race window is brief |
| 29 | No file locking | Single-process use is safe; keyring writes are infrequent |
| 33 | FileKeyring set() no fsync | Data durability issue, not security; crash produces safe failure mode |
| 35 | Bearer case-insensitivity incomplete | Go-compatible; `"Bearer "` and `"bearer "` cover real-world clients |
| 39 | 403 vs 401 semantics | Intentional Go compatibility; internally consistent |
| 40 | CORS wildcard with auth header | Safe: browsers block credentials with `*` origin |
| 42/47 | Key on CLI argument | Go-compatible; `--identity-name` and `--stdin` alternatives exist |
| 44 | WebSocket endpoint no auth | Returns 501; no data processed |
| 46 | Host audience exact match | Correct for DefraDB use case |
| 48 | keyring export prints key | Expected export behavior |
| 50 | Multiple Authorization headers first wins | Standard framework behavior matching Go |
| 51 | Key type ambiguity 32-byte keys | Go-compatible default; keyring stores 64-byte Ed25519 to avoid ambiguity |
| 54 | No anonymous test in acp_basic.rs | Covered in other test files |
| 55 | Node identity test minimal | Low risk; node identity is primarily informational |
| 61 | No wrong-key-type integration test | Covered at unit level |
| 62 | No key rotation test | Key rotation not supported by design |

### No Action (GREEN)

| # | Finding | Verified Property |
|---|---------|-------------------|
| 05 | JWT algorithm dispatch | Only EdDSA/ES256K/ES256 accepted; alg:none rejected; cross-checked against key_type |
| 06 | RawIdentity DID-key binding | DID derived from public key at call time; no stored mismatch possible |
| 07 | Wildcard DID impersonation | Cryptographic key material required; `"*"` unreachable via any external path |
| 08 | Key type conversions bijective | Exhaustive match with compile-time enforcement; BLS12-381 rejected |
| 13 | DER roundtrip correct | All edge cases (leading zeros, high bits, short values) handled correctly |
| 14 | Clock skew implementation | 60s tolerance; overflow-safe `saturating_add`; missing audience rejected |
| 15 | Base64 consistency | `URL_SAFE_NO_PAD` used everywhere; padding-indifferent decode is harmless |
| 16 | Self-authenticating design | Public key in `sub`, DID cross-check, signature proof; sound for DID auth |
| 17 | Signature before claims | Cryptographic verification completes before any claim is trusted |
| 18 | Constant-time verification | ed25519-dalek, k256, p256 all provide constant-time guarantees |
| 19 | HTTP identity extraction | Missing Host + token = reject; correct case normalization; matches Go |
| 30 | JWE construction | josekit library; unique salt per key; standard algorithm chain |
| 31 | SystemKeyring base64 | STANDARD encoding correct for OS keyrings; Go-compatible |
| 32 | Key name validation | Path traversal, dot names, empty, null bytes all rejected |
| 49 | Identity before body read | Axum `FromRequestParts` guarantees; prevents pre-auth DoS |
| 56 | Test helpers use real path | Production signing code exercised in integration tests |
| 57 | P2P peer identity binding | PeerId-to-key verification; signature over message content |
| 59 | Claim validation ordering | Signature first prevents timing oracles on claim values |
| 60 | Identity propagation correct | Clone semantics via function parameters; no shared mutable state |
| 63 | Error path handling clean | All failures produce `None` or 403; no partial identity; no secret leakage |

---

## 4. Recommended Fix Order

### Phase 1: Critical Access Control (Immediate)

**1. Finding 37 -- Add auth to debug dump endpoint**
Direct data exfiltration path. Any network-reachable client can dump the entire database. Fix is small: add `ExtractIdentity` parameter and `require_permission()` call. Consider gating behind `dev_mode` like purge.

**2. Finding 45 -- Add deny-by-default auth middleware**
This is the systemic fix that prevents future occurrences of finding 37. Add a global middleware layer that rejects requests without identity unless the route is explicitly marked public. This protects against developer omissions as the HTTP surface grows.

### Phase 2: Key Material Safety (Pre-1.0)

**3. Finding 24 -- Change Keyring::get() to return Zeroizing<Vec<u8>>**
Single trait signature change that protects all private key material across all three backends and all callers. This is the highest-impact key material fix.

**4. Finding 23 -- Wrap load_secret_from_env() in Zeroizing**
Small change that completes the zeroization chain from environment variable to FileKeyring constructor.

**5. Finding 21 -- Increase PBKDF2 iterations**
Requires Go coordination. The JWE format is self-describing (`p2c` in header), so old keys decrypt at 10k and new keys encrypt at 210k+. Backward-compatible upgrade path exists.

### Phase 3: HTTP Hardening (Pre-1.0)

**6. Finding 41 -- Add X-Forwarded-Host support (opt-in)**
Blocking for any production deployment behind a reverse proxy. Must be opt-in (`--trust-proxy-headers`) to prevent header spoofing in direct-access deployments.

**7. Finding 38 -- Generic 403 error messages**
Replace detailed failure messages with generic "authentication failed" response. Log details server-side via `tracing::debug!`. The audience mismatch message is the most concerning as it reveals the expected Host value.

**8. Finding 36 -- Warn on empty Bearer token**
Add `tracing::warn!` when empty bearer is received. Consider whether Go compatibility requires silent anonymous fallback or if a warning is acceptable.

### Phase 4: Test Coverage (Pre-1.0)

**9. Finding 53 -- Add expired token integration test**
Create end-to-end test that sends an expired JWT through the HTTP stack and verifies 403. Also add clock skew boundary test at unit level.

**10. Finding 58 -- Add identity substitution test**
Verify that Alice's token yields Alice's permissions and Bob's token yields Bob's permissions in the same test, confirming no cross-contamination.

### Phase 5: Type Safety Cleanup (Post-1.0 OK)

**11. Finding 00 -- Guard wildcard DID key_portion()**
Change return type to `Option<&str>` or add wildcard assertion. Apply to both `identity::Did` and `zanzibar::Did`.

**12. Finding 02 -- Restrict zanzibar::Did::new_unchecked() to pub(crate)**
One-line visibility change that matches the identity crate's pattern.

**13. Finding 03 -- Document or fix wildcard DID serde asymmetry**
Either accept wildcards in `TryFrom<String>` or add a test documenting the intentional asymmetry.
