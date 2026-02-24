# Remediation Roadmap

**Date**: 2026-02-21
**Source**: 7-stream security audit, 303 findings, master triage in `AUDIT-TRIAGE.md`

---

## 1. Overview

### Finding Disposition

| Action | Count | Description |
|--------|-------|-------------|
| **Must Fix** | 37 | 1.0 blockers — crashes, UB, bypass vectors, correctness bugs |
| **Should Fix** | ~60 | Pre-1.0 hardening — real exploit potential, operational risk |
| **Accept Risk** | ~55 | Backlog — design trade-offs, defense-in-depth, Go parity items |
| **Not-Issue** | ~75 | GREEN — verified safe, no changes needed |
| **Informational** | ~20 | INFO — documented trade-offs, architectural observations |
| **Test Gaps** | ~20 | Missing negative/security tests (some overlap with Should Fix) |

### Session Model

Work is done in Claude sessions, not developer-weeks. Session estimates by model tier:

| Tier | Best For | Typical Session | Examples |
|------|----------|-----------------|---------|
| **Haiku** | Mechanical, pattern-apply changes | 1 session = 15-30 targeted edits | Feature flags, config changes, Cargo.toml edits, CI yaml |
| **Sonnet** | Focused implementation with context | 1-2 sessions = one coherent feature/fix | Middleware additions, validation logic, test suites |
| **Opus** | Architectural changes, cross-crate refactors | 2-4 sessions = design + implement + validate | WASM sandbox overhaul, P2P auth chain, SE pipeline |

---

## 2. Phase 1: Critical Attack Vectors (Priority 0)

Close the doors that let any network client crash or exfiltrate from the node.

### Session 1.1: FFI Safety — haiku, 1 session

| # | Finding | Fix |
|---|---------|-----|
| 07-00 | No `catch_unwind` in 84 FFI entry points | Create `ffi_entry!` macro, wrap all entry points |
| 07-01 | `from_raw_parts` with uncapped length | Add `MAX_LEN` checks at 5 call sites |
| 07-41 | No overflow-checks in release | Add `overflow-checks = true` to release profile |

### Session 1.2: HTTP Hardening — sonnet, 1 session

| # | Finding | Fix |
|---|---------|-----|
| 05-01 | No HTTP body size limit | `DefaultBodyLimit::max(256KB)` globally + per-route overrides |
| 05-32 | No HTTP rate limiting or connection limits | Add `TimeoutLayer` + `ConcurrencyLimitLayer` via tower |
| 04-45 | Identity extraction per-handler, not middleware | Add deny-by-default auth middleware |
| 04-37 | Debug dump endpoint unauthenticated | Gate behind dev-mode + NAC |
| 02-01 | Dump bypasses ACP and NAC | Same endpoint fix as 04-37 |

### Session 1.3: P2P Resource Limits — sonnet, 1 session

| # | Finding | Fix |
|---|---------|-----|
| 03-00 | Two-stream no message size limit | `stream.take(MAX_MSG_SIZE)` at 5 `read_to_end` sites |
| 03-44 | Two-stream no timeout (Slowloris) | `tokio::time::timeout(30s)` on each `read_to_end` |
| 03-01/43 | No swarm connection limits | Add 100/400 watermarks + per-peer limit |
| 03-30 | Unbounded task spawning per peer | `Semaphore` (64 permits) on two-stream runner |
| 06-11 | Recursive DAG traversal no depth limit | Add depth counter, convert to iterative |

### Session 1.4: ACP Bypass Sealing — sonnet, 1-2 sessions

| # | Finding | Fix |
|---|---------|-----|
| 02-02 | `_commits` queries bypass ACP entirely (CRITICAL) | Add `caller_identity` to commits query path + ACP check |
| 02-08 | GraphQL endpoint bypasses NAC | Add `require_permission()` to GraphQL handlers |
| 01-10 | SE tag UTF-8 lossy Go divergence (HIGH) | Use raw identity bytes instead of `from_utf8_lossy()` |
| 01-00 | Ed25519 private key not zeroized | Enable ed25519-dalek `"zeroize"` feature flag |

**Phase 1 total: 4-5 sessions**

---

## 3. Phase 2: Authentication & Integrity (Priority 1)

Build the trust boundary between network and core.

### Session 2.1: P2P Authentication Chain — opus, 2-3 sessions

| # | Finding | Fix |
|---|---------|-----|
| 03-12 | Two-stream no signature verification (CRITICAL) | Call `verify_message()` after deserialization |
| 03-20 | AccessMode::Controlled never activated (CRITICAL) | Activate when ACP is configured |
| 03-21 | DocSync/BranchableSync/CAR no access checks | Add `check_access()` to all three handlers |
| 02-18 | P2P merge no signature verification | Integrate sig verification into merge handler |
| 02-19 | P2P creator identity from metadata not signature | Derive creator from verified block signature |
| 02-20 | Block verify disconnected from merge path | Refactor `verify_block_signature()` for reuse |

### Session 2.2: CID Verification — sonnet, 1-2 sessions

| # | Finding | Fix |
|---|---------|-----|
| 06-29 | PushLog no CID verification before storage | Add `verify_block_cid()` before `put()` |
| 06-23 | No CID verification on put() | Add optional verify-on-put to blockstore |
| 06-24 | Unsupported hash algorithm bypass | Reject non-SHA2-256 in `verify_hash()` |
| 06-18 | Block CID not verified before merge | Enable `hash_on_read` for P2P blockstores |
| 03-35 | CAR response blocks stored without origin verification | Verify CID content hashes for CAR-decoded blocks |

### Session 2.3: Recovery Mode Hardening — sonnet, 1 session

| # | Finding | Fix |
|---|---------|-----|
| 02-00 | Recovery mode bypasses ACP on P2P merge | Restrict `BlockMetadata::recovery()` to startup only |
| 02-36 | Recovery bypass on-chain SourceHub permissions | Same fix — recovery mode restricted to startup |

**Phase 2 total: 4-6 sessions**

---

## 4. Phase 3: Query Safety & CRDT Correctness (Priority 2)

Harden the query engine and fix data-layer correctness bugs.

### Session 3.1: GraphQL Parser Hardening — sonnet, 1-2 sessions

| # | Finding | Fix |
|---|---------|-----|
| 05-00 | GraphQL no depth or complexity limits | Add depth/width counters to parser |
| 05-02 | Filter recursion unbounded | Add recursion depth limit to filter evaluation |
| 05-05 | No query timeout or cost budget | Wrap execution in `tokio::time::timeout(30s)` |

### Session 3.2: Lens/WASM Sandboxing — opus, 2-3 sessions

| # | Finding | Fix |
|---|---------|-----|
| 05-15 | Lens WASM path traversal via HTTP API | Reject `file://` paths from HTTP; validate paths |
| 05-31 | WASM sandbox no memory/CPU/syscall restrictions | Configure `StoreLimiter`, fuel metering, epoch deadline |
| 07-22 | wasmtime 27.0.0 multiple CVEs | Upgrade wasmtime to 38+ (API migration required) |

### Session 3.3: CRDT Counter Fixes — sonnet, 1-2 sessions

| # | Finding | Fix |
|---|---------|-----|
| 06-00 | Composite counter nonce ordering unsafe | Swap nonce/value write ordering |
| 06-01 | Composite counter missing allow_decrement | Add allow_decrement check in composite path |
| 06-02 | Composite counter missing Float64 | Add NumericKind dispatch for Float64 |
| 06-56 | Index update failure non-blocking | Make index update failure block the commit |

**Phase 3 total: 4-7 sessions**

---

## 5. Phase 4: Searchable Encryption Pipeline (Priority 3)

SE is 1.0-scope and needs real implementation work, not just fixes.

### Session 4.1: SE Receiver Implementation — opus, 2-3 sessions

| # | Finding | Fix |
|---|---------|-----|
| 06-34 | SE receiver not implemented — artifacts discarded | Build CBOR deserialization, validation, storage integration |
| 06-35 | No SE artifact validation on receive | Build validation framework alongside receiver |
| 06-39 | SE merge handler no artifact generation | Generate SE artifacts for replicated docs in merge path |

### Session 4.2: SE Query Planner Integration — opus, 3-4 sessions

| # | Finding | Fix |
|---|---------|-----|
| 06-37 | SE query evaluation not in Rust planner | Add index selection to planner, evaluation to runner |
| 02-04 | Encrypted search queries bypass ACP | Pass `caller_identity` through SE query path |

### Session 4.3: SE Identity Isolation & Key Zeroization — sonnet, 1 session

| # | Finding | Fix |
|---|---------|-----|
| 06-32 | SE push docs no identity isolation | Thread identity pubkey through coordinator |
| 06-36 | SE enc_key not zeroized Vec<u8> | Use `Zeroizing<Vec<u8>>` for enc_key |
| 01-16 | SE enc_key not zeroized / default zeros | Derive `Zeroize` + `ZeroizeOnDrop` on config |
| 01-19 | SE HMAC key no length validation | Add 32-byte length assertion |

### Session 4.4: SE Go Interop — sonnet, 1 session

| # | Finding | Fix |
|---|---------|-----|
| 01-11 | SE tags no Go test vectors | Generate and hardcode Go-generated vectors |
| 01-15 | SE domain separator delimiter collision | Document as shared Go limitation; no solo fix |

**Phase 4 total: 7-9 sessions**

---

## 6. Phase 5: Secondary Hardening (Priority 4)

Important pre-1.0 work that doesn't block the critical path.

### Session 5.1: ACP Secondary Vectors — sonnet, 2 sessions

| # | Finding | Fix |
|---|---------|-----|
| 02-03 | CID time-travel queries bypass ACP | Route CID queries through ACP filtering |
| 02-09 | NAC enable endpoint no authentication | Require CLI-only or pre-shared secret |
| 02-10 | Policy transition guards dead code | Wire `block_unsafe_policy_transition()` into schema update |
| 02-15 | Zanzibar read check error suppression | Propagate errors instead of treating as denial |

### Session 5.2: SourceHub Hardening — sonnet, 1-2 sessions

| # | Finding | Fix |
|---|---------|-----|
| 02-30 | SourceHub verify_access fails open on ABCI error | Return `Err(...)` for non-zero ABCI codes; use prost |
| 02-32 | SourceHub cache staleness no refresh | Add on-chain fallback for cache misses |
| 02-33 | SourceHub network partition no fail-closed | Add circuit breaker with aggressive timeouts |
| 02-34 | SourceHub bearer token signing config dependency | Handle remote identity unregistration gracefully |

### Session 5.3: P2P Secondary Hardening — sonnet, 1-2 sessions

| # | Finding | Fix |
|---|---------|-----|
| 03-31 | DocSyncRequest.doc_ids unbounded array | Add `MAX_DOC_IDS` constant, reject oversized requests |
| 03-42 | No per-peer rate limiting | Token bucket rate limiter at event dispatch |
| 03-33 | DAG fetcher unbounded task fan-out | `Semaphore` or `JoinSet` cap at 16 |
| 03-32 | Pending DAGs unbounded growth | Add TTL (5 min) + capacity limit (1000) |
| 03-50 | CAR response unbounded DAG collection | Cap at 1000 blocks or 16MB |

### Session 5.4: Key Material Zeroization — haiku, 1 session

| # | Finding | Fix |
|---|---------|-----|
| 01-02 | Ed25519 keygen seed not zeroed | Add `seed.zeroize()` calls |
| 04-24 | Keyring get() returns plain Vec | Change return type to `Zeroizing<Vec<u8>>` |
| 04-23 | Keyring secret from env not zeroized | Wrap `load_secret_from_env()` in `Zeroizing` |

### Session 5.5: Build Pipeline — haiku, 1 session

| # | Finding | Fix |
|---|---------|-----|
| 07-29 | No cargo-deny configuration | Create deny.toml with advisory/license policies |
| 07-43 | CI missing cargo audit/deny steps | Add to CI pipeline |
| 07-40 | cbindgen header not verified in CI | Add CI job to diff generated vs committed header |
| 07-42 | CI WASM build uses curl-pipe-sh | Pin wasm-pack install via cargo or GitHub Action |

**Phase 5 total: 6-8 sessions**

---

## 7. Phase 6: Integration Tests for Security Properties (Priority 5)

Guard against regression of all the fixes above.

### Session 6.1: ACP Negative Tests — sonnet, 2 sessions

| # | Finding | Test |
|---|---------|------|
| 02-22 | No _commits ACP integration test | Verify unauthorized user cannot read commits |
| 02-23 | No dump/backup ACP test | Verify dump requires auth |
| 02-24 | ACP P2P never tests merge denial | Verify unauthorized merge rejected |
| 02-25 | No GraphQL NAC integration test | Verify NAC enforcement on GraphQL |
| 02-26 | Weak mutation denial assertions | Strengthen assertion patterns |
| 02-27 | No unauthorized create test | Verify unauthorized create blocked |
| 02-28 | No policy transition test | Test transition guard activation |

### Session 6.2: Identity Negative Tests — sonnet, 1 session

| # | Finding | Test |
|---|---------|------|
| 04-53 | No expired token integration test | Send expired JWT through HTTP, verify 403 |
| 04-58 | No identity confusion integration test | Verify Alice's token yields Alice's permissions |

### Session 6.3: FFI Negative Tests — sonnet, 1-2 sessions

| # | Finding | Test |
|---|---------|------|
| 07-51 | No negative FFI boundary testing | NULL pointers, invalid handles, non-UTF-8 |
| 07-50 | FFI test suite on feature branch only | Port to main, add to CI |
| 07-52 | No handle lifecycle stress testing | Rapid create/destroy, concurrent access |

### Session 6.4: Go Compat Test Vectors — sonnet, 1 session

| # | Finding | Test |
|---|---------|------|
| 01-12 | JWT no Go compat tests | Parse Go-generated JWT tokens |
| 01-13 | secp256r1 systematic compat gaps | Byte-equality signing, low-S normalization |
| 01-04 | secp256r1 Go signature S-normalization gap | Investigate if secp256r1 signs IPLD blocks |

### Session 6.5: SourceHub Test Coverage — sonnet, 1 session

| # | Finding | Test |
|---|---------|------|
| 02-38 | SourceHub integration test coverage gaps | Add ABCI error, partition, and policy sync tests |

**Phase 6 total: 6-7 sessions**

---

## 8. Post-1.0 Backlog

### 8.1 Dependency Modernization — sonnet, 2-3 sessions

| # | Finding | Work |
|---|---------|------|
| 07-24 | serde_cbor unmaintained since 2021 | Migrate to ciborium crate-by-crate, verify wire compat |
| 07-25 | iroh-bitswap git dependency with stale deps | Update beetle fork or replace with simpler client |
| 07-26 | libp2p 0.53 version lag | Unblocked by iroh-bitswap resolution |
| 07-21 | ring 0.16.20 AES panic CVE | Resolves with libp2p upgrade |
| 07-23 | lru unsound IterMut | Monitor upstream fix; audit iter_mut call sites |

### 8.2 Resource Management — sonnet, 2-3 sessions

| # | Finding | Work |
|---|---------|------|
| 06-12 | merged_composites unbounded growth | Replace HashSet with bounded LRU |
| 06-44 | No transaction timeout or concurrent limit | Add txn cap + periodic cleanup |
| 06-41 | ConflictTracker GC misses long-running txns | Track min active read_version |
| 06-14 | No per-document merge locking | DashMap-based lock manager |
| 05-06 | SSE subscription no limits | Connection count, duration, idle timeout |
| 06-59 | No document size limit | Enforce at merge layer |
| 06-61 | Nonce storage cost / P2P amplification | Implement nonce GC |

### 8.3 Identity Hardening — sonnet, 1-2 sessions

| # | Finding | Work |
|---|---------|------|
| 04-21 | PBKDF2 iteration count weak (10k) | Increase to 210k+; requires Go coordination |
| 01-03 | Key::raw() returns unprotected Vec | Refactor trait to return `Zeroizing<Vec<u8>>` |
| 04-00 | Wildcard DID key_portion() panics | Return `Option<&str>` |
| 04-36 | Empty Bearer token treated as anonymous | Add warning log |
| 04-41 | No X-Forwarded-Host support | Add opt-in `--trust-proxy-headers` |
| 04-38 | 403 error leaks failure reason | Generic error messages |

### 8.4 P2P Advanced — sonnet, 2 sessions

| # | Finding | Work |
|---|---------|------|
| 03-52 | No global memory budget | Integrate jemalloc stats or process memory tracking |
| 03-02 | Kademlia Mode::Server instead of Auto | Make configurable |
| 03-23 | GossipSub checks relay peer not originator | Use `message.source` (requires sig verification first) |
| 03-09 | Identify address flooding | Cap addresses per peer from Identify |
| 03-16 | serde_cbor flatten CBOR divergence | Duplicate MetaData fields on PushLogRequest |
| 03-46 | Unbounded failure_tx channel | Switch to bounded mpsc |

### 8.5 Input Validation Backlog — haiku/sonnet, 1-2 sessions

| # | Finding | Work |
|---|---------|------|
| 05-24 | Identifiers accept unbounded length | Add 256-char max |
| 05-23 | Content-Type not enforced on schema | Add enforcement |
| 05-21 | GraphQL introspection always enabled | Add toggle config |
| 05-19 | Multiaddr SSRF no IP blocklist | Block private IP ranges |
| 05-20 | Error messages echo user input | Remove input from error messages |
| 05-33 | Lens transform output not validated | Validate against destination schema |
| 05-36 | WASM transform blocks tokio thread | Move to `spawn_blocking()` |
| 05-13 | Data directory default permissions | Change to 0700 |

### 8.6 CRDT Edge Cases — accept risk, document

| # | Finding | Status |
|---|---------|--------|
| 06-03/55 | Float64 non-associative divergence | Same as Go; document as known limitation |
| 06-63 | Float equality epsilon comparison in queries | Sub-ULP inconsistency; accepted |
| 06-57 | Schema evolution unknown fields discarded | CRDT blocks preserved in DAG; correct |

### 8.7 Remaining LOW/INFO from All Streams

| Stream | Findings | Notes |
|--------|----------|-------|
| 01 | 01 (ECIES keys), 05 (JWT dispatch), 06 (batch signing), 08 (ECIES validation) | Fix opportunistically when touching related code |
| 02 | 05 (DAC thread-local), 06 (view ACP), 11 (operators), 12 (key injection), 14 (YAML size), 28 (policy transition test) | Defense-in-depth; low urgency |
| 03 | 04 (flood_publish), 07 (version leak), 15 (replay), 17 (GossipSub sig), 19 (unsigned error), 24 (topic leak), 34 (CBOR triple-try), 36 (channel backpressure), 45 (mesh params), 49 (PendingResponses), 54 (unlimited depth) | Accept risk or resolves with higher-priority fixes |
| 04 | 01 (DID validation), 02 (new_unchecked), 03 (wildcard serde), 10 (DER lax), 12 (JWT test gaps), 22 (fsync), 25 (systemd timeout), 26 (secure delete), 27-43 (CLI key exposure), 28 (dir TOCTOU), 29 (file locking), 33 (fsync), 35 (bearer case), 44 (WS no auth), 46 (host match), 48 (export stdout), 50 (multi auth), 51 (key type ambiguity), 54-55 (test coverage), 61 (wrong-key test), 62 (key rotation) | Go-compatible behaviors and defense-in-depth |
| 05 | 04 (fragment width), 09 (FFI backup path), 10 (CLI file size), 12 (symlinks), 18 (unknown directives), 22 (schema migration), 34 (WASM module size), 35 (key separator) | Low impact; some resolved by higher-priority fixes |
| 06 | 04 (property tests), 06 (nonce growth), 13 (parent skip), 15 (decrypt fallback), 16 (dedup guard), 17 (dedup TOCTOU), 27 (backup integrity), 42 (memory committed), 43 (conflict atomicity), 45 (drop callbacks) | Design trade-offs or testing improvements |
| 07 | 02 (handle wrap), 03 (double-free), 04 (race close), 05 (unsafe marking), 12-13 (transmutes), 15 (Send/Sync), 17 (rocksdb), 27 (sha2 dups), 28 (blst), 32 (dup crates), 33 (josekit), 34 (cosmrs), 38 (toolchain pin), 39 (git in build.rs), 44 (docker pin), 54 (leak detection) | Accept risk; fix opportunistically |

---

## 9. Not-Issues / Verified Safe

These GREEN findings were audited and confirmed correct. No changes needed.

### Stream 01 — Cryptographic Inventory (3 GREEN)

| # | Finding | Verified Property |
|---|---------|-------------------|
| 09 | ECIES & AES-GCM Correctness Audit | X25519 ECDH, HKDF-SHA256, AES-256-GCM, HMAC-SHA256 all correct and Go-compatible |
| 17 | SE Deterministic Tags Frequency Analysis | Inherent to deterministic SSE; documented in code |
| 20 | Merkle Proof Verification Sound | Implementation cryptographically correct |

### Stream 02 — Access Control Policy (4 GREEN)

| # | Finding | Verified Property |
|---|---------|-------------------|
| 07 | DAC implementation checklist | Core DAC is sound: fail-closed, correct hierarchy, atomic registration |
| 13 | NAC disabled state behavior | Three-state machine correctly blocks privilege escalation |
| 17 | Policy ID double SHA-256 with counter | Correct for Go compatibility |
| 37 | All Session 1-4 findings apply to SourceHub | Meta-finding; fix vectors once via trait |

### Stream 03 — P2P Network Security (15 GREEN)

| # | Finding | Verified Property |
|---|---------|-------------------|
| 06 | Noise protocol mandatory, no downgrade | Noise XX with Ed25519, no plaintext fallback |
| 08 | TCP port reuse safe with Noise | Noise auth prevents hijacking |
| 10 | GossipSub strict validation + SHA-256 IDs | Strict validation, signed messages, content-addressed dedup |
| 11 | No hardcoded bootstrap peers | All user-configurable |
| 18 | sign_message/verify_message logic sound | 4-point verification, strict error handling |
| 25 | Replicator management admin-only | No P2P self-registration |
| 26 | PushLog access check ordering correct | Check before CID parsing |
| 27 | Collection ID matching exact, no wildcards | HashMap exact-match |
| 28 | Registry operations atomic (RwLock) | No TOCTOU within registry |
| 37 | DAG fetch depth capped at 20 | Iterative, not recursive |
| 38 | CID parsing errors handled gracefully | All sites use `try_from` with error handling |
| 39 | PushLog handler always sends response | All code paths send explicit responses |
| 40 | Bitswap retry logic bounded | Per-block timeouts, 20-iteration cap |
| 48 | PeerStateTracker proper memory bounds | Three-level LRU eviction |
| 53 | Replication loop semaphore concurrency | 32-worker pool; correct pattern |

### Stream 04 — Identity & Key Management (20 GREEN)

| # | Finding | Verified Property |
|---|---------|-------------------|
| 05 | JWT algorithm dispatch verified correct | Only EdDSA/ES256K/ES256 accepted; `alg:none` rejected |
| 06 | RawIdentity DID-key binding sound | DID derived from public key at call time |
| 07 | Wildcard DID cannot be impersonated | Cryptographic key material required for all paths |
| 08 | Key type conversions bijective | Exhaustive match; BLS12-381 rejected |
| 13 | DER conversion roundtrip correct | Leading-zero, high-bit, short-value all handled |
| 14 | Clock skew and time validation correct | 60s tolerance; `saturating_add`; missing audience rejected |
| 15 | Base64 URL_SAFE_NO_PAD consistent | Same variant everywhere |
| 16 | Self-authenticating token design sound | Pub key in `sub`, DID cross-check, signature proof |
| 17 | Signature verified before claims trusted | All three decode functions verify first |
| 18 | Signature verification constant-time | ed25519-dalek, k256, p256 all constant-time |
| 19 | HTTP identity extraction correct | Missing Host + token = reject; case normalization |
| 30 | JWE construction sound | Unique salt per encryption; Go-compatible |
| 31 | SystemKeyring base64 encoding correct | STANDARD encoding for OS keyrings |
| 32 | Key name validation prevents path traversal | Rejects `/`, `\`, `\0`, `.`, `..`, empty |
| 49 | Identity extraction before body read | Axum `FromRequestParts` guarantees |
| 56 | Test helpers use real signing path | Production code exercised in tests |
| 57 | P2P peer identity has cryptographic binding | PeerId-to-key verification correct |
| 59 | JWT claim validation ordered correctly | Signature first prevents timing oracles |
| 60 | Identity propagation through query pipeline | Clone semantics via function parameters |
| 63 | Error path identity handling clean | All failures produce `None` or 403 |

### Stream 05 — Input Validation (8 GREEN)

| # | Finding | Verified Property |
|---|---------|-------------------|
| 11 | HTTP handlers do not accept filesystem paths | No HTTP endpoint takes filesystem path from remote |
| 14 | Dump and purge commands safe | HTTP-only, stdout output |
| 16 | Null byte path handling | Rust `CString` rejects interior null bytes |
| 25 | Error responses JSON Content-Type | All errors return `application/json`; no XSS |
| 26 | Schema not replicated via P2P | No schema message type in P2P protocol |
| 27 | Directive arguments not stored or evaluated | Type-checked, consumed, no eval path |
| 28 | Circular type references properly detected | Tarjan's SCC algorithm correct |
| 30 | Storage key construction injection-proof | Three-layer defense verified |

### Stream 06 — Data Integrity & CRDT (12 GREEN)

| # | Finding | Verified Property |
|---|---------|-------------------|
| 07 | LWW tie-breaking correctness | Commutativity, associativity, idempotency proven |
| 08 | Counter nonce idempotency | Duplicate detection correct; scoping prevents collisions |
| 31 | SE tag computation sound for equality | HMAC-SHA256 with domain separator; isolation verified |
| 48 | Snapshot isolation verified all backends | All 4 backends take snapshot at new_txn time |
| 49 | Index-document atomicity | Mutations and index updates share single transaction |
| 50 | Group commit conflict detection correct | Flush loop serializes check + write; inter-batch conflicts detected |
| 51 | Callback panic safety | `catch_unwind` on sync and async callbacks |
| 52 | Cross-backend consistency | Shared ConflictTracker; identical detection semantics |
| 54 | Counter nonces survive deletion | Resurrection correct; prevents duplicate application |
| 58 | Priority from DAG height not user-controlled | u64::MAX unreachable through normal operation |
| 60 | Partition healing convergence correct | LWW priority + counter nonce idempotency ensures convergence |
| 62 | LWW deletion and resurrection deterministic | Delete at higher priority wins; existence preferred at tie |

### Stream 07 — Dependency & Unsafe Code (19 GREEN)

| # | Finding | Verified Property |
|---|---------|-------------------|
| 06 | Null pointer check consistency | Two-tier pattern across all FFI modules |
| 07 | Handle registry design sound, no ABA | Monotonic handles, RwLock, closure-based API |
| 08 | CString ownership sanitization sound | Three-level fallback; correct ownership transfer |
| 09 | C header type mapping correct | All types and calling conventions match |
| 10 | Tokio runtime shared global correct | Single OnceLock; no nested block_on |
| 14 | Iterator lifetime safety all backends | Materialized iterators; no references, no unsafe |
| 16 | Memory backend zero unsafe | Clean reference implementation |
| 18 | No Pin self-referential usage | Standard async trait return types only |
| 19 | Complete non-FFI unsafe inventory | Only 8 items across 2 files outside FFI |
| 30 | Crypto crate versions all current | RustCrypto ecosystem; no CVEs |
| 31 | Build scripts audit clean | All benign; no network access |
| 35 | Feature flag audit clean | No unsafe features enabled |
| 45 | tonic proto codegen safe | Local proto; deterministic |
| 46 | Release profile hardening strong | LTO, single codegen unit, strip, panic=abort |
| 47 | env!() macro usage safe | Version display only |
| 48 | .cargo/config.toml safe | No custom registries or source replacements |
| 53 | FFI test coverage 96% pass rate | 2202/2290 tests on feature branch |
| 55 | Go GC interaction properly handled | Correct C.CString/C.GoString/cgo.Handle patterns |

---

## 10. Session Totals

| Phase | Description | Model Tier | Sessions | Cumulative |
|-------|-------------|------------|----------|------------|
| **1** | Critical Attack Vectors | haiku + sonnet | 4-5 | 4-5 |
| **2** | Authentication & Integrity | opus + sonnet | 4-6 | 8-11 |
| **3** | Query Safety & CRDT | opus + sonnet | 4-7 | 12-18 |
| **4** | Searchable Encryption | opus + sonnet | 7-9 | 19-27 |
| **5** | Secondary Hardening | haiku + sonnet | 6-8 | 25-35 |
| **6** | Security Integration Tests | sonnet | 6-7 | 31-42 |
| **Post-1.0** | Backlog | mixed | 10-15 | 41-57 |

### Critical Path for 1.0

Phases 1-3 are the hard blockers: **12-18 sessions** to close all critical attack vectors, build the P2P trust boundary, harden the query engine, and fix CRDT correctness.

Phase 4 (SE pipeline) adds **7-9 sessions** if SE is in 1.0 scope.

Phase 5 (secondary hardening) and Phase 6 (security tests) are important but can overlap with other 1.0 work: **12-15 sessions** that can run in parallel with feature development.
