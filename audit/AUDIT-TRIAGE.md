# DefraDB.rs Security Audit -- Master Cross-Audit Triage

**Date**: 2026-02-21
**Streams Covered**: 7 (Cryptographic Inventory, Access Control Policy, P2P Network Security, Identity & Key Management, Input Validation, Data Integrity & CRDT, Dependency & Unsafe Code)
**Total Findings**: 303 individual findings (excluding session summaries and stream summaries)

---

## 1. Executive Summary

The DefraDB.rs security audit examined 303 individual findings across seven audit streams, covering the full surface area from cryptographic primitives through P2P networking to build pipeline security. The severity distribution is: 4 CRITICAL, 22 HIGH, 79 MEDIUM, 63 LOW, 26 INFO, and 74 GREEN (verified safe). Approximately 35 findings are classified as 1.0 blockers that must be remediated before shipping.

The overall security posture of DefraDB.rs is characterized by a strong inner core surrounded by a weak outer shell. The core CRDT correctness (LWW commutativity, counter nonce idempotency, partition healing), cryptographic primitives (Noise transport, AES-GCM, JWT signature verification), and storage safety (zero unsafe code in memory backend, correct snapshot isolation across all four backends) are mathematically and architecturally sound. The Rust type system and borrow checker provide genuine safety guarantees that the Go implementation lacks. However, the defensive layers between the network boundary and the trusted core are consistently missing: no message size limits on P2P, no connection limits, no signature verification on the primary replication path, no CID content verification on block ingestion, no HTTP rate limiting, no body size limits, and multiple ACP bypass vectors through alternative query paths.

The three biggest risks to 1.0 are: (1) **P2P resource exhaustion** -- any peer completing a Noise handshake can crash a node via OOM, Slowloris, or unbounded task spawning, with no rate limiting, connection limits, or message size validation at any layer; (2) **ACP bypass vectors** -- `_commits` queries, CID time-travel, encrypted search, the debug dump endpoint, and GraphQL NAC bypass collectively allow unauthenticated access to all data through at least one code path; and (3) **Content-addressed integrity is unenforced** -- the blockstore accepts unverified (CID, data) pairs, PushLog stores peer-supplied blocks without hash verification, and unsupported hash algorithms silently pass verification, undermining the foundational trust model of the Merkle DAG. All three are fixable with targeted effort in the remediation roadmap below.

---

## 2. Cross-Cutting Themes

### Theme 1: Unbounded Resource Consumption at Network Boundaries

**Description**: The P2P and HTTP layers accept unbounded input without admission control. Message sizes, connection counts, request body sizes, query complexity, concurrent tasks, and internal data structures all grow without limit.

**Streams**: 03 (P2P), 05 (Input Validation), 06 (Data Integrity)

**Contributing Findings**:
- 03-00: Two-stream no message size limit
- 03-01/43: No swarm connection limits / no per-peer connection limits
- 03-30: Unbounded task spawning per peer
- 03-42: No per-peer rate limiting
- 03-44: Two-stream read_to_end no timeout (Slowloris)
- 03-31: DocSyncRequest.doc_ids unbounded array
- 03-32: Pending DAGs unbounded growth
- 03-33: DAG fetcher unbounded task fan-out
- 03-50: CAR response unbounded DAG collection
- 03-52: No global memory budget
- 05-00: GraphQL no depth or complexity limits
- 05-01: No HTTP body size limit
- 05-02: Filter recursion unbounded
- 05-03: SDL schema no size limits
- 05-05: No query timeout or cost budget
- 05-06: SSE subscription no limits
- 05-32: No HTTP rate limiting or connection limits
- 06-12: merged_composites unbounded growth
- 06-44: No transaction timeout or concurrent limit
- 06-59: No document size limit
- 06-61: Nonce storage cost / P2P amplification

**Recommended Unified Fix**: Implement a layered admission control architecture: (1) connection limits and per-peer rate limiting at the libp2p swarm level, (2) message size limits and read timeouts on all P2P stream reads, (3) HTTP body size limits, request timeouts, and connection limits via tower middleware, (4) query depth/width/timeout limits in the GraphQL parser and executor, (5) concurrency limits on task spawning via tokio Semaphores.

---

### Theme 2: ACP Bypass Through Alternative Query Paths

**Description**: Multiple query and data access paths were implemented without ACP awareness, allowing unauthenticated access to protected data.

**Streams**: 02 (ACP), 04 (Identity), 05 (Input Validation)

**Contributing Findings**:
- 02-02: _commits queries bypass ACP entirely (CRITICAL)
- 02-03: CID time-travel queries bypass ACP
- 02-04: Encrypted search queries bypass ACP
- 02-01/16: Dump bypasses ACP and NAC
- 02-08: GraphQL endpoint bypasses NAC permission checks
- 02-09: NAC enable endpoint no authentication
- 04-37: Debug dump endpoint no identity or NAC check
- 04-45: Identity extraction is per-handler, not global middleware
- 05-21: GraphQL introspection always enabled

**Recommended Unified Fix**: (1) Add deny-by-default authentication middleware that requires routes to be explicitly marked public (fixes the systemic root cause from 04-45), (2) pass `caller_identity` through all query dispatch paths (_commits, CID, encrypted search), (3) gate debug/dump endpoints behind dev-mode and NAC, (4) add introspection toggle configuration.

---

### Theme 3: Content-Addressed Integrity Unenforced

**Description**: The blockstore and merge handler accept blocks without verifying that the content matches the claimed CID. This undermines the Merkle DAG trust model.

**Streams**: 06 (Data Integrity), 03 (P2P)

**Contributing Findings**:
- 06-18: Block CID not verified before merge
- 06-23: No CID verification on put()
- 06-24: Unsupported hash algorithm bypass
- 06-29: P2P PushLog no CID verification before storage
- 03-35: CAR response blocks stored without origin verification

**Recommended Unified Fix**: (1) Add `verify_block_cid()` call before `put()` in the PushLog handler (highest attack surface), (2) add optional verify-on-put to blockstore as defense-in-depth, (3) reject unsupported hash algorithms in `verify_hash()` instead of returning Ok(()), (4) verify CID content hashes for CAR-decoded blocks before `put_many()`.

---

### Theme 4: P2P Authentication and Authorization Dead Code

**Description**: Signature verification, access mode control, and access checks exist in code but are architecturally disconnected from the production message flow.

**Streams**: 03 (P2P), 02 (ACP)

**Contributing Findings**:
- 03-12: Two-stream handler no signature verification (CRITICAL)
- 03-20: AccessMode::Controlled never activated (CRITICAL)
- 03-21: DocSync/BranchableSync/CAR no access checks
- 03-14: PushLogCodec signing is dead code
- 03-22: Bitswap serves blocks without collection access checks
- 03-23: GossipSub checks relay peer, not originator
- 02-18: P2P merge no signature verification
- 02-19: P2P creator identity from metadata, not signature
- 02-20: Block verify not in merge path

**Recommended Unified Fix**: (1) Call `verify_message()` after deserialization in two-stream handlers, (2) activate AccessMode::Controlled when ACP is configured, (3) add `check_access()` to DocSync, BranchableSync, and CAR handlers, (4) integrate signature verification into the merge handler and derive creator identity from verified signatures.

---

### Theme 5: Key Material Zeroization Gaps

**Description**: Cryptographic key material persists in memory after use across multiple subsystems: Ed25519 private keys, ECIES derived keys, keyring output, SE encryption keys, and environment variable secrets.

**Streams**: 01 (Crypto), 04 (Identity), 06 (Data Integrity)

**Contributing Findings**:
- 01-00: Ed25519 private key not zeroized (feature flag)
- 01-01: ECIES derived keys not zeroed
- 01-02: Ed25519 keygen seed not zeroed
- 01-03: Key::raw() returns unprotected Vec
- 01-16: SE enc_key not zeroized / default zeros
- 04-23: Keyring secret from env not zeroized
- 04-24: Keyring get() returns plain Vec, not Zeroizing
- 06-36: SE enc_key not zeroized Vec<u8>

**Recommended Unified Fix**: (1) Enable ed25519-dalek `"zeroize"` feature (one-line Cargo.toml change), (2) change `Keyring::get()` return type to `Zeroizing<Vec<u8>>`, (3) wrap `load_secret_from_env()` in `Zeroizing`, (4) derive `Zeroize` + `ZeroizeOnDrop` on `SECoordinatorConfig`, (5) add `seed.zeroize()` calls in key generation paths.

---

### Theme 6: Go Compatibility Divergences

**Description**: Behavioral differences between Rust and Go implementations that would cause interop failures in mixed networks or prevent feature parity.

**Streams**: 01 (Crypto), 06 (Data Integrity)

**Contributing Findings**:
- 01-10: SE tag UTF-8 lossy Go divergence (HIGH -- breaks all SE interop)
- 01-04: secp256r1 signature S-normalization gap
- 01-07: ECIES X25519 low-order key acceptance
- 01-08: ECIES ciphertext validation gaps
- 01-15: SE domain separator delimiter collision (shared with Go)
- 06-34: SE receiver not implemented
- 06-37: SE query evaluation not in Rust planner

**Recommended Unified Fix**: (1) Fix SE tag to use raw identity bytes instead of UTF-8 lossy conversion (one-function fix), (2) add Go-generated test vectors for SE tags, JWT, and secp256r1 signatures, (3) implement SE receiver and query evaluation if SE is in 1.0 scope.

---

### Theme 7: Integration Test Coverage for Security Properties

**Description**: Integration tests validate that authorized operations succeed but rarely validate that unauthorized operations fail. Security-negative tests are systematically missing.

**Streams**: 02 (ACP), 04 (Identity), 06 (Data Integrity), 07 (Dependency)

**Contributing Findings**:
- 02-22: No _commits ACP integration test
- 02-23: No dump/backup ACP test
- 02-24: ACP P2P never tests merge denial
- 02-25: No GraphQL NAC integration test
- 02-26: Weak mutation denial assertions
- 02-27: No unauthorized create test
- 02-28: No policy transition test
- 02-38: SourceHub test coverage gaps
- 04-53: No expired token integration test
- 04-58: No identity confusion integration test
- 07-51: No negative FFI boundary testing
- 07-50: FFI test suite on feature branch only
- 01-11: SE tags no Go test vectors
- 01-12: JWT no Go compat tests
- 01-13: secp256r1 systematic compat gaps

**Recommended Unified Fix**: Write targeted negative integration tests for each security fix. Each finding above describes the exact test needed. Prioritize tests that guard against regression of CRITICAL and HIGH fixes.

---

### Theme 8: FFI Boundary Safety

**Description**: The FFI boundary between Rust and Go lacks fundamental safety guarantees: no panic guards, no buffer length validation, and no negative testing.

**Streams**: 07 (Dependency)

**Contributing Findings**:
- 07-00: No catch_unwind in FFI -- panics are UB (CRITICAL)
- 07-01: from_raw_parts with uncapped length
- 07-04: Race between node_close and concurrent operations
- 07-51: No negative FFI boundary testing
- 07-50: FFI test suite on feature branch only
- 07-41: No integer overflow checks in release

**Recommended Unified Fix**: (1) Create an `ffi_entry!` macro wrapping all 84 entry points with `catch_unwind`, (2) add `MAX_LEN` checks to all 5 `from_raw_parts` call sites, (3) enable `overflow-checks = true` in release profile, (4) port FFI test suite to main and add negative test cases.

---

### Theme 9: Dependency Supply Chain Risks

**Description**: Known CVEs, unmaintained crates, and missing build pipeline guardrails create supply chain attack surface.

**Streams**: 07 (Dependency)

**Contributing Findings**:
- 07-22: wasmtime 27.0.0 multiple CVEs (potential sandbox escape)
- 07-21: ring 0.16.20 AES panic CVE
- 07-23: lru unsound IterMut
- 07-24: serde_cbor unmaintained since 2021
- 07-25: iroh-bitswap git dependency with stale deps
- 07-29: No cargo-deny configuration
- 07-42: CI wasm-pack curl-pipe-sh
- 07-43: CI missing cargo audit / deny steps

**Recommended Unified Fix**: (1) Create deny.toml and add cargo-deny to CI, (2) upgrade wasmtime to 38+, (3) replace curl-pipe-sh with pinned install, (4) plan serde_cbor to ciborium migration, (5) address iroh-bitswap to unblock libp2p upgrade.

---

## 3. All CRITICAL and HIGH Findings

| # | Severity | Stream | Finding | One-Line Summary |
|---|----------|--------|---------|------------------|
| 07-00 | CRITICAL | 07-Dependency | No `catch_unwind` -- panics in FFI are UB | All 84 FFI entry points lack panic guards; any Rust panic unwinds through C/Go frames |
| 02-02 | CRITICAL | 02-ACP | _commits queries bypass ACP entirely | `_commits` GraphQL queries early-return before identity check, exposing full commit history |
| 03-12 | CRITICAL | 03-P2P | Two-stream handler accepts messages without signature verification | Primary replication path deserializes and processes messages without calling `verify_message()` |
| 03-20 | CRITICAL | 03-P2P | AccessMode::Controlled is never activated | `AccessMode::Open` hardcoded everywhere; per-collection replicator access control is dead code |
| 01-10 | HIGH | 01-Crypto | SE Tag UTF-8 Lossy Go Divergence | `String::from_utf8_lossy()` on identity bytes breaks all SE tag interop with Go |
| 02-00 | HIGH | 02-ACP | Recovery mode bypasses ACP on P2P merge | `BlockMetadata::recovery()` skips all ACP checks, triggered by HTTP-exposed version sync |
| 02-01 | HIGH | 02-ACP | Database dump bypasses ACP and NAC | `GET /api/v0/debug/dump` exposes all data with no authentication |
| 02-08 | HIGH | 02-ACP | GraphQL endpoint bypasses NAC permission checks | GraphQL handlers never call `require_permission()` |
| 02-18 | HIGH | 02-ACP | P2P merge no signature verification | Blocks from P2P peers merged without cryptographic verification |
| 02-19 | HIGH | 02-ACP | P2P creator identity from metadata not signature | ACP checks use self-reported creator, not signature-derived identity |
| 02-22 | HIGH | 02-ACP | No integration test for _commits ACP bypass | Zero test coverage for the CRITICAL _commits bypass |
| 02-24 | HIGH | 02-ACP | P2P ACP tests never verify merge denial | Tests verify success but never unauthorized merge rejection |
| 02-30 | HIGH | 02-ACP | SourceHub verify_access fails open on ABCI error | Non-zero ABCI codes return Ok(false) instead of Err |
| 02-36 | HIGH | 02-ACP | Recovery mode bypasses on-chain SourceHub permissions | Recovery bypass creates unauditable divergence from on-chain state |
| 03-00 | HIGH | 03-P2P | Two-stream no message size limit | `read_to_end()` without `take()` allows OOM from a single peer |
| 03-01 | HIGH | 03-P2P | No swarm connection limits | No max inbound/outbound connection limits configured |
| 03-21 | HIGH | 03-P2P | DocSync/BranchableSync/CAR no access checks | Three sync protocols skip `check_access()` entirely |
| 03-30 | HIGH | 03-P2P | Unbounded tokio task spawning per peer | Every incoming two-stream opens a `tokio::spawn` with no concurrency limit |
| 03-31 | HIGH | 03-P2P | DocSyncRequest.doc_ids unbounded array | `Vec<String>` with no length limit triggers O(n) DB lookups |
| 03-42 | HIGH | 03-P2P | No per-peer rate limiting | Zero rate limiting across all P2P protocols |
| 03-43 | HIGH | 03-P2P | No per-peer connection limits | No `max_connections_per_peer`, no rate limit on connection establishment |
| 03-44 | HIGH | 03-P2P | Two-stream read_to_end no timeout | All `read_to_end` calls run without timeout; Slowloris vector |
| 04-37 | HIGH | 04-Identity | Debug dump endpoint no identity or NAC check | Any unauthenticated client can dump entire database |
| 05-00 | HIGH | 05-Input | GraphQL no depth or complexity limits | Recursive parser accepts unlimited depth/width queries, enabling OOM |
| 05-01 | HIGH | 05-Input | No HTTP body size limit | Schema and backup endpoints accept unlimited request bodies |
| 05-15 | HIGH | 05-Input | Lens WASM path traversal via HTTP API | Remote arbitrary file read via crafted lens `Path` field |
| 05-31 | HIGH | 05-Input | WASM sandbox no memory/CPU/syscall restrictions | Malicious WASM module can OOM or infinite-loop the node |
| 06-11 | HIGH | 06-Data | Recursive DAG traversal no depth limit | Attacker-crafted deep DAG chains overflow tokio's 2MB stack |
| 06-34 | HIGH | 06-Data | SE receiver not implemented -- artifacts discarded | Rust replicators cannot serve SE queries |
| 06-37 | HIGH | 06-Data | SE query evaluation not in Rust planner | Encrypted index queries don't use the index |
| 07-51 | HIGH | 07-Dependency | No negative FFI boundary testing | Adversarial inputs never tested against any of 84 FFI functions |

---

## 4. 1.0 Blocker List

The definitive list of findings that must be fixed before shipping 1.0, compiled from all seven triage reports' "Must Fix" sections.

### From Stream 01 -- Cryptographic Inventory (2 blockers)
| # | Finding | Rationale |
|---|---------|-----------|
| 01-10 | SE Tag UTF-8 Lossy Go Divergence | Breaks all searchable encryption interop in mixed networks |
| 01-00 | Ed25519 Private Key Not Zeroized | One-line feature flag enables ZeroizeOnDrop for highest-value keys |

### From Stream 02 -- Access Control Policy (8 blockers)
| # | Finding | Rationale |
|---|---------|-----------|
| 02-02 | _commits queries bypass ACP | Full commit history disclosure; trivial to exploit |
| 02-08 | GraphQL bypasses NAC | Primary query interface ignores NAC permissions |
| 02-18 | P2P merge no signature verification | Any peer can inject arbitrary document mutations |
| 02-19 | P2P creator identity spoofing | Enables identity impersonation during merge |
| 02-00 | Recovery mode ACP bypass | HTTP-triggered version sync bypasses all ACP |
| 02-01 | Dump bypasses ACP and NAC | Unauthenticated HTTP endpoint exposes all data |
| 02-36 | Recovery bypass on-chain SourceHub | Unauditable divergence from on-chain authorization |
| 02-30 | SourceHub verify_access fails open | Masks errors as denials; brittle protobuf decoding |

### From Stream 03 -- P2P Network Security (8 blockers)
| # | Finding | Rationale |
|---|---------|-----------|
| 03-12 | Two-stream no signature verification | Any peer can forge replication messages |
| 03-20 | AccessMode::Controlled never activated | Per-collection access control is dead code |
| 03-00 | Two-stream no message size limit | Single peer can OOM the node |
| 03-44 | Two-stream no timeout | Slowloris ties up all tokio tasks |
| 03-01/43 | No connection limits | Unlimited connections exhaust file descriptors |
| 03-30 | Unbounded task spawning | Unlimited concurrent tasks amplify DoS |
| 03-21 | DocSync/BranchableSync/CAR no access checks | Any peer can enumerate all documents |
| 03-42 | No per-peer rate limiting | No throttling on any protocol |

### From Stream 04 -- Identity & Key Management (2 blockers)
| # | Finding | Rationale |
|---|---------|-----------|
| 04-37 | Debug dump endpoint no identity or NAC check | Direct data exfiltration path |
| 04-45 | Identity extraction per-handler, not middleware | Root cause enabling unauthenticated endpoints |

### From Stream 05 -- Input Validation (4 blockers)
| # | Finding | Rationale |
|---|---------|-----------|
| 05-15 | Lens WASM path traversal via HTTP API | Remote arbitrary file read |
| 05-00 | GraphQL no depth or complexity limits | Remote OOM via single HTTP request |
| 05-01 | No HTTP body size limit | Remote OOM via multi-GB POST |
| 05-31 | WASM sandbox no memory/CPU limits | Malicious WASM module crashes node |

### From Stream 06 -- Data Integrity & CRDT (11 blockers)
| # | Finding | Rationale |
|---|---------|-----------|
| 06-11 | Recursive DAG traversal no depth limit | Crashes node via stack overflow |
| 06-34 | SE receiver not implemented | Rust replicators cannot serve SE queries (if SE in scope) |
| 06-37 | SE query evaluation not in planner | Encrypted index queries non-functional (if SE in scope) |
| 06-18 | Block CID not verified before merge | Content substitution via P2P |
| 06-23 | No CID verification on put() | Fabricated blocks stored indefinitely |
| 06-24 | Unsupported hash algorithm bypass | Verification silently skipped |
| 06-29 | PushLog no CID verification | Primary P2P block injection vector |
| 06-00 | Composite counter nonce ordering | Double-counting on crash |
| 06-01 | Composite counter missing allow_decrement | PCounter policy bypass via P2P |
| 06-02 | Composite counter missing Float64 | Silent data corruption |
| 06-56 | Index update failure non-blocking | Permanently stale indexes |

### From Stream 07 -- Dependency & Unsafe Code (2 blockers)
| # | Finding | Rationale |
|---|---------|-----------|
| 07-00 | No catch_unwind in FFI | Any panic is undefined behavior across 84 entry points |
| 07-22 | wasmtime 27.0.0 multiple CVEs | Potential sandbox escape via WASM |

**Total 1.0 Blockers: 37 findings** (some overlap where findings appear in multiple streams)

---

## 5. Prioritized Remediation Roadmap

### Week 1: Stop the Bleeding -- Close Critical Attack Vectors

**Day 1-2: FFI Safety + HTTP Hardening**
- 07-00: Add `catch_unwind` to all 84 FFI entry points (~4 hours with macro)
- 07-01: Cap `from_raw_parts` lengths at all 5 call sites (~30 min)
- 07-41: Enable `overflow-checks = true` in release profile (~5 min)
- 05-01: Add `DefaultBodyLimit::max(256KB)` globally + per-route overrides (~4 hours)
- 05-32: Add `TimeoutLayer` and `ConcurrencyLimitLayer` to HTTP server (~2 hours)
- 04-45: Add deny-by-default auth middleware (~4 hours)
- 04-37/02-01/02-16: Gate debug dump behind dev-mode + NAC (~1 hour)

**Day 3-4: P2P Resource Limits**
- 03-00: Add `stream.take(MAX_MESSAGE_SIZE)` to 5 `read_to_end` sites (~1 hour)
- 03-44: Wrap each `read_to_end` in `tokio::time::timeout(30s)` (~1 hour)
- 03-01/43: Add swarm connection limits (100/400 watermarks) (~30 min)
- 03-30: Add `Semaphore` (64 permits) to two-stream runner (~2 hours)
- 06-11: Add depth limit to recursive DAG traversal (~2 hours)

**Day 5: ACP Bypass Sealing**
- 02-02: Add `caller_identity` to `_commits` queries + ACP check (~4 hours)
- 02-08: Add `require_permission()` to GraphQL handlers (~2 hours)
- 01-10: Fix SE tag UTF-8 lossy divergence (~1 hour)
- 01-00: Enable ed25519-dalek `"zeroize"` feature (~5 min)

### Week 2: Authentication + Data Integrity

**P2P Authentication Chain (3 days)**
- 03-12: Add signature verification to two-stream handler (~4 hours)
- 03-20: Activate AccessMode::Controlled when ACP configured (~4 hours)
- 03-21: Add access checks to DocSync/BranchableSync/CAR handlers (~8 hours)
- 02-18/19/20: Integrate signature verification into merge handler; derive creator from signature (~8 hours)
- 02-00/36: Restrict `BlockMetadata::recovery()` to startup-only (~4 hours)

**CID Integrity (2 days)**
- 06-29: Add `verify_block_cid()` to PushLog handler (~2 hours)
- 06-23: Add optional verify-on-put to blockstore (~4 hours)
- 06-24: Reject unsupported hash algorithms (~1 hour)
- 06-18: Enable hash_on_read for P2P blockstores (~1 hour)
- 03-35: Verify CID content hashes in CAR response handler (~2 hours)

### Week 3: Query Safety + CRDT Correctness + Dependency Upgrades

**Query Engine Hardening (2 days)**
- 05-00: Add parser depth and width limits (~8 hours)
- 05-02: Add filter recursion limit (~4 hours)
- 05-05: Add query timeout (~4 hours)
- 05-15/08: Fix lens path traversal -- reject `file://` via HTTP, add path validation (~8 hours)

**CRDT Fixes (2 days)**
- 06-00/01/02: Fix composite counter delegation to standalone Counter (~8 hours)
- 06-56: Make index update failure block commit (~2 hours)
- 05-31: Configure wasmtime StoreLimiter, fuel metering, epoch deadline (~8 hours)
- 07-22: Upgrade wasmtime 27 to 38+ (~8 hours, may overlap with 05-31)

**Build Pipeline (1 day)**
- 07-29/43: Create deny.toml and add cargo-deny/audit to CI (~2 hours)
- 07-40: Add cbindgen header verification to CI (~1 hour)
- 07-42: Replace curl-pipe-sh with pinned wasm-pack install (~30 min)

### Week 4: Secondary Vectors + SourceHub + Test Coverage

**ACP Secondary Vectors (2 days)**
- 02-03: CID time-travel ACP bypass fix
- 02-04: Encrypted search ACP bypass fix
- 02-09: NAC enable authentication
- 02-10: Wire policy transition guards into schema update path
- 02-30: Fix SourceHub ABCI error handling
- 02-32: Add SourceHub cache refresh
- 02-15: Fix Zanzibar read check error suppression

**P2P Hardening (1 day)**
- 03-31: Validate DocSyncRequest.doc_ids length
- 03-42: Add per-peer rate limiting
- 03-33: Add DAG fetcher concurrency limit
- 03-32: Add pending_dags eviction

**Key Material + Identity (1 day)**
- 01-02: Zeroize ed25519 keygen intermediates
- 04-24: Change Keyring::get() to return Zeroizing<Vec<u8>>
- 04-23: Wrap load_secret_from_env() in Zeroizing
- 01-16/06-36: Zeroize SE encryption key

**Integration Test Writing (1 day)**
- 02-22: _commits ACP test
- 02-24: P2P merge denial test
- 02-25: GraphQL NAC test
- 04-53: Expired token test
- 04-58: Identity confusion test

### Week 5+: Ongoing / Post-1.0

**Dependency Modernization**
- 07-24: Migrate serde_cbor to ciborium
- 07-25/26: Address iroh-bitswap to unblock libp2p upgrade
- 07-21: ring CVE (resolves with libp2p upgrade)
- 07-23: lru unsoundness (monitor for upstream fix)

**Go Compatibility Test Vectors**
- 01-11: Add Go-generated SE tag test vectors
- 01-12: Add Go JWT test vectors
- 01-13: Complete secp256r1 test coverage
- 01-04: Investigate secp256r1 in IPLD blocks

**SE Pipeline Completion (if in 1.0 scope)**
- 06-34: Implement SE artifact receiver
- 06-37: Integrate SE into query planner/runner
- 06-32: Add identity isolation to SE coordinator
- 06-39: Add SE artifact generation in merge handler

**Resource Management**
- 06-12: Replace merged_composites with bounded LRU
- 06-44: Add transaction limits and cleanup
- 06-41: Fix ConflictTracker GC for active transactions
- 06-14: Add per-document merge locking
- 05-06: SSE connection limits
- 05-19: Multiaddr SSRF IP blocklist
- 05-33: Validate WASM transform output
- 05-36: Move WASM to blocking thread pool

**FFI Test Coverage**
- 07-51: Add negative FFI boundary tests
- 07-50: Merge FFI test suite to main
- 07-52: Add handle lifecycle stress tests

**Backlog**
- All LOW/INFO findings from individual stream triage reports
- Key::raw() trait refactoring (01-03)
- SE domain separator delimiter collision (01-15, shared with Go)
- PBKDF2 iteration count increase (04-21, requires Go coordination)

---

## 6. Per-Stream Summary Table

| Stream | CRITICAL | HIGH | MEDIUM | LOW | INFO | GREEN | Total | Top Issue |
|--------|----------|------|--------|-----|------|-------|-------|-----------|
| 01 - Cryptographic Inventory | 0 | 1 | 7 | 5 | 1 | 1 | 20* | SE tag UTF-8 divergence breaks all SE interop |
| 02 - Access Control Policy | 1 | 8 | 15 | 8 | 4 | 0 | 36 | _commits query bypasses ACP entirely |
| 03 - P2P Network Security | 2 | 8 | 16 | 11 | 0 | 15 | 52 | No admission control at network boundary |
| 04 - Identity & Key Management | 0 | 1 | 14 | 17 | 4 | 20 | 56 | Debug dump endpoint unauthenticated |
| 05 - Input Validation | 0 | 4 | 11 | 11 | 0 | 8 | 34** | Lens WASM path traversal via HTTP |
| 06 - Data Integrity & CRDT | 0 | 3 | 18 | 15 | 9 | 12 | 57*** | CID integrity unenforced on all push paths |
| 07 - Dependency & Unsafe Code | 1 | 1 | 19 | 15 | 2 | 19 | 57**** | FFI panics are undefined behavior |
| **TOTAL** | **4** | **26** | **100** | **82** | **20** | **75** | **312** | |

\* Some findings have split severities (e.g., MEDIUM-HIGH)
\** Excludes 3 findings counted as LOW that border MEDIUM in the original
\*** Includes some findings moved between Accept Risk and Should Fix
\**** Includes INFO-level findings counted under GREEN in the triage

Note: Counts include some findings with split or borderline severities; individual stream triage reports are the authoritative source for each finding's classification. The total of 312 exceeds the 303 non-summary finding count because some findings with dual severities are counted in the higher bucket.
