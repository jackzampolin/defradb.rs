# Stream 7: Dependency & Unsafe Code — Triage Summary

**Date:** 2026-02-21
**Scope:** FFI boundary safety, storage unsafe code, dependency CVEs, build pipeline, test coverage
**Findings:** 48 individual findings (excluding 5 session summaries and 1 stream summary)

---

## Findings Table

| # | Severity | Title | Status | One-Line Summary |
|---|----------|-------|--------|------------------|
| 00 | CRITICAL | No `catch_unwind` — Panics in FFI Are UB | CONFIRMED | All 84 FFI entry points lack panic guards; any Rust panic unwinds through C/Go frames causing undefined behavior |
| 51 | HIGH | No Negative FFI Boundary Testing | CONFIRMED | No adversarial inputs (NULL pointers, invalid handles, double-free, non-UTF-8) tested against any of the 84 FFI functions |
| 01 | MEDIUM | `from_raw_parts` with Uncapped Length | CONFIRMED | Five call sites create slices from C-provided length with no upper-bound validation, enabling buffer over-read |
| 04 | MEDIUM | Race Between `node_close` and Concurrent Operations | CONFIRMED | TOCTOU window between handle lookup and usage; concurrent close can cause operations against a closed database |
| 12 | MEDIUM | RocksDB OwnedSnapshot Lifetime Transmute | SUSPECTED | `mem::transmute` extends snapshot lifetime to `'static` for self-referential struct; sound but relies on manual reasoning |
| 13 | MEDIUM | FetcherWrapper Fat Pointer Transmute | SUSPECTED | Decomposes `*const dyn Trait` fat pointer via transmute with lifetime erasure; sound today but fragile under refactoring |
| 15 | MEDIUM | Manual `unsafe impl Send/Sync` Inventory | SUSPECTED | Two manual Send/Sync impls (OwnedSnapshot, FetcherWrapper); both sound but require ongoing vigilance during refactors |
| 21 | MEDIUM | ring 0.16.20 AES Panic CVE (RUSTSEC-2025-0009) | CONFIRMED | Transitive via libp2p-quic; AES overflow panic can cause DoS; blocked by libp2p 0.53 pinning |
| 22 | MEDIUM | wasmtime 27.0.0 Multiple CVEs | CONFIRMED | Three CVEs including potential sandbox escape (f64.copysign segfault); 14 major versions behind latest |
| 23 | MEDIUM | lru 0.12.5 Unsound IterMut (RUSTSEC-2026-0002) | CONFIRMED | Stacked Borrows violation in IterMut; no fix available yet; used by blockstore and libp2p-swarm |
| 24 | MEDIUM | serde_cbor 0.11.2 Unmaintained Since 2021 | CONFIRMED | Core serialization crate used in db, p2p, storage; no security patches; ciborium migration needed |
| 25 | MEDIUM | iroh-bitswap Git Dependency with Stale Deps | CONFIRMED | Only non-crates.io dependency; pulls ~50 duplicate crates including old hyper, reqwest, tonic, prost |
| 26 | MEDIUM | libp2p 0.53.2 Version Lag | CONFIRMED | Pinned by iroh-bitswap; carries alpha libp2p-stream, vulnerable ring via QUIC, soundness-buggy lru |
| 29 | MEDIUM | No cargo-deny Configuration | CONFIRMED | No deny.toml; no enforcement of license, advisory, source, or ban policies |
| 40 | MEDIUM | cbindgen Header Not Verified in CI | CONFIRMED | Committed defra.h can drift from Rust source; ABI mismatch causes undefined behavior for Go consumers |
| 41 | MEDIUM | No Integer Overflow Checks in Release | CONFIRMED | Release profile omits `overflow-checks = true`; silent wrapping in CRDT priority, handle counter, batch counts |
| 42 | MEDIUM | CI WASM Build Uses curl-pipe-sh | CONFIRMED | Release workflow pipes remote shell script directly to sh for wasm-pack install; supply chain attack vector |
| 43 | MEDIUM | CI Missing cargo audit / cargo deny Steps | CONFIRMED | No automated dependency vulnerability scanning; 3 CVEs + 1 unsoundness + 7 unmaintained crates undetected |
| 50 | MEDIUM | FFI Test Suite on Feature Branch Only | CONFIRMED | Comprehensive 73-function Go FFI wrapper exists on jack/ffi-rust-compat but not on main; regressions undetectable |
| 52 | MEDIUM | No Handle Lifecycle Stress Testing | CONFIRMED | No stress tests for rapid create/destroy, high handle counts, or concurrent access patterns in registry |
| 56 | MEDIUM | Cross-Stream Integration Gaps | CONFIRMED | Compound vulnerabilities across audit streams (FFI panic + deep query, ACP bypass + P2P, resource exhaustion chains) |
| 02 | LOW | Handle Counter Wraps to Zero on Overflow | CONFIRMED | AtomicUsize wraps after usize::MAX; unreachable on 64-bit but feasible on 32-bit after ~4B operations |
| 03 | LOW | `defra_free_string` Double-Free No Guard | CONFIRMED | Standard FFI contract (caller responsibility); no runtime protection but Go patterns are correct |
| 05 | LOW | FFI Functions Not Consistently Marked `unsafe` | CONFIRMED | 7 of 84 functions use `extern "C"` without `unsafe` despite dereferencing raw pointers internally |
| 17 | LOW | RocksDB Crate Version Audit | INFORMATIONAL | rocksdb 0.22.0 is current; no known CVEs; C++ FFI by nature |
| 27 | LOW | sha2 Duplicate Versions (0.9.9 + 0.10.9) | INFORMATIONAL | Old version via cosmrs/tendermint chain; types incompatible so no accidental mixing |
| 28 | LOW | blst C Library Audit | INFORMATIONAL | Well-maintained, multi-audited BLS12-381 library; low risk |
| 31 | LOW | Build Scripts Audit | GREEN | Three build.rs files all benign (protobuf, git info, cbindgen); no network access or arbitrary file ops |
| 32 | LOW | Duplicate Crate Inventory (~50 duplicates) | INFORMATIONAL | Majority caused by iroh-bitswap; increases binary size and attack surface |
| 33 | LOW | josekit 0.8.7 JWT Library Outdated | INFORMATIONAL | 2 minor versions behind (latest 0.10.3); used for local keyring JWE only; no CVEs |
| 34 | LOW | cosmrs/tendermint Dependency Chain | INFORMATIONAL | Pulls legacy crypto stack (ed25519-consensus, sha2 0.9.9); well-audited, resolves upstream |
| 38 | LOW | No Rust Toolchain Pinning | INFORMATIONAL | No rust-toolchain.toml; MSRV 1.82 documented but CI uses floating stable; build non-determinism |
| 39 | LOW | defra-version build.rs Uses PATH-Relative git | INFORMATIONAL | Standard pattern; git is read-only; output used only for version display |
| 44 | LOW | Docker Base Images Not Digest-Pinned | INFORMATIONAL | Tags are mutable; standard practice but imprecise for reproducibility |
| 54 | LOW | No Memory Leak Detection in CI | CONFIRMED | No ASan/LSan/MSan/Valgrind/Miri; FFI boundary leaks would be silent |
| 06 | GREEN | Null Pointer Check Consistency | VERIFIED | Consistent two-tier pattern (require_c_str / c_str_to_string); all checked before dereference |
| 07 | GREEN | Handle Registry Design Sound, No ABA | VERIFIED | Monotonic handles, RwLock, closure-based API; correct concurrent access |
| 08 | GREEN | CString Ownership Sanitization Sound | VERIFIED | Three-level fallback for null bytes; into_raw/from_raw ownership transfer correct |
| 09 | GREEN | C Header Type Mapping Correct | VERIFIED | Spot-checked defra.h against Rust; all types, structs, and calling conventions match |
| 10 | GREEN | Tokio Runtime Shared Global Correct | VERIFIED | Single OnceLock<Runtime>; no nested block_on; correctly bridges sync FFI to async |
| 14 | GREEN | Iterator Lifetime Safety All Backends | VERIFIED | All iterators materialize data into owned Vecs; no references, no lifetimes, no unsafe |
| 16 | GREEN | Memory Backend Zero Unsafe | VERIFIED | Reference implementation with zero unsafe code; clean baseline |
| 18 | GREEN | No Pin Self-Referential Usage | VERIFIED | All Pin usage is standard async trait return types; no self-referential pinning |
| 19 | GREEN | Complete Non-FFI Unsafe Inventory | VERIFIED | Only 8 unsafe items (4 blocks + 4 impls) across 2 files outside FFI; remarkably clean |
| 30 | GREEN | Crypto Crate Versions All Current | VERIFIED | All direct crypto deps (RustCrypto ecosystem) at safe, current versions; no CVEs |
| 35 | GREEN | Feature Flag Audit | VERIFIED | No unsafe features enabled; proper gating; crypto features correct |
| 36 | INFO | Comprehensive Dependency Inventory | INFORMATIONAL | 898 unique crate versions; Cargo.lock committed; 1 non-crates.io dep |
| 45 | GREEN | tonic Proto Codegen Safe | VERIFIED | Local proto file; deterministic codegen; no network access |
| 46 | GREEN | Release Profile Hardening Strong | VERIFIED | LTO, single codegen unit, strip, panic=abort; excellent FFI hardening |
| 47 | GREEN | env!() Macro Usage Safe | VERIFIED | 9 uses of env!(); all version display; none in security-critical paths |
| 48 | GREEN | .cargo/config.toml Safe | VERIFIED | Minimal config; no custom registries, rustflags, linker overrides, or source replacements |
| 53 | INFO | FFI Test Coverage Metrics | INFORMATIONAL | 96% pass rate (2202/2290) on feature branch; 3 failures in relational mutations |
| 55 | GREEN | Go GC Interaction Properly Handled | VERIFIED | All Go FFI patterns use C.CString (malloc), C.GoString (copy), cgo.Handle; no GC pointer issues |

---

## Themes

### 1. FFI Panic Safety (Findings 00, 04, 51, 52, 56)

The single most dangerous issue in the codebase. All 84 FFI entry points lack `catch_unwind`, meaning any Rust panic is undefined behavior. This interacts with every other panic source: stack overflow from deep queries, unwrap on unexpected state, arithmetic overflow in debug mode. The absence of negative testing means these paths have never been exercised.

### 2. Dependency Freshness and CVEs (Findings 21, 22, 23, 24, 25, 26, 27, 32, 33, 34)

The dependency tree carries 3 known CVEs (ring, wasmtime x2), 1 unsoundness advisory (lru), and 7 unmaintained crates. The root cause for most issues is iroh-bitswap (the sole git dependency), which pins libp2p at 0.53 and drags in ~50 duplicate crate versions. The serde_cbor unmaintained status affects core data paths.

### 3. Build Pipeline and Supply Chain (Findings 29, 38, 40, 42, 43, 44)

No cargo-deny configuration, no cargo-audit in CI, no cbindgen header verification, no toolchain pinning, Docker images not digest-pinned, and wasm-pack installed via curl-pipe-sh. The build pipeline lacks the automated guardrails expected for a production system shipping unsafe FFI code.

### 4. Storage Unsafe Code (Findings 12, 13, 14, 15, 16, 17, 18, 19)

Remarkably clean. Only 8 unsafe items exist outside the FFI crate. The two transmute sites (OwnedSnapshot, FetcherWrapper) are sound but rely on manual safety reasoning rather than compiler enforcement. The materialized iterator design eliminates an entire class of lifetime bugs. The memory backend serves as a safe reference.

### 5. FFI Test Coverage (Findings 50, 51, 52, 53, 54, 55, 56)

The Go FFI wrapper on the feature branch is well-written with 73/84 functions covered and correct memory management patterns. But it is not on main, has no negative testing, no stress testing, and no memory leak detection. The 96% pass rate is encouraging but only measures happy-path functional correctness.

### 6. String and Pointer Safety (Findings 01, 03, 05, 06, 07, 08, 09)

The CString ownership model, null checks, and handle registry design are all sound. The gaps are in buffer length validation (from_raw_parts with uncapped length) and annotation consistency (some functions not marked unsafe). The defensive coding patterns (sanitize_to_cstring, require_c_str) are well-designed.

### 7. Compiler Hardening (Findings 41, 46)

The release profile is strong (LTO, panic=abort, strip) but missing overflow-checks. The panic=abort setting is especially critical for FFI safety, partially mitigating Finding 00 by converting panics to process termination rather than UB-inducing unwinds (though abrupt termination is still undesirable).

---

## Actionable vs Informational

### Must Fix (1.0 Blockers)

| # | Title | Rationale |
|---|-------|-----------|
| 00 | No `catch_unwind` in FFI | Any Rust panic is UB; affects all 84 entry points; single highest-impact fix |
| 22 | wasmtime 27.0.0 Multiple CVEs | Potential sandbox escape via f64.copysign; lens executes WASM modules |

### Should Fix (Pre-1.0)

| # | Title | Rationale |
|---|-------|-----------|
| 51 | No Negative FFI Boundary Testing | Adversarial inputs have never been tested; unknown failure modes at the boundary |
| 01 | `from_raw_parts` Uncapped Length | Buffer over-read from malformed C caller; 5 affected sites; simple fix |
| 04 | Race node_close vs Operations | TOCTOU window; safe due to Arc but panics possible without catch_unwind |
| 29 | No cargo-deny Configuration | Foundation for all dependency policy enforcement; blocks other fixes |
| 43 | CI Missing cargo audit / deny | Known CVEs go undetected without automated scanning |
| 40 | cbindgen Header Not Verified in CI | ABI drift between header and source causes silent undefined behavior |
| 41 | No Overflow Checks in Release | Silent wrapping in CRDT priority and handle counters; ~2-5% performance cost |
| 24 | serde_cbor Unmaintained | Core data path serialization with no security patch pipeline |
| 42 | CI curl-pipe-sh for wasm-pack | Supply chain attack vector in release workflow |
| 50 | FFI Test Suite Not on Main | Regressions in FFI layer undetectable on main branch |
| 25 | iroh-bitswap Git Dependency | Root cause of ~80% of duplicate crates and libp2p version pinning |
| 56 | Cross-Stream Integration Gaps | Compound vulnerabilities that multiply individual finding severity |
| 13 | FetcherWrapper Fat Pointer Transmute | Sound but fragile; adding lifetime parameter eliminates risk |

### Accept Risk / Backlog

| # | Title | Rationale |
|---|-------|-----------|
| 21 | ring 0.16.20 AES CVE | Blocked by libp2p version; QUIC transport may not be reachable at runtime |
| 23 | lru Unsound IterMut | No fix available; audit whether iter_mut is called in practice |
| 26 | libp2p 0.53 Version Lag | Requires iroh-bitswap resolution first |
| 12 | OwnedSnapshot Transmute | Sound; consider self_cell crate for defense-in-depth |
| 15 | Manual Send/Sync Impls | Both sound; document invariants |
| 02 | Handle Counter Wrap | Unreachable on 64-bit; add checked_add if 32-bit targets needed |
| 03 | defra_free_string Double-Free | Standard FFI contract; Go wrapper is correct |
| 05 | Inconsistent unsafe Marking | Annotation consistency; no runtime impact |
| 33 | josekit Outdated | Local keyring only; no CVEs; minor version bump |
| 27 | sha2 Duplicate Versions | Caused by cosmrs; resolves upstream; no interaction |
| 32 | ~50 Duplicate Crates | Resolves with iroh-bitswap fix; binary bloat not security-critical |
| 34 | cosmrs Dependency Chain | Well-audited Tendermint crypto; resolves upstream |
| 38 | No Toolchain Pinning | Build non-determinism; add rust-toolchain.toml |
| 39 | PATH-Relative git in build.rs | Ubiquitous pattern; theoretical risk |
| 44 | Docker Images Not Digest-Pinned | Standard practice; add Dependabot/Renovate |
| 52 | No Handle Lifecycle Stress Tests | Important but not a blocker; add after negative testing |
| 54 | No Memory Leak Detection | Important for long-running nodes; add ASan/LSan to CI |
| 17 | RocksDB Crate Version | Current; no action needed |
| 28 | blst C Library | Well-audited; no action needed |

### No Action (GREEN)

| # | Title | Assessment |
|---|-------|------------|
| 06 | Null Check Consistency | Consistent and correct two-tier pattern across all modules |
| 07 | Handle Registry Design | Sound monotonic ID scheme; no ABA problem; proper locking |
| 08 | CString Ownership Sanitization | Three-level fallback; correct into_raw/from_raw lifecycle |
| 09 | C Header Type Mapping | All type mappings verified correct |
| 10 | Tokio Runtime Shared Global | Single OnceLock; correct sync-to-async bridge |
| 14 | Iterator Lifetime Safety | Materialized iterators eliminate lifetime unsafety entirely |
| 16 | Memory Backend Zero Unsafe | Clean reference implementation |
| 18 | No Pin Self-Referential Usage | No complex pinning patterns; simplifies safety audit |
| 19 | Non-FFI Unsafe Inventory | Only 8 items across 2 files; remarkably clean for a database engine |
| 30 | Crypto Crate Versions | All direct crypto deps current and safe |
| 31 | Build Scripts Audit | All benign; no network access |
| 35 | Feature Flag Audit | No unsafe features; proper gating |
| 36 | Dependency Inventory | Reference document; 898 crate versions tracked |
| 45 | tonic Proto Codegen | Local-only; deterministic; safe |
| 46 | Release Profile Hardening | Strong settings; LTO + panic=abort + strip |
| 47 | env!() Macro Usage | All version display; none security-critical |
| 48 | .cargo/config.toml | Minimal and safe |
| 53 | FFI Test Coverage Metrics | 96% pass rate on feature branch |
| 55 | Go GC Interaction | Correct patterns throughout |

---

## Recommended Fix Order

### Phase 1: FFI Safety Foundation (Week 1)

1. **Finding 00: Add `catch_unwind` to all 84 FFI entry points.** This is the single highest-leverage fix. Create an `ffi_entry!` macro and wrap every function. With `panic = "abort"` in release, panics already terminate the process, but catch_unwind provides graceful error reporting in debug/test builds and future-proofs against profile changes. Fix this first because every other FFI-related fix is less impactful without it.

2. **Finding 01: Cap `from_raw_parts` lengths.** Five call sites, each needs a `MAX_LEN` check before the unsafe slice creation. Takes 30 minutes and eliminates buffer over-read from malformed callers.

3. **Finding 41: Add `overflow-checks = true` to release profile.** One-line change in Cargo.toml. Converts silent integer wrapping into panics (caught by the new catch_unwind from step 1).

### Phase 2: Build Pipeline Hardening (Week 1-2)

4. **Finding 29 + 43: Create deny.toml and add cargo-deny to CI.** This is the foundation for all dependency policy enforcement. Block known CVEs, enforce license compliance, restrict sources. About 1 hour of work that permanently improves the security posture.

5. **Finding 40: Add cbindgen header verification to CI.** A 10-line CI job that catches ABI drift before it reaches production.

6. **Finding 42: Replace curl-pipe-sh with pinned wasm-pack install.** Switch to `cargo install wasm-pack@0.13.1` or a pinned GitHub Action. Quick fix for a supply chain risk.

### Phase 3: Dependency Upgrades (Weeks 2-3)

7. **Finding 22: Upgrade wasmtime 27 to 38+.** Three CVEs including a potential sandbox escape. This is a major version jump requiring API changes in the lens crate, so it needs dedicated effort.

8. **Finding 24: Migrate serde_cbor to ciborium.** Affects db, p2p, and storage crates. Must verify byte-level compatibility with Go for P2P wire format. Plan for incremental migration crate-by-crate.

9. **Finding 25 + 26: Address iroh-bitswap.** Either update the beetle fork to modern dependencies, or replace with a simpler Bitswap client. This unblocks libp2p upgrade and eliminates ~80% of duplicate crates.

### Phase 4: Test Coverage (Weeks 3-4)

10. **Finding 51: Add negative FFI tests.** Start with NULL pointers to the 10 most-called functions, then invalid handles, then use-after-close. ~288 tests total, but the critical subset is ~40.

11. **Finding 50: Merge FFI test infrastructure to main.** The Go FFI wrapper on jack/ffi-rust-compat provides 73-function coverage. Get it running in CI on main.

12. **Finding 52: Add handle lifecycle stress tests.** Rapid create/destroy cycles, concurrent access, subscription cleanup verification.

### Phase 5: Refinement (Ongoing)

13. **Finding 13: Add lifetime parameter to FetcherWrapper.** Eliminates the fragile transmute and lets the borrow checker enforce safety.

14. **Finding 04: Fix race between node_close and concurrent operations.** Reorder subscription cleanup in node_close to happen after registry removal.

15. **Findings 21, 23: Monitor for upstream fixes.** ring CVE resolves with libp2p upgrade; lru unsoundness resolves when patched version ships.

16. **Remaining LOW/INFO findings:** Toolchain pinning, Docker digest pinning, josekit upgrade, memory leak detection tooling. Address as part of ongoing maintenance.

---

## Key Metrics

| Category | Count |
|----------|-------|
| Total findings | 48 |
| CRITICAL | 1 |
| HIGH | 1 |
| MEDIUM | 19 |
| LOW | 15 |
| GREEN | 12 |
| Must Fix (1.0 blockers) | 2 |
| Should Fix (pre-1.0) | 13 |
| Accept Risk / Backlog | 17 |
| No Action (GREEN) | 19 (includes INFO) |
