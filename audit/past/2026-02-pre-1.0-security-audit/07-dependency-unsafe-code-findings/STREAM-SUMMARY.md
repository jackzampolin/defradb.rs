# Stream 07: Dependency & Unsafe Code Audit — Complete Summary

**Stream:** 07 — Dependency & Unsafe Code
**Sessions:** 5 of 5 (complete)
**Date:** 2026-02-21
**Auditor:** Claude Opus 4.6
**Total Findings:** 58 (numbered 00-57, excluding 5 session summaries)

## Executive Summary

The dependency and unsafe code audit examined the FFI boundary (84 entry points), all non-FFI unsafe code, the full dependency tree (~898 crate versions), the build and CI pipeline, and FFI test coverage. The codebase demonstrates strong architectural discipline in its unsafe code usage — only 8 unsafe items exist outside the FFI crate — but has a **critical gap in panic safety** (no `catch_unwind` in any FFI entry point) and **significant dependency hygiene debt** driven primarily by the iroh-bitswap git dependency.

The most important finding is that **any Rust panic crossing the FFI boundary is undefined behavior**, and there are multiple reachable panic sources in downstream crates. This is the single highest-priority fix for the entire FFI layer. The dependency audit identified 5 known CVEs across ring, wasmtime, and lru, with wasmtime being 14 major versions behind latest. The build pipeline lacks cargo-audit in CI, has no rust-toolchain pinning, and the C header has no staleness verification.

FFI test coverage is comprehensive on the `jack/ffi-rust-compat` feature branch (~73/84 functions covered), but this coverage does not exist on `main`. No negative testing (adversarial inputs, double-free, NULL pointers) exists at the FFI boundary.

## All Findings

| # | Title | Severity | Session | Status |
|---|-------|----------|---------|--------|
| 00 | No `catch_unwind` — panics in 84 FFI entry points are UB | **CRITICAL** | S1 | Open |
| 01 | `from_raw_parts` raw slice with no length cap | MEDIUM | S1 | Open |
| 02 | Handle counter wrapping — no overflow protection | LOW | S1 | Open |
| 03 | `defra_free_string` double-free — no guard | LOW | S1 | Open |
| 04 | Race between `node_close` and concurrent operations | MEDIUM | S1 | Open |
| 05 | `new_node` not marked `unsafe` | LOW | S1 | Open |
| 06 | Null pointer check consistency verified | GREEN | S1 | Verified |
| 07 | Handle registry design sound — no ABA problem | GREEN | S1 | Verified |
| 08 | CString ownership and sanitization sound | GREEN | S1 | Verified |
| 09 | C header type mapping correct | GREEN | S1 | Verified |
| 10 | Tokio runtime shared global architecture correct | GREEN | S1 | Verified |
| 11 | *(Session 1 Summary)* | -- | S1 | -- |
| 12 | RocksDB OwnedSnapshot lifetime transmute | MEDIUM | S2 | Open |
| 13 | FetcherWrapper fat pointer transmute — lifetime erasure | MEDIUM | S2 | Open |
| 14 | Iterator lifetime safety — all backends materialized | GREEN | S2 | Verified |
| 15 | Unsafe Send/Sync impls inventory — all justified | GREEN | S2 | Verified |
| 16 | Memory backend — zero unsafe, reference implementation | GREEN | S2 | Verified |
| 17 | RocksDB crate v0.22.0 — no known CVEs | GREEN | S2 | Verified |
| 18 | No Pin self-referential patterns | GREEN | S2 | Verified |
| 19 | Complete non-FFI unsafe inventory — 8 items total | INFO | S2 | Verified |
| 20 | *(Session 2 Summary)* | -- | S2 | -- |
| 21 | ring 0.16.20 — AES panic CVE (RUSTSEC-2025-0009) | MEDIUM | S3 | Vulnerable |
| 22 | wasmtime 27.0.0 — 3 CVEs (sandbox runtime) | MEDIUM | S3 | Vulnerable |
| 23 | lru 0.12.5 — unsound IterMut (RUSTSEC-2026-0002) | MEDIUM | S3 | Vulnerable |
| 24 | serde_cbor 0.11.2 — unmaintained since 2021 | LOW | S3 | Confirmed |
| 25 | iroh-bitswap — git dependency, primary dependency debt source | MEDIUM | S3 | Confirmed |
| 26 | libp2p 0.53 — version lag behind latest | INFO | S3 | Confirmed |
| 27 | sha2 duplicate versions in dependency tree | INFO | S3 | Confirmed |
| 28 | blst C library — well-audited, no concerns | GREEN | S3 | Verified |
| 29 | No cargo-deny configuration — no policy enforcement | MEDIUM | S3 | Confirmed |
| 30 | Crypto crate versions — all current, RustCrypto project | GREEN | S3 | Verified |
| 31 | Build scripts — all benign (protobuf codegen, git hash) | GREEN | S3 | Verified |
| 32 | Duplicate crate inventory — ~50 duplicates, mostly iroh-bitswap | INFO | S3 | Confirmed |
| 33 | josekit JWT library — outdated but functional | GREEN | S3 | Verified |
| 34 | cosmrs/tendermint dependency chain — crypto duplicates | GREEN | S3 | Verified |
| 35 | Feature flag audit — no security-relevant misconfigurations | GREEN | S3 | Verified |
| 36 | Dependency inventory — 898 crate versions catalogued | INFO | S3 | Informational |
| 37 | *(Session 3 Summary)* | -- | S3 | -- |
| 38 | No rust-toolchain pinning — compiler version drift | LOW | S4 | Confirmed |
| 39 | defra-version git PATH dependency — build reproducibility | INFO | S4 | Confirmed |
| 40 | cbindgen header — no CI staleness verification | MEDIUM | S4 | Confirmed |
| 41 | No overflow-checks in release profile | MEDIUM | S4 | Confirmed |
| 42 | CI wasm-pack installed via curl-pipe-sh | LOW | S4 | Confirmed |
| 43 | CI has no cargo-audit step | MEDIUM | S4 | Confirmed |
| 44 | Docker base image not digest-pinned | LOW | S4 | Confirmed |
| 45 | tonic protobuf codegen — build script safe | GREEN | S4 | Verified |
| 46 | Release profile hardening — excellent for FFI | GREEN | S4 | Verified |
| 47 | `env!()` macro usage review — all safe | GREEN | S4 | Verified |
| 48 | .cargo/config.toml review — no source overrides | GREEN | S4 | Verified |
| 49 | *(Session 4 Summary)* | -- | S4 | -- |
| 50 | FFI test suite comprehensive on feature branch, absent from main | MEDIUM | S5 | Confirmed |
| 51 | No negative testing at FFI boundary | **HIGH** | S5 | Confirmed |
| 52 | No handle lifecycle stress testing | MEDIUM | S5 | Confirmed |
| 53 | FFI integration test coverage metrics (2290 tests, 96% pass) | INFO | S5 | Informational |
| 54 | No memory leak detection in CI | LOW | S5 | Confirmed |
| 55 | Go GC interaction with FFI — properly handled | GREEN | S5 | Verified |
| 56 | Cross-stream integration gaps | MEDIUM | S5 | Confirmed |
| 57 | *(Session 5 Summary)* | -- | S5 | -- |

## Severity Distribution

| Severity | Count | Findings |
|----------|-------|----------|
| **CRITICAL** | 1 | 00 |
| **HIGH** | 1 | 51 |
| **MEDIUM** | 15 | 01, 04, 12, 13, 21, 22, 23, 25, 29, 40, 41, 43, 50, 52, 56 |
| **LOW** | 6 | 02, 03, 05, 24, 38, 42, 44, 54 |
| **GREEN/INFO** | 35 | 06-10, 14-19, 26-28, 30-36, 39, 45-48, 53, 55 |

**Total**: 58 findings (1 CRITICAL, 1 HIGH, 15 MEDIUM, 6 LOW, 35 GREEN/INFO)

## Session-by-Session Summary

### Session 1: FFI Boundary Deep-Dive (Findings 00-10)

**Scope:** All 84 `pub unsafe extern "C"` entry points in `crates/ffi/src/`, handle registry, string ownership model, raw pointer handling, concurrency, panic safety, C header, and runtime architecture.

**Key Finding:** The CRITICAL finding (00) dominates this session. Every FFI function follows a naked entry pattern with no `catch_unwind` wrapper. A Rust panic from any source — `unwrap()`, index out of bounds, nested `block_on()`, arithmetic overflow — will unwind across the C/Go boundary, which is undefined behavior per the Rust reference. This can corrupt the Go process stack, cause segfaults, or silently corrupt memory.

**Strengths Identified:**
- Consistent null-check pattern via `require_c_str()` across all 84 entry points (Finding 06)
- Sound handle registry design with monotonic IDs, HashMap + RwLock, and closure-based API preventing dangling references (Finding 07)
- Correct CString ownership and null byte sanitization (Finding 08)
- Correct C header type mapping via cbindgen (Finding 09)
- Sound tokio runtime architecture with shared global (Finding 10)

### Session 2: Storage Backend Unsafe Code (Findings 12-19)

**Scope:** All non-FFI unsafe code across the entire codebase — storage backends, query runner, and any remaining unsafe blocks.

**Key Finding:** Only 8 unsafe items (4 blocks + 4 `impl Send/Sync`) exist outside the FFI crate across the entire codebase. This is remarkably clean for a database engine. The most significant architectural finding is that all storage backends use **materialized iterators** (data copied into owned Vecs at creation), which eliminates the entire category of iterator-lifetime bugs at the cost of increased memory usage.

The two MEDIUM findings (12, 13) both use `mem::transmute` for sound but compiler-unverifiable lifetime extension patterns. Both are currently correct but rely on manual safety reasoning rather than type-level guarantees.

### Session 3: Dependency & Supply Chain Audit (Findings 21-36)

**Scope:** Full dependency tree scan (~898 crate versions) using cargo-audit, cargo-deny, cargo-outdated, and manual review.

**Key Finding:** The single git dependency `iroh-bitswap` is responsible for the majority of dependency debt — 5 of 7 unmaintained crate warnings, ~80% of duplicate crate versions, and the ring 0.16.20 CVE via its libp2p-quic transitive dependency. The directly-chosen cryptographic dependencies are all current and from the well-maintained RustCrypto project.

The absence of a `deny.toml` (Finding 29) means there is no automated policy enforcement in CI for vulnerabilities, license violations, or banned crates. Combined with the missing cargo-audit CI step (Finding 43, Session 4), known CVEs can enter the dependency tree without detection.

### Session 4: Build Scripts & Compilation Pipeline (Findings 38-48)

**Scope:** Every build.rs script, cbindgen configuration, compiler hardening flags, CI/CD pipeline, Docker images, and code generation patterns.

**Key Finding:** The release profile is excellent for FFI — `panic = "abort"` + `lto = true` + `strip = true` + `codegen-units = 1` is the gold standard configuration. However, `overflow-checks` are disabled in release (Finding 41), which means integer overflow wraps silently in production. The CI pipeline covers basic quality (fmt, clippy, test) but lacks dependency vulnerability scanning, header staleness verification, and integration test execution.

### Session 5: FFI Test Coverage & Cross-Stream Integration (Findings 50-57)

**Scope:** FFI test coverage analysis across both the `main` and `jack/ffi-rust-compat` branches, concrete test metrics from the `ffi-test` tool, negative testing patterns, handle lifecycle testing, Go GC interaction, memory leak detection, and cross-stream integration gap analysis.

**Key Finding:** The `ffi-test status` command reveals **2202 of 2290 tests pass (96% pass rate)** across 102 Go integration test packages when running against the Rust FFI implementation (Finding 53). This confirms strong functional compatibility. The Go GC interaction is correctly handled via `cgo.Handle` and `C.CString()` patterns (Finding 55, GREEN). However, this coverage exists only on the feature branch (Finding 50), no negative testing exercises adversarial inputs (Finding 51, HIGH), no stress testing exercises handle lifecycle under load (Finding 52), and no memory leak detection tools are configured in CI (Finding 54). Cross-stream analysis identified 6 compound vulnerability chains where findings from different streams interact to create amplified risks (Finding 56).

## Thematic Analysis

### Theme 1: Panic Safety — The Critical Gap

The FFI boundary's most important invariant — that Rust panics must never cross into C/Go — is completely unenforced. Finding 00 (CRITICAL) identifies the root cause, and Finding 51 (HIGH) confirms there is no testing for this invariant. The release profile's `panic = "abort"` (Finding 46) provides partial mitigation by converting panics to process aborts rather than unwinding, but this still terminates the Go process without cleanup. The correct fix is `catch_unwind` wrappers on all 84 entry points, converting panics to `FfiResult::error()` returns.

### Theme 2: Dependency Hygiene Debt

Five findings (21, 22, 23, 25, 29) identify dependency-level risks. The iroh-bitswap git dependency is the single largest source of technical debt, pulling in unmaintained crates and old dependency versions. The wasmtime version lag (14 major versions, 3 CVEs) is concerning for a sandbox runtime. The absence of cargo-deny (29) and cargo-audit in CI (43) means these issues have no automated detection or prevention.

### Theme 3: Build Pipeline Gaps

Four findings (38, 40, 41, 43) identify CI/CD gaps. The most impactful are the missing cargo-audit step (43) and the missing overflow-checks (41). The cbindgen header staleness gap (40) creates ABI drift risk. The rust-toolchain pinning gap (38) affects build reproducibility but is lower severity.

### Theme 4: Unsafe Code Minimalism

The strongest finding of this stream is the remarkably small unsafe surface area. Only 8 unsafe items exist outside the FFI crate (Finding 19). The materialized iterator architecture (Finding 14) eliminates the most common source of unsafe code in database engines. All Send/Sync impls are justified (Finding 15). The memory backend serves as a zero-unsafe reference implementation (Finding 16). This is a testament to deliberate architectural choices that minimize the need for unsafe code.

### Theme 5: Test Coverage Asymmetry

FFI test coverage is strong in breadth (73/84 functions on the feature branch) but completely absent in depth (no negative testing, no stress testing, no concurrent testing). This creates a false sense of security — the happy path works, but adversarial inputs could trigger any of the safety issues identified in Sessions 1-2.

## Overall Security Posture Assessment

### Strengths

1. **Minimal unsafe surface area** — 8 items outside FFI is exceptional for a database engine
2. **Materialized iterators** eliminate iterator-lifetime bugs by design
3. **Sound handle registry** with monotonic IDs, HashMap + RwLock, closure-based API
4. **Consistent null-check patterns** across all 84 FFI entry points
5. **Correct CString ownership model** with null byte sanitization
6. **Excellent release profile** — panic=abort, LTO, strip, codegen-units=1
7. **Clean crypto dependency choices** — all from RustCrypto project, current versions
8. **No nightly features, no source overrides, no custom registries**
9. **Cargo.lock committed** — reproducible transitive dependency resolution
10. **All build scripts benign** — no network access, no untrusted binary execution

### Weaknesses

1. **No catch_unwind** — any panic is undefined behavior at the FFI boundary
2. **No negative FFI testing** — adversarial inputs never exercised
3. **No cargo-audit or cargo-deny in CI** — CVEs not automatically detected
4. **wasmtime 14 major versions behind** — 3 accumulated CVEs in sandbox runtime
5. **iroh-bitswap git dependency** — pulls in unmaintained crates and old versions
6. **No overflow-checks in release** — integer overflow wraps silently
7. **FFI test coverage absent from main branch** — regressions undetected
8. **C header no CI verification** — ABI drift possible

### Risk Rating

For **development/testing deployments**: LOW-MEDIUM risk. The unsafe code is minimal and sound. The dependency CVEs are low individual severity. The FFI boundary works correctly on the happy path.

For **production deployments with FFI**: HIGH risk. The absence of catch_unwind means any panic path — including rare edge cases in downstream crates — is undefined behavior. The lack of negative testing means these panic paths have never been exercised. The missing cargo-audit in CI means new CVEs are not automatically detected.

For **Rust-native deployments (no FFI)**: LOW risk. The FFI boundary findings are irrelevant. The unsafe code in storage backends is sound. The dependency CVEs are individually low severity.

## Prioritized Remediation

### Immediate (P0 — Do Now)

| Action | Finding | Effort | Impact |
|--------|---------|--------|--------|
| Add `catch_unwind` to all 84 FFI entry points | 00 | 4 hours | Eliminates UB from panics |
| Add cargo-audit to CI pipeline | 43 | 30 min | Automates CVE detection |
| Add `overflow-checks = true` to release profile | 41 | 5 min | Prevents silent integer wraps |

### Short Term (P1 — This Sprint)

| Action | Finding | Effort | Impact |
|--------|---------|--------|--------|
| Add negative test suite for top 10 FFI functions | 51 | 8 hours | Validates error paths |
| Create deny.toml with advisory/license policies | 29 | 1 hour | Automated policy enforcement |
| Upgrade wasmtime 27 to 38+ | 22 | 4 hours | Fixes 3 CVEs |
| Add `from_raw_parts` length cap | 01 | 1 hour | Prevents buffer over-read |
| Add cbindgen header verification to CI | 40 | 1 hour | Prevents ABI drift |

### Medium Term (P2 — This Month)

| Action | Finding | Effort | Impact |
|--------|---------|--------|--------|
| Merge FFI test client to main or add CI step | 50 | 2 hours | Prevents FFI regressions |
| Add handle lifecycle stress tests | 52 | 4 hours | Validates registry under load |
| Migrate serde_cbor to ciborium | 24 | 8 hours | Removes unmaintained dependency |
| Add FetcherWrapper lifetime parameter | 13 | 4 hours | Compiler-enforced safety |
| Add concurrent close protection | 04 | 4 hours | Prevents use-after-close |
| Consider self_cell for OwnedSnapshot | 12 | 2 hours | Type-level safety |

### Long Term (P3 — Before 1.0)

| Action | Finding | Effort | Impact |
|--------|---------|--------|--------|
| Modernize or replace iroh-bitswap | 25 | 16 hours | Eliminates dependency debt |
| Create rust-toolchain.toml | 38 | 15 min | Reproducible builds |
| Pin Docker base images by digest | 44 | 30 min | Supply chain hardening |
| Replace curl-pipe-sh wasm-pack install | 42 | 30 min | CI supply chain |
| Add double-free guard to defra_free_string | 03 | 2 hours | Defense-in-depth |
| Add handle counter overflow protection | 02 | 1 hour | Defense-in-depth |
