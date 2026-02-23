# Stream 07: Dependency & Unsafe Code -- Verification Re-Audit

**Date**: 2026-02-23
**Auditor**: Claude Opus 4.6
**Scope**: Verify remediations for all findings in `audit/07-dependency-unsafe-code-findings/`

---

## Summary

| Finding | Severity | Remediation Status | Verdict |
|---------|----------|--------------------|---------|
| 07-00 | CRITICAL | REMEDIATED (81/84) | PASS with residual |
| 07-01 | MEDIUM | REMEDIATED (all 5 sites) | PASS |
| 07-41 | MEDIUM | REMEDIATED | PASS |
| 07-22 | MEDIUM | NOT REMEDIATED | FAIL |
| 07-51 | HIGH | REMEDIATED | PASS |
| 07-29 | MEDIUM | REMEDIATED | PASS |
| 07-40 | MEDIUM | REMEDIATED | PASS |
| 07-42 | MEDIUM | REMEDIATED | PASS |
| 07-43 | MEDIUM | REMEDIATED | PASS |
| 07-50 | MEDIUM | PARTIALLY REMEDIATED | CONDITIONAL PASS |
| 07-52 | MEDIUM | REMEDIATED | PASS |

**Overall: 9 PASS, 1 CONDITIONAL PASS, 1 FAIL**

---

## Phase 1.1: CRITICAL Findings

### 07-00: No `catch_unwind` in FFI Entry Points

**Original finding**: None of the 84 `pub unsafe extern "C"` FFI entry points were wrapped in `std::panic::catch_unwind()`. Panics crossing the FFI boundary are undefined behavior.

**Remediation implemented**: An `ffi_entry!` macro was created at `crates/ffi/src/lib.rs:88-99` that wraps function bodies in `std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| { ... }))`. Caught panics are converted to error results via the `FfiPanicResult` trait.

**Verification -- Exhaustive Function Audit**:

Total `#[no_mangle]` functions in `crates/ffi/src/`: **84**
Functions wrapped in `ffi_entry!`: **81**
Functions NOT wrapped: **3**

The 3 unwrapped functions are:

| Function | File | Risk Assessment |
|----------|------|-----------------|
| `defra_init()` | `lib.rs:182` | LOW -- Calls `init_runtime()` (OnceLock) and an atomic store. Cannot panic under any realistic input. |
| `defra_version()` | `lib.rs:194` | LOW -- Returns a compile-time constant via `env!("CARGO_PKG_VERSION")`. The `CString::new("unknown").unwrap()` fallback is safe (no null bytes). Cannot panic. |
| `defra_free_string()` | `types.rs:257` | LOW -- Null check followed by `CString::from_raw(ptr)`. Cannot panic if the pointer was allocated by this crate. Double-free is UB but `ffi_entry!` would not prevent that. |

**Analysis of unwrapped functions**:

- `defra_init()` returns `void` (unit type `()`), which does not implement `FfiPanicResult`. Wrapping it would require either changing its return type or adding a `FfiPanicResult` impl for `()`. Since it cannot panic in practice, this is acceptable.
- `defra_version()` returns `*mut c_char`, which also does not implement `FfiPanicResult`. Since the only code path is a compile-time string, it cannot panic.
- `defra_free_string()` is a deallocation function. If it panicked (which it cannot for valid inputs), `ffi_entry!` would try to allocate an error string -- problematic in an allocator failure scenario.

All 81 remaining functions (the ones that take user input, perform I/O, or call into the database/query engine) ARE wrapped.

**Quality of the macro**:
- Uses `AssertUnwindSafe`, which is correct for FFI boundaries.
- The `FfiPanicResult` trait has implementations for all three FFI return types: `FfiResult`, `NewNodeResult`, `NewTxnResult`.
- The `extract_panic_message()` helper properly downcasts both `String` and `&str` panic payloads.
- Note: With `panic = "abort"` in release profile, `catch_unwind` is a no-op (panics abort before unwinding). This is defense-in-depth for debug/test builds.

**Verdict**: **PASS with acceptable residual**. 81/84 functions wrapped. The 3 unwrapped functions cannot panic and have return types incompatible with the error-return pattern. No action needed.

---

### 07-01: `from_raw_parts` with Uncapped Length

**Original finding**: Five call sites use `std::slice::from_raw_parts(ptr, len)` where `len` comes from the C caller with no upper-bound validation.

**Verification -- All 5 call sites checked**:

| Call Site | File:Line | MAX_LEN Check | Order | Verdict |
|-----------|-----------|---------------|-------|---------|
| `signing_private_key` | `node.rs:115-125` | YES: `> MAX_PRIVATE_KEY_LEN` (128) before `from_raw_parts` | Correct | PASS |
| `sourcehub_signer_key` | `node.rs:229-239` | YES: `> MAX_PRIVATE_KEY_LEN` (128) before `from_raw_parts` | Correct | PASS |
| `signing_private_key` (P2P) | `p2p/node.rs:76-87` | YES: `> MAX_PRIVATE_KEY_LEN` (128) before `from_raw_parts` | Correct | PASS |
| `sourcehub_signer_key` (P2P) | `p2p/node.rs:484-490` | YES: `> MAX_PRIVATE_KEY_LEN` (128) before `from_raw_parts` | Correct | PASS |
| `key_ptr` (SE key) | `se_key.rs:31-38` | YES: `key_len != 32` check before `from_raw_parts` | Correct | PASS |

**Details**:
- `MAX_PRIVATE_KEY_LEN` is defined as `128` in both `node.rs:19` and `p2p/node.rs:22`. This is a generous upper bound covering all key types.
- The SE key check at `se_key.rs:31` is exact (`!= 32`), which is even stricter than a cap. Since AES-256 keys are always exactly 32 bytes, this is correct.
- All length checks occur BEFORE the `from_raw_parts` call, preventing the UB from ever being reached.
- The SE key is wrapped in `Zeroizing::new(...)` (line 38), which is a bonus remediation from finding 01-16.

**Verdict**: **PASS**. All 5 call sites have length validation before `from_raw_parts`.

---

### 07-41: No Overflow Checks in Release

**Original finding**: The `[profile.release]` section in root `Cargo.toml` did not include `overflow-checks = true`.

**Verification**:

```toml
# Cargo.toml lines 121-127
[profile.release]
opt-level = 3
lto = true
codegen-units = 1
strip = true
panic = "abort"
overflow-checks = true
```

The line `overflow-checks = true` is present at `Cargo.toml:127`.

**Verdict**: **PASS**. Release builds now panic on integer overflow instead of wrapping silently.

---

## HIGH Findings

### 07-22: wasmtime 27.0.0 Multiple CVEs

**Original finding**: `wasmtime` v27.0.0 is affected by at least 3 CVEs:
- RUSTSEC-2025-0046: `fd_renumber` host panic
- RUSTSEC-2025-0118: Unsound shared linear memory access
- RUSTSEC-2026-0006: `f64.copysign` segfault (potential sandbox escape)

**Verification**:

**Current version in Cargo.lock**: `wasmtime 27.0.0` (UNCHANGED)

The project is still on wasmtime 27.0.0, which is 14 major versions behind the latest (41.x). All three CVEs remain unpatched.

**Mitigations in place**:

1. **WasmSandboxConfig exists** (`crates/lens/src/wasm.rs:27-46`) with `StoreLimiter`, fuel metering, and epoch deadline support.
2. **BUT** the sandbox is opt-in and **NOT enabled by default**. Production instantiation at `crates/db/src/database.rs:341` calls `WasmTransformStore::new()`, which passes `None` for sandbox config.
3. **deny.toml ignores two of the three CVEs**: RUSTSEC-2025-0046 and RUSTSEC-2025-0118 are in the `[advisories] ignore` list. RUSTSEC-2026-0006 is NOT in the ignore list, which means `cargo deny check` would flag it.

**Risk assessment**:
- The lens module executes WASM transforms for schema migrations. These are typically project-authored, not arbitrary user code.
- However, RUSTSEC-2026-0006 (f64.copysign segfault) is a potential sandbox escape on x86-64, which is the primary deployment target.
- The sandbox config infrastructure exists but is not wired into production code paths.

**Verdict**: **FAIL**. wasmtime remains at 27.0.0 with 3 known CVEs. The sandbox mitigation exists in code but is not enabled by default. Upgrade to wasmtime >= 38.0.4 is required.

---

### 07-51: No Negative FFI Boundary Testing

**Original finding**: None of the 84 FFI entry points were tested with adversarial inputs (NULL pointers, invalid handles, non-UTF-8, double-frees, etc.).

**Verification**:

A comprehensive negative test module exists at `crates/ffi/src/negative_tests.rs` (451 lines, test-only module).

**Test coverage**:

| Category | Tests Present | Functions Covered |
|----------|---------------|-------------------|
| NULL pointer -- string params | `add_schema_null_sdl_returns_error`, `exec_request_null_query_returns_error`, `commit_txn_null_id_returns_error`, `rollback_txn_null_id_returns_error`, `free_string_null_is_safe` | 5 functions |
| Invalid handle (0) | `node_close_zero_handle_returns_error` | 1 function |
| Invalid handle (usize::MAX) | `node_close_large_handle_returns_error` | 1 function |
| Stale handle (use-after-close) | `stale_handle_operations_return_errors` | 5 operations (node_close, add_schema, exec_request, begin_txn, create_subscription) |
| Invalid subscription handles | `invalid_subscription_handle_returns_errors` | 3 operations (poll 0, poll MAX, close 0) |
| Non-UTF-8 strings | `add_schema_invalid_utf8_sdl_returns_error`, `exec_request_invalid_utf8_query_returns_error`, `commit_txn_invalid_utf8_id_returns_error` | 3 functions |
| Handle lifecycle stress | `handle_lifecycle_rapid_sequential_create_destroy` (50 nodes) | Sequential monotonic handles |
| Subscription lifecycle stress | `subscription_handle_lifecycle_rapid_cycles` (30 cycles) | Create/poll/close + stale check |
| Concurrent node create/destroy | `concurrent_node_create_destroy_is_safe` (4 threads x 10 nodes) | RwLock + AtomicUsize under contention |
| Concurrent subscription access | `concurrent_subscription_access_is_safe` (4 threads x 10 ops) | Shared node, concurrent sub lifecycle |

**Quality assessment**:
- Tests properly initialize the runtime before use.
- All tests clean up allocated CStrings via `defra_free_string`.
- Stress tests use `std::sync::Barrier` for synchronized thread startup (maximizes contention).
- Handle uniqueness verified with `HashSet`.
- Tests are on main branch (in `crates/ffi/src/negative_tests.rs`, included via `#[cfg(test)] mod negative_tests` in `lib.rs:203`).

**Gaps remaining**:
- Double-free of CString (`defra_free_string(ptr); defra_free_string(ptr)`) is not tested. This is inherently UB and cannot be safely tested in-process.
- Not all 84 functions have individual NULL pointer tests (only the 5 most critical are tested). This is acceptable given the shared `require_c_str()` validation function.

**Verdict**: **PASS**. Comprehensive negative tests exist on main branch covering all major adversarial input categories.

---

## Phase 5.5: Should Fix Findings

### 07-29: No cargo-deny Configuration

**Original finding**: No `deny.toml` existed in the repository.

**Verification**:

File exists at `/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb.rs/deny.toml` (45 lines).

**Configuration review**:

| Section | Setting | Assessment |
|---------|---------|------------|
| `[advisories]` | 12 advisories in ignore list | Acceptable -- all are unmaintained transitive deps or known wasmtime issues being tracked |
| `[licenses]` | Allow list: MIT, Apache-2.0, Apache-2.0 WITH LLVM-exception, BSD-2-Clause, BSD-3-Clause, ISC, MPL-2.0, Unicode-3.0, Zlib | Comprehensive and appropriate |
| `[bans]` | `multiple-versions = "warn"` | Correct -- warn, not deny, given iroh-bitswap causing ~50 duplicates |
| `[sources]` | `unknown-registry = "warn"`, `unknown-git = "warn"`, allow-git includes beetle fork | Correct |

**Minor concern**: `[advisories]` does not have explicit `vulnerability = "deny"` or `unsound = "deny"` settings. The defaults for cargo-deny v0.19+ are to deny vulnerabilities, so this is likely fine, but being explicit would be clearer.

**Notable**: RUSTSEC-2026-0006 (wasmtime f64.copysign segfault) is NOT in the ignore list, meaning `cargo deny check` will flag it. This is correct -- it should be flagged until the upgrade happens.

**Verdict**: **PASS**. deny.toml exists with reasonable policies.

---

### 07-40: cbindgen Header Not Verified in CI

**Original finding**: No CI step verified that the committed `defra.h` matches what cbindgen would generate.

**Verification**:

CI job exists in `.github/workflows/ci.yml:52-65`:

```yaml
cbindgen:
  name: CBIndgen Header Verification
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - uses: dtolnay/rust-toolchain@stable
    - uses: Swatinem/rust-cache@v2
    - run: sudo apt-get update && sudo apt-get install -y libssl-dev pkg-config protobuf-compiler
    - name: Install cbindgen
      run: cargo install cbindgen
    - name: Generate header
      run: cbindgen --config crates/ffi/cbindgen.toml --crate ffi --output defra.h.generated
    - name: Verify header matches
      run: diff -u defra.h defra.h.generated || (echo "Generated header does not match committed header"; exit 1)
```

**Assessment**:
- Generates a fresh header and diffs against committed `defra.h`.
- Uses `diff -u` for readable output on failure.
- Fails the CI job with a clear error message if headers don't match.
- Runs on both push to main and pull requests.

**Verdict**: **PASS**. CI verifies header freshness on every PR and push.

---

### 07-42: CI WASM Build Uses curl-pipe-sh

**Original finding**: Release workflow installed wasm-pack via `curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh`.

**Verification**:

At `.github/workflows/release.yml:145-146`:

```yaml
- name: Install wasm-pack
  run: cargo install wasm-pack
```

The `curl | sh` pattern has been replaced with `cargo install wasm-pack`, which downloads from crates.io with integrity verification (Cargo.lock hash, crate checksums).

**Verdict**: **PASS**. Supply chain risk eliminated.

---

### 07-43: CI Missing cargo audit/deny Steps

**Original finding**: Neither CI nor release workflows ran `cargo audit` or `cargo deny check`.

**Verification**:

CI job exists in `.github/workflows/ci.yml:45-50`:

```yaml
deny:
  name: Cargo Deny
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - uses: EmbarkStudios/cargo-deny-action@v2
```

**Assessment**:
- Uses the official `EmbarkStudios/cargo-deny-action@v2` GitHub Action.
- Runs on both push to main and pull requests.
- Combined with the `deny.toml` configuration, this provides advisory checking, license checking, and source checking.
- `cargo-deny` subsumes `cargo audit` (it uses the same RustSec database).

**Verdict**: **PASS**. Automated dependency scanning is now in CI.

---

## Phase 6.3: Test Coverage Findings

### 07-50: FFI Test Suite on Feature Branch Only

**Original finding**: The Go FFI client wrapper existed only on the `jack/ffi-rust-compat` branch, not on main.

**Verification**:

The Go FFI wrapper (the 2298-line `tests/clients/rustffi/defra.go`) is a Go-side artifact that lives in the Go repository. The Rust-side negative tests, however, ARE on main:

- `crates/ffi/src/negative_tests.rs` -- 451 lines of Rust-native FFI boundary tests (on main)
- `crates/ffi/src/query/tests.rs` -- query execution tests (on main)
- `crates/ffi/src/subscription/tests.rs` -- subscription tests (on main)
- `crates/ffi/src/lib.rs` tests -- lifecycle and workflow tests (on main)

The Rust-native integration test suite (`tools/integration-test/`) exercises the full node via CLI + HTTP API, providing functional coverage independent of the Go FFI wrapper.

**Assessment**: The primary validation mechanism has shifted from Go FFI tests to Rust-native integration tests (as documented in CLAUDE.md). The Go FFI wrapper is a cross-language interop test, not the primary validation. Rust-side FFI boundary tests are comprehensive and on main.

**Verdict**: **CONDITIONAL PASS**. Rust-side FFI tests are on main and comprehensive. Go-side cross-language FFI testing remains on a feature branch. This is acceptable if the Go FFI wrapper is merged before 1.0 release for cross-language validation.

---

### 07-52: No Handle Lifecycle Stress Testing

**Original finding**: No stress tests for the handle registry (rapid create/destroy, concurrent access, wrapping).

**Verification**:

Stress tests exist in `crates/ffi/src/negative_tests.rs`:

| Test | What It Exercises | Scale |
|------|-------------------|-------|
| `handle_lifecycle_rapid_sequential_create_destroy` | 50 sequential create+destroy, uniqueness verification | 50 nodes |
| `subscription_handle_lifecycle_rapid_cycles` | 30 create/close cycles, stale handle detection | 30 subscriptions |
| `concurrent_node_create_destroy_is_safe` | 4 threads x 10 nodes, barrier-synchronized | 40 concurrent nodes |
| `concurrent_subscription_access_is_safe` | 4 threads x 10 ops on shared node | 40 concurrent subscription ops |

**Quality assessment**:
- Sequential test verifies monotonically increasing handles with `HashSet` uniqueness check.
- Concurrent tests use `Barrier` to maximize contention at the RwLock.
- Stale handle detection verified (poll after close returns error).
- Node cleanup verified (all `node_close` calls must succeed).

**Gap**: Handle counter wrapping (usize::MAX) is not tested. This is acceptable because:
1. 64-bit `usize::MAX` is ~18.4 quintillion, unreachable in any test.
2. The `overflow-checks = true` in release profile (07-41 fix) would panic on wrapping.

**Verdict**: **PASS**. Comprehensive stress tests cover rapid creation, destruction, stale handles, and concurrent access.

---

## Cross-Cutting Observations

### wasmtime Sandbox Not Activated by Default

The WASM sandbox infrastructure (`WasmSandboxConfig`, `StoreLimiter`, fuel metering, epoch deadline) exists at `crates/lens/src/wasm.rs` but is **NOT used in production**. The production instantiation at `crates/db/src/database.rs:341` calls `WasmTransformStore::new()`, which passes `None` for sandbox config. This means:

1. No memory limit on WASM modules
2. No CPU fuel budget
3. No epoch deadline interruption
4. All three wasmtime CVEs are exploitable by a malicious WASM module

**Recommendation**: Change `create_lens_store()` to use `WasmTransformStore::with_sandbox(Some(WasmSandboxConfig::restrictive()))` as the default. This provides defense-in-depth regardless of the wasmtime version.

### deny.toml Advisory Ignores

The deny.toml ignores 12 advisories. Three of these are wasmtime-related:
- RUSTSEC-2025-0046 (ignored)
- RUSTSEC-2025-0118 (ignored)
- RUSTSEC-2024-0320 (ignored)

RUSTSEC-2026-0006 is NOT ignored, meaning `cargo deny check` will fail on it. This is the correct behavior -- it keeps pressure on the wasmtime upgrade. However, CI may currently be failing on this advisory. If CI passes, it may mean RUSTSEC-2026-0006 is not yet in the advisory database used by the installed cargo-deny version.

### Three Unwrapped FFI Functions

`defra_init()`, `defra_version()`, and `defra_free_string()` are not wrapped in `ffi_entry!`. These functions cannot panic under normal operation, and their return types (`void`, `*mut c_char`, `void`) are not compatible with the `FfiPanicResult` trait. This is an intentional design choice, not an oversight. If desired, trivial `FfiPanicResult` impls could be added for these types to provide belt-and-suspenders protection, but it is not security-critical.

---

## Remediation Scorecard

| Phase | Finding | Required By | Status | Notes |
|-------|---------|-------------|--------|-------|
| 1.1 | 07-00 | 1.0 | DONE | 81/84 wrapped; 3 cannot-panic exceptions |
| 1.1 | 07-01 | 1.0 | DONE | All 5 sites have MAX_LEN checks |
| 1.1 | 07-41 | 1.0 | DONE | overflow-checks = true in release |
| 3.2 | 07-22 | 1.0 | BLOCKED | wasmtime still 27.0.0; upgrade required |
| 5.5 | 07-29 | Pre-1.0 | DONE | deny.toml with comprehensive policies |
| 5.5 | 07-40 | Pre-1.0 | DONE | CI cbindgen header verification |
| 5.5 | 07-42 | Pre-1.0 | DONE | cargo install replaces curl pipe sh |
| 5.5 | 07-43 | Pre-1.0 | DONE | cargo-deny-action@v2 in CI |
| 6.3 | 07-50 | Pre-1.0 | PARTIAL | Rust-side tests on main; Go wrapper on branch |
| 6.3 | 07-51 | Pre-1.0 | DONE | Comprehensive negative test suite |
| 6.3 | 07-52 | Pre-1.0 | DONE | Stress + concurrent tests |

**Remaining blockers for 1.0**: wasmtime 27.0.0 upgrade (07-22).

---

## Recommendations

1. **CRITICAL**: Upgrade wasmtime from 27.0.0 to >= 38.0.4 (fixes all three CVEs). This is the only remaining 1.0 blocker in Stream 07.

2. **HIGH**: Enable WASM sandbox by default in `crates/db/src/database.rs:341`. Change `WasmTransformStore::new()` to `WasmTransformStore::with_sandbox(Some(WasmSandboxConfig::restrictive()))`. This provides immediate mitigation for wasmtime CVEs until the upgrade is complete.

3. **LOW**: Consider adding explicit `vulnerability = "deny"` and `unsound = "deny"` to `deny.toml` `[advisories]` section for clarity, even though these are the defaults.

4. **LOW**: Merge Go FFI wrapper from `jack/ffi-rust-compat` branch before 1.0 release for cross-language validation.
