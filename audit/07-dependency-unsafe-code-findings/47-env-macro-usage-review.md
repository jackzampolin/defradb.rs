# env!() Macro Usage Review

**Severity:** Informational
**Category:** Build integrity — Compile-time environment dependencies
**Status:** Green — all uses are safe and non-security-critical

## Summary

The project uses `env!()` in 9 locations across 5 files. All uses reference Cargo-provided variables (`CARGO_PKG_VERSION`, `CARGO_PKG_RUST_VERSION`) or custom build-script variables (`GIT_COMMIT`, `BUILD_DATE`). None are used in security-critical decision paths.

## Affected Files

| File | Variable | Purpose |
|---|---|---|
| `crates/defra-version/src/lib.rs:36` | `CARGO_PKG_VERSION` | Version display |
| `crates/defra-version/src/lib.rs:37` | `GIT_COMMIT` | Version display |
| `crates/defra-version/src/lib.rs:38` | `BUILD_DATE` | Version display |
| `crates/defra-version/src/lib.rs:44` | `CARGO_PKG_RUST_VERSION` | Version display |
| `crates/wasm/src/lib.rs:89` | `CARGO_PKG_VERSION` | WASM version API |
| `crates/ffi/src/lib.rs:159` | `CARGO_PKG_VERSION` | FFI version API |
| `crates/p2p/src/behaviour.rs:175` | `CARGO_PKG_VERSION` | P2P agent version |
| `crates/p2p/src/behaviour.rs:274` | `CARGO_PKG_VERSION` | P2P agent version |
| `crates/defra-core/src/lib.rs:34` | `CARGO_PKG_VERSION` | Core version constant |

## Details

### Categories

**Cargo-provided (safe, deterministic):**
- `CARGO_PKG_VERSION` — from `Cargo.toml` version field, under version control
- `CARGO_PKG_RUST_VERSION` — from `Cargo.toml` rust-version field, under version control

**Build-script-provided (safe, non-critical):**
- `GIT_COMMIT` — from `git rev-parse HEAD`, version display only
- `BUILD_DATE` — from `git show --format=%cd`, version display only

### Security Assessment

1. **No `env!()` in authentication or authorization code** — version strings are display-only
2. **No `option_env!()` usage** — no optional environment-dependent behavior changes
3. **No `env!()` in crypto code** — no key material or algorithm selection from environment
4. **P2P agent version** uses `CARGO_PKG_VERSION` for the libp2p identify protocol. This is visible to peers but only reveals the software version, which is also visible via HTTP headers.

### No Conditional Compilation from Environment

No `#[cfg(env = ...)]` or `cfg!(env = ...)` patterns were found. All conditional compilation uses feature flags or target triples.

## Remediation

No action needed.

## Exploitability

Not exploitable. Version strings are informational only.
