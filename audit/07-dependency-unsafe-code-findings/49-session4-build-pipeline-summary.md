# Session 4 Summary: Build Scripts & Compilation Pipeline

## Scope

Deep-dive into every build.rs script, cbindgen C header generation, compiler hardening flags, CI/CD pipeline security, Docker images, and code generation patterns.

## Files Audited

| File | Purpose |
|---|---|
| `crates/orbis/build.rs` | tonic protobuf compilation |
| `crates/defra-version/build.rs` | Git commit hash/date embedding |
| `crates/ffi/cbindgen.toml` | C header generation config |
| `crates/ffi/Cargo.toml` | cbindgen build-dependency |
| `defra.h` | Committed C header (1657 lines) |
| `Cargo.toml` | Workspace profiles and dependencies |
| `.cargo/config.toml` | Build configuration |
| `.github/workflows/ci.yml` | CI pipeline |
| `.github/workflows/release.yml` | Release pipeline |
| `Dockerfile` | Development container |
| `Dockerfile.release` | Production container |
| `crates/orbis/proto/orbis.proto` | gRPC service definition |
| 5 files with `env!()` usage | Compile-time environment references |

## Findings Summary

### By Severity

| Severity | Count | Findings |
|---|---|---|
| Medium | 4 | cbindgen header no CI verification (#40), no overflow-checks in release (#41), wasm-pack curl-pipe-sh (#42), CI missing cargo audit (#43) |
| Low | 2 | No rust-toolchain pinning (#38), git PATH dependency (#39), Docker images not digest-pinned (#44) |
| Informational | 4 | tonic proto codegen green (#45), release profile hardening green (#46), env!() usage review (#47), cargo config review green (#48) |

### Build Script Inventory

| Script | Category | Risk | Finding |
|---|---|---|---|
| `crates/orbis/build.rs` | Code generation (protobuf) | None | #45 |
| `crates/defra-version/build.rs` | External tool (git) | Low | #39 |
| `crates/ffi/build.rs` | Does not exist (cbindgen unused as build-dep) | N/A | #40 |
| `crates/db/src/block_builder/build.rs` | Not a build script (regular source file) | None | N/A |

### Checklist Results

| Check | Status |
|---|---|
| Build scripts access network? | No |
| Build scripts execute untrusted binaries? | No (only git) |
| cbindgen invoked as library or subprocess? | Neither — no build.rs exists, CLI tool in CI |
| Generated header committed? | Yes, but no staleness check |
| CI verifies header matches source? | No |
| RocksDB native compilation flags? | N/A — rocksdb is optional, uses rocksdb crate defaults |
| overflow-checks in release? | No (Rust default: disabled) |
| debug-assertions in release? | No (Rust default: disabled in release — correct) |
| LTO enabled? | Yes |
| Symbols stripped? | Yes |
| panic = abort? | Yes (critical for FFI safety) |
| Nightly features used? | No |
| Rust toolchain pinned? | No |
| cargo audit in CI? | No |
| cargo deny in CI? | No |
| Release builds reproducible? | Partially (no toolchain/image pinning) |
| Proc macros from trusted sources? | Yes (all well-known crates) |
| Vendored dependencies? | openssl-sys optional, feature-gated |
| Source overrides in .cargo/config? | No |
| include!() macros? | 1 — tonic::include_proto!() (safe) |

## Key Observations

### 1. Release Profile is Excellent for FFI

The `panic = "abort"` + `lto = true` + `strip = true` + `codegen-units = 1` combination is the gold standard for FFI libraries. The only gap is `overflow-checks`.

### 2. CI Pipeline is Minimal

The CI pipeline (fmt, clippy, test) covers basic quality but lacks:
- Dependency vulnerability scanning (cargo audit / cargo deny)
- Header staleness verification
- Integration tests (these exist but aren't in CI)

### 3. cbindgen Architecture Has a Gap

cbindgen is listed as a build-dependency but never invoked at build time. The header is committed to the repo and regenerated only in the release workflow. No automated check prevents header drift during development.

### 4. Supply Chain Hygiene is Good

No custom registries, no source overrides, no patch sections. The only non-crates.io dependency is iroh-bitswap (project-controlled fork). The wasm-pack curl-pipe-sh is the only notable supply chain risk.

## Recommended Priority Actions

1. **Add `overflow-checks = true` to release profile** — Low effort, meaningful safety improvement
2. **Add cargo audit to CI** — Low effort, catches new CVEs automatically
3. **Add cbindgen header verification to CI** — Prevents ABI drift bugs
4. **Replace curl-pipe-sh with pinned wasm-pack install** — Eliminates supply chain risk
5. **Create rust-toolchain.toml** — Ensures reproducible builds
