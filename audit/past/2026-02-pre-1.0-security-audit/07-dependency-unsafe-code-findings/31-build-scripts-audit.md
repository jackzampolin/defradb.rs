# Build Scripts and Build-Time Dependencies Audit

**Severity:** Low
**Category:** Supply chain — Build-time code execution
**Status:** Green — all build scripts are benign

## Summary

Three build.rs files exist in the project, plus two build-time dependencies. All are low-risk: one compiles protobuf definitions, one reads git commit info, and one generates C headers.

## Build Scripts

### 1. crates/orbis/build.rs — Protobuf Compilation
```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/orbis.proto")?;
    Ok(())
}
```
**Risk:** Low. Compiles a local `.proto` file using `tonic-build`. No network access, no arbitrary file operations.

### 2. crates/defra-version/build.rs — Git Commit Info
```rust
fn main() {
    let commit = Command::new("git").args(["rev-parse", "HEAD"]).output()...
    let date = Command::new("git").args(["show", "-s", "--date=short", "--format=%cd", "HEAD"]).output()...
    println!("cargo:rustc-env=GIT_COMMIT={commit}");
    println!("cargo:rustc-env=BUILD_DATE={date}");
}
```
**Risk:** Low. Runs `git rev-parse HEAD` and `git show` to embed commit hash and date at compile time. No network access. Falls back to "unknown" on failure.

### 3. crates/ffi/build.rs (implicit via cbindgen)
The FFI crate has `cbindgen = "0.28"` as a build dependency. cbindgen parses Rust source files and generates C header files. No network access.

## Build-Time Dependencies

| Crate | Version | Used By | Purpose |
|-------|---------|---------|---------|
| `cbindgen` | 0.28.0 | ffi | Generate C headers from Rust FFI |
| `tonic-build` | 0.12.x | orbis | Compile .proto files |

## Third File: crates/db/src/block_builder/build.rs

This is a Rust source file named `build.rs` inside the block_builder module — it's **not** a Cargo build script. It's a regular source file that happens to be named `build.rs`.

## Proc-Macro Dependencies

The project uses standard proc-macro crates:
- `serde_derive` — serialization derive macros
- `thiserror` — error type derive
- `async-trait` — async trait support
- `clap_derive` — CLI argument parsing
- Various libp2p/wasmtime internal proc-macros

All are well-known, widely-used crates with thousands of dependents.

## Remediation

No action needed. Build scripts are minimal and benign. No custom registries configured (`.cargo/config.toml` only sets build parallelism and debug settings).
