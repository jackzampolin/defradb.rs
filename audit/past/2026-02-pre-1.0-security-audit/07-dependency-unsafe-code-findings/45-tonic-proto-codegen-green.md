# tonic_build Proto Compilation — Safe

**Severity:** Informational
**Category:** Build script — Code generation
**Status:** Green — safe local-only code generation

## Summary

The `crates/orbis/build.rs` uses `tonic_build::compile_protos()` to compile a local `.proto` file into Rust code. The generated code is included via `tonic::include_proto!()`. This is a standard, safe pattern with no supply chain risk.

## Affected Files

- `crates/orbis/build.rs:1-4` — `tonic_build::compile_protos("proto/orbis.proto")`
- `crates/orbis/proto/orbis.proto` — 122 lines, Orbis gRPC service definition
- `crates/orbis/src/lib.rs:9` — `tonic::include_proto!("orbis.utility.v1")`

## Details

### What Happens at Build Time

1. `tonic_build` reads `proto/orbis.proto` from the crate directory
2. It generates Rust source files in `OUT_DIR` (typically `target/debug/build/orbis-<hash>/out/`)
3. The generated code implements gRPC client/server stubs for the `UtilityService`
4. At compile time, `tonic::include_proto!()` includes the generated code

### Security Properties

- **No network access**: The proto file is local, committed to the repository
- **No arbitrary code execution**: tonic_build only parses proto syntax and generates Rust structs/traits
- **Proto file is small and auditable**: 122 lines defining 2 RPCs, 5 messages, 1 enum
- **Generated code is type-safe**: The output is idiomatic Rust with proper error handling
- **Output is deterministic**: Same proto file + same tonic_build version = same generated code

### Proto File Content

The proto defines the Orbis threshold signing service:
- `DerivePublicKey` RPC — derive child public keys from ring master keys
- `Sign` RPC — request threshold signatures from a ring
- `SignAlgorithm` enum — BLS, FROST_DECAF377

No sensitive configuration or credentials in the proto file.

## Remediation

No action needed.

## Exploitability

Not exploitable. The proto file is under version control and the build tool is a crates.io dependency.
