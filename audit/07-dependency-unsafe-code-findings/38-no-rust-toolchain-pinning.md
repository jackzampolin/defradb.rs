# No Rust Toolchain Pinning

**Severity:** Low
**Category:** Build reproducibility — Compiler version drift
**Status:** Yellow — MSRV documented but no pinning file

## Summary

No `rust-toolchain.toml` or `rust-toolchain` file exists. The minimum supported Rust version (MSRV) is documented as `1.82` in the workspace `Cargo.toml` (`rust-version = "1.82"`), but nothing forces developers or CI to use a specific compiler version.

## Affected Files

- Missing: `rust-toolchain.toml` (does not exist)
- `Cargo.toml:36` — `rust-version = "1.82"`
- `.github/workflows/ci.yml:18` — `dtolnay/rust-toolchain@stable`
- `Dockerfile:1` — `FROM rust:1.93-bookworm`

## Details

### Current State

| Build Environment | Rust Version Used |
|---|---|
| CI (ci.yml) | Latest stable (floating via `@stable`) |
| Docker build | 1.93 (hardcoded in Dockerfile) |
| Developer local | Whatever `rustup` provides |
| Release build | Latest stable at release time |

### Security Implications

1. **Different compiler versions produce different codegen.** While semantics should be preserved, optimizer behavior, stack layout, and padding can vary. For a project with FFI boundaries and `unsafe` code, this means the binary behavior could subtly differ between environments.

2. **Rust occasionally fixes soundness bugs** that change compilation behavior. A newer compiler might reject previously-accepted code, or an older compiler might miss a diagnostic.

3. **The MSRV (1.82) is 11 minor versions behind 1.93** (Docker) and even further behind current stable. This wide gap means the project technically supports a large range of compiler versions, increasing the testing surface.

4. **No nightly features are used.** The project is stable-only, which is good.

### Not a Critical Risk

Rust's stability guarantee means different stable versions should produce semantically identical behavior. The risk is non-deterministic optimization behavior affecting timing, not correctness.

## Remediation

Create `rust-toolchain.toml` at the project root:

```toml
[toolchain]
channel = "1.84.0"
```

This ensures all developers and CI use the same compiler version. Update periodically (e.g., quarterly) after verifying the new version builds and tests cleanly.

## Exploitability

Not directly exploitable. The risk is build non-determinism, not a vulnerability.
