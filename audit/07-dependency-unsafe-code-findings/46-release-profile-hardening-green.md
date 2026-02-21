# Release Profile Hardening — Strong

**Severity:** Informational
**Category:** Compiler hardening — Release configuration
**Status:** Green — excellent hardening for FFI binary

## Summary

The release profile has strong security hardening settings: LTO, single codegen unit, symbol stripping, and abort-on-panic. These are especially important for an FFI library where unwinding across language boundaries is undefined behavior.

## Affected Files

- `Cargo.toml:121-126` — `[profile.release]`
- `.cargo/config.toml:1-12` — dev profile optimizations

## Details

### Release Profile

```toml
[profile.release]
opt-level = 3      # Maximum optimization
lto = true          # Link-Time Optimization
codegen-units = 1   # Single compilation unit
strip = true        # Remove debug symbols
panic = "abort"     # No unwinding
```

### Security Assessment

| Setting | Value | Security Benefit |
|---|---|---|
| `opt-level = 3` | Max optimization | Enables dead code elimination, reduces attack surface |
| `lto = true` | Full LTO | Cross-module optimization, removes unused functions at link time |
| `codegen-units = 1` | Single unit | Better optimization, consistent behavior |
| `strip = true` | Symbols removed | Prevents reverse engineering of internal structure |
| `panic = "abort"` | No unwinding | **Critical for FFI** — prevents stack unwinding across C/Go boundaries which is UB |

### panic = "abort" Is Especially Important

This project exposes 84+ FFI functions via `extern "C"`. If a Rust function panics and the stack unwinds into C/Go code, the behavior is undefined — typically a segfault or worse. `panic = "abort"` ensures any panic terminates the process immediately rather than attempting to unwind.

### Dev Profile Optimizations

```toml
[profile.dev.package."*"]
opt-level = 2
```

This optimizes dependencies (but not the project code) in dev builds. This is motivated by crypto library performance (`ed25519-dalek`, `k256`, `sha2` are unusably slow at opt-level 0) and does not affect security.

### Test Profile

```toml
[profile.test]
opt-level = 1
```

Slight optimization in test builds for performance. Tests run with debug assertions and overflow checks enabled (Rust default for non-release profiles).

### Gap: overflow-checks

The one missing hardening option is `overflow-checks = true` (see finding 41). All other settings are optimal.

## Remediation

No action needed for existing settings. See finding 41 for the overflow-checks recommendation.

## Exploitability

N/A — this finding documents positive security controls.
