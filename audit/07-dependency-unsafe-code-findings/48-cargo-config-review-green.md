# .cargo/config.toml Review — Safe

**Severity:** Informational
**Category:** Build configuration — Cargo settings
**Status:** Green — minimal, safe configuration

## Summary

The `.cargo/config.toml` contains only build parallelism and dev profile settings. No custom rustflags, linker overrides, target configurations, source replacements, or registry overrides are configured.

## Affected Files

- `.cargo/config.toml` (12 lines)

## Details

### Full Configuration

```toml
[build]
jobs = 16

[profile.dev]
split-debuginfo = "unpacked"

[profile.dev.package."*"]
opt-level = 2
```

### Security Assessment

| Setting | Value | Risk |
|---|---|---|
| `jobs = 16` | Build parallelism | None — only affects build speed |
| `split-debuginfo = "unpacked"` | Faster incremental macOS builds | None — dev only, not in release |
| `opt-level = 2` for dependencies | Optimize deps in dev | None — performance only |

### What's Not Configured (Good)

- **No `[source]` overrides** — all dependencies come from crates.io (except iroh-bitswap git dep in Cargo.toml)
- **No `[registries]`** — no custom registries that could serve malicious crates
- **No custom `rustflags`** — no flags that disable safety checks (e.g., `-C overflow-checks=no`, `-Z allow-features=...`)
- **No custom `linker`** — uses the default system linker, no injection point
- **No `[target.*.linker]`** — no cross-compilation linker overrides
- **No `[env]`** — no environment variable overrides that could affect build behavior
- **No `[net]`** — no custom network settings for crate downloads

## Remediation

No action needed.

## Exploitability

Not exploitable. The configuration is minimal and safe.
