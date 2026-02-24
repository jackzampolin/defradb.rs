# Feature Flag Security Audit

**Severity:** Informational
**Category:** Configuration — Feature flags
**Status:** Green — no unsafe features enabled, proper feature gating

## Summary

Audit of all `default-features = false` and explicit `features = [...]` selections across all Cargo.toml files. No security-relevant features are missing, and no `unsafe-*` or `unchecked` features are enabled.

## Feature Selections

### Crypto Features
| Crate | Features | Assessment |
|-------|----------|------------|
| ed25519-dalek | `["serde"]` | Safe — serde only |
| k256 | `["ecdsa", "serde"]` | Correct — ECDSA signing + serde |
| sha2 | `["asm"]` | Good — hardware acceleration |
| x25519-dalek | `["static_secrets"]` | Correct — needed for ECIES |
| aes-gcm | (default) | Safe — includes `aes` feature by default |

### P2P Features
| Crate | Features | Assessment |
|-------|----------|------------|
| libp2p | `["tcp", "noise", "yamux", "gossipsub", "kad", "identify", "relay"]` + per-crate additions | Correct transport/protocol selection |
| p2p crate also adds | `["request-response", "macros", "tokio"]` | Needed for two-stream protocol |

### Storage Features
| Crate | Features | Assessment |
|-------|----------|------------|
| redb | (default, optional) | Default backend |
| fjall | (optional) | Opt-in |
| rocksdb | (optional) | Opt-in |
| rusty-leveldb | `default-features = false` | Correct — disables fs for WASM |

### default-features = false Usage
| Declaration | Crate | Reason |
|-------------|-------|--------|
| `acp = { ..., default-features = false }` | db | Allows `native` feature to be optional |
| `events = { ..., default-features = false }` | db | Allows `channel` feature to be optional |
| `lens = { ..., default-features = false }` | db, query | Allows `wasmtime-runtime` to be optional (for WASM build) |
| `db = { ..., default-features = false }` | wasm | Disables P2P and native features for browser |
| `storage = { ..., default-features = false, features = ["leveldb"] }` | wasm | LevelDB for browser |

### No Unsafe Features

No Cargo.toml files contain `unsafe`, `unchecked`, or similar feature flags. The `grep` for these patterns returned no results.

## openssl-sys Vendored Feature

The CLI crate has an optional `vendored-openssl` feature:
```toml
[dependencies.openssl-sys]
version = "0.9"
features = ["vendored"]
optional = true
```

This is **not enabled by default** (it's behind the `vendored-openssl` feature flag). When enabled, it compiles OpenSSL from source. This is a common pattern for cross-compilation and is not a security concern.

## Remediation

No action needed. Feature flag configuration is appropriate and secure.
