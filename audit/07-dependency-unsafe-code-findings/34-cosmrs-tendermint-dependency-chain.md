# cosmrs 0.22: Tendermint Dependency Chain Analysis

**Severity:** Low
**Category:** Dependency chain — Cosmos SDK integration
**Status:** Informational — pulls in legacy crypto stack

## Summary

`cosmrs` v0.22.0 (Cosmos SDK Rust client) is used by the `sourcehub` crate for SourceHub blockchain integration. It pulls in the Tendermint Rust libraries, which bring a separate crypto stack including `ed25519-consensus`, `curve25519-dalek-ng`, and `sha2 0.9.9`.

## Affected Crate(s)

- `cosmrs` v0.22.0 (direct dependency of `sourcehub`)
- Transitively: `tendermint` v0.40.4, `tendermint-rpc` v0.40.4

## Transitive Crypto Dependencies

```
cosmrs 0.22.0
  └── tendermint 0.40.4
      └── ed25519-consensus 2.1.0
          ├── curve25519-dalek-ng 4.1.1  (separate from main curve25519-dalek 4.1.3)
          ├── sha2 0.9.9                  (old version, separate from sha2 0.10.9)
          └── digest 0.9.0               (old version)
```

Also pulls in:
- `reqwest 0.11.27` via `tendermint-rpc` (old HTTP client)
- `hyper 0.14.32` transitively

## Details

The Tendermint Rust ecosystem uses `ed25519-consensus` instead of `ed25519-dalek` for historical reasons. This crate depends on `curve25519-dalek-ng` (a fork of curve25519-dalek) and the older `sha2 0.9.x`. These are all correct, audited implementations — just different versions from what the rest of the project uses.

## Risk Assessment

**Low.** The Tendermint crypto stack is well-audited (it secures billions in Cosmos ecosystem assets). The duplicate versions don't interact with our crypto primitives — they're used exclusively for Tendermint consensus verification within the SourceHub client.

## Remediation

No immediate action needed. This resolves when the Tendermint Rust libraries upgrade their crypto dependencies. Monitor `cosmrs` releases for version bumps.
