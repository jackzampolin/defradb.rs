# sha2 Duplicate Versions: 0.9.9 and 0.10.9

**Severity:** Low
**Category:** Duplicate crypto crate — Correctness risk
**Status:** Informational — caused by cosmrs/tendermint dependency chain

## Summary

Two versions of `sha2` exist in the dependency tree: v0.9.9 (old, via cosmrs/tendermint) and v0.10.9 (current, used directly). While both versions are correct implementations, having two SHA-256 implementations increases binary size and could cause confusion if types from different versions are mixed.

## Affected Crate(s)

- `sha2` v0.9.9 (transitive)
- `sha2` v0.10.9 (direct workspace dependency)

## Dependency Chains

### sha2 0.10.9 (direct — correct version)
Used by: `crypto`, `db`, `ffi`, `lens`, `acp`, `wasm`, `cli`, `sourcehub`, `document`

### sha2 0.9.9 (transitive — old version)
```
sha2 0.9.9
  └── ed25519-consensus 2.1.0
      └── tendermint 0.40.4
          └── cosmrs 0.22.0
              └── sourcehub 0.5.0
```

## Also Duplicated: digest 0.9.0

The old sha2 pulls in `digest 0.9.0` (alongside `digest 0.10.7`), and also brings in `curve25519-dalek-ng 4.1.1` (a different curve25519 implementation from the main `curve25519-dalek 4.1.3`).

## Risk Assessment

**Low risk.** The old sha2 is only used internally by `ed25519-consensus` (a Tendermint-specific Ed25519 crate) within the Cosmos SDK dependency chain. It doesn't interact with our crypto crate's sha2 usage. The types are incompatible (different `Digest` trait versions), so accidental mixing would be a compile error, not a silent bug.

## Remediation

This resolves automatically when `cosmrs` upgrades its `tendermint` dependency to use `ed25519-consensus` with sha2 0.10.x, or switches to `ed25519-dalek`. No action needed from our side unless `cosmrs` is upgraded.
