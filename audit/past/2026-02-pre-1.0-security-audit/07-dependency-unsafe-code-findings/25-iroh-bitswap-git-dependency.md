# iroh-bitswap: Git Dependency with Stale Transitive Dependencies

**Severity:** Medium
**Category:** Supply chain risk — Git dependency
**Status:** Pulls in multiple unmaintained and outdated transitive dependencies

## Summary

`iroh-bitswap` is sourced from a git repository (`https://github.com/sourcenetwork/beetle`, branch `main`) rather than crates.io. This is the **only non-crates.io dependency** in the entire project. It pulls in a chain of outdated dependencies that account for the majority of unmaintained crate warnings.

## Affected Crate(s)

- `iroh-bitswap` v0.2.0 (git dependency)
- Transitively: `iroh-util` v0.2.0, `iroh-metrics` v0.2.0

## Stale Transitive Dependencies Pulled By iroh-bitswap

| Crate | Issue |
|-------|-------|
| `derivative` v2.2.0 | Unmaintained (RUSTSEC-2024-0388) |
| `instant` v0.1.13 | Unmaintained (RUSTSEC-2024-0384) |
| `yaml-rust` v0.4.5 | Unmaintained (RUSTSEC-2024-0320) |
| `reqwest` v0.11.27 | Old version (current: 0.13.x), pulls in hyper 0.14 |
| `hyper` v0.14.32 | Old version (current: 1.x) |
| `tonic` v0.8.3 | Old version (current: 0.12.x) |
| `axum` v0.6.20 | Old version (current: 0.7.x) |
| `prost` v0.11.9 | Old version (current: 0.13.x) |
| `parking_lot` v0.11.2 | Old version (current: 0.12.x) |
| `opentelemetry` v0.18.0 | Very old (current: 0.20+) |

## Supply Chain Risk

1. **No crates.io audit trail:** Git dependencies bypass crates.io's download counting, version history, and yanking mechanisms
2. **Branch tracking:** The dependency tracks `main` branch, meaning any push to `main` in the beetle repo changes what's compiled
3. **Commit pinned in Cargo.lock:** Currently pinned to `48e70b03`, but `cargo update` would silently advance it
4. **Fork ownership:** The repo is `sourcenetwork/beetle` (project-controlled), which mitigates external compromise risk but still means a compromised GitHub account could inject code

## Impact Analysis

The iroh-bitswap dependency is responsible for **most** of the duplicate crate versions in the tree:
- 2 versions of `hyper` (0.14 + 1.x)
- 2 versions of `reqwest` (0.11 + 0.12)
- 2 versions of `tonic` (0.8 + 0.12)
- 2 versions of `axum` (0.6 + 0.7)
- 2 versions of `prost` (0.11 + 0.13)
- 2 versions of `parking_lot` (0.11 + 0.12)

This significantly increases binary size and attack surface.

## Remediation

1. **Short-term:** Pin to a specific commit hash in Cargo.toml instead of tracking `main`
2. **Medium-term:** Update the beetle fork to use modern dependency versions (hyper 1.x, reqwest 0.12+, tonic 0.12)
3. **Long-term:** Publish iroh-bitswap to crates.io or vendor the code directly into the project
4. **Alternative:** Evaluate whether the iroh-bitswap implementation could be replaced with a simpler Bitswap client built directly on libp2p
