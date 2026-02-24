# Session 3 Summary: Dependency & Supply Chain Audit

## Scope

Systematic scan of all direct and transitive dependencies (~898 crate versions) for known CVEs, supply chain risks, outdated crates, and configuration gaps.

## Tools Used

- `cargo audit` v0.22.1 (RustSec advisory database)
- `cargo deny` v0.19.0 (advisory/license/source checks)
- `cargo outdated` v0.17.0 (version comparison)
- `cargo tree --duplicates` (duplicate detection)
- Manual review of all security-critical Cargo.toml files

## Findings Summary

### By Severity

| Severity | Count | Findings |
|----------|-------|----------|
| Medium | 6 | ring CVE, wasmtime CVEs, lru unsoundness, serde_cbor unmaintained, iroh-bitswap supply chain, no cargo-deny |
| Low | 5 | sha2 duplicates, blst audit, cosmrs chain, josekit outdated, duplicate inventory |
| Informational | 3 | crypto versions green, build scripts green, feature flags green |

### Vulnerabilities (cargo audit)

| ID | Crate | Severity | Fix Available |
|----|-------|----------|---------------|
| RUSTSEC-2025-0009 | ring 0.16.20 | Medium | Yes (ring ≥ 0.17.12, blocked by libp2p) |
| RUSTSEC-2025-0046 | wasmtime 27.0.0 | Low (3.3) | Yes (≥ 24.0.4) |
| RUSTSEC-2025-0118 | wasmtime 27.0.0 | Low (1.8) | Yes (≥ 24.0.5) |
| RUSTSEC-2026-0006 | wasmtime 27.0.0 | TBD | Yes (patched versions) |
| RUSTSEC-2026-0002 | lru 0.12.5 | Medium | **No** (unsound, no fix yet) |

### Unmaintained Crates

| Crate | Since | Root Cause |
|-------|-------|------------|
| serde_cbor 0.11.2 | 2021 | Direct dependency |
| derivative 2.2.0 | 2024 | via iroh-bitswap |
| fxhash 0.2.1 | 2025 | via wasmtime |
| instant 0.1.13 | 2024 | via iroh-bitswap |
| yaml-rust 0.4.5 | 2024 | via iroh-bitswap |
| proc-macro-crate 1.1.3 | — | via iroh-bitswap |
| wasm-timer 0.2.5 | — | via iroh-bitswap |

### Supply Chain

| Risk | Status |
|------|--------|
| Non-crates.io deps | 1 (iroh-bitswap, project-controlled fork) |
| Patch overrides | None |
| Custom registries | None |
| Cargo.lock committed | Yes |
| cargo-deny policy | **Missing** |
| Build scripts | 3 (all benign) |

## Key Observations

### 1. iroh-bitswap is the Primary Debt Source
The single git dependency `iroh-bitswap` from the beetle fork is responsible for:
- 5 of 7 unmaintained crate warnings
- ~80% of the ~50 duplicate crate versions
- The `ring 0.16.20` vulnerability (via libp2p-quic)
- Old versions of hyper, reqwest, tonic, axum, prost, parking_lot

### 2. Crypto Stack is Clean
All directly-chosen cryptographic dependencies are at current, safe versions from the RustCrypto project. ed25519-dalek is correctly at 2.x (post-vulnerability). The only crypto duplicates are caused by transitive dependencies (cosmrs/tendermint).

### 3. wasmtime is Significantly Behind
At v27.0.0 vs latest v41.x (14 major versions), wasmtime has accumulated 3 CVEs. While individual severities are low, the cumulative risk for a sandbox runtime is concerning.

### 4. No Automated Policy Enforcement
The absence of `deny.toml` means no CI gate for vulnerabilities, license violations, or banned crates.

## Recommended Priority Actions

1. **Upgrade wasmtime** 27 → 38+ (fixes all 3 CVEs)
2. **Create deny.toml** with advisory, license, and source policies
3. **Migrate serde_cbor → ciborium** in db, p2p, storage crates
4. **Modernize or replace iroh-bitswap** to eliminate dependency debt
5. **Verify QUIC transport is not runtime-reachable** (mitigates ring CVE)

## Remaining for Sessions 4-5

- Session 4: `unsafe` code patterns in third-party dependencies
- Session 5: Comprehensive unsafe inventory across the full dependency tree, MIRI testing recommendations
