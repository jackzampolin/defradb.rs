# Comprehensive Security-Critical Dependency Inventory

**Category:** Reference document
**Status:** Session 3 baseline — 2026-02-21

## Dependency Scale

| Metric | Count |
|--------|-------|
| Workspace members | 30 |
| Direct root dependencies | ~75 |
| Total dependency tree lines | ~2,749 |
| Unique crate versions (Cargo.lock) | ~898 |
| Duplicate crate names | ~50 |
| Non-crates.io dependencies | 1 (iroh-bitswap) |

## Cargo.lock Status

Cargo.lock is **committed** to the repository. All versions are deterministically pinned.

## cargo-deny Status

**No `deny.toml` exists** — cargo-deny runs with defaults only.

## cargo audit Results

| Category | Count | Details |
|----------|-------|---------|
| Vulnerabilities | 3 | ring 0.16.20, wasmtime 27.0.0 (×2 CVEs) |
| Unsound | 1 | lru 0.12.5 |
| Unmaintained | 7 | serde_cbor, derivative, fxhash, instant, yaml-rust, proc-macro-crate 1.x, wasm-timer |

## Security-Critical Dependencies

### Cryptography

| Crate | Version | Source | CVEs | Status |
|-------|---------|--------|------|--------|
| ed25519-dalek | 2.2.0 | RustCrypto | None | Safe (post-2.0) |
| k256 | 0.13.4 | RustCrypto | None | Current |
| p256 | 0.13.2 | RustCrypto | None | Current |
| aes-gcm | 0.10.3 | RustCrypto | None | Current |
| sha2 | 0.10.9 | RustCrypto | None | Current (asm) |
| hmac | 0.12.1 | RustCrypto | None | Current |
| hkdf | 0.12.4 | RustCrypto | None | Current |
| x25519-dalek | 2.0.1 | RustCrypto | None | Current |
| chacha20poly1305 | 0.10.1 | RustCrypto | None | Transitive only |
| blst | 0.3.16 | Supranational | None | C lib, audited |
| josekit | 0.8.7 | josekit | None | Outdated (latest 0.10.3) |

### P2P Networking

| Crate | Version | CVEs | Status |
|-------|---------|------|--------|
| libp2p | 0.53.2 | None direct | Behind latest, pinned by iroh-bitswap |
| libp2p-noise | 0.44.0 | None | Noise mandatory, no downgrade |
| libp2p-yamux | 0.45.2 | None | Uses yamux 0.12.1 |
| libp2p-gossipsub | 0.46.1 | None | Current for 0.53 |
| libp2p-kad | 0.45.3 | None | Current for 0.53 |
| libp2p-quic | 0.10.3 | Transitive (ring) | Pulls vulnerable ring 0.16.20 |
| libp2p-stream | 0.1.0-alpha.1 | None | **Alpha** crate |
| iroh-bitswap | 0.2.0 | None | **Git dep**, stale transitive deps |

### Storage

| Crate | Version | Type | CVEs | Status |
|-------|---------|------|------|--------|
| redb | 2.6.3 | Pure Rust | None | Current |
| fjall | 3.0.2 | Pure Rust | None | Current |
| rocksdb | 0.22.0 | C++ wrapper | None | Optional feature |
| rusty-leveldb | 4.x | Pure Rust | None | WASM only |

### WASM Runtime

| Crate | Version | CVEs | Status |
|-------|---------|------|--------|
| wasmtime | 27.0.0 | **3 CVEs** | 14 versions behind (latest 41.x) |

### Serialization

| Crate | Version | CVEs | Status |
|-------|---------|------|--------|
| serde | 1.0.228 | None | Current |
| serde_json | 1.0.149 | None | Current |
| serde_cbor | 0.11.2 | None | **Unmaintained since 2021** |
| ciborium | 0.2.2 | None | Current (serde_cbor replacement) |
| serde_ipld_dagcbor | 0.6.x | None | Current |

### HTTP Stack

| Crate | Version | CVEs | Status |
|-------|---------|------|--------|
| axum | 0.7.9 | None | Current |
| hyper | 1.8.1 | None | Current |
| tower | 0.4.13 | None | Current (also 0.5.3 in tree) |
| tower-http | 0.5.2 | None | Current |
| reqwest | 0.12.28 | None | Slightly behind (0.13.2 available) |

### Build-Time

| Crate | Version | Type | Status |
|-------|---------|------|--------|
| cbindgen | 0.28.0 | Build dep | Slightly behind (0.29.2) |
| tonic-build | 0.12.x | Build dep | Current |

## Supply Chain Summary

| Check | Result |
|-------|--------|
| Cargo.lock committed | Yes |
| Non-crates.io deps | 1 (iroh-bitswap git) |
| [patch] overrides | None |
| Custom registries | None |
| cargo-deny configured | **No** |
| Build scripts | 3 (all benign) |
| Proc-macro deps | ~20 (all well-known) |
| Unsafe features enabled | None |

## Priority Remediation

1. **Critical:** Upgrade wasmtime 27 → 38+ (3 CVEs)
2. **High:** Add deny.toml and CI enforcement
3. **High:** Migrate serde_cbor → ciborium
4. **Medium:** Update iroh-bitswap or replace (eliminates ~80% of duplicates)
5. **Medium:** Upgrade libp2p when iroh-bitswap allows it
6. **Low:** Upgrade josekit 0.8 → 0.10
7. **Monitor:** lru unsoundness fix, ring CVE via QUIC path reachability
