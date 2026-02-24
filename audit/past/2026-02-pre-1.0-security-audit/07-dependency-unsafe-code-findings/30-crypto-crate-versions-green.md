# Crypto Crate Version Audit — All Current

**Severity:** Informational
**Category:** Crypto dependency versions
**Status:** Green — all directly-used crypto crates are at current, safe versions

## Summary

All security-critical cryptographic crates used directly by the project are at safe, current versions from the RustCrypto project. No known vulnerabilities in the pinned versions.

## Inventory

| Crate | Version | Source | Status |
|-------|---------|--------|--------|
| ed25519-dalek | 2.2.0 | RustCrypto | Safe (post-2.0, fixes double-pub-key vuln) |
| k256 | 0.13.4 | RustCrypto | Current |
| p256 | 0.13.2 | RustCrypto | Current |
| aes-gcm | 0.10.3 | RustCrypto | Current |
| sha2 | 0.10.9 | RustCrypto | Current (asm feature enabled) |
| hmac | 0.12.1 | RustCrypto | Current |
| hkdf | 0.12.4 | RustCrypto | Current |
| x25519-dalek | 2.0.1 | RustCrypto | Current (static_secrets feature) |
| blst | 0.3.16 | Supranational | Current |
| chacha20poly1305 | 0.10.1 | RustCrypto | Current (transitive, not directly used) |

## Key Observations

### ed25519-dalek 2.2.0 — Safe
Version 2.0+ fixes the "double public key" vulnerability (CVE-2022-41948) that affected 1.x. Our version 2.2.0 is safe.

### aes-gcm 0.10.3 — Correct Feature Set
Uses `aes` + `ghash` + `ctr` internally. The `aes` crate enables hardware AES-NI acceleration via `cpufeatures`. No missing security-relevant features.

### sha2 0.10.9 with `asm` Feature
The `asm` feature enables SHA-2 assembly acceleration via `sha2-asm`, which compiles platform-specific assembly using the `cc` crate. This is the recommended configuration for performance.

### chacha20poly1305 — Transitive Only
Version 0.10.1 is in the tree but not directly used by any defradb.rs crate. It's a transitive dependency (likely via libp2p-noise for the Noise protocol).

### All RustCrypto Ecosystem
All direct crypto dependencies (except blst) are from the RustCrypto project, ensuring consistent API design, trait implementations, and security audit coverage.

## Remediation

No action needed. Continue tracking RustCrypto releases for security updates.
