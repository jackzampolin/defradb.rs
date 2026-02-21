# josekit 0.8.7: JWT/JWE Library Audit

**Severity:** Low-Medium
**Category:** Crypto dependency — JWT/JWE processing
**Status:** Outdated (current: 0.10.3), used for keyring JWE encryption

## Summary

`josekit` v0.8.7 is used by the `keyring` crate for JWE encryption (PBES2-HS512-A256KW format, Go-compatible). It's 2 minor versions behind the latest (0.10.3). josekit is a JOSE (JSON Object Signing and Encryption) toolkit.

## Affected Crate(s)

- `josekit` v0.8.7 (direct dependency of `keyring`)

## Details

- **Usage:** JWE encryption for storing keys in the OS keyring in a Go-compatible format
- **Latest version:** 0.10.3
- **Known issues:** No CVEs in RustSec database for josekit

## Risk Assessment

**Low-Medium.** josekit processes cryptographic material (encrypting/decrypting private keys). Being 2 minor versions behind means missing potential security hardening. The JWE format is used for local key storage, not for network-facing operations, which limits the attack surface to local file access.

## Relationship to Stream 1 Finding

Stream 1 finding `05-jwt-algorithm-dispatch-from-header` identified that JWT algorithm selection comes from the header (alg field), which is the standard JOSE behavior but means a tampered JWT could request an unexpected algorithm. This applies to josekit's JWT verification path if used for that purpose.

## Remediation

Upgrade `josekit` from 0.8.7 to 0.10.3. Check changelog for breaking changes.
