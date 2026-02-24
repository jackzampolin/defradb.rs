# blst 0.3.16: C Library for BLS12-381

**Severity:** Low
**Category:** C/C++ wrapper — Memory safety boundary
**Status:** Green — well-maintained, audited library

## Summary

`blst` v0.3.16 is a Rust wrapper around the `blst` C library for BLS12-381 threshold signatures, used by the `crypto` crate for Orbis ring signature verification. The underlying C library is developed and maintained by the Ethereum Foundation and Supranational, and has been extensively audited.

## Affected Crate(s)

- `blst` v0.3.16 (direct dependency of `crypto`)

## Details

- **Upstream:** https://github.com/supranational/blst
- **Language:** C with inline assembly (x86-64, ARM64)
- **Audits:** Multiple formal audits (NCC Group, others) funded by Ethereum Foundation
- **Build:** Compiles C code from source via `cc` crate during build
- **No known CVEs** in current version
- **Assembly optimizations:** Uses hand-optimized assembly for performance-critical operations

## Risk Assessment

**Low risk.** blst is one of the most scrutinized BLS12-381 implementations in the ecosystem. It's used across major blockchain projects (Ethereum 2.0 validators). The C code does cross the memory safety boundary, but the attack surface is limited to well-defined cryptographic operations with fixed-size inputs.

## Compiler Flags

The blst build.rs compiles with the `cc` crate's default flags. On macOS/Linux with release profile, this typically includes `-O2` but may not include `-fstack-protector` or `-D_FORTIFY_SOURCE=2`. This is standard for crypto libraries where performance is critical and the code has been formally audited.

## Remediation

No action needed. Keep updated as new versions are released.
