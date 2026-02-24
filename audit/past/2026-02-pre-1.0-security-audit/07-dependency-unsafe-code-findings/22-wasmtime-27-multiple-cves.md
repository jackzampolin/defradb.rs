# wasmtime 27.0.0: Multiple CVEs (RUSTSEC-2025-0046, RUSTSEC-2025-0118, RUSTSEC-2026-0006)

**Severity:** Medium (cumulative; individual severities Low-to-Medium)
**Category:** Known CVEs — WASM sandbox
**Status:** Vulnerable — 14 major versions behind latest

## Summary

`wasmtime` v27.0.0 is affected by at least three known security advisories. The current latest is v41.0.3 — we are **14 major versions behind**. While individual CVE severities are low, the cumulative risk of running such an old version of a sandbox runtime is significant.

## Affected Crate(s)

- `wasmtime` v27.0.0 (direct dependency of `lens` crate)

## CVE Details

### RUSTSEC-2025-0046: Host panic with `fd_renumber` WASIp1 function
- **Severity:** 3.3 (Low)
- **Fix:** `>= 34.0.2` or `>= 24.0.4, < 25.0.0`
- **Impact:** Guest WASM module can trigger host panic via `fd_renumber` with specific parameters, causing state corruption and subsequent panic in `path_open`. No memory unsafety or sandbox escape.

### RUSTSEC-2025-0118: Unsound API access to shared linear memory
- **Severity:** 1.8 (Low)
- **Fix:** `>= 38.0.4` or `>= 24.0.5, < 25.0.0`
- **Impact:** Unsound access to WebAssembly shared linear memory. API soundness issue that could theoretically lead to memory safety violations if shared memory is used.

### RUSTSEC-2026-0006: f64.copysign segfault on x86-64
- **Severity:** Not yet scored
- **Fix:** Specific patched versions (check advisory)
- **Impact:** The `f64.copysign` operator can cause a segfault or an unused out-of-sandbox load on x86-64 architecture. This is a potential sandbox escape vector.

## Dependency Chain

```
wasmtime 27.0.0
  └── lens 0.5.0
      ├── query 0.5.0
      ├── db 0.5.0
      ├── cli 0.5.0
      └── ffi 0.5.0
```

## Risk Assessment

The lens crate executes user-provided WASM modules for schema migration transforms. A malicious or crafted WASM module could:
1. Crash the node (DoS via RUSTSEC-2025-0046)
2. Potentially escape the sandbox (RUSTSEC-2026-0006)
3. Access memory unsafely (RUSTSEC-2025-0118)

## Remediation

Upgrade `wasmtime` from 27.0.0 to latest stable (41.x). This is a major version jump that will likely require API changes in the `lens` crate.

**Minimum safe version:** 38.0.4 (fixes all three CVEs), but 41.x preferred for ongoing security support.

**Interim mitigation:** The WASM modules executed by lens are typically project-authored transform modules, not arbitrary user code. If lens only loads trusted modules, the attack surface is reduced but not eliminated (a compromised transform module could exploit these).
