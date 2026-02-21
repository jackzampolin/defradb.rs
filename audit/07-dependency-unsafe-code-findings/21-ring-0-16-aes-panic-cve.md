# ring 0.16.20: AES Panic on Overflow (RUSTSEC-2025-0009)

**Severity:** Medium
**Category:** Known CVE — Crypto dependency
**Status:** Vulnerable (transitive, blocked by libp2p 0.53 pinning)

## Summary

`ring` v0.16.20 has a known vulnerability where some AES functions may panic when overflow checking is enabled. Our pinned version (0.16.20) is within the affected range. The fix requires upgrading to `ring >= 0.17.12`.

## Affected Crate(s)

- `ring` v0.16.20 (transitive dependency)

## Dependency Chain

```
ring 0.16.20
  └── rcgen 0.11.3
      └── libp2p-tls 0.4.1
          └── libp2p-quic 0.10.3
              └── libp2p 0.53.2
```

## Details

- **Advisory ID:** RUSTSEC-2025-0009
- **Fix Available:** Yes — `ring >= 0.17.12`
- **Impact:** AES operations can panic under specific conditions, causing a denial-of-service. The panic occurs in overflow-checked arithmetic within AES routines.
- **Trigger:** Specific AES operations with overflow checking enabled. In practice, this is reachable through the QUIC transport layer via libp2p-tls.
- **Exploitability:** Low-to-medium. Requires crafting TLS handshake parameters that trigger the overflow path. Remote attacker could potentially cause node crash.

## Remediation

Cannot upgrade `ring` independently — it's pinned by `libp2p-quic 0.10.3` which depends on `rcgen 0.11.3` which depends on `ring 0.16.x`. The fix requires upgrading libp2p to a version that uses `ring >= 0.17.x`.

**Workaround:** Since libp2p-quic is pulled in as part of libp2p 0.53 but the project only uses TCP transport (features = ["tcp", ...]), the QUIC code path may not be actively reachable. Verify that QUIC transport is not instantiated at runtime.

**Long-term:** Upgrade libp2p to latest (0.54+) which uses updated dependencies.
