# serde_cbor 0.11.2: Unmaintained Since 2021 (RUSTSEC-2021-0127)

**Severity:** Medium
**Category:** Unmaintained dependency — Serialization
**Status:** Actively used in 3 crates; migration to ciborium required

## Summary

`serde_cbor` v0.11.2 has been officially unmaintained since August 2021. The crate author recommends migrating to `ciborium`. Despite this, `serde_cbor` is a direct workspace dependency used in `db`, `p2p`, and `storage` — all core crates in the data path.

## Affected Crate(s)

- `serde_cbor` v0.11.2 (direct workspace dependency)

## Direct Consumers

| Crate | Usage |
|-------|-------|
| `db` | CRDT delta serialization, document encoding |
| `p2p` | P2P message serialization (two-stream protocol) |
| `storage` | Key-value serialization for storage backends |

## Details

- **Advisory ID:** RUSTSEC-2021-0127
- **Maintained Replacement:** `ciborium` (already in workspace as `ciborium = "0.2"`)
- **Risk:** No known CVEs in serde_cbor itself, but:
  - No security patches will be issued if vulnerabilities are found
  - No compatibility updates for newer Rust editions
  - Stream 3 (P2P) finding `16-serde-cbor-flatten-indefinite-map-divergence` documented a behavioral divergence between serde_cbor and other CBOR libraries regarding `#[serde(flatten)]` with indefinite-length maps

## Dual CBOR Library Issue

The workspace currently depends on **both** `serde_cbor` (0.11) and `ciborium` (0.2). The `db` crate uses both:
```toml
serde_cbor.workspace = true
ciborium.workspace = true
```

This dual-library situation is itself a risk: different CBOR libraries may produce different byte representations for the same data, leading to divergent content hashes and CIDs.

## Remediation

1. **Phase 1:** Audit all serde_cbor usage sites and verify they're byte-compatible with ciborium
2. **Phase 2:** Replace `serde_cbor` with `ciborium` in `p2p` and `storage` crates
3. **Phase 3:** Remove `serde_cbor` from workspace dependencies entirely
4. **Critical constraint:** P2P wire format must remain backward-compatible during migration. Both the Rust and Go nodes must produce identical CBOR encodings.
