# PIR research archive

This directory and the root-level research ledgers preserve experiments used to
select the three POC paths. They are not runtime protocol choices.

The default build excludes DefraDB embedding, unsafe PtrHash/epserde client
metadata, SinglePass, finite-differences adapters, Fuse/Ribbon/subset-XOR layouts,
portable gates and historical benchmark matrices. Opt in explicitly:

```bash
cargo check -p pir-poc --features research
cargo run -p pir-poc --release --features research -- research cold quick
```

Available research benchmark names:

- `active-nullifier`
- `billion-tag`
- `cold`
- `defra-events` (executable DefraDB `EventName::Update` adapter demonstration)
- `dense-batch`
- `end-to-end`
- `endpoints`
- `fuse`
- `mphf`
- `mphf-subset-xor`
- `optimization`
- `ribbon`
- `single-pass`
- `subset-xor`
- `warm-stateful`

Primary evidence ledgers:

- `../COMPARISON.md`: protocol comparison and decisions;
- `../EXPLORATION.md`: exploration history;
- `COMPUTATIONAL_FRONTIER.md`: computational PIR artifacts;
- `../HARDWARE_COUNTERS.md`: phase-scoped counter methodology;
- `../PORTABLE_READINESS.md`: portability evidence and blockers.

Research results may use different privacy, result-shape, state and threat-model
assumptions. Do not publish direct speed ratios unless those scopes match.

`research defra-events` listens to updates from an embedded DefraDB node,
evaluates a Compact-DPF subscription, seals a snapshot, and privately retrieves
the matching value. It proves the intended integration seam; it is not a
production listener and remains outside the default sidecar binary.
