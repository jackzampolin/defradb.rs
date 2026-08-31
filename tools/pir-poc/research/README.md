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
- `cpu-snapshot` (same-host two-replica Dense and 100-candidate control at `2^23 x 120 B`)
- `defra-events` (executable DefraDB `EventName::Update` adapter demonstration)
- `dense-batch`
- `end-to-end`
- `endpoints`
- `fuse`
- `gpu-reference-decoy` (100 visible ordinal lookups at the published InsPIRe GPU geometries)
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

The CUDA comparison pins the archived GPU-DPF artifact, checks every Dense and
DPF reconstruction, measures NVML power, and includes the packed-presence live
epoch design plus a matched 100-visible-bucket control:

```bash
bash tools/pir-poc/research/run-gpu-pir-defra.sh quick
bash tools/pir-poc/research/run-gpu-pir-defra.sh full
```

See [`gpu_dpf_adapter/README.md`](gpu_dpf_adapter/README.md) for its exact scope,
hardware/toolchain requirements and interpretation limits.

The Ethereum-oriented InsPIRe CUDA server now has a separate pinned same-GPU
runner with cold-client, first-online, preprocessing and small-batch phase
boundaries:

```bash
bash tools/pir-poc/research/run-inspire-gpu-defra.sh quick
bash tools/pir-poc/research/run-inspire-gpu-defra.sh full
```

See [`FULL_COMPARISON.md`](FULL_COMPARISON.md) for the apples-to-apples CPU/GPU
matrix and [`inspire_gpu_adapter/README.md`](inspire_gpu_adapter/README.md) for
the capacity and security qualifications.

The combined runner executes both pinned CUDA suites and joins only matching
hardware/table/batch rows:

```bash
bash tools/pir-poc/research/run-full-gpu-comparison.sh quick
bash tools/pir-poc/research/run-full-gpu-comparison.sh full
```

The final small-batch publication runner starts five fresh processes,
alternates both suite order and Dense/DPF internal order, and aggregates
p50/min/max without deleting prior evidence:

```bash
bash tools/pir-poc/research/run-repeated-gpu-comparison.sh
```

The same-host CPU lane compares the Rust control with pinned Poulpy InsPIRe2
on AVX2/FMA:

```bash
cargo run -p pir-poc --release --features research -- \
  research cpu-snapshot full
bash tools/pir-poc/research/run-poulpy-cpu-defra.sh full
```

Poulpy's batch list defaults to `1 8 32` and can be changed with
`DEFRA_POULPY_BATCHES`. The generated JSON distinguishes server wall from the
sum of its measured parallel phases.

`research defra-events` listens to updates from an embedded DefraDB node,
evaluates a Compact-DPF subscription, seals a snapshot, and privately retrieves
the matching value. It proves the intended integration seam; it is not a
production listener and remains outside the default sidecar binary.
