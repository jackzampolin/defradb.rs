# PIR research archive

This directory contains every historical experiment and evidence ledger used to
select the product paths. They are not runtime protocol choices. Start with
[the decision guide](../DECISIONS.md) or [benchmark evidence](../BENCHMARKS.md);
enter this directory only to audit a decision or reproduce a measurement.

The default build excludes DefraDB embedding, unsafe PtrHash/epserde client
metadata, SinglePass, finite-differences adapters, Fuse/Ribbon/subset-XOR layouts,
portable gates and historical benchmark matrices. Opt in explicitly:

```bash
cargo check -p pir-poc --features research
cargo run -p pir-poc --release --features research -- research cold quick
```

Protocol research also excludes DefraDB dependencies. Opt into the embedded
query/export and committed-event contract demos explicitly:

```bash
cargo run -p pir-poc --release --features defra-integration -- research defra-events
```

Available research benchmark names:

- [Complete B0–B8 benchmark suite](ALL_BENCHMARKS.md): `run_all_benchmarks.py`,
  including served private indexes, MPC, Zelda, ORAM, GPU and lifecycle cases.

- `total-work CONFIG.json` — [fresh-process aggregate-work runner](TOTAL_WORK_RUNNER.md), field-bit index experiments and bounded feasibility screening

- `active-nullifier`
- `billion-tag`
- `cold`
- `cpu-snapshot` (same-host two-replica Dense and 100-candidate control at `2^23 x 120 B`)
- `defra-events` (requires `defra-integration`; executable committed-event/export seam)
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

- [Indexed Dense across all use-case shapes](INDEXED_USE_CASE_MEASUREMENTS.md):
  five-repeat cold and reused-client comparisons with XOR layouts and SinglePass,
  canonical Poseidon witnesses, skewed all-match queries and public/packed/indexed
  epoch presence. Reproduce with `run_indexed_use_cases.py`; machine-readable
  results and resource failures are retained beside the report. This follow-up
  updates [the decision guide](../DECISIONS.md).
- [COLD_QUERY_EXPERIMENT_PLAN.md](COLD_QUERY_EXPERIMENT_PLAN.md) and
  [execution ledger](COLD_QUERY_EXECUTION.md): complete Shinzo/Mizu-style cold
  predicate searches, persistent service roles, independent clients, bit owners,
  packed indexes and newer PIR artifact screening. This campaign keeps product
  cold/catch-up searches separate from client-state reuse and cold hardware caches.
  [Measured results and all 24 experiment dispositions](COLD_QUERY_RESULTS.md)
  identify the winning layouts, resource failures and unresolved constructions.
- [TOTAL_WORK_BENCHMARK_PLAN.md](TOTAL_WORK_BENCHMARK_PLAN.md): current benchmark
  plan for aggregate server work, distributed field-bit indexes and client limits;
- [MANY_SERVER_INDEXING.md](MANY_SERVER_INDEXING.md): September 2026 research on
  bit indexes, distributed workers, preprocessing and many-server tradeoffs;
- `COMPARISON.md`: complete protocol comparison archive;
- `EXPLORATION.md`: exploration program and history;
- `ARTIFACTS.md`: external artifact pins and reproduction status;
- `FUSE_BENCHMARK.md`, `RIBBON_BENCHMARK.md`, and `WARM_STATEFUL.md`:
  layout/stateful benchmark records;
- `COMPUTATIONAL_FRONTIER.md`: computational PIR artifacts;
- `HARDWARE_COUNTERS.md`: phase-scoped counter methodology;
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
production listener and remains outside both the default and protocol-only research builds.

## Private index compositions

The six follow-up bit-index experiments, compiled controls, Ramen integration,
and reproduction commands are in [PRIVATE_INDEX_COMPOSITIONS.md](PRIVATE_INDEX_COMPOSITIONS.md).
