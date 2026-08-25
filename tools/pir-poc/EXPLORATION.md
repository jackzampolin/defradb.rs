# PIR exploration and optimization program

This program is complete for the current POC selection and is retained as a
research archive. Runtime behavior and current measurements are documented in
`USE_CASES.md`.

This document is the execution ledger for the DefraDB PIR POC. It separates
measured results from analytical estimates and prevents results with different
privacy, leakage, result, or amortization assumptions from being presented as
direct comparisons.

## Objective

The primary objective is to minimize amortized **aggregate server work per
useful private result**:

```text
global build work / all queries in the generation
+ per-client server setup work / that client's queries
+ sum of online work over every server
+ amortized maintenance work
```

There is deliberately no single synthetic "work score". Reports keep these
independent:

- aggregate server CPU time, cycles, and instructions;
- aggregate logical and physical memory/storage bytes;
- aggregate server energy when a real counter is available;
- build time, peak build memory, and persisted storage;
- client CPU, peak memory, persistent state, upload, and download;
- wall latency, throughput, and maximum individual-server work.

The useful result includes private predicate lookup, every matching locator,
the requested projection or document, cardinality padding, and client-side
verification. A benchmark that returns only a locator is not directly
comparable with one that returns the complete projection.

## Main security line

- `n` servers and information-theoretic query privacy against any `n - 1`
  colluding semi-honest servers.
- All `n` answers are required unless the result is explicitly labeled as a
  weaker privacy threshold or a robust/verifiable construction.
- Two servers are the default because ordinary replicated Dense XOR performs
  approximately `n / 2` tables of selected-row work in aggregate.
- A one-honest-of-three privacy claim does not imply correctness if two servers
  are malicious. Signed manifests and CIDs can detect some corruption but do
  not recover the correct answer.

Single-server computational PIR, weaker collusion thresholds, TEEs, and ORAM
are retained as separate Pareto lanes. Their measurements never silently enter
the main security comparison.

## Leakage classes

| Class | Server-visible query scope |
|---|---|
| strict-global | Collection/schema/snapshot only |
| public-window | A coarse time range or immutable segment set |
| public-partition | Window plus namespace or hash-prefix partition |
| decoy | Candidate set, access volume, and ordinary lookup behavior |

The selected collection, snapshot generation, requested projection, batch
size, response size, timing, and failure behavior are recorded explicitly.

## Client classes

These are reporting labels, not research rejection thresholds.

| Class | Peak RAM | Online traffic | Online CPU | Persistent state |
|---|---:|---:|---:|---:|
| phone-friendly | <= 256 MiB | <= 8 MiB | <= 2 s | <= 256 MiB |
| phone-capable | <= 512 MiB | <= 128 MiB | <= 30 s | <= 2 GiB |
| desktop-oriented | measured | measured | measured | measured |
| research-frontier | uncapped initially | measured | measured | measured |

## Workloads

- Physical scales: 1M, 10M, and 100M searchable pages; 1B is analytical or a
  large-runner experiment until a suitable machine is available.
- Values: 8, 32, 96, 256, and 1,024 bytes, plus complete representative
  projections.
- Queries: present and absent unique hashes, small enums, and tag fanouts of 1,
  4, 16, 100, 1K, and 10K+.
- Distributions: uniform, Zipf, and an anonymized real Defra/Shinzo
  distribution when available.
- Amortization: 1, 2, 10, 100, and 1,000 queries/client and 1, 1K, and 1M
  clients/generation.
- Load: batch sizes 1-1,024 and concurrency 1, 8, 64, and saturation.
- Lifecycle: immutable snapshots, value updates, key-set updates, mutable head,
  sealing, compaction, tombstones, and stale generations.

## Candidate waves

1. Common accounting and identical populated corpus.
2. Public MPHF to exact ordinal Dense, Standard Ribbon, BuRR, Fuse-4, and
   packed cuckoo.
3. Four-Russians/subset-XOR preprocessing for group sizes 2, 4, 6, 8, and 10.
4. Locator-only, inline projection, full query capsule, and private two-stage
   document retrieval with explicit cardinality padding.
5. Scalar/branchless/SIMD/cache-blocked Dense and multi-client GF(2) batching.
6. Finite-differences PIR, SimplePIR/DoublePIR, YPIR, InsPIRe,
   Chalamet/FrodoPIR, SparsePIR, and the KPIRkvs/KPIRhash/KPIRindex
   key-to-index mappings on the same rows.
7. SinglePass, BALANCED-PIR, Piano/QuarterPIR, and Zelda with all per-client
   maintenance charged.
8. Compact DPF subscriptions and batch-specific constructions.
9. NUMA, GPU, PIM, and FPGA/HBM after the CPU table format and counters are
   stable.
10. VeriSimplePIR, committed linear PIR, Reed-Solomon/Shamir threshold PIR,
    weaker-threshold coded storage, TEE+ORAM, and production integration as
    separately labeled trust/security lanes.

Distributional PIR is retained as a separate best-effort correctness lane. It
can reduce work materially on a skewed popularity distribution, but a missed
result must not cause a query-dependent retry or fallback: that behavior would
leak information. It is never compared as an exact substitute without the
success probability and client decision rule in the report.

## Advancement gates

```text
security and leakage classification
-> analytical cost model
-> official artifact reproduction where available
-> common-corpus correctness
-> 1M measurement
-> 10M/100M measurement or documented hardware limit
-> phone/desktop feasibility
-> adversarial and production review
```

A candidate is removed only when it is dominated under identical assumptions
or its blocker is recorded. Paper throughput is never mixed with local
measurements.

## Current runner

- CPU: AMD Ryzen 7 3700X, 8 cores / 16 threads, AVX2, one NUMA node.
- Host RAM: approximately 16 GiB; current WSL2 VM exposes approximately 8 GiB.
- GPU: NVIDIA RTX 2070 SUPER, 8 GiB, visible to WSL through `/dev/dxg`.
- Linux `perf` was installed during the program and a sanity check successfully
  measured cycles, instructions, cache events, branches, faults, and scheduling
  events. The WSL kernel exposes no CPU/DRAM RAPL interface, so joules remain
  unavailable on this runner. Missing counters are reported as unavailable,
  never estimated as if measured.
- `scripts/bench-perf.sh` now uses a cooperative server-phase gate rather than
  wrapping the whole benchmark process. It records one collector per replica
  TID and sums those readings explicitly; package/uncore counters, when
  available, remain aggregate-only. See `HARDWARE_COUNTERS.md`. Earlier
  process-wide sanity readings are capability checks, not server-work results.
- Native MSVC builds currently fail in the workspace's upstream `sha2-asm`
  dependency. Correctness and release measurements use the Linux toolchain in
  WSL without altering Defra's dependency features.

## Initial verified baseline

- `cargo test -p pir-poc --all-targets` under WSL: 36 passed, 0 failed before
  the new exploration wave.
- Existing POC modules already cover replicated Dense XOR, packed cuckoo,
  Fuse-3/Fuse-4, SinglePass, compact-DPF subscriptions, indexed decoys, and a
  two-server finite-differences correctness/cost spike.

The machine and toolchain are part of every emitted benchmark report so later
runs on larger or mobile hardware remain distinguishable.
