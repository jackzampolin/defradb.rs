# Phase-scoped server hardware counters

`scripts/bench-perf.sh` measures one selected `bench-dense-batch` server
evaluation. It does **not** wrap the benchmark process in an always-on
collector. Corpus/MPHF construction, query-share generation, reconstruction,
JSON serialization, and all other benchmark samples are outside the enabled
counter interval.

## What is measured

The benchmark publishes each selected replica worker's Linux TID and blocks at
a barrier. The runner attaches one disabled `perf stat -t TID` instance per
replica. After every collector acknowledges readiness, each worker enables its
own collector, calls `BatchEvaluator::evaluate`, and disables the collector
before returning. Therefore:

- `per_server_core_counters` is a real per-thread measurement of one replica;
- `aggregate_core_counters` is the sum of all measured replicas, not elapsed
  wall time and not a process-wide measurement;
- cycles, instructions, generic cache references/misses, branches, software
  task time, context switches, and faults are retained independently;
- the counter's `percent_running` is retained and multiplexing below 90% is
  flagged rather than hidden.

Package energy, DRAM energy, and memory-controller events cannot be attributed
to one worker. When present, they use one coordinated aggregate envelope that
starts before the replicas are released and stops after all replicas finish.
They are never divided by the number of replicas or labelled per-server.

Generic `cache-misses` is **not** physical DRAM traffic. Physical DRAM bytes are
reported only when the operator supplies a platform-validated uncore event and
its bytes-per-count conversion with `--dram-event EVENT@BYTES`. That result is
labelled as a measured counter plus an operator-supplied derivation. No guessed
conversion is provided.

## Run

Use an otherwise idle Linux host, reserve enough CPUs for the coordinator and
replicas, and choose one exact measured sample:

```bash
tools/pir-poc/scripts/bench-perf.sh \
  --profile quick \
  --cpus 2-4 \
  --servers 3 \
  --batch 64 \
  --kernel grouped-four-russians-g6 \
  --sample 0 \
  --result-dir target/pir-poc-results/perf-g6-n3
```

The runner builds the release binary before starting the benchmark, but that
build is not measured. Use `--no-build` only after deliberately building the
current revision. Valid kernel names are emitted by `bench-dense-batch`, such
as `independent-query-major`, `shared-row-major`, `shared-cache-blocked`,
`shared-selector-transposed`, and the batch-dependent
`grouped-four-russians-gN` names.

For a validated memory-controller event, for example a 64-byte CAS counter on
hardware whose PMU documentation confirms that mapping:

```bash
tools/pir-poc/scripts/bench-perf.sh ... \
  --dram-event 'PLATFORM_EVENT_NAME@64'
```

Do not copy that multiplier to another processor without checking its vendor
PMU documentation. Multiple read/write/channel events may be supplied; the
JSON retains each source counter and derived byte count.

## Output and evidence rules

`hardware-counters.json` is the authoritative sidecar. It includes:

- the exact profile, server count, batch, kernel, sample, and scope;
- each replica TID and event status;
- aggregate core-counter sums;
- separately scoped package/uncore readings;
- an explicit list of unavailable events and reasons.

Raw `server-N.perf.csv`, optional `aggregate.perf.csv`, the normal benchmark
JSON, stderr, Git/toolchain environment, phase manifest, and selector TIDs are
kept beside it. Preserve all of them when publishing a result.

The live FIFO gate is created under `/tmp`, even when the repository is on
`/mnt/c`, because WSL DrvFS does not reliably implement POSIX named FIFOs. The
runner copies the immutable phase manifest and TIDs into the result directory
before removing that validated temporary directory.

An unavailable RAPL or uncore interface stays unavailable. The current WSL2
kernel does not expose `power/energy-pkg/`, `power/energy-ram/`, or an uncore
memory-controller PMU, so this machine can measure core events but not joules
or DRAM bytes. `perf_event_paranoid`, PMU permissions, event-open failures, and
multiplexing must be recorded with the result.

## Isolation and overhead caveats

- `taskset` constrains the benchmark but does not remove unrelated work from a
  package-level or uncore counter. Energy/DRAM publication requires an idle or
  otherwise isolated host and a recorded CPU topology.
- One control acknowledgement and two clock reads occur at the boundary of a
  per-server interval. This is small but not zero; measure an empty gated
  closure before using the harness for sub-microsecond kernels.
- Replica threads run concurrently and may contend for shared cache and memory,
  by design. Aggregate cycles/instructions sum work; wall time remains the
  benchmark's co-located latency metric.
- A sidecar is not silently merged into `AggregateWorkReport`. Merge only after
  matching its exact phase selectors and evidence label; unrelated process-wide
  `perf stat` results are invalid for server-work comparisons.

## Validated WSL run

On 2026-08-18, revision `945df3a737fd50ba7884c1fa6e586879ef0af8b3`
with a dirty POC worktree, the runner completed one quick-profile,
three-server, batch-64, `grouped-four-russians-g6`, sample-0 phase on CPUs 2-4.
Every event ran 100% of the enabled interval. The immutable local artifact is
`target/pir-poc-results/perf-phase-g6-n3-r3/hardware-counters.json`.

| Scope | Cycles | Instructions | Generic cache references | Generic cache misses | Task clock |
|---|---:|---:|---:|---:|---:|
| Server 0 | 188,219,089 | 673,562,887 | 8,407,545 | 60,557 | 47.91 ms |
| Server 1 | 219,992,591 | 673,562,290 | 8,726,179 | 61,357 | 55.91 ms |
| Server 2 | 188,757,431 | 673,562,046 | 4,999,047 | 54,898 | 47.81 ms |
| Aggregate sum | 596,969,111 | 2,020,687,223 | 22,132,771 | 176,812 | 151.63 ms |

Aggregate IPC was 3.38491 and generic misses/references was 0.799%. Those
generic cache events are PMU-defined and are not a DRAM-byte measurement. The
run also recorded zero context switches and six page faults across the three
worker intervals. Package energy, DRAM energy, and physical DRAM traffic were
explicitly unavailable on this WSL kernel.

The normal quick report produced in the same process reports distribution
statistics across its full sample set: 186.617/189.292 ms aggregate-server
p50/p95 and 79.289/343.382 ms co-located-wall p50/p95 for this kernel. They are
not presented as the timing of the one hardware-counter sample.

Two prior diagnostics are intentionally excluded: `r1` failed before attaching
because DrvFS could not host the FIFOs, and `r2` counted only gate/error overhead
after perf 7.0 left a NUL byte between FIFO acknowledgements. `r3` moved live
FIFOs to Linux `/tmp`, accepted only NUL/whitespace framing around the literal
`ack` token, completed enable/evaluate/disable for all replicas, emitted
`phase.done`, finished the normal benchmark, and passed parser/sum validation.
