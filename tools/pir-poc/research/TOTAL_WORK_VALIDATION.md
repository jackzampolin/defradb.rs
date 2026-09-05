# B0–B8 implementation validation, 2026-09-04

The [unified suite](ALL_BENCHMARKS.md) has runnable implementations for all nine
families. This report records bounded validation, not a new performance ranking.
The earlier [262,144-row screening analysis](TOTAL_WORK_RESULTS.md) remains
separate. In particular, tiny Python/GPU setup-dominated cases cannot establish
an indexed-search speedup over a native Rust row retrieval.

## Checks and retained results

The full Rust library suite passed **142 tests**. The final Python suite passed
**14 tests**, including private bit-owner placement and both mid-run and closing
base/delta compaction. Focused Rust checks were rerun after later accounting
changes. Five fresh process repetitions were used in these suites:

| Suite under `target/` | Successful runs | Gated runs | Correctness failures |
|---|---:|---:|---:|
| `pir-all-protocol-final-20260904` | 430 | 15 | 0 |
| `pir-all-native-final-20260904` | 280 | 0 | 0 |
| `pir-all-gpu-final-20260904` | 20 | 0 | 0 |
| `pir-index-lifecycle-final-20260904` | 110 | 0 | 0 |
| `pir-witness-caps-final-20260904` | 5 | 0 | 0 |

These total 845 successful repetitions. The index/lifecycle suite supersedes the earlier
compressed-index and base/delta observations and adds real bit-owner placement;
do not combine superseded observations into confidence intervals. Its 2 GiB
resident budget allowed the 16-owner process experiment. The other suites use
the default 512 MiB preflight. The final witness suite supersedes the original
witness rows and checks the client verification CPU cap as well as byte caps.
Some correctness smoke runs overlapped build or
report-generation work; their timings are **not qualified isolated-host
performance evidence**. No new winner is selected from them.

The 15 gates are five repetitions each of the 32/64/128-server Hermite pilot
under the default process-memory estimate. An additional dry-run scale matrix
is in `pir-all-scale-preflight-20260904`; it checks dimensions through one
billion rows without claiming those datasets were allocated or measured.

GPU boundary checks additionally exercised 96/256/1,024-byte physical rows and
512-query batches. The first 1,024-byte build exposed excessive shared memory
in both Dense reductions. Reducing the block size for wide rows fixed it;
all four candidates subsequently passed in `pir-gpu-wide-fixed-20260904`.
The original four failed build logs remain in `pir-gpu-boundaries-20260904`.
The successful smaller-width and maximum-batch observations remain there too.
This is an explicit fixed regression, not four discarded negative results.

`pir-perf-check-20260904` verifies real `perf stat` collection: task-clock,
cycles, instructions, cache references/misses, page faults and context switches
were available. These counters cover the client/coordinator and descendants;
they are not added to server-only CPU. WSL exposes no RAPL energy or uncore
device paths in the recorded probe. Physical DRAM traffic and energy are null.

## The proposed bit-owner layout

The 16-bit, 32-row pilot used 16 logical bit owners, each with two index-role
processes, plus two payload processes. Every one of the 32 index processes
stored exactly **one 4-byte bitplane**. Aggregate bitplane storage was 128 bytes,
the same as the two replicas of the unsplit 64-byte index. All four complete
queries in each repetition reconstructed and verified correctly, including
an absent search and fixed dummy payload requests.

This validates the intended storage distribution: fewer bits per role reduces
per-role index size. It does not establish lower aggregate work. The pilot
charges all 34 process starts, projected-field publication, private bitmap
selection and padded payload retrieval. It also reports roughly 1.31 GiB of
summed role RSS high-water marks in one 16-owner run—real interpreter/process
overhead that a table-size-only comparison would miss. Those processes ran on
one physical host; independent machines and latency scaling remain unmeasured.

## Reports and figures

- [Native comparison](../../../target/pir-all-native-report-20260904/comparison.md)
- [Served protocol comparison](../../../target/pir-all-protocol-report-20260904/comparison.md)
- [Updated bit-owner/lifecycle comparison](../../../target/pir-index-lifecycle-report-20260904/comparison.md)
- [GPU comparison](../../../target/pir-all-gpu-report-20260904/comparison.md)
- [Setup break-even projection](../../../target/pir-native-figures-reviewed-20260904/break-even.svg)
- [Update sensitivity projection](../../../target/pir-native-figures-reviewed-20260904/update-sensitivity.svg)
- [Storage/work projection](../../../target/pir-native-figures-reviewed-20260904/storage-work.svg)
- [Client-state/work projection](../../../target/pir-native-figures-reviewed-20260904/client-state-work.svg)

The figures use native **256-row × 96-byte smoke inputs** and stationary
single-client formulas recorded in their `projection-inputs.json`. They
demonstrate the reporting machinery. They are not measured long-horizon,
high-update-rate or large-client-population results. Matplotlib 3.11.1 produced
the SVG/PNG artifacts; the plotting packages were isolated under `target/`.

Each suite retains its matrix, source/binary hashes, per-run logs and JSON.
External adapters retain their patched source copies, exact upstream pins,
compiler commands and process metrics. Missing client/hardware eligibility
measurements stay explicit. None of these results enables production promotion.
