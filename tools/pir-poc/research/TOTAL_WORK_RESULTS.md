# Aggregate-work screening, 2026-09-04

This is the preserved first-stage performance analysis. See
[the complete benchmark coverage](ALL_BENCHMARKS.md) for subsequently implemented
B0–B8 protocols and validation; those smoke timings do not change these conclusions.

The first implemented benchmark stage does **not** support replacing the current
private inline lookup with grouped bitmaps followed by private payload reads.
That complete-search pipeline consumed 3.4–3.7 times the aggregate server CPU
of the inline baseline in this experiment. This result applies to client bitmap
intersection plus Dense payload retrieval, not to every possible privately
indexed construction.

Run on an AMD Ryzen 7 3700X through WSL/Linux, with two co-located logical
operators evaluated sequentially. CPU is process user+kernel time across all
threads. Each case has five fresh process repetitions with alternating order.
The corpus contains 262,144 real rows of 96 bytes. Equality cases use a synthetic
32-bit field, four matches per present value, permuted row order, every fourth
query absent, and four fixed payload slots. The inline control uses the known
contiguous synthetic value domain as its public row mapping; it is not a measured
arbitrary-key MPHF construction.

The table reports medians of per-run server CPU/query, including initial source
replica materialization and index build amortized over 100 complete queries.
Ratios are ratios of mean run costs, so they need not equal ratios of displayed
medians. These rows exclude update cycles; separate update cases are retained
in the raw results.

| Complete equality search | CPU ms/query | CPU ratio to inline | Measured client limits |
|---|---:|---:|---|
| Private inline IDs + payloads | 4.215 | 1.00 | Pass |
| Private bitmap groups, g=1 | 14.708 | 3.43 | Download fails |
| Private bitmap groups, g=2 | 14.324 | 3.41 | Download fails |
| Private bitmap groups, g=4 | 14.798 | 3.43 | Pass |
| Private bitmap groups, g=8 | 16.173 | 3.72 | Pass |

The g=2 case downloads 1 MiB of bitmap answers **plus** 768 payload bytes, so it
exceeds the 1 MiB complete-query cap. Counting only bitmap retrieval would have
incorrectly admitted it. The g=1 representation stores both complementary
bitmaps; storing one plane and deriving its complement remains an optimization
to test. Splitting these indexes among more workers changes per-worker storage,
but not aggregate storage or the number of payload reads in this implementation.

The separate known-row workload shows a different result:

| Known-row retrieval | CPU ms/query | CPU ratio to Dense |
|---|---:|---:|
| Dense, two operators | 3.633 | 1.00 |
| Persisted subset XOR, g=2 | 8.426 | 2.28 |
| Persisted subset XOR, g=4 | 9.199 | 2.52 |
| SinglePass, Q=2 | 0.352 | 0.095 |
| SinglePass, Q=32 | 0.289 | 0.079 |

SinglePass is the strongest measured server-work candidate in this local
known-row screening. Its Q=32 case downloads 24 MiB during client setup and
retains about 2.75 MiB of client state. One recorded run measured 10.69 ms of
client setup CPU; its first online query uploaded 320 bytes and downloaded
6,208 bytes, including refresh material. These client costs are separate from
server CPU. This is a stateful single-client experiment, not evidence that cold
clients, membership updates or rollback/recovery have negligible cost.

Larger subset groups g=6/8/10 exceed the configured 512 MiB analytical resident
budget. No enumerated two-server finite-differences variant meets both that
budget and the 1 MiB online download cap. These four case configurations account
for the 20 budget-rejected runs. There were 235 measured screening runs and no
other failed or timed-out runs. All 155 smaller smoke runs completed, and the
four Rust plus four Python regression tests passed.

The many-server preflight is a bounded prime-field parameter enumeration, not
an implementation or a global optimum. Even its smallest 128-server storage
choice is about 8,782 times the logical input bits under the conservative
one-bit-per-field-symbol encoding, beyond the plan's 512x frontier. This does
not exclude prime-power fields, packed encodings or other published constructions.

Artifacts, relative to the repository root:

- `target/pir-total-work-screen-20260904/`: immutable manifest, configurations,
  raw per-query counters, errors, preflight, comparison JSON and Markdown.
- `target/pir-total-work-smoke-20260904/`: 155 correctness-screening runs.
- `target/pir-total-work-preflight-20260904/`: the unexecuted screen matrix and
  exact grouped-bitmap dimension calculations through one billion rows.

Use [the runner instructions](TOTAL_WORK_RUNNER.md) to reproduce these cases.
The current results are microbenchmarks: network/transport CPU, physical DRAM
traffic, energy, peak client RSS, multi-host execution and production integrity
are unmeasured. No candidate is automatically promoted to production.

Those additional benchmark families are now implemented in the
[unified suite](ALL_BENCHMARKS.md). Their bounded validation is not evidence of
performance at the 262,144-row scale used above. No cross-language or
cross-workload speedup is inferred from their results.
