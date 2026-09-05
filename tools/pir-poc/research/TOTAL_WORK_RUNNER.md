# Aggregate-work runner

The complete B0–B8 suite now uses **[run_all_benchmarks.py](ALL_BENCHMARKS.md)**.
This page documents the original native screening runner, which remains usable.

See [the first measured results](TOTAL_WORK_RESULTS.md) for the 2026-09-04 run.

The opt-in `research total-work CONFIG.json` command executes one bounded case
in the existing Rust POC. `total_work.py` runs cases in fresh processes, alternates
their order, pins the binary and source hashes, and preserves each config and
raw result. Output directories must be new; earlier measurements are never overwritten.

Run from the repository root in Linux/WSL:

```bash
cargo test -p pir-poc --features research total_work --lib
python3 -m unittest discover -s tools/pir-poc/research -p test_total_work.py
cargo build -p pir-poc --features research --release --example total-work
python3 tools/pir-poc/research/total_work.py --profile smoke \
  --output target/pir-total-work-smoke
python3 tools/pir-poc/research/total_work.py --profile screen \
  --output target/pir-total-work-screen
```

`--dry-run` writes the matrix, reproducibility manifest and exact dimension
preflight without running the binary. `--matrix FILE.json` accepts an array of
case configurations. Unknown Rust config fields are rejected. `--timeout` bounds
each child process; failed and incomplete runs never become zero-work results.
The default is five fresh runs per case. Smoke is correctness screening, not
performance evidence. Screen uses 262,144 real 96-byte rows and a 512 MiB
analytical resident budget. Memory dimensions are not an OS-enforced RSS limit.

Example individual configuration:

```json
{
  "candidate": "field-bitmap",
  "rows": 262144,
  "row_bytes": 96,
  "field_bits": 32,
  "group_bits": 2,
  "fanout": 4,
  "payload_slots": 4,
  "queries": 100,
  "max_resident_bytes": 536870912
}
```

Implemented adapters:

| Candidate | Work completed by one logical query |
|---|---|
| `dense` | One known row, two fresh XOR shares |
| `subset` | Same row using persisted source-selector subset XOR, g=2/4/6/8/10 |
| `batch` | Independent row queries using independent/shared/blocked/transposed/Four-Russians kernels; denominator includes every batch member |
| `single-pass` | Stateful known-row retrieval, including client setup and parity refresh; Q=2/4/8/16/32 |
| `finite-differences` | Encoded row retrieval using an admissible storage/download-bounded parameter set |
| `field-bitmap` | Every grouped field bitmap retrieved privately, client intersection, all matching IDs and fixed padded private payload requests |
| `field-inline` | Complete padded posting IDs and payloads in one private row; same equality workload, using the synthetic contiguous value domain as its public ordinal mapping |
| `field-public` | Public bitmap lookup and padded public payload control; selection leaks |
| `field-postings` | Full-field ordered posting-map control and padded public payload; selection leaks |

Field groups index actual field-value bits. Every g-bit group stores 2^g
N-bit membership bitmaps. The g=1 implementation stores both complementary
bitmaps; a single-plane representation would halve its index storage and is
not claimed here. The corpus uses bounded fanout and permuted physical rows.
Every fourth search is absent. The public fixed payload schedule is unchanged
for absent/present searches. Overflow is an error, never truncated output.
Field width must accommodate the generated distinct values plus an absent value.

Two noncolluding logical operators evaluate roles sequentially in one process.
Index builds are executed twice, then a single retained immutable copy is used
for role simulation. CPU counters use `CLOCK_PROCESS_CPUTIME_ID` on Unix, covering
all process threads and both user/kernel time. Other platforms report null CPU.
Summed role elapsed time remains a separate wall metric. Protocol payload bytes
count each upload/download once; no TLS/network service is simulated. Random
seeds are public reproducibility inputs only and cannot be used in deployment.

Server CPU/query includes initial build and actual full rebuilds divided by
completed logical queries. `rebuild_every` and `update_batch` mutate payload rows
and rebuild immutable generations. SinglePass client state is rebuilt too.
Corpus generation and correctness-oracle scans are outside server work. Source
tables are retained and included in storage. Tree/allocator overhead, publication
framing, concurrent generations and durable state writes are excluded.

Reports separate server/client setup and online CPU. Client byte/CPU checks
cover measured dimensions only. Peak memory, physical traffic, energy, network
CPU and production deployment gates remain null/unmeasured. The runner therefore
never grants production promotion. Comparisons require matching workloads,
completed query horizons and update schedules; public search is never ranked
against private known-row retrieval. Confidence intervals resample paired
fresh-process runs, not individual queries. Amortization projections are marked
estimated and omit stateful-client and update cases until their lifecycle model
is complete.

The preflight records exact grouped-bitmap storage/traffic at 262K through 1B
rows and placement over 1–128 workers. It also enumerates bounded prime-field
parameters for the multivariate/Hermite many-server construction. These are
dimension calculations, not measured billion-row or many-server executions.

The [unified suite](ALL_BENCHMARKS.md) adds private intersection/compaction,
role-separated Zelda, executable Hermite PIR, Path ORAM, compressed indexes,
membership updates, recovery, canonical witnesses, GPU work and arrival
scheduling. Multi-host and phone measurements require those physical devices.
