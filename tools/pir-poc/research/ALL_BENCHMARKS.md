# Complete aggregate-work benchmark suite

The follow-up six-family private index implementation and its compiled/Ramen
comparisons are documented in [PRIVATE_INDEX_COMPOSITIONS.md](PRIVATE_INDEX_COMPOSITIONS.md).
See [PRIVATE_INDEX_FINDINGS.md](PRIVATE_INDEX_FINDINGS.md) for measured outcomes.

`run_all_benchmarks.py` implements the B0–B8 experiment families from
[the plan](TOTAL_WORK_BENCHMARK_PLAN.md). Its primary denominator is a complete
successful logical answer. CPU includes every server process, preprocessing
role, exporter/helper and measured maintenance phase. GPU time remains a
separate resource. This is research code; serving defaults are unchanged.

## Run

Linux/WSL is required for process CPU/RSS, TCP role isolation and the external
adapters. Tested with Python 3.14, NumPy 2.3.5, cryptography 46.0.5, Go 1.26,
Rust 1.97, CUDA 12.4 and g++ 13. Python dependencies are pinned in
`benchmarks/requirements.txt`. Install them in your benchmark environment.

From the repository root:

```bash
cargo test -p pir-poc --features research --lib
python3 -m unittest discover -s tools/pir-poc/research -p 'test_*bench*.py'
python3 -m unittest discover -s tools/pir-poc/research -p test_total_work.py
cargo build -p pir-poc --features research --release --example total-work
python3 tools/pir-poc/research/prepare_benchmarks.py target/pir-artifacts-pinned
python3 tools/pir-poc/research/run_all_benchmarks.py \
  --output target/pir-all-smoke --profile smoke --repeats 5 \
  --native target/release/examples/total-work \
  --zelda-source target/pir-artifacts-pinned/zelda \
  --gpu-source target/pir-artifacts-pinned/gpu-dpf
python3 tools/pir-poc/research/report_benchmarks.py \
  target/pir-all-smoke target/pir-all-smoke-report --plots
```

Plots additionally require Matplotlib. They are standard SVG figures with
explicitly modeled setup amortization, full-generation update sensitivity,
aggregate storage and client-state tradeoffs. Raw measured inputs and projection
assumptions accompany them. They do not extrapolate a measured billion-row result.

Output directories must be new. `--family B2`, `--engine protocol`, and
`--name substring` select experiments. `--dry-run` emits capability probes,
the matrix and cheap budget checks; external compiler/VRAM checks occur when
their adapter starts. `--matrix FILE` accepts an explicit list of
`{family,engine,name,config}` objects. For an individual served configuration:

```bash
cd tools/pir-poc/research
python3 -m benchmarks.run_case case.json /absolute/new/result-directory
```

`smoke` exercises correctness and lifecycle corners. `screen` uses 262,144
96-byte native rows, 4,096 served Python rows, 128-row compaction/Hermite pilots,
and 262,144-row GPU/Zelda cases. These are **different comparison lanes**.
`scale` adds 1M/10M/100M/1B dimension cases; the 512 MiB resident preflight rejects
infeasible allocations. Increase `--resident-bytes` only for actual available
capacity. Bounds are analytical, not an operating-system memory quota.

## Coverage

| Family | Runnable implementation | Parameters / complete result |
|---|---|---|
| B0 | Native and served Dense; public and shuffled 100-decoy controls; CPU worker pools; fresh Dense/DPF GPU | Same-width row retrieval; 1/2/4/8 workers per logical operator; local TCP and hardware probes |
| B1 | `field-index`, `public-index`, native `field-bitmap` and original inline baseline | 16/32/64-bit fields; g=1/2/4/8; one-plane complement derivation, bitmap, runs and postings; equality/range/conjunction; uniform/skewed/clustered, sorted/permuted; present/absent, fixed complete-result padding |
| B2 | `mpc-dense`, `mpc-oram`, `mpc-compact-dense`, `mpc-compact-oram` | Three-party replicated Boolean sharing; balanced private AND; fixed Batcher compaction; every padded payload slot fetched privately |
| B3 | Native `subset` | Source-selector subset XOR g=2/4/6/8/10; warm and declared cache-scrub conditions; full build/rebuild |
| B4 | Native batch kernels and fresh GPU batches; `registered` | Independent/shared/blocked/transposed/Four-Russians; batch 1/8/32/128/512; arrival deadline 0/5/20/100 ms; separately labeled linkable registration |
| B5 | SinglePass and pinned official Zelda adapter | Per-client setup/refresh; multiple clients; native generation replacement; Zelda discarded-setup recovery, preprocessing and online roles separately metered |
| B6 | Native finite differences and `hermite` | Actual field encoder, derivative stores, queries and client interpolation at m=1; 4/8/16/32/64/128 role frontier, bounded to 128 logical rows |
| B7 | `path-oram` | Published nonrecursive Path ORAM, Z=5; encrypted full-path reads/writebacks, position map, bounded stash, updates, durable checkpoints and interrupted-state rebuild |
| B8 | Canonical `witness`; `base-delta`; served lifecycle cases | Membership, predecessor/gap and lower/terminal absence witnesses under current root; stale-root rejection; insert/delete/value updates; query every base/delta component, actual compaction |

The private compressed-index path scans random selector shares over compressed
group buckets, reconstructs full fixed-size bitmap answers at the client, and
fetches all padded payload slots. Compression is an at-rest representation;
the benchmark counts decompression work and does not assume ordinary public
index lookup complexity. The `planes` variant stores one plane per field bit
and derives the zero bucket by complementing within the declared row domain.
`index_workers=1/2/4/8/16/32/64` assigns groups round-robin to actual processes.
The exporter sends each worker only its projected field bits. Each owner has
two noncolluding replicas, plus the payload roles: sixteen one-bit owners mean
32 index-role processes, not a sixteen-party privacy theorem. Aggregate index
storage and maximum per-role storage are both measurable in the role stats.

MPC uses the semi-honest replicated multiplication construction from
[ABY3](https://eprint.iacr.org/2018/403), with pairwise private seeds and fresh
SHAKE masks. Peer messages travel over TCP between three OS processes. The
client combines returned shares; secret ANDs and compaction run on those roles.
The Batcher network is bounded to power-of-two tables of at most 256 rows.
The version returning a whole bitmap and the compacted version are separate
client-disclosure lanes. This is an unaudited implementation of the construction.

Zelda pins [the official repository](https://github.com/p-b-p-b/Zelda) at
`11b8e70ffcb3ee8d2ea72824c04ed8faa1fa558a`, following
[the paper's two-server implementation](https://eprint.iacr.org/2025/1340).
Its redundancy parameter is not the number of privacy parties. Artifact copies
replace private `math/rand` sampling with OS cryptographic randomness, route
preprocessing and online RPCs to different processes, enforce allowed methods,
reject the preprocessing bypass and verify every answer. All discarded hints,
replacement entries, client starts and discarded-state recovery work are
charged. Correctness follows the artifact's non-adaptive experiment; this is
not a claim of adaptive malicious security. Updates use the other lifecycle
lanes; the Zelda database in each run is immutable.

Hermite executes the m=1 specialization of
[Scalable Multi-Server PIR](https://eprint.iacr.org/2024/765), including arbitrary
row bytes packed into two field symbols per byte and Hasse-derivative decoding.
It is a concrete correctness/storage frontier, not the full multivariate
asymptotic construction. Server count is real process count. An S=128 case
does not claim to have run on 128 independent machines.

Path ORAM follows [the published algorithm](https://elaineshi.com/docs/pathoram.pdf).
Each owner has a nonrecursive position map and a bounded stash. Every access
remaps the block and reads/writes a complete padded path with fresh AES-GCM
nonces. Checkpoint restore needs a trusted current epoch and digest. An
interrupted writeback invalidates that state; recovery republishes a fresh
encrypted tree and charges the work. This is a single serialized honest-owner
benchmark, not a stateless concurrent-client ORAM or a malicious-server freshness
proof. Its privacy assumptions differ from information-theoretic two-server PIR.

## Accounting and limits

- Native kernels measure process user+kernel CPU per phase. Private roles are
  sequential logical replicas; public/decoy controls build one replica. Native
  client RNG is reproducibly seeded and must not be used with public seeds in
  a deployment. Source data generation is a fixture; role publication is counted.
- Served protocols spawn separate processes over framed loopback TCP. Each
  process reports full lifetime CPU, including interpreter startup and RPC
  serialization. Exporter CPU is added once; peer traffic is counted once at
  its sender. Client RSS conservatively includes the oracle corpus. Client CPU
  excludes synthetic query selection and the full-scan correctness oracle.
- GPU-DPF pins `ce23a06af884ee54300b5bc5fd5350e445f10b0b` and patches all secret
  random draws in an artifact copy to `getrandom`. Every batch uses fresh keys,
  performs both evaluations and verifies every complete row. Two actual tables
  reside on one GPU. CPU phases, CUDA compute, H2D and D2H remain separate;
  transfer sizes use full physical rows. No network transport is claimed.
- The older replay GPU report now aggregates paired per-request observations
  before taking medians. It remains a kernel microbenchmark. The new complete
  GPU entry point does not run its fixed-selector replay loops.
- Pacing with `client_mbps` / `fabric_mbps` is an application-level byte-rate
  experiment. It is not packet loss/congestion or NIC shaping. Available local
  hardware is recorded. `--perf` collects whole-invocation CPU counters, including
  client and server descendants; those counters are not labeled server-only.
- Missing physical DRAM/energy/remote-device measurements remain null. The
  existing `HARDWARE_COUNTERS.md` harness provides phase-gated CPU counters for
  supported Linux hosts. Independent hosts, ARM client measurements and
  calibrated DRAM/energy collection require suitable hardware; no synthetic
  extrapolation passes those deployment gates.
- Client caps cover measured setup CPU, online CPU, traffic and conservative
  RSS. Native reports retain unmeasured RSS explicitly. Output overflow fails
  the complete query; it never silently truncates a match set. Fixed schedules
  include dummy payload requests for absence and low fanout.
- Timeout handling terminates the invocation's own process session. Failure
  logs and any partial server work ledger are retained. A result with missing
  successful repetitions is not promoted. Bootstrap intervals resample whole
  runs; comparisons must match workload, engine, disclosure and client class.

Shared helpers, generations, clients and scales are configurable rather than a
full Cartesian sweep. `Case` exposes query/client counts up to 10,000, update
batch/rate, insert/delete/value mutations, compaction/recovery frequency, result
padding and memory limits. The matrix stages expensive combinations. Large
client-population plots use an explicit fresh-client restart model; they do not
assume that preprocessed client state can be shared for free.

## Validation artifacts

The implementation was checked with 142 Rust library tests and 14 Python
protocol/accounting/matrix tests. Those include every small Hermite row,
served XOR recovery, all compressed representations, private range and equality,
MPC compaction, ORAM updates/checkpoints/rollback rejection, and canonical
membership/absence/current-root invalidation. See `target/pir-*-check-v1` and
`target/pir-zelda-lifecycle-v1` for early adapter checks; these are correctness
runs, not performance comparisons. Repeated final run locations and outcomes
are recorded in [the companion validation report](TOTAL_WORK_VALIDATION.md).
