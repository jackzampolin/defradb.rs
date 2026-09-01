# DefraDB PIR POC: research comparison archive

The bundled default POC exercises three protocol shapes. The current product
ranking and eight use-case decisions are in [`../USE_CASES.md`](../USE_CASES.md).
This file preserves the experiments that produced those choices; their code is
compiled only with the `research` feature.

This document is the experiment-stage summary that preceded the current
decision record. The primary metric is
**aggregate server work per complete useful private result**. Build, client
setup, online server work, maintenance, traffic, storage, latency, and client
CPU are kept separate. Results with different privacy, leakage, or result
scope are not divided into a headline speedup.

The main security lane is replicated information-theoretic PIR with `n`
servers, privacy while at least one server remains non-colluding, and all `n`
answers required. All measurements below are local in-process results on an
AMD Ryzen 7 3700X under WSL2 unless explicitly labelled as paper or upstream
artifact data. They exclude HTTP/TLS/WAN time and are not audited production
cryptography.

The selected paths are implemented as correctness-checked modules and benchmark
commands. Treat the tables below as an experiment ledger: where they record an
earlier winner, the later decision and benchmark in `../USE_CASES.md` wins.

## Choices at this experiment stage

| Query shape | Selected POC path | Why | Main cost or condition |
|---|---|---|---|
| Cold, one or a few two-server strict-private snapshot queries, server work first | Official finite-differences PIR over exact 96-byte pages | 4.699x fewer expected selected-payload bytes and 44.6% lower measured aggregate server p50 than Dense on the common corpus | 8x storage/replica, 5.36 MB response, 39.8 s build; official implementation is two-server only |
| Cold with 3+ servers, low storage/traffic, or simplest serving path | Exact PtrHash MPHF ordinal + replicated Dense XOR over an inline fixed projection | Smallest exact table tested; one sequential private pass; tiny public index; supports arbitrary n-out-of-n replicas | Every query still performs about `n/2` tables of aggregate selected-row XOR work |
| Cold, several ready independent queries | Same table with shared-row traversal, then ephemeral Four-Russians for larger batches | Preserves independent Dense shares while reusing table traversal/cache work; no persistent storage expansion | Requires queries to overlap; queue delay must be bounded and reported |
| Warm, many sequential queries by one authorized client | Two-server SinglePass over the same exact table, normally `Q=2` | Replaces scans with a few indexed rows and tiny online upload | Client downloads the full 24.09 MiB generation and keeps 14.09 MiB state; exactly two asymmetric roles; mutable state cannot roll back |
| Live alert when block/epoch cadence is acceptable | Registered replicated Dense XOR over a packed 65,536-bucket presence bitmap; private snapshot fetch on hit | Minimum measured strict server work, one bit of result/replica, information-theoretic privacy, and direct 3+ replica support | Reveals public epoch cadence; stores 8 KiB selector/subscriber/server and waits until epoch close |
| Immediate live alert when epoch delay is unacceptable | Two-server Compact DPF | Compact 2,080-byte registration/server and the target is computationally hidden from either non-colluding evaluator | Evaluates every subscription for every event, emits fixed output for every subscription, and is exactly two-party |
| Snapshot or live when candidate-set privacy is acceptable | Public indexed 100-decoy lookup | Ordinary indexed work is much cheaper than exact PIR | Server sees the candidate set, event/tag bucket, access volume, and timing; not privacy-equivalent |
| Single-server computational-security snapshot | SimplePIR experiment on the same 96-byte pages | Removes the non-collusion assumption and measured server time is competitive | Larger client work/traffic and reusable hint; separate security lane and implementation stack |
| Very large or variable projection | Private locator page followed by padded private document batch | Keeps document choice private when inline bounds are impossible | A second full-table stage grows with padded fanout; inline wins strongly when a bounded projection is acceptable |

Public time windows remain an optional routing policy, not a requirement. The
strict-global production mode uses one generation-wide table. The optional
public-window mode intentionally discloses the selected coarse windows and uses smaller
tables built from the same authenticated cutoff. Numbers from different
partitions are never presented as a cryptographic improvement.

## Exact static layout

The common layout corpus has 1,048,576 documents, 262,144 populated tag pages,
four 16-byte locators per tag, and a 96-byte encoded page. Each candidate runs
one Dense request per server and reconstructs the same page.

| Layout | Table/server | 2-server upload | Download | Expected aggregate payload XOR | Summed server p50 | Decision |
|---|---:|---:|---:|---:|---:|---|
| Exact MPHF Dense | 24.00 MiB | 64.00 KiB | 192 B | 24.00 MiB | 5.66 ms | Default |
| Fuse-4 Dense | 26.44 MiB | 70.50 KiB | 192 B | 26.44 MiB | 5.85 ms | Constant-metadata fallback |
| Standard Ribbon, width 64, 10% slack | 26.67 MiB | 71.13 KiB | 192 B | 26.67 MiB | 6.67 ms | Correct but dominated here |
| Packed two-choice cuckoo | 26.67 MiB | 35.56 KiB | 1,536 B | 53.33 MiB | 6.47 ms | Lower upload, but two scans and larger response |

These are the seven-sample identical-corpus layout measurements. MPHF reduces
deterministic table size, expected payload work, and upload by 9.22% versus
Fuse-4. Its generation-specific public artifact is 98,534 bytes and a lookup
is about 0.2 microseconds. The artifact is key-set-dependent public state: it
is not a direct membership oracle, but guessed-key injectivity and
cross-generation relations need an application leakage review. Absent keys
map somewhere and are rejected only after the privately retrieved 128-bit
fingerprint is checked.

Standard Ribbon remains useful when distributing the MPHF artifact is
undesirable. BuRR was not faked: its bump/layer metadata must be counted and
needs an official implementation before it enters the measured table.

### Ten-million-page production-scale gate

The guarded execute-mode run materialized 10,000,000 populated tag pages with
32-byte inline rows. This is 10M searchable rows, not 10M documents divided
across a smaller tag set.

| Metric | Result |
|---|---:|
| Table/server | 320,000,000 B |
| Exact public MPHF artifact | 3,737,938 B |
| Build wall / max process RSS | 10.58 s / 1.75 GiB |
| Cold client index load / ordinal lookup | 1.743 ms / 0.201 us |
| 2-server aggregate p50 / p95 | 79.03 / 81.42 ms |
| 2-server upload / download | 2,500,000 B / 64 B |
| 2-server expected aggregate XOR | 320,000,000 B |
| 3-server aggregate p50 / p95 | 119.95 / 154.14 ms |
| 3-server upload / download | 3,750,000 B / 96 B |
| 3-server expected aggregate XOR | 480,000,000 B |

The exact layout therefore remains linear and buildable at a genuine
multi-million-row scale on the 8 GiB WSL runner. Three-server aggregate p50 was
51.79% above two servers, matching the deterministic 50% work/upload increase;
co-located wall rose only 7.03% because replicas ran concurrently on the same
host. The run used three timed samples after a warm-up and needs repetition on
the intended server/NUMA hardware before capacity planning.

### Two-server finite-differences cold path

The official Henzinger--Ragavan artifact was pinned at commit
`4574a4f8c52eeda165e110cbb64f834397d7c049` and adapted to the exact
262,144 x 96-byte corpus. One encoding was reused for three correct online
queries; there was no online warm-up.

| Metric | Finite differences | Exact-MPHF Dense, 2 servers |
|---|---:|---:|
| Storage/replica | 201,326,592 B | 25,165,824 B |
| Build | 39.78 s | about 0.25 s corpus + MPHF |
| Aggregate selected/probed payload | 5,356,032 B | 25,165,824 B expected |
| Aggregate server p50 / p95 | 3.329 / 3.752 ms | 6.013 / 8.090 ms |
| Client query + recovery p50 | 0.226 ms | 0.024 ms |
| Upload / download | 16 B / 5,356,032 B | 65,536 B / 192 B |

Finite differences therefore used 4.699x fewer selected/probed payload bytes
and 44.6% less measured aggregate server p50 on this cold page lookup. It is
the best measured two-server cold option when total server work is primary and
8x storage plus a 5.36 MB response are acceptable. The response still fits the
POC's preliminary 8 MiB phone-network envelope, but no ARM/phone run exists.

The byte comparison is not a memory-latency equivalence: Dense sequentially
streams selector/table state and conditionally XORs rows, while the artifact
probes its encoded cloud. The result has only three unwarmed samples, no
HTTP/TLS, and no 10M execute-mode validation. Most importantly, the official
implementation is two-server `t=1`; the paper's general `s`-server theorem is
not implemented here. Exact-MPHF Dense remains the generic 3+ server and
low-storage/low-download path.

## Persistent subset-XOR indexing

Precomputed subset tables reduce logical row operands on the older 384-byte
packed-cuckoo layout. Group size six improved aggregate server p50 by 1.77x
with two servers and 1.94x with three, at 11.5x total storage. On the compact
96-byte exact-MPHF table the same idea did not give a stable elapsed win:
group-eight used 32.875x storage and ranged from 7.5% slower to 15.9% faster
depending on topology/percentile. It is therefore not the production default.

The ephemeral batched Four-Russians kernel is different: it builds only a tiny
per-request group table, adds no persisted storage, and wins for sufficiently
large ready batches.

## Dense batch result

The full run uses 21 samples per kernel through batch 16 and 11 samples for
larger batches. Every query has independent fresh n-out-of-n shares and returns
one validated 96-byte page. Aggregate server time is the sum of single-core
replica evaluation time; co-located wall is reported separately in JSON.

| Ready queries | 2-server independent | Best 2-server kernel | Best aggregate | Speedup | 3-server independent | Best 3-server kernel | Best aggregate | Speedup |
|---:|---:|---|---:|---:|---:|---|---:|---:|
| 1 | 6.18 ms | cache-blocked | 6.10 ms | 1.01x | 9.81 ms | independent | 9.81 ms | 1.00x |
| 2 | 12.35 ms | shared-row | 6.35 ms | 1.95x | 18.13 ms | shared-row | 9.90 ms | 1.83x |
| 4 | 24.13 ms | shared-row | 7.92 ms | 3.05x | 36.36 ms | shared-row | 11.81 ms | 3.08x |
| 8 | 48.90 ms | shared-row | 13.88 ms | 3.52x | 78.41 ms | shared-row | 21.67 ms | 3.62x |
| 16 | 77.22 ms | shared-row | 26.72 ms | 2.89x | 151.89 ms | shared-row | 40.61 ms | 3.74x |
| 32 | 172.64 ms | ephemeral FR-g4 | 46.04 ms | 3.75x | 308.16 ms | ephemeral FR-g4 | 74.02 ms | 4.16x |
| 64 | 350.89 ms | ephemeral FR-g5 | 71.21 ms | 4.93x | 599.85 ms | ephemeral FR-g5 | 106.45 ms | 5.63x |
| 128 | 741.42 ms | ephemeral FR-g6 | 129.47 ms | 5.73x | 1,210.25 ms | ephemeral FR-g5 | 184.17 ms | 6.57x |

This does not make a batch free: two-server upload is 64 KiB per independent
query, and a production scheduler must add the observed arrival and maximum
queue-dwell delay. The grouped kernel also has fixed work for maliciously
chosen shares, while set-bit traversal can be driven toward its worst case and
therefore needs authentication, admission control, and rate limits.

One validated phase-scoped `perf stat` sample measured only the three replica
worker threads around a 64-query ephemeral FR-g6 evaluation: 596,969,111
aggregate cycles, 2,020,687,223 instructions (3.385 IPC), 22,132,771 generic
cache references, 176,812 generic cache misses (0.799%), and 151.63 ms summed
task clock. It had zero context switches and six page faults. This is one
counter sample, not a latency distribution. WSL exposes neither RAPL energy nor
an uncore DRAM-byte mapping on this host, so CPU/DRAM joules and physical memory
traffic remain unavailable; cache misses are not relabelled as DRAM bytes.

## Complete private result: inline versus two-stage

The full end-to-end matrix stores `[ordinal | 128-bit fingerprint | payload]`,
uses an operational public cardinality class (1, 4, 16, 128, or 1,024), and
retrieves every padded slot. The following rows use 96-byte projection slots
and two servers.

| Actual/padded results | Inline table/server | Inline aggregate p50 | Two-stage aggregate p50 | Deterministic two-stage/inline work |
|---:|---:|---:|---:|---:|
| 1 / 1 | 122.0 MiB | 39.73 ms | 35.97 ms | 1.20x |
| 4 / 4 | 104.0 MiB | 19.76 ms | 40.59 ms | 4.00x |
| 16 / 16 | 99.5 MiB | 16.52 ms | 116.79 ms | 15.71x |
| 100 / 128 | 125.7 MiB | 14.08 ms | timing guard | 98.04x |
| 1,000 / 1,024 | 100.4 MiB | 18.37 ms | timing guard | 489.90x |

Inline is the default whenever the application can define a bounded useful
projection. The two-stage design remains a correctness implementation for
large/variable records or a shared document table; batching improves its
constant factors but cannot erase the need to retrieve every padded document
slot privately. A locator-only result is a diagnostic, not an end-to-end
private document result. At fanout one the elapsed ordering flipped between
full runs despite inline having 20% less deterministic logical work; that
single noisy timing is not used to override the work, pass, traffic, and
higher-fanout evidence.

### Fair snapshot decoy baseline

The same end-to-end run now includes one target plus 99 present decoy tags on
the exact inline MPHF table. Every candidate uses the same padded cardinality
class and continuation count, and the server returns every complete page. The
request uploads 800 raw tag bytes before transport framing. For 96-byte
projection slots, its deterministic one-server row-copy/network scope is:

| Actual/padded results per candidate | Point lookups | Download and logical row bytes | Server p50 / p95 | Client verify p50 | Useful target bytes |
|---:|---:|---:|---:|---:|---:|
| 1 / 1 | 100 | 12,200 B | 0.019 / 0.020 ms | 0.048 ms | 96 B |
| 4 / 4 | 100 | 41,600 B | 0.020 / 0.024 ms | 0.137 ms | 384 B |
| 16 / 16 | 100 | 159,200 B | 0.033 / 0.037 ms | 0.578 ms | 1,536 B |
| 100 / 128 | 100 | 1,256,800 B | 0.133 / 0.161 ms | 3.386 ms | 9,600 B |
| 1,000 / 1,024 | 200 | 10,040,000 B | 1.576 / 2.041 ms | 34.409 ms | 96,000 B |

Those are ordinary MPHF point reads rather than Dense scans, so lower server
work is expected. The comparison is intentionally labelled candidate-set
privacy, not a PIR speedup: the server sees all candidate identities, the
public fanout class, repeats, and cache/popularity timing. Longitudinal decoy
selection quality remains a production research problem.

These are seven-sample release measurements from the same full end-to-end run
as the strict-private rows above. The raw request is 800 bytes before
transport framing in every row. At high fanout, client verification and 10 MB
of decoy output become material even though server point lookup remains cheap.

## Warm SinglePass

Dense and SinglePass use the identical 262,144-row, 96-byte MPHF table.
SinglePass has no server-produced hint: the authorized client downloads the
entire 24.00 MiB locator table plus the 98,534-byte public index and builds its
own generation-bound state. Serving that setup transfer over TLS/CDN is
charged in bytes but its server CPU is not measured.

| Scheme | Setup download | Persistent client state | Aggregate server/query | Client online/query | Upload/query | Download/query |
|---|---:|---:|---:|---:|---:|---:|
| Dense | 96.22 KiB | 96.22 KiB | 6.01 ms | 23.53 us | 65,536 B | 192 B |
| SinglePass Q=2 | 24.09 MiB | 14.09 MiB | 2.29 us | 2.88 us | 80 B | 448 B |
| SinglePass Q=4 | 24.09 MiB | 8.09 MiB | 2.26 us | 3.38 us | 96 B | 832 B |
| SinglePass Q=8 | 24.09 MiB | 5.09 MiB | 3.25 us | 4.36 us | 128 B | 1,600 B |
| SinglePass Q=16 | 24.09 MiB | 3.59 MiB | 6.79 us | 9.14 us | 192 B | 3,136 B |
| SinglePass Q=32 | 24.09 MiB | 2.84 MiB | 8.29 us | 12.26 us | 320 B | 6,208 B |

`Q` is the SinglePass partition count, not batch size. Q=2 is the normal
server-work/traffic choice; higher Q trades state memory for more online row
reads. The traffic figures include the 32-byte immutable generation ID on each
server query and answer; measured CPU timings predate that small post-run
framing hardening. State must be atomically persisted after show-and-shuffle and discarded
on ambiguous rollback. The implementation has exactly two server roles and no
safe drop-in three-server generalization.

## Live subscriptions

### Immediate per-event baseline

The full Compact-DPF batch run uses a 16-bit domain, two servers, subscriber
counts 1/100/1,000/10,000, event batches 1/8/64/1,024, and hit, miss, uniform,
and finite-Zipf streams. Timed cells satisfy `subscriptions * events <= 65,536`;
larger cells retain exact work accounting.

Per event with `S` subscriptions, Compact DPF performs `2S` point evaluations,
`32S` tree-level expansions, processes `640S` logical key bytes, and sends a
fixed `64S` response bytes. The measured best sequential kernels were roughly
0.28--0.57 microseconds per point evaluation. At 10,000 subscriptions one
event costs 10.5--11.6 ms aggregate server time and 625 KiB of fixed response.
Event-major, preprocessed, subscription-major, cache-blocked, and bounded
parallel kernels are byte-for-byte equivalent; parallel wall latency is not
misreported as reduced aggregate work.

The indexed 100-decoy event path is orders of magnitude cheaper because it
does one ordinary bucket lookup and notifies only candidates. It also reveals
the event bucket, candidate set, repeats, output count, and timing. Its elapsed
time is descriptive only; the report deliberately blocks a direct ratio with
Compact DPF.

The selected `fss-rs` DPF is exactly two-party. Copying either key share to a
third server gives neither three-party privacy nor fault tolerance. A reviewed
threshold/multi-party DPF is a different protocol and remains research work.

### Fixed-epoch packed-presence result

Most wallet alerts do not need one network response per event. Ethereum logs
already have a natural public block boundary, and Mizu or generic DefraDB can
choose an explicit one-second, block, or multi-block public cadence. During an
epoch the server sets a bit in a fixed 65,536-bucket bitmap for every observed
bucket. At epoch close, every subscriber privately retrieves its one presence
bit and performs the ordinary padded snapshot protocol only on a hit.

This is exact even if a bucket occurs repeatedly: event ingestion ORs the bit;
the PIR result is not event parity. Building the 8 KiB bitmap is ordinary
`O(events)` work outside the timed retrieval. The following nine-sample full
run used an RTX 2070 SUPER. Strict times are aggregate GPU kernel work across
two sequentially measured replicas; 100 visible buckets are one host-CPU
server reading the same warm bitmap.

| Ready subscribers | Packed Dense strict | 16-byte-row Dense control | GPU DPF | 100 visible buckets | Strict parallel wall/batch |
|---:|---:|---:|---:|---:|---:|
| 1 | 56.947 us | 65.102 us | 2,199.250 us | 0.075 us | 0.029 ms |
| 32 | 2.010 us | 3.844 us | 71.403 us | 0.229 us | 0.034 ms |
| 128 | 0.527 us | 2.497 us | 42.583 us | 0.361 us | 0.035 ms |
| 512 | **0.182 us** | 2.733 us | 32.589 us | 0.206 us | 0.048 ms |
| 1,024 | **0.212 us** | 2.700 us | 31.614 us | 0.242 us | 0.113 ms |
| 2,048 | 0.187 us | 2.729 us | 31.046 us | **0.090 us** | 0.196 ms |

The bold elapsed results are not a claim that strict PIR is intrinsically
cheaper than decoys: the packed kernel ran on the GPU while the visible control
ran on the CPU, and energy was not measured for the CPU control. They do show
that once 512+ subscribers are ready, strict packed presence no longer carries
a meaningful server-latency penalty on this host. At batch 128 it is 81x less
aggregate kernel time than the pinned GPU-DPF path. The older 16-byte histogram
was unnecessary for a hit alert and is retained only as a control.

| Per subscriber | Packed Dense | GPU DPF | 100 visible buckets |
|---|---:|---:|---:|
| One-time registration to all servers | 16,384 B | 4,160 B | 400 B |
| Stored registration/server | 8,192 B | 2,080 B | about 400 B plus index overhead |
| Result/epoch before framing | 2 B | 32 B | 1,600 B |
| Server-count flexibility | 2, 3, or more XOR replicas | exactly 2 | one visible server |

Selectors/keys may be retained on the GPU. If packed selectors are copied from
host memory every epoch, the measured aggregate H2D cost at batch 512 is an
additional 4.778 us/subscriber, far above its 0.182-us kernel. Production must
therefore budget GPU-resident registrations, stage long-lived pinned batches,
or explicitly include PCIe work. One million Dense subscriptions occupy about
8.2 GB on each server and need roughly 0.18 seconds aggregate kernel time per
epoch by the batch-512 result (about 0.09 seconds with two replicas in parallel),
before scheduling and network. The DPF state is four times smaller but its
measured server work is much larger.

The architecture changes the scale law from per-event Compact DPF
`O(events * subscriptions)` to ordinary `O(events)` bitmap construction plus
packed PIR `O(65,536 * subscriptions)` once per epoch. Against the measured
0.78-us immediate Compact-DPF baseline, batch-512 packed presence breaks even
at one event/epoch and wins increasingly as event count rises. At the 5,000 TPS
maximum and a two-second epoch, 10,000 events and 10,000 subscribers imply
about 1.82 ms of aggregate packed GPU kernel work versus about 78 seconds of
aggregate per-event CPU DPF work. This cross-hardware estimate is a capacity
direction, not an identical-hardware speedup.

Use packed presence for Mizu routing-tag hints, Shinzo address/topic alerts and
DefraDB equality change feeds whenever the product accepts the declared epoch
latency. Keep immediate Compact DPF only for a genuinely sub-epoch requirement.
On a hit, the client still needs a fixed-window, padded private snapshot fetch;
the alert does not return the matching document.

## Single-server computational lane

### Same-GPU Dense versus pinned GPU-DPF

The CUDA adapter pins the archived
[`facebookresearch/GPU-DPF`](https://github.com/facebookresearch/GPU-DPF) POC at
commit `ce23a06af884ee54300b5bc5fd5350e445f10b0b`. It compares the upstream
ChaCha12 DPF expansion/fused table reduction with a bit-packed Dense XOR control
on the identical 120-useful-byte/128-physical-byte GPU table. Every sample
reconstructs and verifies every selected record. The two replicas execute
sequentially on one RTX 2070 SUPER; values below are their aggregate kernel
time per query.

| Rows / physical table per replica | Ready queries | Dense XOR | GPU DPF | Dense upload/query | DPF upload/query |
|---:|---:|---:|---:|---:|---:|
| `2^20` / 128 MiB | 1 | 0.769 ms | 51.044 ms | 256 KiB | 4,160 B |
|  | 8 | 0.734 ms | 6.275 ms | 256 KiB | 4,160 B |
|  | 32 | 0.726 ms | 1.602 ms | 256 KiB | 4,160 B |
|  | 128 | **0.709 ms** | 0.788 ms | 256 KiB | 4,160 B |
| `2^23` / 1 GiB | 1 | 5.786 ms | 408.237 ms | 2 MiB | 4,160 B |
|  | 8 | 5.733 ms | 50.545 ms | 2 MiB | 4,160 B |
|  | 32 | 5.625 ms | 12.626 ms | 2 MiB | 4,160 B |
|  | 128 | **5.636 ms** | 6.461 ms | 2 MiB | 4,160 B |
| `2^25` / 4 GiB | 1 | 23.073 ms | 1,667.076 ms | 8 MiB | 4,160 B |
|  | 8 | 23.195 ms | 203.977 ms | 8 MiB | 4,160 B |
|  | 32 | 23.255 ms | 52.561 ms | 8 MiB | 4,160 B |
|  | 128 | **23.481 ms** | 28.836 ms | 8 MiB | 4,160 B |

DPF amortizes unused GPU occupancy, but it does not overtake Dense by batch 128
at any locally executable size. At `2^25`, batch 128, Dense also used about
3.99 J/query versus DPF's 4.51 J/query by coarse 10-ms NVML sampling. DPF's
real advantage is 63x--2,017x smaller upload as `N` grows, not lower total
server work. This archived implementation is useful evidence, not the last word
on modern DPF kernels; production evaluation should repeat on the intended GPU
and a reviewed library.

Dense retains information-theoretic `n`-replica flexibility. This DPF path is
computational and exactly two-party. The largest run is the local fit limit,
not a protocol limit: the 4 GiB table plus selectors and workspaces fit the
8 GB card; a 16 GiB table does not.

Reproduce both the snapshot and live matrices with:

```bash
bash tools/pir-poc/research/run-gpu-pir-defra.sh full
```

### Same-GPU Dense, DPF and InsPIRe result

The Ethereum-oriented
[`inspire-gpu`](https://github.com/keewoolee/inspire-gpu) server now also runs
locally at pinned commit `c14d1d84a425cdaa9f86ed09465b09c9c9802f13`.
The checked adapter encodes the same deterministic 120-byte records into
InsPIRe's 64 15-bit slots. It changes no cryptographic kernel or parameter.
All three results below therefore use the same RTX 2070 SUPER, `2^23` records,
one 1 GiB physical table and matching ready-query batch sizes. They are p50s
over five fresh processes with alternating suite and kernel order:

| Ready queries | Dense aggregate server/query | GPU-DPF aggregate server/query | InsPIRe server/query |
|---:|---:|---:|---:|
| 1 | **6.17 ms** | 437.73 ms | 32.21 ms |
| 2 | **6.15 ms** | 215.55 ms | 22.42 ms |
| 4 | **6.12 ms** | 108.00 ms | 21.17 ms |
| 8 | **6.18 ms** | 54.18 ms | 20.03 ms |
| 16 | **6.14 ms** | 27.17 ms | 18.86 ms |
| 32 | **6.14 ms** | 13.74 ms | 18.86 ms |

This is aggregate work: Dense and DPF sum two replica evaluations, while
InsPIRe uses one server. Its service p50, including measured CPU query unpack
and response compression, was 33.64 ms at batch 1 and 20.37 ms/query at batch
32. Dense used 5.22x less aggregate server time than the InsPIRe GPU primitive
at batch 1 and 3.07x less at batch 32 on this card.

The client/network direction differs sharply. At batch 1, Dense generated both
shares in 2.68 ms and uploads 2 MiB; GPU-DPF generated its keys in 0.084 ms and
uploads 4,160 B; InsPIRe generated and packed its query in 47.48 ms and uploads
379,904 B. InsPIRe downloads 12,288 B, while each replicated adapter returns
240 aggregate bytes before framing. At a hypothetical 10 Mbit/s link, byte
serialization alone is about 1,678 ms for Dense, 3.52 ms for DPF and 314 ms for
InsPIRe. These are network projections, not server work or complete latency.

The exact-corpus InsPIRe cold-snapshot p50 spent 9.03 s on host materialization,
5.56 s on GPU preprocessing and 3.91 s constructing the context. First-online
p50s were 8.10 ms Dense, 446.93 ms GPU-DPF and 179.01 ms InsPIRe. InsPIRe's
58.27--308.05 ms range exposes material lazy-runtime variation rather than
hiding it in a warm mean. Dense's GPU table setup remains a deterministic
initializer rather than a common storage read, so cold-storage totals are not
directly comparable. Reproduce and inspect the complete schema in
[`FULL_COMPARISON.md`](FULL_COMPARISON.md).

### Same-CPU Dense versus Poulpy InsPIRe2

The pinned `poulpy-pir` adapter consumes the same deterministic 120 useful
bytes (128 physical) at `2^23` entries on the AVX2 Ryzen 7 3700X. Dense uses
eight worker threads per replica; Poulpy uses its AVX2/FMA backend:

| Batch | Dense two-replica aggregate wall/query | Poulpy wall/query | Poulpy summed phase work/query |
|---:|---:|---:|---:|
| 1 | **115.90 ms** | 415.10 ms | 414.41 ms |
| 8 | **49.11 ms** | 396.22 ms | 1,450.53 ms |
| 32 | **67.59 ms** | 224.38 ms | 1,178.94 ms |

Poulpy had the smaller query (428,117 B versus 2 MiB) but downloaded 196,889 B,
used 5.71--6.87 GiB peak RSS and required 30.5--36.1 seconds of offline work.
The Dense rows use the exact common 120 useful/128 physical-byte encoding; the
last eight bytes are zero in both adapters. The Dense adapter does not yet
report thread CPU-seconds, so its parallel wall
column is not relabeled energy or exact CPU work. The deployment conclusion is
narrow but sufficient: on this edge-class AVX2 host, CPU InsPIRe did not beat
Dense server wall time. The GPU lane is the relevant InsPIRe deployment.

### Ethereum GPU reference versus 100 visible candidates

The `gpu-reference-decoy` experiment accepts the published warm, index-based
[`inspire-gpu`](https://github.com/keewoolee/inspire-gpu) results as the strict
single-server reference. It measures one target plus 99 visible present
ordinals over the same 120-byte entry counts. Every row is copied into the
response; the client is not credited with server-side filtering.

| Logical database | Visible 100 p50 / p95 | Visible q/s | InsPIRe single | Single/visible | InsPIRe batched/query | Batched/visible |
|---:|---:|---:|---:|---:|---:|---:|
| 1,006,632,960 B (`2^23` rows) | 11.38 / 13.53 us | 87,871 | 2.6 ms | 228.5x | 1.727 ms | 151.8x |
| 4,026,531,840 B (`2^25` rows) | 12.51 / 14.00 us | 79,935 | 7.9 ms | 631.5x | 3.876 ms | 309.8x |
| 16,106,127,360 B (`2^27` rows) | 12.84 / 13.87 us | 77,911 | 31.1 ms | 2,423.0x | 8.7 ms | 677.8x |

Visible-candidate traffic is 800 B up plus 12,000 B down. InsPIRe publishes a
383 KiB round trip, 30.64x more. The GPU is entirely server-side: the published
16 GB instance occupies 25.77 GB on one 32 GB RTX 5090. Its client is portable
CPU code, holds no database-dependent hint, takes about 31 ms to construct a
query, and spends only a few additional milliseconds packing and extracting.

The table reports the median process result from five fresh full runs, each
using eleven samples. The process-level p50 ranges were 9.67--16.04 us,
11.05--16.32 us, and 11.88--15.08 us respectively. A temporary file mapping spans each
exact logical address space; 102,400 scheduled rows touch an estimated 388--431
MB of randomly distributed host pages and are prefaulted before timing. This is
a cache-defeating warm point-read benchmark, not proof that the entire 16 GB
database was resident and not a cold-storage measurement. It begins at public
ordinals because `inspire-gpu` is also index-based; keyword mapping is an outer
layer for both.

The result is decisive only for total server work under weaker privacy. One
hundred indexed reads stay `O(100)` as `N` grows; double-stateless PIR remains
`O(N)`. GPU acceleration makes strict PIR latency and capacity plausible, but
does not make it comparable to visible candidates in compute or traffic. The
trade is candidate-set privacy with severe longitudinal leakage versus strict
computational privacy among every entry.

The pinned official SimplePIR implementation was adapted to the same 262,144 x
96-byte corpus and reconstructed every byte. Three-sample local results:

The reproduction runners now recompute the export's SHA-256 before invoking an
external artifact. The recorded SimplePIR and YPIR timings predate this
post-run guard; their selected-page checks and recorded BLAKE3 still identify
the corpus, and no timing was silently rerun after hardening.

| Scheme | Server p50 | Client query | Client recover | Upload | Download | Reusable hint/state |
|---|---:|---:|---:|---:|---:|---:|
| SimplePIR | 3.17--3.50 ms | 151.0 ms | 34.8 ms | 481,824 B | 20,352 B | 20,840,448 B client hint |
| DoublePIR | 7.95--11.61 ms | 1,745.2 ms | 855.7 ms | 6,300,864 B | 1,639,936 B | 67,108,864 B client hint |
| YPIR, AVX2 fallback | 80.66 ms | 62.31 ms | 5.33 ms | 573,440 B | 24,576 B | no client hint; 741,573,720 B offline server state |

SimplePIR passes the POC's preliminary desktop payload/resource envelope and
is a serious fallback when non-collusion is unacceptable; actual ARM phone CPU,
RSS, energy, and networking remain unmeasured. It is not directly ranked
against Dense because it makes a computational single-server assumption and
has different setup/traffic. DoublePIR is dominated on this corpus.

YPIR commit `b9801521301f34502496d694b2ac034857104ebc` (tag
`artifact-evaluation`, Zenodo 13117988) passed both official tests and four
common-corpus reconstructions. The adapter packs 70 useful pages per aligned
row, including the final partial row. This host lacks AVX-512, so YPIR used the
artifact's scalar/non-explicit path: it reduced client online CPU by about 64%
versus SimplePIR and removed the client hint, but server online was about 23x
slower, preprocessing took 2.86 s, maximum process RSS was 2,084,840 KiB, and
serialized offline server state was 741,573,720 B. This is an AVX2-host result,
not a reproduction of the paper's AVX-512 result.

The official InsPIRe archive is pinned by Zenodo record 17361471 and
`artifact-final.zip` MD5 `bfa9edb2d8403f0dc20830fb40608b78` because it has no
Git metadata. Its source invokes AVX-512 unconditionally, so it is recorded as
blocked on this Ryzen host; no substitute timing is invented.

The pinned MPC4J KPIR artifact also passed the requested PGM_INDEX,
SIMPLE_BIN, SIMPLE_NAIVE, and CHALAMET individual/batch hit-and-miss gates at
4,093 keys x 8 bytes. That is a correctness gate, not common-corpus
performance data; the selected four passed, while five unrelated native
parameterizations required an unavailable `mpc4j-native-tool`.

## Three servers

Replicated Dense query sharing is server-count agnostic: for `n` servers the
client samples `n-1` random selectors and sets the final share so all shares
XOR to the target. With three servers the query remains private if any one
server is non-colluding, including when the other two collude. Expected
aggregate selected-row work grows from one table at two servers to 1.5 tables
at three, and upload/response grow by 50%.

This is a privacy improvement, not Byzantine correctness or one-server failure
tolerance. All three answer shares are necessary. A threshold-1-of-3 design
would use different assumptions and algebra; a maliciously robust result also
needs authentication/proofs or coded recovery.

## Rejected or separate lanes

| Candidate | Evidence-based disposition |
|---|---|
| Legacy overprovisioned document/hash-table Dense | Scans empty/document rows; replaced by compact populated tag pages and exact ordinals |
| Fuse-3 | More storage/upload/build memory than Fuse-4; timing order was unstable |
| Persistent subset XOR on exact MPHF | Up to 32.875x storage for inconsistent elapsed benefit |
| Standard Ribbon | Correct constant-metadata fallback, but MPHF and Fuse-4 use less work here |
| BuRR | Promising near-minimal cell overhead, but bump/layer metadata and official implementation must be measured |
| Compact DPF for snapshot | Tiny upload but full-domain expansion is too much server CPU; selected library is two-party |
| Chalamet as the primary mobile path | Small upstream correctness scale passed, but common-corpus client cost is not yet measured; SimplePIR is currently the stronger single-server artifact result |
| Path ORAM | Solves mutable access-sequence privacy, requiring a position map, stash, path reads/writes, reshuffles, and client state far beyond immutable retrieval |
| TEE + ORAM | Adds hardware/attestation and side-channel trust while retaining ORAM complexity |
| RAID-PIR at 3 servers with privacy against 2 colluders | `r=k=3` leaves a complete table at every server; no distribution saving for this topology |
| Pairwise two-party DPF keys on three servers | Unsafe: a colluding pair can obtain a complete key pair and recover the target |
| GPU/FPGA/PIM | Separate accelerator lane after CPU counters and table format stabilize; no measured claim yet |

## Production boundary

PIR remains a sidecar immutable serving index. It does not change DefraDB's
CRDT merge, document store, transaction, ACP, or normal query planner. A
generation is built from an authorized deterministic export, committed by a
signed manifest, replicated unchanged, and atomically published. The existing
Defra update event/subscription boundary can feed the live evaluator without
putting experimental cryptography in the mutation path.

PIR hides selection; it does not grant access. A plaintext PIR artifact may be
shared only within one authorization cohort. Otherwise the projection is
encrypted before table construction under an application/cohort key that the
PIR replicas do not hold.

See `../PRODUCTION.md`, `EXPLORATION.md`, `ARTIFACTS.md`,
`../PORTABLE_READINESS.md`, and the benchmark JSON output for assumptions and
phase-separated evidence.

## Reproduce

Use the current commands in [`README.md`](README.md). Every benchmark emits
JSON and correctness-checks recovered pages, projections, or event outputs.
Redirect stdout to a file when retaining an artifact.
