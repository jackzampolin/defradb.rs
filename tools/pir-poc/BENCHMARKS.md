# PIR benchmark evidence

Evidence for [DECISIONS.md](DECISIONS.md), grouped by the selected paths first
and alternative protocols second. Existing results only; no new benchmark run.

`M` = measured; `P` = projected; `L` = logical execution with a reused resident
plane. Server figures aggregate replicas unless labelled otherwise. See
[measurement notes](#reading-the-measurements) for hardware and exclusions.
Decoys are the weaker-privacy comparison baseline, not the preferred protocol.

## Selected protocols

Dense snapshots and packed-presence epoch alerts are the recommendations.
Their large-workload limits stay here alongside favorable results.

### Snapshot costs

PIR projections below are compared with CPU decoy measurements on small fixtures
with matching result shapes, not matching large-table sizes. Ratios are
planning estimates, not measured same-scale speedups. Fixed row sizes, safe
keyword mapping, and a bounded result schedule are prerequisites.

Server-time increase = `100 × (PIR aggregate server time / decoy server time − 1)`.
Percentages use the displayed timings and are rounded; they are not energy
measurements. New scope-matched benchmarks are needed before capacity promises.

Routing retrieval, historical logs and secondary-index rows below measure one
private page versus one page per candidate. They do **not** measure returning
every match for a tag. The complete-result billion-tag experiment is separate.

| Snapshot use case | Production query class | Strict upload / download | Strict server / client CPU | 100-decoy upload / download | Decoy server / client CPU | Server-time increase vs decoys |
|---|---|---:|---:|---:|---:|---:|
| Mizu routing-tag retrieval stage | At most 320K populated routing pages in 32 blocks | 80.0 KB / 1.61 KB | **~1.48 / ~0.11 ms `P`** | 3.10 KB / 80.4 KB | 0.0284 / 0.0042 ms `M` | ~+5,100% `P` |
| Mizu active nullifier witness | 1.05M initial nullifiers, 1.08M after block update | 1.082 MB / 116.7 KB | **34.45 / 22.29 ms `M`** | 3.20 KB / 200.8 KB | 0.141 / 0.00010 ms `M` | ~+24,300% `M` |
| Shinzo historical logs | At most 320K populated pages in 32 blocks | 80.0 KB / 1.10 KB | **~1.01 / ~0.11 ms `P`** | 3.61 KB / 54.8 KB | 0.0264 / 0.0042 ms `M` | ~+3,700% `P` |
| Shinzo transaction receipt | At most 10K receipts in one block | 2.50 KB / 368 B | **~0.011 / <0.01 ms `P`** | 3.79 KB / 18.4 KB | 0.0257 / 0.0039 ms `M` | ~−57% `P` |
| DefraDB document by ID | 1M fixed 256-byte projections | 250 KB / 560 B | **~1.61 / ~0.33 ms `P`** | 3.30 KB / 28.0 KB | 0.0265 / 0.0042 ms `M` | ~+6,000% `P` |
| DefraDB secondary-index page | 1M fixed four-value pages | 250 KB / 1.10 KB | **~3.15 / ~0.33 ms `P`** | 3.90 KB / 54.8 KB | 0.0266 / 0.0039 ms `M` | ~+11,700% `P` |

#### Why receipt retrieval is cheaper

Dense uses the same XOR sharing in these cases. For a fixed replica count and
one row retrieval, scan cost grows approximately with **row count × encoded row
width**. The receipt model knows the inclusion block; it does not scan global
transaction history. These are fixed projections, not arbitrary full receipts.

| Modeled artifact | Rows | Encoded row | Table bytes per replica |
|---|---:|---:|---:|
| Shinzo receipt, one known block | 10,000 | 184 B | 1.84 MB |
| Mizu routing retrieval, catch-up window | 320,000 | 804 B | 257.28 MB |
| Relative routing / receipt footprint | 32x | ~4.37x | ~140x |

The receipt advantage is a smaller search domain and narrower result, not
better cryptography. **Its projected win over decoys is unverified at the
modeled scale**: the projection excludes GPU launch overhead and the decoy
control uses a smaller CPU fixture. Unknown inclusion blocks require a
different, larger workload.

This footprint comparison explains absolute scan cost for the modeled pages;
it does not rank receipt versus complete-tag retrieval relative to decoys.

#### Complete-query work relative to decoys

For an all-match request, charge all real-tag pages on the private path and
all pages for all 100 candidates on the decoy path. Fetching continuations only
for the real candidate can reveal it. The small fixtures implement page-level
reads in [the fixture runner](src/use_case_gallery.rs), not this complete schedule.

In an ideal compact layout, let `N` be total values and `f` the values per tag.
If all candidates have equal cardinality and PIR traverses each payload once
across the complete group retrieval, payload work is approximately:

`Dense / decoys ≈ N / (100 × f) = 1 / (100 × match fraction)`.

This is a geometry model, **not an elapsed-time or energy prediction**. It omits
kernel constants, selector work, metadata and padding; extra full-table scans
for each continuation can invalidate the one-traversal assumption. With skewed
tags, use the actual total payload of all candidate groups in the denominator.

| Workload growth | Expected relative payload work | Why |
|---|---|---|
| More documents, fixed matches per tag | Grows approximately linearly | Dense grows; decoy output stays fixed |
| More documents, fixed matching fraction | Approximately constant | Both Dense and complete decoy output grow |
| Wider values, same layout and matching fraction | Approximately constant | Both paths process wider values |

| Illustrative equal-cardinality query | Candidate payload as fraction of corpus | Ideal Dense / decoy payload work |
|---|---:|---:|
| Each tag matches 1% | 100% across 100 distinct candidates | ~1x |
| Each tag matches 0.01% | 1% across 100 distinct candidates | ~100x |
| Unique-key lookup over 1M values | 0.01% across 100 candidates | ~10,000x |

These examples assume disjoint, equally sized groups and equal value widths.
There is no automatic 1% performance crossover. The measured logical 1B-row
experiment below uses complete candidate groups at 0.01% selectivity; it does
not validate the other illustrative cases or resident billion-row performance.

#### Larger scopes and continuation cost

| Workload | PIR server | Decoy server | PIR client / decoy client | PIR response / decoy response | Evidence / decision |
|---|---:|---:|---:|---:|---|
| Global 1B documents, 0.01% tag matches, five encrypted fields | 10.40 s | 106.79 ms | ~38.9 / 31.39 ms | 38.86 MB / 1.943 GB | `L`; ~97x server cost. Require a partition; neither global response path is the recommended endpoint. |
| 256 independent continuation scans of a resident 1 GiB artifact | ~1.58 s | Not measured | Not measured | Depends on result row | `P`; shorten the window/cap pages. Not a measurement of an optimized batched traversal. |
| Active nullifier tree beyond the 1,081,344-nullifier updated workload | Not measured | Not measured at this scale | Not measured | Not measured | Beyond the tested envelope; the guide uses ~1M as a conservative initial target, not a measured crossover. |
| Sparse tree with 32B possible coordinates | Depends on populated state/layout | Depends on witness layout | Not measured | Not measured | Coordinate space is not table row count; checkpointing alone does not reduce active state. |

The billion-document run traverses each logical stripe but reuses one resident
plane. It preserves XOR/wire geometry, not large-memory or storage behavior.

| Billion-document geometry | Value |
|---|---:|
| Distinct tags / target matches | 10,000 / 100,000 |
| Encrypted value width | 188 B |
| Stripe planes | 391 |
| Representative resident plane | 496.88 MB |
| Logical projection per replica | 194.28 GB |

Sources: [active-nullifier benchmark](src/benchmark/active_nullifier.rs),
[billion-tag benchmark](src/benchmark/billion_tag.rs),
[small-fixture runner](src/use_case_gallery.rs).
Generic scale geometry and blind-search measurements are below under
[blind exact search](#blind-exact-search-experiment).

### Live costs

Same public epoch, fixed bucket domain, and ready subscriber batch. Strict
kernels run on GPU; the visible control runs on CPU. Similar elapsed time is
not an intrinsic speed or energy-equivalence claim.

| Epoch protocol | Aggregate server/subscriber | One-time registration | Response/epoch | Servers |
|---|---:|---:|---:|---|
| Packed-presence Dense | **0.182 us** | 16,384 B | 2 B | 2, 3, or more |
| 100 visible buckets | 0.206 us | 400 B | 1,600 B | 1 visible server |

| Capacity/setup item | Packed Dense | Visible control | Scope |
|---|---:|---:|---|
| Ready batch / bucket domain | 512 / 65,536 | Same | Measured comparison |
| Server-time increase vs visible | ~−12% | 0% (baseline) | Derived from resident GPU kernel versus CPU control |
| Client registration CPU | 33.8 us | 0.240 us | Once per registration |
| Retained selector state | 8 KiB/subscriber/server | Different indexed structure | Required for resident-kernel result |
| Host-to-GPU selector transfer | +4.778 us/subscriber/epoch | Not applicable | Measured; excluded from resident kernel figure |
| One million subscribers | ~182 ms aggregate kernel/epoch; ~8.2 GB selector state/server | Not extrapolated | Linear projection, not a demonstrated deployment |
| Per-epoch client combine | Not separately measured | Not separately measured | Do not infer end-to-end latency from kernel time |

The selected layout stores a yes/no bit per bucket, not a payload or count.
This is still Dense: bitmap packing and block-level aggregation make the job
smaller. Reusing registrations avoids repeated selector upload. A generic
Dense implementation specialized to the same bit representation is this path,
not a competing privacy protocol.

These alert costs exclude the subsequent private retrieval. Fetching only after
a hit also creates timing leakage; see [PRIVACY.md](PRIVACY.md).

Source: [epoch measurements and batch sizes](research/COMPARISON.md#fixed-epoch-packed-presence-result).
For full-row Dense and DPF controls, see [alternative alert layouts](#alternative-alert-layouts).

### Small executable fixtures

The `use-cases` command covers the product rows in the decision guide. Mizu
routing-tag alert/retrieval has separate alert and snapshot fixtures.
Snapshot fixtures verify recovery, fixed decoding and absent-key handling;
live fixtures verify immediate Compact-DPF match/miss.

| Fixture property | Value |
|---|---|
| Rows / samples | 256 rows / median of 31 release-mode operations |
| Snapshot engines | Two-replica Dense versus one-server 100-key reads on identical PrivateTable rows |
| Snapshot query upload | 64 B across both replicas |
| Client directory | About 37.2 KB, separately from query upload |
| Excluded phases | HTTP/OHTTP, queues, artifact construction and metadata download |
| Reproduction | `cargo run -p pir-poc --release -- use-cases [mizu\|shinzo\|defra]` |

| Use case | PIR server | Decoy server | PIR server delta | PIR client | Decoy client | Upload PIR / decoy | Download PIR / decoy |
|---|---:|---:|---:|---:|---:|---:|---:|
| Mizu routing-tag retrieval | 5.9 us | 28.4 us | 79% faster | 0.8 us | 4.2 us | 64 B / 3,103 B | 1,608 B / 80,400 B |
| Mizu nullifier witness | 13.1 us | 29.1 us | 55% faster | 1.0 us | 3.7 us | 64 B / 4,190 B | 4,064 B / 203,200 B |
| Shinzo historical logs | 4.3 us | 26.4 us | 84% faster | 1.1 us | 4.2 us | 64 B / 3,614 B | 1,096 B / 54,800 B |
| Shinzo transaction receipt | 2.3 us | 25.7 us | 91% faster | 0.7 us | 3.9 us | 64 B / 3,794 B | 368 B / 18,400 B |
| DefraDB document by ID | 2.9 us | 26.5 us | 89% faster | 0.8 us | 4.2 us | 64 B / 3,299 B | 560 B / 28,000 B |
| DefraDB secondary-index page | 4.2 us | 26.6 us | 84% faster | 0.8 us | 3.9 us | 64 B / 3,898 B | 1,096 B / 54,800 B |

These tiny fixtures establish correctness, not capacity. Their directory lookup
overhead explains why decoys can be slower at this size. Use the scale-qualified
tables above for planning. Source: [fixture implementation](src/use_case_gallery.rs).

## Alternatives and conditional fallbacks

These results explain why other paths are not the default for minimizing total
server work. Some save client bandwidth, remove non-collusion assumptions, or
support immediate delivery; others were excluded for complexity or weaker
privacy, **not because a benchmark proved them slower**.
SinglePass remains conditional on amortizing immutable-client setup; its
[setup and warm-query evidence](research/WARM_STATEFUL.md) is separate.

### Protocol comparison

Same deterministic useful rows, checked reconstruction, and alternating fresh
processes. GPU columns below are per-query aggregate server time; the decoy
column is a separate same-host CPU indexed-read control.

| Physical table | Batch | Dense XOR, 2 servers | GPU-DPF, 2 servers | InsPIRe GPU, 1 server | 100 visible candidates |
|---:|---:|---:|---:|---:|---:|
| 1 GiB / 8.39M rows | 1 | **6.17 ms** | 437.73 ms | 32.21 ms | 0.01138 ms |
| 1 GiB / 8.39M rows | 32 | **6.14 ms** | 13.74 ms | 18.86 ms | 0.01138 ms |
| 4 GiB / 33.55M rows | 1 | **23.07 ms** | 1,667.08 ms | capacity-blocked | 0.01251 ms |
| 4 GiB / 33.55M rows | 128 | **23.48 ms** | 28.84 ms | capacity-blocked | 0.01251 ms |

| Cold client, 1 GiB corpus | Query-generation CPU | Query upload |
|---|---:|---:|
| Dense, two replicas | 2.68 ms | 2 MiB |
| GPU-DPF, two replicas | 0.084 ms | 4,160 B |
| InsPIRe, one server | 47.48 ms | 379,904 B |

| Same-host CPU, 1 GiB corpus | Dense server wall time | Poulpy InsPIRe2 server wall time |
|---|---:|---:|
| Batch 1 | 115.90 ms | 415.10 ms |
| Batch 8 | 49.11 ms | 396.22 ms |
| Batch 32 | 67.59 ms | 224.38 ms |

The CPU wall-time table is not aggregate core work. Complete phase accounting,
pins, repetitions and hardware limits:
[full comparison](research/FULL_COMPARISON.md).
Warm setup and amortization:
[SinglePass benchmark](research/WARM_STATEFUL.md).

### Alternative alert layouts

Same epoch and bucket domain. The full-row controls keep an unnecessary
histogram for a yes/no alert; this comparison tests layout specialization,
not a universally faster Dense primitive. All strict timings sum two replicas.

| Ready subscribers | Packed Dense GPU / subscriber | 16-byte-row Dense GPU / subscriber | GPU-DPF / subscriber | 100-visible CPU / subscriber |
|---:|---:|---:|---:|---:|
| 1 | 56.947 us | 65.102 us | 2,199.250 us | 0.075 us |
| 512 | 0.182 us | 2.733 us | 32.589 us | 0.206 us |

| Layout comparison | Packed presence | Full-row Dense control |
|---|---:|---:|
| Data table, 65,536 buckets | 8 KiB | 1 MiB |
| Stored information per bucket | One presence bit | 16-byte histogram row |
| Kernel-time reduction at batch 512 | ~93.3% (about 15x faster) | Baseline |

Packing shrinks the data table but not the registered selector state. The
large ready batch amortizes GPU launch overhead; the single-subscriber row
does **not** show parity with decoys. Bitmap construction, selector transfers
and hit retrieval are outside these kernels.

GPU-DPF needs less registration traffic/state, but does more server work in
this measured alert workload. Immediate DPF remains conditional on needing
sub-epoch delivery, rather than being eliminated from every application.

Source: [full epoch results](research/COMPARISON.md#fixed-epoch-packed-presence-result).

### Immediate per-event endpoint

| Use case | DPF server/event | Decoy server/event | DPF slowdown | DPF client/event | Decoy client/event | One-time registration DPF / decoy | Response DPF / decoy |
|---|---:|---:|---:|---:|---:|---:|---:|
| Mizu routing-tag alert | 0.779 us | 0.0046 us | 169x | 0.0087 us | 0.0003 us | 640 B / 400 B | 32 B / 1 B |
| Shinzo contract alert | 0.782 us | 0.0040 us | 195x | 0.0088 us | 0.0003 us | 640 B / 400 B | 32 B / 1 B |
| DefraDB private change feed | 0.779 us | 0.0054 us | 144x | 0.0090 us | 0.0003 us | 640 B / 400 B | 32 B / 1 B |

This is the served Compact-DPF fixture, not the selected packed-epoch design.
Per-event work multiplies by both event and subscriber counts. Different
baseline response policies mean this table must not be substituted for the
fixed-epoch comparison.

Sources: [epoch CUDA adapter](research/gpu_dpf_adapter/README.md) and
[immediate CPU fixture runner](src/use_case_gallery.rs).

### Blind exact-search experiment

An independent exporter computes a keyed BLAKE3 search token and encrypts the
fixed value with AES-256-GCM. The query server performs a token-index lookup;
the authorized client decrypts the result. This is not PIR.

| Rows | Build | Raw entries | Client token | Server lookup/copy | Client decrypt | Upload | Download |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1K | 0.963 ms | 69 KB | 0.2 us | 0.1 us | 0.6 us | 32 B | 37 B |
| 1M | 1,471.9 ms | 69 MB | 0.2 us | 0.1 us | 0.5 us | 32 B | 37 B |
| 1B | not resident-executed | at least 69 GB | exact geometry only | exact geometry only | exact geometry only | 32 B | 37 B |

| Measurement scope | Qualification |
|---|---|
| Payload / samples | Eight-byte useful value; median of 101 resident lookups |
| Timing | Excludes transport; sub-microsecond lookup values are near timer resolution |
| Raw storage | Token plus encrypted value, excluding hash-table/allocator overhead |
| Largest executed resident index | 1M rows; 1B is geometry only |

#### Scale geometry, not latency measurements

| Rows | Dense positions visited | Dense payload XORs | 100-decoy rows | Blind-index rows | Dense XORs / decoy | Dense upload | Blind upload | Current JSON locator | 2.4-bit MPHF | Min blind index |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1K | 2,000 | 1,000 | 100 | 1 | 10x | 250 B | 32 B | 145 KB | 300 B | 69 KB |
| 1M | 2,000,000 | 1,000,000 | 100 | 1 | 10,000x | 250 KB | 32 B | 145 MB | 300 KB | 69 MB |
| 1B | 2,000,000,000 | 1,000,000,000 | 100 | 1 | 10,000,000x | 250 MB | 32 B | 145 GB | 300 MB | 69 GB |

Dense figures sum two replicas. Locator estimates are separate cold-client
metadata, not query upload. The MPHF size is a research assumption, not the
served JSON directory or a promise that arbitrary keys can be mapped privately.
A larger inline value increases blind-index storage; the minimum above is for
the tiny locator payload only.

#### Leakage conditions

- Repeated tokens, selected entries, result sizes and update timing are visible.
- The server must not hold the search key or know the plaintext-to-token map.
- Encryption inside a PIR row protects its contents but does not reduce scan work.

Source: [blind-index implementation](src/encrypted_search.rs).
Leakage research:
[SEAL](https://www.usenix.org/system/files/sec20-demertzis.pdf),
[Hiding the Access Pattern Is Not Enough](https://www.usenix.org/system/files/sec21summer_oya.pdf),
[SQL leakage-abuse attacks](https://www.usenix.org/conference/usenixsecurity24/presentation/hoover).

### Alternatives not selected

| Alternative | Reason it is not a default |
|---|---|
| ChalametPIR | Stateful hint and client work conflict with the cold-client objective. |
| Finite differences | Favorable small CPU case, but storage/response expansion and no validated large/GPU result. |
| Path ORAM | Adds client state and interactive read/write machinery beyond the retrieval requirement. |
| TEE + ORAM | Adds hardware trust, attestation and side-channel deployment burden. |
| Cuckoo / persistent subset-XOR / Ribbon variants | No consistent overall win over the selected compact layouts. |

Detailed evidence remains in the [research archive](research/README.md).

## Reading the measurements

- Server figures sum the participating replicas unless explicitly labelled wall
  time. They are elapsed compute measurements, not CPU-seconds or joules.
- `M`: measured. `P`: linear projection. `L`: logical workload execution
  reusing a representative resident plane, not a fully resident database.
- Query/answer bytes exclude metadata setup, transport framing and padding.
  An ordinal directory can dominate cold setup and expose populated keys.
- Snapshot projections use a resident GPU bandwidth model. Small-table launch
  overhead, HTTP/OHTTP, storage faults and queue delay are excluded.
- Decoys have weaker privacy. The client processes only its target, never
  decrypts all decoy values.

| Projection assumption | Value / qualification |
|---|---|
| GPU / CPU hosts | RTX 2070 SUPER / Ryzen 7 3700X |
| Dense server anchor | 6.17 ms aggregate per resident GiB, two replicas |
| Dense client generation anchor | 2.68 ms per 2 MiB aggregate selector upload |
| Maximum event-rate sizing scenario | 5K TPS and a two-second epoch; not a forecast or a universal chain block time |
| Default catch-up model | 32 epochs, at most 320K events/pages; populated-page count and padding must be checked |
| Nullifier tested workload | 1,048,576 initial nullifiers + 32,768 inserts = 1,081,344 final nullifiers, plus a sentinel; no measured failure cliff |
| Nullifier timing scope | Synthetic tree-shaped row fixtures, not production consensus/Poseidon execution; canonical verification is tested separately |
