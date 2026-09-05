# Cold predicate search: results

Follow-up: [indexed Dense across use-case shapes](INDEXED_USE_CASE_MEASUREMENTS.md)
tests wider application projections, longer sessions, canonical witnesses and
public epoch bitmaps. See [the updated decisions](../DECISIONS.md) for selection.

Research cutoff: 2026-09-05. This pass tests **complete ad hoc tag and tree-value
queries**, including absent values, collisions, duplicate matches, padded
continuations and payloads. “Cold” is the Shinzo/Mizu product workload; a fresh
client without hints is the conservative primary measurement lane. Client reuse
is a separate experiment. None of these measurements is a production rollout.

The best measured tag setup is **compact public navigation + binary answer
pages + two-server Dense PIR**. The index improves the complete search; Dense
still performs a linear private retrieval over the resulting page table.
A private prefix index or XOR retrieval dictionary is useful when a public key
directory is undesirable. Bit-owner and wavelet implementations work, but did not
beat the public-directory control.

## Larger complete tag searches

Five repetitions, 262,144 source rows, uniformly hashed 64-bit tags, two payloads
per tag, 32-byte payloads, independent one-query clients. All figures below sum
the replica CPU and query-specific metadata-service CPU. Fresh navigation delivery
is included; generation construction is shown separately.

| Layout | Fresh public download | Service CPU / answer | Build + publication CPU |
|---|---:|---:|---:|
| Binary hashed pages, Dense | 137 B | 15.47 ms | 2,710 ms |
| Directory about 1 KiB, Dense | 903 B | 16.18 ms | 1,273 ms |
| Directory about 16 KiB, Dense | 16,207 B | 2.17 ms | 1,271 ms |
| Directory about 64 KiB, Dense | 63,683 B | **2.04 ms** | 1,286 ms |
| Directory about 256 KiB, Dense | 233,153 B | 2.93 ms | 1,370 ms |
| Fused XOR dictionary, Dense | 109 B | 5.09 ms | 3,260 ms |

The 64 KiB directory saves about **7.6x query-time aggregate CPU** against the
hashed-page control. Its build/publication is also cheaper in this harness.
The 1 KiB directory loses the advantage because each private result must carry a
much larger block. With 96-byte payloads, the 64 KiB directory takes 3.33 ms;
the 1 KiB variant exceeds the online download cap.

The XOR dictionary combines its three recovery locations into **one randomized
linear Dense query**, avoiding three scans. It needs no key list on the client.
It uses a 256-bit digest to detect invalid absent-key decodings; that is a
probabilistic retrieval check, **not an authenticated nonmembership proof**.
Its measured generation crossover against hashed pages is approximately 54
queries in the 262,144-row/32-byte case. The directory beats both its generation
and service cost here.

These are synthetic projections, not imported Shinzo history or live wallet
traffic. Real wider tags, skewed output sizes, encrypted projections and actual
public-range boundaries still determine the final serving format. A public
directory reveals its entries to clients; query privacy is retained, but database
confidentiality is not supplied by this benchmark.

## Bit indexing directly tested

At 16,384 uniformly hashed 64-bit tags with shuffled physical placement:

| Structure | Variant | Aggregate service CPU | Private reads / answer |
|---|---|---:|---:|
| Hierarchical bit owners | 1 bit / owner | 139.02 ms | 576 |
| Hierarchical bit owners | 2 bits / owner | 134.89 ms | 544 |
| Hierarchical bit owners | 4 bits / owner | 119.43 ms | 473 |
| Hierarchical bit owners | 8 bits / owner | 23.43 ms | 94 |
| Packed wavelet | 4-bit radix | 4.93 ms | 34 |
| Compressed Patricia | 4-bit radix | 1.82 ms | 6 |
| Binary hashed pages | Dense control | 1.32 ms | 3 |
| Fused XOR dictionary | Dense | 0.63 ms | 1 |
| Public directory | Dense | **0.53 ms** | 1 |

Each owner has two native noncolluding replicas. All owners are contacted on a
fixed public schedule; bit values and chosen payload blocks are privately
selected. Occupancy summaries use padded bitmap/array/run containers. The fixed
candidate bound is valid for absent predicates too. Narrow groups admit too many
candidate blocks; padding those fetches dominates. The 1/2/4-bit owner cases also
fail the online wire cap at this size. Moving these processes to separate machines
would change latency and placement, not erase their aggregate work.

The packed wavelet uses separate tables per public level and packs rank counters
with local digit blocks. Equality reporting returns full payloads after private
navigation. Its 1/2/4/8-bit variants and the compressed Patricia variants were
tested; these figures are not count-only queries.

**A better bit-index variant emerged from sorted placement.** Query only one
private prefix index, retrieve its fixed padded candidate blocks, and filter the
full tag locally. At 262,144 tags, a **16-bit private prefix** used **3.66 ms**
aggregate service CPU with three private reads and no public key directory.
The paired sorted-data XOR control used 4.78 ms, while directory/Dense used
1.94 ms (consistent with the separate approximately 2.04 ms campaign). The prefix
layout uses about 26.94 MB total replicated index/payload storage, versus 48.44 MB
for XOR. Four native roles represent two replica pairs: prefix and payload tables.

This is a real private bit index and a useful cold alternative when downloading
a key directory is undesirable. It is not the fastest overall variant. Smaller
8/12-bit prefixes took 50.82/7.16 ms at this size because of candidate padding.
At 16,384 tags, XOR remained faster than the tested private-prefix variants.
Sorting also reduced the all-bit 8-bit-owner variant from 23.43 to 2.32 ms at
16,384 tags, but it still lost to directory/Dense. Array/run codec paths have
explicit tests in addition to the actual representation-choice benchmarks.

## Client reuse, proofs and maintenance

For 16,384 hashed tags, fresh SinglePass costs 119.20 ms service CPU per answer
versus 1.33 ms for Dense. At 16 queries per client, including the actual hint
download once, those become 8.56 ms and 0.82 ms. This does not contradict earlier
warm-query results: reusable hints and the ad hoc product workload are distinct
axes. Generation lifetime projections for G=1/16/256/4096 are in the data files;
these projections are arithmetic amortization, not 4,096-client load tests.

Canonical-format Mizu pilots now retrieve **unchanged Poseidon witnesses** and
verify them against the original root. The existing depth-20 quaternary fixture
builder supplies the sentinel, membership and predecessor witnesses. Tampered
witnesses and wrong roots are rejected. At 1,024 values, a directory plus one
2,008-byte witness takes 0.75 ms service CPU, about 7.33 ms query/verification CPU,
and about 74.91 ms complete fresh client process CPU. Grouping 16 witnesses into
a page increases service CPU to 3.16 ms; grouping 64 increases it to 11.03 ms.
Large payload blocks are not automatically beneficial.

This canonical pilot has sorted physical positions and u64 values embedded into
the field. It is not a live insertion-ordered Shieldd corpus. Separate SHA-256
topology experiments test scattered positions: a sample group of 16 leaves needs
30,720 B of independent sibling hashes, 1,728 B when adjacent, and 8,640 B when
scattered, before addressing/framing. The SHA-256 authenticated block-root layout
is a different commitment and cannot substitute for Mizu's existing root.

The actual base/delta pilot verifies 32 upserts/deletions and 96 private answers
for each delta threshold, including compaction and fixed reads of empty slots.
Thresholds 4/8/16 consumed 132/168/269 ms of aggregate native server lifecycle
CPU in that small stream. These are measured maintenance components, not a tuned
production threshold. Canonical witness refresh under live roots remains a
deployment-specific maintenance issue.

## Other protocol results

- **Finite differences:** the default real two-server encoder was slower than
  matched Dense at 256/1,024/4,096 rows. Complete tag queries at the latter two
  sizes exceeded the online download cap. These runs execute real encodings and
  reconstruction, not the reference's fake-encoding benchmark. A separate larger
  encoding frontier uses an exact in-place Boolean zeta transform, checked
  byte-for-byte against the reference, and explicit M/D parameters. See the
  frontier campaign for its storage-versus-online tradeoff. At 262,144 rows,
  the 3.31 GB and 4.97 GB encodings used 2.93 and 3.24 ms service CPU, respectively,
  plus 25.4 and 38.2 seconds generation CPU. They did not beat the best 2.04 ms
  Dense layout. The larger-page alternative failed its wire preflight. Default-
  parameter failures alone would not have established this result. Many-server
  cost formulas were also screened up to 100 roles, with separate 512 MiB,
  128x-source and 5 GiB memory frontiers; those are cost candidates, not measured
  complete-search implementations.
- **Ramen:** persistent three-party state with four fresh clients worked. The
  16-row scalar-field tag pilot used roughly 67 ms aggregate online CPU per
  answer in its phase timers, which exclude final response framing; full role
  lifecycle totals are also retained. The artifact exposes scalar `PrimeField`
  accesses; a genuine packed
  block construction is not supplied by changing the adapter's loop.
- **HintlessPIR:** optimized build, actual fresh keys and complete unique-tag
  retrieval worked. The 64-record pilot returned about 5.9 MB per answer and
  used 565–907 ms server CPU. It fails the download cap at those reference
  parameters. Global preprocessing was about 17.2 seconds CPU.
- **ZipPIR:** full setup and correctness ran after a local fallback for an
  unguarded AVX-512 loop. At the reference's resulting 565,248-bit database,
  client-dependent server setup used 42,223 ms CPU and first query generation
  used 31,977 ms CPU. This is a first-admission gate, not a complete keyword-search
  winner. The earlier reference timing fields are wall time; added CPU phase
  counters provide the CPU figures here. Key constructor work is additional.
- **SandwichPIR:** actual GPU HTTP service, independently spawned clients,
  navigation downloaded over HTTP, fresh native keys per continuation, and
  complete results all worked. Five repetitions covered 1/8/32/128 arrivals,
  public 0/5/20 ms windows, plus a spaced-arrival lane. All isolated-client cap
  checks passed. At 32 arrivals a 20 ms window used 2.50 ms server CPU per answer;
  the matched 2,048-byte-page Dense batch kernel used 0.40 ms. GPU active time is
  separate in the raw logs and is not added to CPU. CPU counters have 10 ms
  resolution per batch. Colocated client startup dominates batch wall time, so
  those wall times are not predictions for a physical client fleet.
- **CRT preprocessing:** exact one-level lookup/CRT kernels reconstruct all
  tested products correctly; larger tables hit the memory preflight. This is
  not the full two-level Williams algorithm or complete LWE DEPIR. Its Python
  lookup orchestration versus NumPy control is a kernel diagnostic, not a
  cryptographic performance lower bound.

## Disposition of all 24 planned experiments

“Measured” means the stated implementation/variant, not every variant in its
family. “Screened” records a numerical, artifact, construction or hardware gate;
it does not mean a complete protocol was implemented or disproved.

| # | Experiment | Disposition |
|---:|---|---|
| 1 | Persistent service, fresh clients | Measured direct replica connections; real 1/2/4/16-query client lanes; G projections separately labeled |
| 2 | Minimal complete pages | Measured binary/JSON, 32/96/2008-byte payload cases, fixed continuation, compact IDs in binary records |
| 3 | Sparse keyword retrieval | Measured peelable XOR dictionary and fused Dense selector; digest-based absence caveat retained |
| 4 | Radix/Patricia | Measured compressed 1/2/4/8-bit navigation, including hashed 64-bit values |
| 5 | Hierarchical compressed bitmaps | Measured occupancy + private candidate blocks, arrays/bitmaps/runs, clustered/shuffled/sorted layouts |
| 6 | Packed wavelet/rank | Measured separate level tables, 1/2/4/8-bit rank blocks and complete equality reporting |
| 7 | Bit ownership | Measured logical owner/replica counts, 64-bit fields and single private-prefix owners; not a physical hundred-machine deployment |
| 8 | Public navigation | Measured roughly 1/16/64/256 KiB directories and broader group sizes, fresh delivery charged |
| 9 | Persistent private memory | Measured actual three-party Ramen with fresh client processes; small scalar pilot |
| 10 | Fused/block DORAM | Interface/construction gate: no supported vector block type in pinned artifact; full new block protocol unimplemented |
| 11 | CHOO-PIR | Full construction read; published SS communication screened out; smaller instances/FHE helper remain unimplemented |
| 12 | Finite differences | Real two-server campaigns, exact alternative encoder, larger-memory parameter sweep; many-server cost formulas separately screened |
| 13 | Barely DE SimplePIR | H-size frontier and exact CRT kernel; full Williams/LWE construction unimplemented |
| 14 | Practical DEPIR 2026 | Published batch/state frontier screened; no complete singleton artifact port, no inferred singleton speedup |
| 15 | Secret-key DEPIR | Provisioning/storage screen; linked artifact page/API unavailable; no claimed public-client construction |
| 16 | Stateless HE | Hintless and Sandwich implementations measured; TensorPIR/YPIR/WhisPIR and every other named adapter were not all ported |
| 17 | Admission-heavy schemes | ZipPIR actual full-admission gate; YsPIR/Pirouette/Pirex source and paper checks, not full new ports |
| 18 | GPU execution | Measured Sandwich CPU/GPU diagnostics and complete HTTP tag searches |
| 19 | Independent batching | Measured isolated GPU clients, public windows/spacing, 128-client lane; matched native Dense batch kernel separately qualified |
| 20 | Memory/cluster frontier | Actual native role storage/CPU and large encodings; logical roles and memory formulas; physical remote cluster, SSD and energy frontier unmeasured |
| 21 | Proof compaction | Canonical Poseidon witness retrieval/verification plus separate shared-proof topology experiments; no live-root production corpus |
| 22 | Base/private delta | Actual fixed delta queries, tombstones, upserts and compactions; workload-specific tuning remains |
| 23 | Distributional PIR | Excluded from exact complete-answer ranking; correctness relaxation was not assumed acceptable |
| 24 | Trusted hardware | Hardware/trust gate: local AMD platform has no SGX; enclave protocol not implemented |

The unresolved constructions above remain research possibilities. This pass does
not prove that sublinear-work multi-server PIR or private bit indexing is
impossible. It establishes which **implemented, fully accounted variants** win
on these workloads and this hardware.

## Evidence and reproduction

- [Measurements, CSV](COLD_QUERY_MEASUREMENTS.csv) and
  [JSON](COLD_QUERY_MEASUREMENTS.json) contain medians, repetition counts, client
  CPU/wire/RSS, service CPU, global costs, cap failures and lifetime projections.
- [Execution ledger](COLD_QUERY_EXECUTION.md) and
  [plan](COLD_QUERY_EXPERIMENT_PLAN.md) retain scope, pins and limitations;
  [runner instructions](COLD_QUERY_RUNNER.md) give reproduction commands.
- Raw per-case results, failed preflights, logs and manifests live under
  `target/pir-cold-*`. Later campaign manifests include frozen Python source
  snapshots; the first two campaigns have source hashes and were held unchanged
  while running. Native binary hashes are recorded in each manifest.
- `run_cold_search.py` has smoke/screen/finite/directory/extensions/frontier/
  bit64/reuse profiles and supports an explicit matrix file.
  `run_cold_canonical.py`, `run_finite_frontier.py`, `run_cold_maintenance.py`,
  `run_dense_batch.py`, `run_sandwich_batch.py` and `screen_cold_frontiers.py`
  reproduce their separate lanes.
- Ten Python tests cover complete results, padding schedules, fresh processes,
  real finite encoding and native segmented/fused services. Canonical negative
  checks and actual query verification run in the canonical campaign. The fast
  finite encoder is compared against the author's complete encoding in Go.

The original Python `ru_maxrss` included a publisher high-water mark inherited
before exec. The probe preserved in `probe_cold_rss.py` demonstrated this. New
clients use `/proc/self/status` `VmHWM`; old configurations were rerun for memory
qualification. Old CPU samples were not rewritten or silently discarded. GPU
client CPU retains a conservative upper bound for GNU time's rounded counters.

Build costs include this research publisher's fixture/index construction and
JSON publication, plus all native lifecycle CPU outside measured client requests.
Canonical generation projections also charge the separately executed Poseidon
corpus builder once per generation; its component is retained in the data.
Transport is native JSON/hex over local pipes or HTTP, not a
production binary deployment. The host has about 7.7 GiB WSL RAM and an RTX 2070
SUPER. Concurrent research/desktop activity and colocated clients limit latency
interpretation. Serving defaults have not changed.

## Primary construction sources

- [Finite-differences reference](https://github.com/ahenzinger/finite-diffs-pir)
- [SandwichPIR reference](https://github.com/sidsabh/sandwichpir)
- [Hintless reference](https://github.com/google/hintless_pir)
- [ZipPIR reference](https://github.com/RasoulAM/ZipPIR)
- [Ramen reference](https://github.com/AarhusCrypto/Ramen)
- [CHOO-PIR full paper](https://www.jstage.jst.go.jp/article/transfun/advpub/0/advpub_2026CIP0012/_pdf/-char/en): SS hint-table transfer and refresh, including approximately 128/263 MB published online examples
- [Barely DE SimplePIR](https://eprint.iacr.org/2025/1305): full H matrix in the response and Williams/CRT preprocessing
- [Practical DEPIR](https://eprint.iacr.org/2026/243): smallest reported batch uses about 0.39 GB state and takes 10 seconds for 5,461 items
- [Secret-key DEPIR](https://eprint.iacr.org/2026/1480): separate secret provisioning and published encoding estimates
- [YsPIR](https://eprint.iacr.org/2026/955), [Pirouette artifact](https://github.com/KULeuven-COSIC/Pirouette), [Pirex artifact](https://github.com/vt-asaplab/pirex): additional admission/implementation checks, not measured new complete-search winners
