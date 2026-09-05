# Benchmark plan: minimize total server work

Date: 2026-09-04. Status: B0–B8 benchmark implementations are available in
[the unified runner](ALL_BENCHMARKS.md). Bounded correctness and local measurements
have run; larger scales and physical deployment remain subject to explicit gates.

The objective is minimum aggregate server work per complete private search,
subject to reasonable client CPU, memory and traffic. Include every machine,
protocol role, setup phase and maintenance operation. A faster individual
server, or lower latency obtained by splitting the same work, is insufficient.

The user's proposed design distributes a field's index by bit position: each
machine stores one or a few bits' indexes. This can make each machine's storage
manageable and allow a larger aggregate index. We will test whether that layout
also reduces complete-query work after private combination and result retrieval.
It does not require exposing the requested bits to their index owners.

This plan supersedes the experiment priorities in
[MANY_SERVER_INDEXING.md](MANY_SERVER_INDEXING.md). It extends the accounting
principles in [EXPLORATION.md](EXPLORATION.md); it does not change serving defaults.

**1. Success metric and accounting**

For a finite workload horizon, separately in each resource unit:

```text
work per complete successful query =
  (global build and publication
   + setup work for every client
   + online work on every server and helper
   + hint refresh, updates, rebuilds and compaction
   + transport, padding, retries and recovery work)
  / number of complete successful logical queries
```

Sixteen requests to sixteen bit-index servers are ONE logical query. A bitmap,
an intermediate locator, or a page in an unfinished result does not increase
the denominator. Actual independent user queries in a batch do.

Report both online aggregate work and the fully amortized total. Charge
background work even if it is hidden from latency. Include failed attempts in
the numerator. Close the measured maintenance cycle, or explicitly account for
work deferred beyond the run; a shrinking reserve of precomputed hints is not
a sustainable throughput result.

The work report keeps units separate:

| Resource | Measurement and use |
|---|---|
| Server CPU | Sum user+kernel CPU time over all participating processes/threads; cycles and instructions on matching hardware |
| GPU work | Sum active device time over devices, plus host CPU and transfers; do not sum overlapping kernel durations as elapsed device time |
| Energy | Aggregate server/device joules where supported; report gross and baseline-adjusted energy with measurement boundaries |
| Memory and storage I/O | Logical reads/writes and independently measured physical bytes where available |
| Network | Client traffic plus inter-server, replication, setup, refresh and relay traffic, counting each transfer once |
| Capacity | Per-machine and aggregate RAM/VRAM, peak build memory, persisted bytes, and storage retained over the generation |
| Client | Setup/online/refresh CPU, peak memory, persistent state, wire traffic and verification cost |
| Latency | Complete verified p50/p95 and queue delay; a constraint and diagnostic, not the work objective |

Do not call summed server elapsed time CPU-seconds. Do not add CPU milliseconds
to GPU milliseconds or bytes to form a synthetic work score. Rank within
matching hardware lanes; use measured aggregate energy and a resource Pareto
comparison for CPU/GPU tradeoffs. If a counter is unavailable, preserve that
fact. In particular, the recorded WSL environment has no usable RAPL/uncore
DRAM measurement. A fleet-energy claim needs a suitable isolated runner.

Extend [accounting.rs](../src/benchmark/accounting.rs) and the phase-scoped
[counter harness](HARDWARE_COUNTERS.md). Preserve old metric meanings and add
explicit CPU/device/energy fields with an appropriate schema version. Associate
every phase and physical machine with a unique ID to avoid double counting
shared exporters, CPU packages, GPU energy or helper services. Record sums for
each query before computing percentiles; never sum per-server p95 values.

**2. Privacy and client constraints**

The target field value, chosen bits, selected shard and match-dependent
intermediate access must stay hidden from each individual operator under the
stated non-collusion model. Record the exact tolerated coalition size and
number of independent operators separately from worker count. Start at
individual-server privacy (t=1), which fits the user's non-collusion premise;
retain stronger t=n-1 results separately. Information-theoretic and
computational constructions remain explicitly labeled comparison lanes.

Plaintext bit selection, visible match IDs and unprotected payload fetches are
controls only. No candidate can win by changing the leakage class. Use the same
public scope, metadata disclosure, padded result schedule and integrity checks
within a direct comparison. A public MPHF/digest directory and a hidden-key
directory are different metadata-disclosure cases.

Use the current [portable compatibility envelope](../PORTABLE_READINESS.md)
as provisional client limits, not a new claim about phones:

| Per-client resource | Initial limit |
|---|---:|
| Persistent state, including metadata and hints | 64 MiB |
| Peak transient owned payload; also measure real peak RSS | 128 MiB |
| Setup download | 64 MiB |
| Online upload for one bounded logical lookup, across all subrequests | 1 MiB |
| Online download for one bounded logical lookup, across all replies | 1 MiB |
| Named-device setup CPU | 10 seconds |
| Named-device online CPU | 1 second; additionally report a 100 ms target |

A full-table client preload is allowed only if it fits these limits and the
client may receive that projection. Classify archive clients with larger budgets
separately. Client refreshes count toward setup/lifecycle costs; exceeding the
limits does not become acceptable merely by calling the transfer offline.

All-match results can inherently exceed 1 MiB. Give those workloads a separate,
explicit public result-size budget and streaming memory limit. Report the full
bytes, padding ratio and CPU for the entire result, including every page. Do
not reuse a per-page limit to label an arbitrarily expensive query lightweight.
If result count is hidden, use a fixed public schedule adequate for the workload's
maximum; do not silently choose the schedule from the secret result count.

**3. Common workloads and staged scale**

Reuse authenticated corpus export and exact reconstruction. Keep raw documents,
logical results and queries fixed while varying the index/physical encoding.

| Workload | Required complete answer | Why include it |
|---|---|---|
| Known row / unique document key | One fixed authenticated projection | Measures private access without unnecessary predicate discovery |
| Equality on a 16-, 32- or 64-bit field | All matching IDs and requested projections | Direct test of the distributed bit-index proposal |
| Secondary-index combinations | Equality conjunctions and a bounded numeric range | Tests whether bit-sliced operations help real search |
| Current-root nullifier query | Complete verified 2,008-byte witness, including absence/predecessor handling | Tests a wide answer whose updates can invalidate preprocessing |

Presence-only alerts are not substitutes for these results. Retain existing
packed-presence numbers as a separate workload; do not use their smaller answer
to claim a document-retrieval win.

Initial executable anchor: the existing 262,144 x 96-byte corpus. Add roughly
1M rows at 32, 96, 256 and 1,024 bytes, plus the witness workload. Preserve the
existing 2^23 x 128-byte physical GPU corpus for historical comparisons. Promote
survivors to 10M, then 100M rows when the complete representation fits. One
billion rows starts as a concrete parameter/capacity estimate; it becomes a
measured result only with an actual resident or explicitly out-of-core corpus.
Repeatedly executing one small representative plane stays labeled logical-work
evidence, not a physical billion-row benchmark.

For field indexes, vary uniform, skewed and clustered values; random and
value-sorted row order; present, absent and repeated searches. Sorting must
include its row-ID mapping and maintenance costs. Use synthetic result fanouts
0/1/4/16/256/1,024 where the field domain permits, and larger all-match groups in
the dedicated result-size lane. A uniformly populated 16-bit field over 1B rows
has about 15,259 matches per value, not one. Charge hash collisions and full-key
verification if a short field is a routing hash.

Avoid a Cartesian product of every parameter. Screen on the anchor and two
larger/width corners, then expand only candidates that survive.

**4. Benchmark families**

| ID | Experiment | Parameters and complete cost boundary | Existing starting point / new work |
|---|---|---|---|
| B0 | Correct aggregate-work baseline | Dense CPU/GPU; one indexed read and 100 decoys as weaker-privacy controls; raw and served requests | Existing Dense, GPU runner, `cpu-snapshot`, counter harness; add complete CPU/transport accounting |
| B1 | Field-bit index storage and local kernels | 16/32/64-bit fields; 1/2/4/8 bits per index group; bitmaps, compressed bitmaps and grouped postings; build, memory, selection, intersection, update | New field-index corpus and evaluator; public kernels are labeled lower-bound controls |
| B2 | Private distributed field-bit search | Private group selection, server-side intersection, private result extraction and payload retrieval; all operators and helpers counted | New protocol-backed adapter; first a feasibility model and small correctness implementation |
| B3 | Persistent selector-bit subset-XOR index | Groups of 2/4/6/8/10 source rows; cold/warm cache; 32–1,024-byte rows; initialization and rebuilds | `subset-xor`, `mphf-subset-xor`; add hardware and lifecycle accounting |
| B4 | Share work across useful queries | Independent Dense, shared traversal, cache blocking, transposed selectors, ephemeral Four-Russians, eligible GPU kernels; batches 1/8/32/128/512 | `dense-batch` and GPU runner; add arrival-driven runs and full scratch/answer/transport costs |
| B5 | Client-preprocessed PIR | SinglePass Q=2/4/8/16/32 and Zelda; client state, every helper and recurring maintenance included | `warm-stateful`; new pinned, role-correct Zelda adapter |
| B6 | Global server-preprocessed PIR | Finite differences and scalable many-server constructions; concrete parameters, all encoded storage, preprocessing and replies | Official finite-differences adapter; new many-server parameter enumerator before implementation |
| B7 | Stateful server-side oblivious index | A published ORAM/oblivious-map or communicating-server PIR construction; path/position state, interaction, reshuffles and persistence | Feasibility screen first; implement only an admissible published construction |
| B8 | Complete result layouts and lifecycle | Inline projection versus private locator+payload; complete group retrieval; immutable base plus mutable delta where valid | Existing tag/Fuse/Ribbon/active-generation/verification code; compose surviving protocols |

**B1–B2: the user's index partition is a main experiment.**

For a w-bit field and N documents, one packed bitmap per bit costs wN/8 bytes
before replicas and metadata; the complementary value can be derived. With
w=16 and N=1B, that is 2 GB total and 125 MB per machine for sixteen machines.
Splitting changes the per-machine footprint, not that aggregate byte count.

Indexing groups of g field bits is a different tradeoff: a dense bitmap for
every group value costs approximately (w/g)*2^g*N/8 bytes, whereas postings and
compression have data-dependent size. Include the ordinary full-field equality
posting index as a control; it need not enumerate a dense bitmap for all 2^w
possible values. This separates bit-plane storage, grouped value indexes and
the exponential subset-XOR cache in B3.

Vary **logical index groups**, **physical workers**, **cryptographic parties**
and **replication/storage overhead** independently. Test one/few groups per
machine, a co-located layout and row-partitioned layouts at matching total RAM.
Physical counts 2/4/8/16/32 are the initial sweep; extend to 64/128 only when a
surviving design gains aggregate efficiency or needs that memory capacity.

B2 must specify its transcript and storage placement before timing: use a
published private-selection/MPC construction, state which operators hold which
shares, and verify its coalition guarantee. Count secret-share creation,
secure ANDs, any preprocessing correlations, compressed-data handling,
oblivious compaction and output extraction. Query-dependent decompression or
posting access cannot be treated as free if it reveals the selection.

Compare three pipelines:

1. Private bitmap selection followed by client intersection: a small-corpus
   reference, rejected from the lightweight lane when bitmap traffic/state
   exceeds the budget.
2. Private selection and server-side intersection, followed by Dense payload
   retrieval: establishes whether the second stage erases the index benefit.
3. The same index stage followed by a qualifying preprocessed or oblivious
   payload store from B5/B6/B7: tests a complete work-saving architecture.

Do not implement a full MPC stack merely to rediscover a traffic lower bound.
First estimate all bitmaps, messages, circuit operations and payload requests;
then benchmark small protocol primitives with real communication. Local XOR/AND
timings alone are not a strict-private benchmark. Include empty intersections
and false positives under the same public result schedule.

**B3–B4: reduce repeated reads with existing code.**

B3 indexes bits of a random Dense share over groups of source rows, rather than
bits of a searchable field. For g=8 the index alone is 31.875 times the source;
the expected payload-operand reduction is about 4 times. Measure whether actual
CPU/energy improves after random memory accesses, index building and updates.
Do not jump to g=16 (about 4,096 times index storage) without a favorable
parameter/memory result. Multi-machine placement is useful only if measured
aggregate work benefits from its memory hierarchy or capacity.

For B4, include setup of ephemeral combination tables, reads/writes of every
answer accumulator, and all independent selector uploads. Measure total work
for the batch, then divide by its independent completed queries. Sweep a ready
batch separately from arrival-driven scheduling with 0/5/20/100 ms maximum
queue dwell. Pre-registering reusable selectors is a separate stateful workload;
include its setup, linkability policy and retained server memory.

**B5–B7: try actual scan avoidance with bounded setup/state.**

B5 uses SinglePass as the local stateful reference. Pin Zelda at
`11b8e70ffcb3ee8d2ea72824c04ed8faa1fa558a`, confirm the protocol against the
paper, separate its required independent roles, use appropriate randomness,
and leave correctness enabled. Its one-endpoint benchmark is not a deployment
privacy test. Measure initial and recurring hints, replacement entries, discarded
hints, state persistence and client reconnect/recovery. A trusted hint helper
does not disappear from server accounting.
[Zelda paper](https://eprint.iacr.org/2025/1340),
[official implementation](https://github.com/p-b-p-b/Zelda).

B6 begins with the existing two-server finite-differences implementation, then
evaluates exact feasible parameter sets at 4/8/16/32/64/128 independent parties
for the paper-supported generalizations and Scalable Multi-Server PIR. Counts
are candidate inputs, not a claim that every construction supports them. For
each configuration enumerate encoded bytes per server AND across all servers,
actual symbol/row width, upload/download, client operations, query probes,
preprocessing operations and collusion threshold. Report storage frontiers at
2/8/32/128/512 times raw data, but execute only within the declared host/fleet
budget. Include any larger theoretical points as estimates, not allocated runs.
[Finite-differences implementation](https://github.com/ahenzinger/finite-diffs-pir),
[Scalable Multi-Server PIR](https://eprint.iacr.org/2024/765).

Choose one or two non-dominated, concretely feasible many-server configurations
for a correctness prototype before running large tables. Do not choose a design
from N-to-an-exponent expressions alone. Cold-client global preprocessing and
per-client preprocessing have separate amortization denominators.

B7 asks whether moving evolving access state into independent servers keeps
the client small at lower aggregate lifecycle cost. Include position maps,
stash, secure shuffles/reshuffles, concurrency serialization, fresh randomness
and recovery. Pin and read the exact full construction first; the revised
communicating-server PIR paper's abstract alone is not an implementation spec.
[Communicating-server research lead](https://eprint.iacr.org/2024/829).

**B8: ensure the primitive wins survive the real result.**

Compare complete fixed projections against narrow private locator tables plus
private document fetches. Include compact ordinal, Fuse-4 and existing Ribbon
layouts only where they change metadata, padding or total footprint; do not
repeat already dominated layout runs without that new condition. Both stages
must preserve the declared secrecy, and a secret partition cannot be fetched
publicly. Batch complete matching groups instead of rescanning once per page
where the protocol supports that operation.

For current-root witnesses, measure current-root verification and update
invalidation. The existing active-nullifier benchmark is shape-based evidence,
so add canonical witness verification before claiming an application result.
For base/delta designs, consult every public component required for correctness
and include growth and compaction. Old state cannot be discarded using a time
filter if it contributes to the requested current answer.

**5. Lifecycle, controls and experimental discipline**

| Dimension | Planned sweep |
|---|---|
| Queries per client per generation | 1, 10, 100, 1,000; 10,000 only for surviving warm designs |
| Clients per generation | 1, 100, 10,000; large client populations initially a labeled sum of measured phases |
| Aggregate generation-query horizon | Derived from clients and queries/client; plot break-even curves, not independently inconsistent counts |
| Update/query ratio | 0, 1/1,000, 1/100, 1/10, 1; insert, value update and delete cases |
| Update batch | 1, 100, 10,000 affected records as scale permits |
| Lifecycle | Build, publish, query, refresh, generation replacement, interrupted-state recovery |
| Cache/capacity | Resident warm, cold index access, near-capacity; out-of-core only as an explicit separate configuration |
| Client devices | Named x86 client first; named ARM/phone runner before any phone-CPU eligibility claim |
| Network | Local compute isolation; 100 Mbit/s client and 10 Gbit/s server fabric as initial shaped scenarios, then slower client/inter-operator sensitivity for survivors |

Plain sharding and RAID-style distribution get a small control: fixed protocol
and two logical operators, with 1/2/4/8 workers each. Its purpose is to detect
changes in actual aggregate CPU, energy, cache behavior and coordination cost.
A pure wall-time improvement earns no promotion. Do not spend the main effort
on a hundred-machine latency sweep or add Dense replicas indiscriminately.

Initial screening: correctness plus five fresh paired process runs over a
bounded table, with fixed query corpora and alternating candidate/baseline order.
Run builds and benchmarks sequentially on an otherwise idle host. Finalists:
five fresh runs, at least 100 completed queries/run where feasible, cold starts
separate, and whole maintenance cycles. Use enough independent runs to resolve
a close comparison; bootstrap uncertainty at run level rather than pretending
every warm request is an independent experiment. Slow candidates with fewer
samples retain an explicit limitation and no confident p95 claim.

Compare each candidate with the best eligible baseline for that workload and
client class, not only unoptimized Dense. A provisional promotion target is at
least 20% lower dominant measured aggregate work with an uncertainty interval
excluding parity and without an unreported resource regression. Mixed-resource
tradeoffs remain on the Pareto table rather than becoming an unconditional win.
For a complex new protocol, favor a larger margin before scaling implementation.

Stop or classify a candidate when it fails correctness/privacy, exceeds client
limits, cannot fit the declared complete representation, or is dominated after
setup/maintenance. An online-only win is reported with its break-even horizon;
it is not accepted when the generation ends before that horizon. Preserve
negative results and counterexamples, including missed/absent queries.

**6. Execution order and deliverables**

1. **Accounting and cheap feasibility:** B0, B1 and concrete B2/B6/B7 resource
   estimates. This establishes the distributed field-index geometry and rejects
   expensive combinations before large allocations.
2. **Existing implementation experiments:** B3/B4, SinglePass in B5 and the
   two-server part of B6, all on common corpora with complete resource accounting.
3. **New bounded prototypes:** B2 and Zelda; then one or two feasible B6/B7
   configurations if their estimates and small kernels justify implementation.
4. **Complete application and lifecycle tests:** B8 for survivors, followed by
   scale, client-device and real-network validation.

Produce a versioned matrix/config, immutable corpus manifests, protocol and
toolchain pins, per-run phase JSON and raw counters under a new results
directory. Existing archived results remain intact. Publish one comparison table
with: complete answer, privacy threshold, independent operators/workers,
amortized server resources, client eligibility, aggregate storage, measured
scale and evidence label. Add break-even plots against queries/generation,
update rate, client state and total index storage.

The final recommendation may differ for cold clients, warm archival clients,
secondary search and current-root witnesses. The successful outcome is a
measured reduction in total server work within the relevant client budget,
including a clear conclusion if distributing bit indexes does not provide it.
