# Cold-client PIR: final research pass and experiment queue

Research cutoff: 2026-09-05. This is an experiment plan, not new benchmark results.
Broad coverage of relevant literature and implementation directions is not a claim
that every possible construction has been enumerated. Some leads were verified
only through author abstracts or publisher records, as marked below.

## Objective and comparison contract

User clarification (2026-09-05): the primary workload is Shinzo/Mizu search
starting from a tag, address/topic, transaction hash, or nullifier value, not a
known physical row address. Cold also denotes ad hoc/catch-up retrieval rather
than a registered live subscription. Fresh-client versus reusable-client state
is an additional measured axis, not the definition of the product workload.
Primary scenarios are complete routing-tag catch-up, historical log pages with
continuation, and current-root predecessor/nonmembership witnesses. A known-row
PIR microbenchmark is only a backend diagnostic. Public block ranges are valid
for the first two when requested, but cannot truncate active nullifier state.
See [product contracts](../USE_CASES.md).

Minimize aggregate server work for a fresh client obtaining one complete answer
from an already-published index. No database-dependent client hints are available
at arrival. Include every helper, preprocessing provider, replica, and payload
server. Keep the existing client limits unless explicitly reporting a separate
resource frontier. Preserve complete answers, absence verification, and the
declared privacy/collusion model.

Separate three conditions: fresh client; restarted/unprepared server; cold CPU
cache or nonresident disk pages. The primary target is the first. Report the
other two independently.

For a generation serving G independent one-query clients, measure:

`server CPU/answer = (global build + maintenance + refresh + wasted preprocessing
+ sum(per-client admission + all online server/helper CPU)) / G`

Show the unamortized components and generation break-even as well. Client CPU is
a separate metric, not mislabeled server work. GPU time, physical bytes read,
network bytes, and energy are separate resource dimensions; do not add CPU and
GPU milliseconds. Count failed attempts, padded output, retries, proofs, and
all consumed preprocessing. Public-key registration work is per-client even
when its network message is small or it runs before the measured online phase.

The six previous compositions did not establish a cold-client winner. Existing
coverage already includes grouped bitmaps and complements, subset-XOR tables,
small Hermite encodings, Fuse/Ribbon-related layouts, batches, CPU/GPU DPF,
Zelda, SinglePass, Path ORAM, Ramen, wavelets, MPC, and update/proof pipelines.
The following queue identifies extensions or different constructions, rather
than presenting those families as entirely untested.

## Experiments

### 1. Persistent servers, independent one-query clients — prerequisite

Keep server processes and their shared index alive. Start a fresh client with
fresh keys for every answer; include registration and first response. Compare
native Dense, DPF, existing HE adapters, and streamed SinglePass on identical
binary answer pages. This corrects the mismatch between fresh processes per run
and thousands of queries from a single client. Add G=1/16/256/4096 generation
lifetimes and one-query versus 2/4/16-query clients as separate lanes.

### 2. Minimal complete-answer pages — practical priority

Remove JSON, repeated keys, redundant row identifiers and oversized pointers
from the benchmark serving layout. Compare inline payloads versus compact IDs
plus private payload fetches; use fixed-width binary records and actual proof
bytes. Variants: 32/96/2008-byte source rows, fixed overflow pages, narrow field
projections, dictionary coding, and fixed-block compression. Apply each layout
to Dense too: a layout win is not evidence of a new PIR algorithm.

### 3. Complete cold sparse keyword retrieval

Compose sparse key-to-answer encoding with a stateless backend. Compare
SparsePIR-style linear encoding, Fuse/Ribbon/XOR recovery, and cuckoo/two-choice
tables with inline pages. Test occupancy, verified key fingerprints, duplicates,
absent keys, and fixed overflow handling. Where algebraically valid, combine
recovery locations into one private linear query instead of separate scans.
Client dictionary downloads count in full. This extends earlier layout tests
to a complete cold composition. [SparsePIR](https://www.usenix.org/conference/usenixsecurity23/presentation/patel).

### 4. Radix/Patricia navigation with compact, separately stored levels

Try 1/2/4/8-bit radix steps, compressed unary paths, breadth-first layout,
per-level tables, and a fixed early stop into padded leaf pages. Separate tiny
routing records from payloads. Benchmark total bytes privately processed over
the entire path. Ordinary server-visible child selection is disallowed; a
private access backend must conceal it. Extend the existing early-stop result,
which only beat full radix traversal, not the best Dense index.

### 5. Hierarchical bitmaps that avoid materializing the full answer bitmap

Variants: block occupancy summaries; Roaring-like array/bitmap/run containers;
WAH/EWAH-style runs; one-plane complement derivation; block-local compressed
postings. Traverse and combine summaries privately, then access a fixed number
of candidate blocks and payload pages. Compare against the existing full
compressed-bitmap scan. Padding and overflow behavior must not reveal which
blocks matched. Compression alone does not provide private random access.

### 6. Wavelet and rank structures with block access

Compare binary wavelets, multiary wavelets, succinct rank directories, and
Elias-Fano-style monotone posting layouts. Fetch rank metadata and the relevant
block together; fuse adjacent rank operations where possible. Measure exact
range counts separately from complete range reporting. The extension is packed
private block access, not another scalar rank traversal.

### 7. Bit-group ownership and correlated-index sharing

Try 1/2/4/8 bits per owner, complementary-plane reuse, shared prefixes between
fields, and bit-sliced predicates combined inside MPC. Vary owner count while
holding total available memory constant. Then vary encoded memory separately.
Count all noncolluding replicas. This explicitly tests the user's one/few-bits
per-server proposal; ordinary sharding alone reduces per-server storage, not
aggregate work. No owner may learn the selected bit value through routing.

### 8. Small public navigation download

Download the same query-independent directory for every fresh client: 1/16/64/
256 KiB variants. Resolve public navigation locally, then privately retrieve
the selected final page. Try top trie levels and compact key mappings. Include
download and client processing in the first query. A public directory must not
turn final bucket routing into a visible key prefix. Useful only if it removes
enough private navigation work to cover its admission cost.

### 9. Persistent multi-client private-memory service

Keep the ORAM/DORAM state on noncolluding service roles and let each fresh client
submit secret shares. Audit how keys and recursive position maps are managed
without giving one service the clear access pattern. Compare block Ramen/DORAM,
recursive Path-style storage, and hierarchical designs where implementable.
Charge eviction, rebuilds, recovery and concurrency coordination. Existing
single-client Path ORAM is not automatically such a service.
[Ramen](https://github.com/AarhusCrypto/Ramen).

### 10. Block and fused private-memory operations

Replace repeated tiny scalar accesses in the Ramen adapter with supported packed
blocks, then execute a fixed-depth lookup inside the secure computation service.
Try radix, cuckoo lookup and rank navigation. Compare CPU, interaction rounds,
and bytes against the old adapter. Do not assume adding block support is free
or cryptographically equivalent; validate the actual construction first.

### 11. CHOO-PIR commodity-server construction — promising, verification gated

Obtain and audit the complete construction before coding. Investigate the
reported secret-sharing and FHE helper variants: can independently arriving
clients use server-managed hints, under which collusion assumptions, and what
is the aggregate admission/refresh cost? A small database-server online cost
does not establish a small helper-plus-server total. The publisher record and
conference listing were verified, but accessible publisher text did not expose
the full construction; no performance or security conclusion is asserted here.
[Publisher record](https://globals.ieice.org/en_transactions/fundamentals/10.1587/transfun.2026CIP0012/_advpub_f).

### 12. Actual finite-differences encoding beyond the toy frontier

Use the authors' full two-server encoder and correctness path, then evaluate
their many-server parameter calculations. Sweep encoded storage, packed field
representations, record width, and index-only versus payload encoding. Our
earlier restricted parameter enumeration and m=1 pilot do not cover this full
construction. Run feasible real encodings first; reject impossible memory or
communication parameters before allocating. The reference's TestFakePIR skips
real preprocessing and is not an end-to-end cold benchmark.
[Reference implementation](https://github.com/ahenzinger/finite-diffs-pir).

### 13. Barely Doubly-Efficient SimplePIR

Study the concrete parameters and implement its preprocessed matrix-vector
kernel before the complete protocol. Sweep table size, group width and record
packing; include table construction and refresh. This is a server-preprocessing
construction from LWE, not ordinary client-hinted SimplePIR. Its asymptotic
improvement may have no useful crossover at our sizes. Stop after parameter
screening if constants defeat Dense.
[Paper, CRYPTO 2026](https://eprint.iacr.org/2025/1305).

### 14. Practical DEPIR: singleton versus batch feasibility

Evaluate the 2026 construction at batch 1 before reproducing larger batches.
Its abstract's 21 ms/item example is a 5,461-item batch taking 112 seconds with
171 GB server state; it is not a 21 ms cold singleton result. Test smaller
instances, actual encoding, and generation break-even before a full port.
The older LMW implementation is a related starting point, not automatically an
implementation of this new paper.
[2026 paper](https://eprint.iacr.org/2026/243),
[older implementation](https://github.com/FeanorTheElf/depir-impl).

### 15. Secret-key DEPIR with low storage — separate provisioning model

Audit the July 2026 construction and its candidate permuted-code assumptions.
Compare Reed-Muller/curve-based encoding choices and our actual record widths.
The abstract gives a 4.2x storage example for 18-bit records; that does not
establish the same cost for complete posting pages. Determine who provisions
the short secret key, whether encoding is client-specific, and whether ordinary
public clients can safely obtain it. Only classify as our default cold model
if that provisioning requirement can be met; otherwise report separately.
[Preprint](https://eprint.iacr.org/2026/1480).

### 16. Stateless CPU/HE protocol comparison

Add HintlessPIR/LinPIR and TensorPIR, YPIR, and WhisPIR where artifacts and
parameters support our workload; rerun existing InsPIRe/Poulpy adapters with
first-client costs. Compare singleton complete answers and small records,
including key generation, key upload, expansion, and first-use preprocessing.
These can reduce concrete cold cost without reducing asymptotic scan work.
[Hintless artifact](https://github.com/google/hintless_pir),
[YPIR](https://www.cs.utexas.edu/~dwu4/papers/YPIR.pdf),
[WhisPIR](https://eprint.iacr.org/2024/266.pdf).

### 17. Small-key and outsourced-state protocols: admission-cost gate

Compare ZipPIR, YsPIR, Pirouette and Pirex/Pirex+ only with their complete fresh
client setup. ZipPIR explicitly retains server state per client; YsPIR has an
offline phase; small communication does not make either computation free.
Pirouette trades more server computation for a smaller query. Pirex online-only
simulation must not substitute for its actual offline preparation. Reject from
the main queue if full first-answer cost already exceeds the matched control.
[ZipPIR](https://arxiv.org/abs/2603.09190),
[YsPIR](https://eprint.iacr.org/2026/955),
[Pirouette](https://eprint.iacr.org/2025/680),
[Pirex artifact](https://github.com/vt-asaplab/pirex).

### 18. SandwichPIR tensor-core execution

Use the newly released reference artifact for fresh singleton queries first.
Test our small records and packed complete-answer pages, then GPU-resident and
host-memory conditions. The authors' fast example uses 32 KiB records, so it
cannot be transferred directly to our 32-byte workload. Measure CPU overhead,
GPU active time, transfers, client costs and memory. No offline communication
is attractive for cold clients; this remains scan acceleration, not bit-index
navigation. [August 2026 preprint](https://eprint.iacr.org/2026/1816),
[code](https://github.com/sidsabh/sandwichpir).

### 19. Independent cold-client batching

Extend existing batching to the strongest new backends: 1/8/32/128 simultaneous
independent clients, with 0/5/20 ms public waiting windows and arrival-rate
sweeps. Charge key processing and total CPU/GPU work for the whole batch. Test
shared scans, tensor-core matrix multiplication, and valid batch-code layouts.
Do not label batch amortization a singleton speedup. Compare against batched
Dense under the same latency cap.

### 20. Memory hierarchy and physical cluster frontier

For surviving candidates only, compare binary packing, SIMD, NUMA placement,
RAM-resident versus SSD encoding, and co-location versus actual separated roles.
Report total memory traffic and aggregate CPU rather than the fastest worker.
At equal total storage, sweep 2/4/8/16 roles; larger counts start with analytical
screening. Distinguish a better cache fit from less algorithmic work.

### 21. Compact authenticated pages and shared proof retrieval

Extend the existing authenticated index to combine leaf records and proof
material, share path nodes across padded results, and compare fixed multiproof
layouts with independent proofs. Include nonmembership and current-root
verification. Query-dependent proof size or extra requests must not leak the
answer. Apply the optimized proof layout to Dense as well.

### 22. Stable base plus bounded private delta

Extend existing base/delta maintenance to winning server-preprocessed encodings.
Always query the prescribed base and delta components; vary delta thresholds,
update rates and compaction intervals. Include rebuilds, deleted records,
duplicate suppression and generation changes. This targets lifetime work under
updates, not just immutable-index speed.

### 23. Distributional PIR — separate correctness contract

Test only if occasional retrieval failures are acceptable. Sweep skew, shifted
popularity and adversarially uncommon keys; count failed attempts and any
privacy-preserving fallback. Distributional PIR retains PIR privacy but relaxes
correctness, so it is not interchangeable with our exact complete search.
Observable fallback can itself leak information.
[Authors' paper page](https://people.eecs.berkeley.edu/~henrycg/pubs/distpir/).

### 24. Trusted-hardware indexed search — separate trust contract

If hardware trust becomes acceptable, compare enclave-protected ordinary
indexes with access-oblivious variants. Include attestation, paging, side-channel
model and cold client admission. Keep outside the default cryptographic PIR
ranking. [ObliDB](https://www.vldb.org/pvldb/vol13/p169-eskandarian.pdf).

## Screening and execution order

First implement experiment 1 and common binary layouts (2). Then run 3, 4,
16 and 18; investigate 11's construction in parallel conceptually, without
assuming it is implementable. Next prioritize 9/10 and real finite differences
(12), followed by the bit-index extensions 5–8. Screen 13–15 numerically before
large implementations. Admission-gate 17. Apply 19–22 to survivors. Experiments
23–24 require a distinct acceptable correctness/trust model.

Start with 2^16 and 2^18 rows, then 2^20 only when memory permits. Use existing
32/96/2008-byte workloads with identical output bounds. Include equality,
absence, range count, bounded range reporting and conjunction; uniform,
clustered, scattered and skewed keys. Do not compare a count against full rows.
Keep a 512 MiB aggregate-state lane for continuity and separately screen
2/4/8/16/64/128x encoded-storage frontiers against actually available hardware.
Client caps remain 64 MiB persistent state, 128 MiB transient RSS, 64 MiB setup
download, 1 MiB each upload/download per complete query, 10 s setup CPU and
1 s online CPU (100 ms target). Show first-answer total client CPU as well.

Use five alternating paired repetitions initially. Report median aggregate
CPU, first-answer p50/p95 latency, setup and online client CPU, wire bytes,
memory/storage, maintenance and generation crossover. Require complete answer
verification and successful cap checks before a performance claim. A candidate
must beat the best matched cold control after full accounting; less CPU at one
server, smaller queries alone, or more parallelism alone is insufficient.

## Leads not promoted

- VIA/VIA-C/VIA-B and HydraPIR remain additional artifact/construction audit
  leads. The third-party VIA Rust artifact exposes selected variants, so its
  mere presence does not establish a full no-offline-communication benchmark.
- The ePrint record formerly surfaced as PIRCOR now names RECPIR and is marked
  withdrawn. Do not implement it on the strength of search-result speedups.
  [Record](https://eprint.iacr.org/2025/756).
- A pool of client-specific hints can move latency out of the request path,
  but cannot erase creation work. Count unused packages and expiration; only a
  proven reusable/helper construction changes the aggregate-work calculation.
- Additional plain one-bit sharding, warm SinglePass runs, or public bucket
  selection are not new evidence for the strict cold objective.
- Recent lower bounds have specific single-server/blackbox models. They do not
  prove that all multi-server bit indexes or server-preprocessed PIR are
  impossible. [2026 lower bounds](https://arxiv.org/abs/2607.06451).

No new implementation or measured speedup is claimed by this research document.
