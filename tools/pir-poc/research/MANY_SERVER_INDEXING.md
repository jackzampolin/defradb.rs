# Many-server PIR and indexes on bits

Research date: 2026-09-04. Repository inspected at `faf37c5b`.

Follow-up clarification: the user prioritizes total work across all servers,
subject to reasonable client costs, and proposes distributing the stored index
by field-bit position. The current experiment order and admission criteria are
in [TOTAL_WORK_BENCHMARK_PLAN.md](TOTAL_WORK_BENCHMARK_PLAN.md). Sharding is a
control in that plan; its latency improvement alone does not meet the objective.

More machines can lower private-read latency. To reduce the total work of a
lookup, they need to support a different data structure or preprocessing
protocol. Assigning one bit of the requested address to each machine does not
by itself create a private equivalent of a B-tree or hash lookup.

This note checks the current POC, the supplied conversation, primary papers,
and the Zelda implementation. Existing timings below are archived measurements,
not new benchmark runs. Cluster scaling figures are analytical ceilings.

**What the POC actually does**

The served client already finds the row locally through the ordinal directory
in [selected.rs](../src/selected.rs). `strict_lookup` in
[selected_http.rs](../src/selected_http.rs) creates random Dense shares, sends
them concurrently, combines replies, and decodes the fixed result.

[dense.rs](../src/dense.rs) traverses every selector byte and XORs only the rows
whose share bits are set. With N rows of B bytes and two replicas, it processes
approximately N/2 payload rows per replica, or NB payload bytes in total, in
expectation. This is linear selection work; it does not evaluate an unknown
search predicate against every document. Logical payload operands are not a
measurement of physical DRAM traffic.

The unindexed-search analogy is therefore useful for scaling, but the missing
piece is *oblivious access*, not finding the row number. Bitmap equality search
would add a step to an exact-key lookup whose ordinal is already known.

The POC also contains:

- A research `ParallelEvaluator` that splits a selector into ranges and XORs
  partial answers. This is an algebraic starting point for distributed workers,
  but is currently a thread pool within one machine.
- Persistent subset-XOR indexing and ephemeral Four-Russians batch kernels.
- Stateful SinglePass and an official two-server finite-differences adapter.

**Three interpretations of “one machine per bit”**

| Interpretation | What happens | Performance implication |
|---|---|---|
| Each operator receives one plaintext address bit | Each learns that predicate, even without collusion | Does not preserve the current zero-information view of an individual operator |
| Each bit choice is itself protected by PIR or secret sharing | The bit need not leak; the earlier conversation assumed plaintext transmission | Private selection, intersection, and payload retrieval still need to be specified and charged |
| Each machine stores a bit plane or a slice of each result row | The payload width is distributed | Can parallelize payload work, but each plane still has N positions; private selector expansion may be repeated |

Non-collusion and zero leakage to an individual server are different properties.
If the requested bits remain cryptographically hidden, there is no reason to
reject the idea merely on the grounds of “one bit per server.” The question is
how those hidden choices become an efficient memory-access pattern.

For a bit-sliced equality index, sixteen uncompressed N-bit result bitmaps alone
contain 2N bytes. At one billion documents that is 2 GB, before replica replies,
framing, or payloads. Intersecting them at the client moves substantial work and
traffic to the client. Intersecting them privately at servers requires a secure
computation protocol; secret-shared AND is not obtained by simply XORing the
shares. Compression benefits depend on actual data distribution and ordering.

The apparent 128x saving versus 256-byte documents compares only sixteen bitmap
bits per document with the document bytes. It does not include private bitmap
selection, intersection, discovering matching ordinals, or retrieving the
documents. If the last stage uses the existing Dense payload table, its scan
returns. If it uses ordinary indexed reads, it reveals the selected rows unless
another access-hiding mechanism is added. For a row ordinal, the address-bit
patterns are public arithmetic and need no remote bitmap search at all.

A 16-bit domain contains 65,536 positions. It is an 8 KiB table only when the
answer is one presence bit per position. With 256-byte rows it is 16 MiB.

**Sharding as a latency control: two operators, many workers**

Generate the existing two shares qA and qB, with qA XOR qB = e(target).
Partition both shares and both copies of the table using the same public ranges.
Every range participates in every query. Worker j at operator A returns the XOR
of its local rows selected by qA[j]; operator B does the same for qB[j]. XOR all
the partial answers to recover the requested row.

```text
                         client
                    /              \
               share A            share B
                  |                  |
          independent A       independent B
          A0 A1 ... A7        B0 B1 ... B7
          range workers      range workers
                  |                  |
           XOR reductions     XOR reductions
                    \              /
                     combine + verify
```

All A workers together still see only one random share; likewise B. The trust
boundary is the two independent operators. Splitting one operator into many
workers does not create many independent cryptographic parties. A coalition
holding both shares of a range can distinguish whether that range was selected.

For S workers per operator, M = 2S machines:

| Machines | Workers/operator | Ideal scan speedup over two equivalent machines | Aggregate table storage | Expected aggregate payload XOR bytes |
|---:|---:|---:|---:|---:|
| 2 | 1 | 1x | 2NB | NB |
| 16 | 8 | 8x | 2NB | NB |
| 100 | 50 | 50x | 2NB | NB |
| 128 | 64 | 64x | 2NB | NB |

These ceilings assume balanced ranges, independent memory bandwidth, and no
coordination overhead. Client preparation, transfer, reductions, verification,
and the slowest worker limit complete-query speedup. All required responses
must arrive; ordinary Dense adds no straggler tolerance.

Raw client upload remains about N/4 bytes for two Dense shares when slices are
routed without duplication. It does not inherently increase with S. Direct
worker requests return 2SB raw response bytes. Reducing inside each operator
keeps the client download at 2B, while still charging internal traffic. JSON,
base64, HTTP and OHTTP add their own costs.

The existing 1 GiB / 2^23-row benchmark uploads 2 MiB and spends 2.68 ms creating
Dense shares. At an assumed 100 Mbit/s client uplink, raw upload serialization
alone takes about 168 ms. The archived GPU evaluation is 6.17 ms summed over two
replicas. Even eliminating all server evaluation cannot eliminate that upload.
See [BENCHMARKS.md](../BENCHMARKS.md) and
[FULL_COMPARISON.md](FULL_COMPARISON.md). These are different components, not a
measured network latency prediction.

Compact-DPF shares are a useful comparison when upload dominates. A real
distributed evaluator must visit only its assigned DPF subtree/range rather
than expand the complete selector on every worker. This saves communication
under a computational, two-party construction; it does not remove aggregate
linear database work.

For throughput, compare the same fleet as many independent two-server pairs.
Splitting one query across the entire fleet uses that fleet for the duration of
the query; capacity and single-query latency are separate objectives. Batch
size also changes when traffic is spread among more workers.

RAID-PIR supplies a related published distribution design. With K servers and
replication parameter r, each stores rN/K rows and privacy holds against up to
r-1 colluding servers. Its expected total XOR operands remain rN/2. The basic
random-share version is information-theoretic; its seed-compressed variants
rely on a PRG. It is a route to distributing storage and work, not automatically
sublinear aggregate work. The archive's rejection at K=r=3 does not rule out
K=16,r=2 under the user's non-collusion assumption.
[RAID-PIR, sections 3.2–3.3](https://encrypto.de/papers/DHS14.pdf).

**A private index on selector bits already exists here**

[subset_xor.rs](../src/subset_xor.rs) groups g source rows and stores the XOR of
every nonempty subset. A server reads g bits of its *random query share* and
uses that value as an ordinary index into the precomputed answers. For example,
share mask 1010 selects the stored XOR of rows 1 and 3. The accessed subset is
independent of the target from that server's perspective.

For complete groups, the derived costs are:

- Index-only storage factor: (2^g - 1)/g.
- Expected selected-row reduction versus Dense: g / (2(1 - 2^-g)).
- Every query still visits about N/g groups; fixed g leaves linear complexity.

| Group bits g | Index-only storage factor | Expected payload-operand reduction |
|---:|---:|---:|
| 4 | 3.75x | 2.13x |
| 6 | 10.5x | 3.05x |
| 8 | 31.875x | 4.02x |
| 16 | 4095.9375x | 8.00x |

Keeping the original source table adds another 1x. The implementation supports
g=2 through 10; the g=16 row is a formula, not an implemented configuration.
Updating one source row changes 2^(g-1) cached combinations; this POC rebuilds
immutable indexes.

The archived group-six experiment on wider 384-byte rows improved server p50
by 1.77x with two replicas, at 11.5x total storage. On compact 96-byte rows,
group-eight took 32.875x storage and ranged from 7.5% slower to 15.9% faster
depending on topology/percentile. Random access and cache effects matter.
[Existing measurements](COMPARISON.md).

More machines could make a distributed g=4/6/8 index fit in RAM. That is worth
a bounded experiment on wide results, but it should be compared with plain
sharding at the same aggregate memory and machine budget. It is not a new
unexplored cryptographic shortcut.

**Research that can reduce online work further**

SinglePass is already a strong local reference for clients that can keep state.
On the same 262,144 x 96-byte table, the archive reports 6.01 ms aggregate Dense
server time versus 2.29 microseconds for SinglePass Q=2. SinglePass requires
24.09 MiB setup download and 14.09 MiB retained client state. This is an online
result, not a cold-client or lifecycle speedup. Generation refresh and atomic
post-query state persistence remain material.
[Warm-stateful evidence](WARM_STATEFUL.md), [result table](COMPARISON.md).

The official finite-differences adapter already demonstrates reduced online
payload reads without that client preload: 4.699x fewer expected payload bytes
than Dense on the 24 MiB common corpus. It pays 8x encoded storage per replica
and 5.36 MB download for a 96-byte result. The tested artifact is two-server;
the paper's many-server generalization was not implemented in that experiment.
[Local result and limitations](finite_diffs_artifact/RESULTS.md),
[official research implementation](https://github.com/ahenzinger/finite-diffs-pir).

The supplied discussion cites Singh–Wei–Zikas, whose revised TCC 2024 paper
does establish roughly square-root online computation with client preprocessing
and threshold-one privacy. Its 2t notation means twice t servers, with a stated
parameter range; this is not an arbitrary-hundreds-of-servers performance law.
Exponents with hidden factors cannot be converted into measured operation counts
by writing 4*sqrt(N).
[Revised paper metadata and abstract](https://eprint.iacr.org/2024/780).

Zelda, published at IEEE S&P 2026, is a more concrete follow-up. It uses
client-specific hints and reports an implementation; its authors identify an
impractical underlying PIR component in Singh et al. and remove that dependency.
Its reported comparison improves online response/client space against
QuarterPIR under the stated network assumptions, while increasing offline
maintenance. This motivates evaluation, not a claimed win against our Dense
GPU or SinglePass measurements.
[Zelda paper](https://eprint.iacr.org/2025/1340).

The official [Zelda repository](https://github.com/p-b-p-b/Zelda) was found and
inspected at `11b8e70ffcb3ee8d2ea72824c04ed8faa1fa558a`. It is an unaudited Go
benchmark, with a default 2^32 x 32-byte database (128 GiB). Crucially, its
client uses one gRPC endpoint for hint generation, replacement entries and
online parity requests. It does not demonstrate independently operated roles.
It also uses time-seeded non-cryptographic randomness, and its
`ignorePreprocessing` option skips correctness verification. Any adapter must
map the paper's roles and collusion threshold explicitly, use appropriate
randomness, keep correctness enabled, and include recurring hint maintenance.
[Client source](https://github.com/p-b-p-b/Zelda/blob/11b8e70ffcb3ee8d2ea72824c04ed8faa1fa558a/client/client.go),
[database parameters](https://github.com/p-b-p-b/Zelda/blob/11b8e70ffcb3ee8d2ea72824c04ed8faa1fa558a/util/util.go).

For cold clients, global server preprocessing is the closer research match.
Ghoshal et al.'s paper is now titled *Scalable Multi-Server Private Information
Retrieval* (TCC 2025). The retrieved PDF table does contain the cited 16-server
exponents: work/communication 0.2725 and storage 1.2725. These omit additional
factors and represent per-server costs. Even the exponent-only illustration
at n=10^9 gives about 283x base storage per server, or 4,535x over 16 servers.
That is not a capacity estimate for billion-row Defra documents: the symbol
size, encoding parameters, constants and result width must be instantiated.
The ePrint record has major revisions; pin a version before implementation.
[Current record](https://eprint.iacr.org/2024/765),
[retrieved parameter table](https://eprint.iacr.org/2024/765.pdf).

Another relevant update is the revised *Multi-Server Doubly Efficient PIR in
the Classical Model and Beyond*. In addition to non-communicating-server
results, it gives a stateful, communicating-server model with polylogarithmic
query work for more than three servers. That is closer to an oblivious service
with coordinated state than independent stateless PIR replicas. It deserves
separate evaluation if server interaction and durable shared state are
acceptable; the abstract's asymptotics are not a validated deployment budget.
The web tool returned an older cached PDF, so this update was checked against
the revised record, not treated as a verified implementation.
[Revised record](https://eprint.iacr.org/2024/829).

**Original latency-oriented experiment order, superseded**

The following initial order is retained as research history. Execute the
aggregate-work plan linked above instead.

1. **Measure distributed Dense and range-evaluated DPF.** Use two independent
   operator groups with 1/2/4/8 workers each, then 16/32/64 if scaling remains
   useful. Exercise every shard. Measure verified p50/p95 latency, client
   preparation, actual wire bytes, operator reductions, aggregate evaluation
   work and the slowest worker. Compare LAN and a shaped client uplink. A
   same-host multi-process run validates routing but is not a multi-machine
   performance result.
2. **Compare the same fleet against replicas and batches.** Report sustainable
   throughput and queue time as well as single-query latency. Use the existing
   immutable 96-byte corpus, the 1 GiB/128-byte GPU geometry and a wider result
   class. Keep result semantics and physical layout identical within comparisons.
3. **Evaluate distributed subset-XOR only where memory and row width justify it.**
   Start at g=4 and 6. Charge source plus index memory, rebuild cost and internal
   network traffic. Compare against the same hardware budget spent on ordinary
   sharding. Stop if complete-query gains disappear.
4. **Evaluate Zelda against existing SinglePass for warm clients.** First build
   a role-correct adapter on a bounded corpus. Include 1/10/100/1,000-query
   lifetimes, retained client bytes, initialization and recurring maintenance.
   Do not equate the prototype's one endpoint or parameter named kappa with
   a verified count of independent protocol parties.
5. **For cold, unwindowed lookup, instantiate global-preprocessing parameters.**
   Compare 4/8/16-server candidates with the finite-differences reference before
   attempting a large implementation. Require concrete encoded bytes, bytes
   read, total replies, preprocessing time and exact collusion threshold.

Keep bit-sliced MPC as a secondary-index experiment only when the workload
actually needs predicate discovery, ranges or intersections. For a known row A,
sharded retrieval and preprocessed PIR address the existing bottleneck more
directly. For an all-match query, include every returned page and private
payload fetch; a fast bitmap alone is not a complete search result.
