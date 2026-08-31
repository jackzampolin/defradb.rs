# Full PIR comparison protocol

## Decision being measured

The primary objective is minimum aggregate server work for one correct private
read. Latency, client work, upload, download, memory, preprocessing, update
cost, trust assumptions and replica count remain separate constraints. A
single score would hide the trade that matters most to Defra.

The comparison has two independent server lanes:

| Lane | Implementations | Why it exists |
|---|---|---|
| Same CPU | Rust Dense XOR and pinned `poulpy-pir` InsPIRe2 on AVX2; the official artifact remains an AVX-512 portability gate | Decides whether a CPU-only Defra sidecar is sensible |
| Same GPU | Defra Dense XOR, pinned GPU-DPF, and pinned `inspire-gpu` | Decides whether a GPU changes the cold/small-batch protocol choice |

Do not run the `inspire-gpu` CPU oracle as a server candidate. It exists for
correctness, while the repository's serving implementation is CUDA. The valid
CPU InsPIRe candidates are the pinned official artifact and a separately
qualified optimized CPU implementation such as `poulpy-pir`.

### Should InsPIRe be evaluated on CPU?

Yes, but as a separate deployment lane, not as the presumed winner. The runner pins
[`poulpy-fhe/poulpy-pir`](https://github.com/poulpy-fhe/poulpy-pir) at
`533081a74301c8ba6ddd5e1dfc0c9daa6e3e75ef` and tested its supported AVX2/FMA
backend on ordinary Defra hardware. AVX-512 remains a future server-class
qualification; the scalar backend is a portability/correctness gate only.

The CPU question matters when a provider cannot dedicate a GPU or when a large
NUMA server can batch many requests. It is unlikely to improve the first-query
case on a small edge CPU: InsPIRe performs lattice/FHE work and preprocessing,
whereas Dense is a sequential XOR scan. A CPU InsPIRe win must therefore be
demonstrated for the real table and batch; do not infer it from the CUDA result.
The current `inspire-gpu` client remains CPU-only in every case, including a
phone or laptop client; “GPU InsPIRe” refers to the server.

## Identical workload

Every admitted result must use:

- 120 useful bytes per entry and explicit physical encoding bytes;
- exact populated powers of two at `2^20`, `2^23`, `2^25`, and `2^27` where
  hardware capacity permits;
- the same deterministic logical records and target ordinals;
- batch sizes 1, 2, 4, 8, 16 and 32, with 128 retained as a throughput stress
  point for Dense/DPF;
- correctness reconstruction for first, middle, last, hit and miss fixtures;
- a resident immutable snapshot, no keyword-to-ordinal lookup in the kernel;
- aggregate server work as the primary metric. For replicated protocols this
  is the sum across servers, even if their wall latency is parallel.

If a case does not fit, record `capacity_blocked` with required and available
memory. Never silently shrink the table or substitute a paper number into the
same-hardware table.

## Cold means three different things

| Boundary | Start point | End point | Why it matters |
|---|---|---|---|
| Cold client | Public parameters and row ordinal are available; no retained database hint or selector | Serialized query ready to send | A wallet or new process can enter immediately |
| Cold online server | Snapshot is preprocessed and resident, but no query/kernel warmup has run | First response bytes are ready | Captures launch, allocation and lazy-runtime penalties |
| Cold snapshot | Immutable table exists on host storage | All server state is resident and ready | Captures H2D/loading and InsPIRe preprocessing; amortize only over the declared generation lifetime |

Warm batch results start after warmup. Queue dwell is not server work and must
be added using a real arrival rate and flush deadline. Report both batch wall
latency and per-query aggregate work.

## Required fields

For every `(protocol, hardware, N, batch)` tuple record:

- security assumption, server count, collusion threshold and required answers;
- useful, logical, physical, resident and peak bytes per server;
- client query generation, serialization, recovery and peak memory;
- upload and download bytes, both per server and aggregate;
- server first-online, warm p50/p95, batch wall and aggregate time/query;
- preprocessing/build time, first snapshot time, and update/rebuild policy;
- H2D, D2H and kernel phases separately on GPU;
- energy/query and peak power when NVML/RAPL is available;
- correctness, upstream revision, compiler flags and complete hardware ID.

Derived end-to-end latency should be reported at measured local transport and
at explicit 10, 50 and 100 Mbit/s client upload rates. Network projections are
not server-computation results.

## Execution sequence

1. Run `run-gpu-pir-defra.sh full` and
   `run-inspire-gpu-defra.sh full` on the same idle GPU. Repeat in alternating
   process order at least five times.
2. Compare only matching entry count, row width and batch. Keep Dense aggregate
   work and parallel replica wall time as distinct columns.
3. On an AVX-512 host, run the official CPU InsPIRe adapter, an optimized
   `poulpy-pir` adapter, and CPU Dense in alternating isolated processes.
4. Repeat the deployment candidates on a server-class GPU with at least 32 GB
   VRAM so the `2^27`/16 GiB table is locally comparable.
5. Add keyword mapping, OHTTP/Tor and real RPC only after the primitive matrix
   is frozen; those layers are common service costs and have separate leakage.

For one screening pass, `run-full-gpu-comparison.sh quick` executes and joins
the matching rows. `run-full-gpu-comparison.sh full` expands the Dense/DPF
matrix and runs all upstream InsPIRe tests. The publication comparison uses
`run-repeated-gpu-comparison.sh`: five fresh processes alternate both suite
order and Dense/DPF internal order, then emit p50/min/max JSON.

## Final privacy-first ranking

The first gate is the privacy guarantee, not raw server time. The 100-candidate
control is therefore last even though it is faster: it reveals the candidate
set, supports longitudinal intersection and provides no cryptographic query
privacy. There is no honest single ordering across different query lifecycles,
so the final ranking is three short ladders:

| Rank | Cold snapshot | Why |
|---:|---|---|
| 1 | Exact-MPHF/Fuse plus replicated Dense XOR | Lowest measured aggregate strict server work, cold client, simple XOR construction and 2/3/more-server flexibility |
| 2 | GPU InsPIRe | Strict computational single-server fallback when independent operators are unavailable; heavier locally than Dense |
| 3 | GPU-DPF | Strict compact-upload fallback for exactly two operators and a large ready batch; poor batch-1 server work |
| 4 | Split-trust blind exact search | One point lookup, but equality/access/update leakage means it is not PIR |
| 5 | 100 indexed decoys | Last-resort privacy downgrade after every strict deployment path fails |

For the normal cold single-query case InsPIRe ranks above GPU-DPF: the measured
batch-1 work was 32.21 versus 437.73 ms. With exactly two operators and a ready
batch of 32, GPU-DPF moves ahead (13.74 versus 18.86 ms) and also has the
smaller upload. This is a conditional fork, not a claim that one dominates.

| Rank | Warm immutable generation | Why |
|---:|---|---|
| 1 | SinglePass | Microsecond online work after generation download; ideal when many reads amortize exactly-two-server client state |
| 2 | Replicated Dense XOR | No full preload or mutable client state; retains 3+ server support |
| 3 | GPU InsPIRe or batched GPU-DPF | Same single-server versus compact-upload split as the cold ladder |
| 4 | Split-trust blind exact search | Weaker access-pattern tier |
| 5 | 100 indexed decoys | Final candidate-visible fallback |

| Rank | Live presence | Why |
|---:|---|---|
| 1 | Packed-presence Dense per public epoch | Ingest once, answer once/subscriber/epoch, lowest measured strict work and 2/3/more-server support |
| 2 | Immediate Compact DPF | Only when sub-epoch delivery is required; exactly two parties and work grows as events times subscribers |
| 3 | Visible subscriptions/decoys | Last resort; exposes watched buckets, hits, popularity and repeated interest |

Finite-differences PIR stays a narrow research result: on the 262,144 x 96 B
CPU corpus it beat Dense server time (3.33 versus 6.01 ms) but required 8x
server storage, a 5.36 MiB response, exactly two servers, and has no validated
large/GPU result. It does not displace the production default. Fuse and cuckoo
are table-layout alternatives, not new privacy protocols; exact MPHF Dense is
smaller/faster when every populated key can be enumerated, Fuse is the robust
fallback, and cuckoo was dominated in the POC.

### Decision gates

- Prefer Dense when two or more independently operated replicas are available,
  aggregate server work wins, and its `N/8` bytes per server upload is within
  the client/network budget.
- Prefer SinglePass over Dense only for repeated reads of the same immutable
  generation when the client can download it, retain mutable state and complete
  the atomic two-server update after every query.
- Prefer packed-presence Dense for live queries whenever a block/epoch delay is
  acceptable. Use immediate Compact DPF only for a measured sub-epoch product
  requirement.
- Prefer GPU InsPIRe when a single-server trust model is required or Dense's
  growing upload dominates end-to-end latency. Its lack of a client database
  hint makes it a legitimate cold-client design, despite heavier query crypto.
- Keep GPU-DPF only if compact upload is essential and same-hardware measured
  server work becomes competitive at the deployment's real batch size. The
  current batch-1 result is not competitive.
- Use split-trust blind search only when an independent index provider cannot
  map tokens to plaintext and equality/access leakage is acceptable.
- Use 100 visible decoys only after the strict and split-trust options miss the
  deployment budget and candidate-set leakage is explicitly accepted. It is
  the final degraded mode, not a PIR optimization or the default recommendation.

## Final same-card GPU result

Five alternating fresh-process RTX 2070 SUPER runs used the same `2^23 x 120 B`
logical corpus (128 physical bytes, 1 GiB) and verified every answer:

| Ready batch | Dense XOR, 2 servers | GPU-DPF, 2 servers | InsPIRe GPU, 1 server |
|---:|---:|---:|---:|
| 1 | **6.17 ms** | 437.73 ms | 32.21 ms |
| 2 | **6.15 ms** | 215.55 ms | 22.42 ms |
| 4 | **6.12 ms** | 108.00 ms | 21.17 ms |
| 8 | **6.18 ms** | 54.18 ms | 20.03 ms |
| 16 | **6.14 ms** | 27.17 ms | 18.86 ms |
| 32 | **6.14 ms** | 13.74 ms | 18.86 ms |

Values are p50 aggregate server milliseconds per query. Dense and DPF sum both
replicas; InsPIRe has one server. At batch 1 the corresponding client query
times were 2.68, 0.084 and 47.48 ms, and aggregate uploads were 2,097,152,
4,160 and 379,904 B. InsPIRe recovery was 4.09 ms and its 12,288 B response is
larger than the 240 B replicated responses.

The first-online p50 was 8.10 ms Dense, 446.93 ms GPU-DPF and 179.01 ms
InsPIRe. InsPIRe first-online varied from 58.27 to 308.05 ms because of lazy
runtime effects. Its cold-snapshot p50 spent 9.03 s materializing host data,
5.56 s preprocessing and 3.91 s constructing the context. Those phases are
reported rather than amortized into an undeclared number of queries.

The full local GPU matrix also completed the 4 GiB Dense/DPF tier. At batches
1/8/32/128, Dense used 23.07/23.20/23.26/23.48 ms per query and GPU-DPF used
1,667.08/203.98/52.56/28.84 ms. The 4 GiB InsPIRe state needs about 6.44 GiB
before CUDA/display/scratch overhead and was capacity-blocked on this 8 GiB
card; it was not silently replaced by a paper number. A 32 GiB server GPU is
still required for the locally comparable 16 GiB tier.

## Same-host CPU result

The AVX2 Ryzen 7 3700X run used the same `2^23 x 120 B` useful corpus in 128
physical bytes with the final eight bytes zero. Dense used eight worker threads
per replica. Poulpy used its AVX2/FMA InsPIRe2 backend. All cells reconstructed
the selected bytes:

| Batch | Dense, 2 replicas aggregate wall/query | Poulpy server wall/query | Poulpy summed phase work/query | Dense advantage over Poulpy wall |
|---:|---:|---:|---:|---:|
| 1 | **115.90 ms** | 415.10 ms | 414.41 ms | 3.58x |
| 8 | **49.11 ms** | 396.22 ms | 1,450.53 ms | 8.07x |
| 32 | **67.59 ms** | 224.38 ms | 1,178.94 ms | 3.32x |

Poulpy's client query took 2.04/1.99/1.72 ms per query, uploaded 428,117 B,
downloaded 196,889 B, used 5.71--6.87 GiB peak RSS and spent 30.5--36.1 s in
offline preprocessing. Dense client query took 3.24/4.44/4.70 ms and uploaded
2 MiB. Parallel wall time is not total CPU energy: the Poulpy summed-phase
column exposes its parallel work, while the Dense adapter does not yet record
thread CPU-seconds. The result is nevertheless sufficient for the deployment
decision: CPU InsPIRe did not beat Dense wall time and is not the edge-server
default. Re-run both on AVX-512/NUMA hardware before making a server-class CPU
claim.
