# PIR POC comparison and recommendation

This document consolidates the POC work into one decision matrix. It separates cold snapshot retrieval, warm repeated retrieval, public-window routing, and live subscriptions because a protocol that wins one workload can be a poor choice for another.

Unless noted otherwise, numbers are from the full release benchmarks run on 2026-08-17 on the same host. Server timings exclude TLS and a real network. "Server work" is the sum across replicas; "wall" is the co-located parallel latency. Storage is per replica. The synthetic layouts measure the allocated row capacity and cryptographic work; they are not a DefraDB GraphQL benchmark.

## Executive recommendation

| Situation | Recommended POC path | Why |
|---|---|---|
| Public query | Ordinary index | Essentially free; no query privacy |
| Candidate-set privacy is acceptable | 100 indexed decoys | By far the lowest server cost, but the server sees all candidates and repeated sets can be intersected |
| Strict-private cold/occasional snapshot query | Packed Dense | Stateless phone, 86-byte public metadata, modest upload and response, no preprocessing state |
| Strict-private query with a public coarse time range | Dense over immutable window tables | Same tag privacy, much less server work and phone upload while the selected tables are materially smaller than global |
| Strict-private repeated/warm queries | SinglePass `Q=16` | Tiny online server work and 128-byte total upload, in exchange for 48 MiB of mutable phone state at 4M rows |
| Strict-private cold query where upload matters more than download/storage | Finite differences | 32-byte total upload and low server work, but about 11.1 MiB download and 768 MiB storage per replica |
| Live subscription, candidate-set privacy | 100-decoy inverted index | Nanosecond event lookup; leaks candidates and the matching candidate |
| Live subscription, exact two-server privacy | Compact DPF | Small registration keys and exact target privacy; CPU and output grow linearly with active subscriptions |
| Live subscription, stronger `n-1` collusion tolerance and small population | Dense subscription shares | Indexed bit evaluation is extremely cheap, but persistent keys are huge |

The proposed production direction remains a compact immutable tag-page layout, potentially a 4-wise Binary-Fuse/RAID-style layout, evaluated through Dense XOR. That layout is **not implemented or benchmarked yet**. The current measured implementation is the two-candidate packed cuckoo layout in `tag_pages.rs`.

## Snapshot retrieval comparison

The cold tag workload contains 4,194,304 documents, 1,048,576 distinct tags, four 16-byte locators per tag, and one tag page per lookup. Normal Dense and SinglePass rows use a separate 4,194,304 × 64-byte synthetic database, so their timing is useful for protocol scaling but is not an identical row layout to the 384-byte packed tag pages.

| Option | Privacy/trust | Servers | Phone state | Total upload | Total download | Server work p50 | Wall p50 | Replica storage | Status |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| Ordinary public lookup | none; server sees tag | 1 | none | about 20 B | 64 B | 0.00011 ms | same | ordinary index | measured baseline |
| 100 indexed decoys | target hidden only among visible candidates | 1 | none | 800 B | 9.4 KiB padded | 0.0716 ms | same | ordinary index | measured; fastest private-ish option |
| Normal Dense, global 4M × 64 B | exact if at least one of `n` servers does not collude | 2 | none | 1 MiB | 128 B | 56.15 ms | 28.22 ms | 256 MiB | measured; server-count-neutral |
| Public-window Dense, 1/64 of 4M | exact tag privacy; coarse window is public | 2 | none | 16 KiB | 128 B | 0.47 ms | 0.32 ms local / 0.76 ms HTTP | 4 MiB selected; 512 MiB if global plus all windows are retained | measured endpoint |
| Packed cuckoo Dense | exact if two servers do not collude | 2 | 86 B public metadata | 142.2 KiB | 1.5 KiB | 23.13 ms | 11.93 ms | 106.7 MiB | measured strict cold default |
| Finite differences `m=21,d=9` | exact information-theoretic privacy against either server | 2 | 86 B layout metadata | 32 B | 11.1 MiB | 4.67 ms | 2.73 ms + 1.61 ms client reconstruction | 768 MiB | measured; 2.16 s reusable preprocessing |
| SinglePass `Q=16` | exact if two servers do not collude | 2 | 48 MiB mutable state | 128 B | 2 KiB | 0.00545 ms | 0.0885 ms | 256 MiB database plus client state | measured warm default; 236 ms setup |
| ChalametPIR | computational single-server privacy | 1 | extrapolated public matrix around 7.8 GiB at 1M records | 22.5 KiB at 4K records | 292 B at 4K | 1.91 ms at 4K | client query 17.3 ms at 4K | 492 KiB hint at 4K | rejected for phone-scale POC |

There is no zero-upload strict-private option here. A private client must send at least a compact query or maintain synchronized state. The smallest measured cold upload is finite differences at 32 bytes total; the smallest warm upload is SinglePass `Q=16` at 128 bytes total. A public indexed query is the only effectively upload-free choice, and it reveals the tag.

### Public-window crossover

The endpoint benchmark partitions the same 4,194,304-bucket capacity into 64 immutable windows. It includes fresh share generation, loopback HTTP, JSON/base64, server evaluation, and client reconstruction.

| Public range | Total upload, 2 / 3 servers | Summed server work, 2 / 3 servers | HTTP p50, 2 / 3 servers | HTTP reduction vs global, 2 / 3 servers |
|---|---:|---:|---:|---:|
| none (`global`) | 1 MiB / 1.5 MiB | 64.50 / 102.62 ms | 34.69 / 40.62 ms | baseline |
| 1 window | 16 / 24 KiB | 0.47 / 0.65 ms | 0.76 / 0.74 ms | 97.8% / 98.2% |
| 4 windows | 64 / 96 KiB | 4.89 / 5.97 ms | 4.03 / 3.37 ms | 88.4% / 91.7% |
| 16 windows | 256 / 384 KiB | 18.54 / 33.57 ms | 9.39 / 12.94 ms | 72.9% / 68.1% |
| 32 windows | 512 / 768 KiB | 33.65 / 53.08 ms | 17.97 / 24.20 ms | 48.2% / 40.4% |
| 64 windows | 1 MiB / 1.5 MiB | 68.62 / 118.43 ms | 35.76 / 52.05 ms | -3.1% / -28.2% |

Use public-window lookup while the sum of the selected table capacities is materially below the global table. Switch to global near full history. Querying every window performs the same Dense scan work as global and adds per-table dispatch and response overhead. Storing both alternatives in this synthetic benchmark costs 512 MiB per replica: 256 MiB global plus 256 MiB across all windows.

## Warm-query comparison

SinglePass is not a better cold first query. It becomes attractive after a client has acquired and durably stored its state and expects repeated queries against the same version.

At 4,194,304 × 64-byte rows with `Q=16`:

| Metric | Normal Dense, 2 servers | SinglePass, 2 servers |
|---|---:|---:|
| Total upload | 1 MiB | 128 B |
| Total download | 128 B | 2 KiB |
| Rows read/server | expected 2,097,152 | 16 |
| Summed server work p50 | 51.60 ms | 0.00545 ms |
| Co-located wall p50 | 25.92 ms | 0.0885 ms |
| Client state | none | 48 MiB mutable |
| One-time setup | none | 236 ms plus state transfer/persistence |
| In-flight behavior | stateless batching | one ordered query per mutable state |
| Server count | any `n >= 2` | exactly 2 |

SinglePass is the strongest result for server CPU, but production needs authenticated state transfer, atomic persistence, ordered updates, and recovery after an ambiguous request. A cold phone that performs only one or two queries should not pay this state cost.

## Live subscription comparison

The 4,194,304-bucket, 10,000-subscription result is the useful capacity point:

| Option | Privacy | Registration/client | Server state at 10k | Summed server work/event | Output/event | Important limitation |
|---|---|---:|---:|---:|---:|---|
| 100-decoy inverted index | candidate-set only | 400 B | about 73 MiB index | 106 ns matching lookup on one server | only matching subscriber handles | server sees candidates and which one matched; repeated sets/timing leak |
| Compact DPF | exact if two servers do not collude | 844 B total | about 8.05 MiB encoded keys total | 11.69 ms | 312.5 KiB total | evaluates every subscription for every event; all result shares required |
| Dense subscription shares, 3 servers | exact if at least one server does not collude | 1.5 MiB | about 4.88 GiB **per server** | about 1.3 ns/server/subscription hot-key lower bound | 29.3 KiB total (3 B/subscription) | huge working set; measured bit read is not a 5 GiB fanout benchmark |
| Threshold/multi-party DPF | construction-dependent | unknown | unknown | unknown | unknown | research-only; cannot be obtained safely by pairing ordinary 2-party DPF keys |

For live events, 100 decoys are overwhelmingly cheaper when candidate-set privacy is acceptable. Compact DPF is the exact-private tier, not the default performance tier. Dense subscription shares are interesting only for a small subscriber population or a deliberate memory-for-CPU deployment.

## What was rejected or remains research

| Option | Why it is not the current default |
|---|---|
| Legacy one-hash `build_paged` Dense | Estimated 3.56 GiB per replica and 2 MiB two-server upload for the cold tag workload; it sizes from documents, repeats page keys, reserves empty slots, and can overflow one bucket |
| 4-wise Binary-Fuse / RAID-Dense | Promising proposed immutable layout, but not implemented or benchmarked; it must beat the measured packed cuckoo layout on storage, candidate count, build reliability, and lookup work |
| Compact DPF for snapshot retrieval | Compact client key, but expanding it across the complete snapshot made server CPU substantially worse than Dense; the selected library is exactly two-party |
| ChalametPIR | Good single-server response and server timing at small scale, but the tested client matrix extrapolates far beyond phone memory |
| Path ORAM | Solves mutable access-sequence privacy with position maps, stash, path reads/writes, and reshuffling; much broader complexity than immutable tag retrieval |
| TEE plus ORAM | Adds hardware trust, attestation, deployment constraints, side-channel review, and still retains ORAM state/access-pattern work |
| Pairwise DPF on three servers | Unsafe shortcut: any colluding pair owns one complete DPF key pair and can recover the target |

## Proposed production shape

1. Build authenticated immutable generations from one DefraDB cutoff.
2. Publish a global table and coarse UTC window tables from the same generation.
3. Route strict cold queries to packed Dense; use public-window tables when the disclosed range materially reduces capacity.
4. Switch established, high-query clients to SinglePass only after durable state setup.
5. Offer 100 decoys as an explicit lower-privacy, low-cost tier.
6. Offer Compact DPF as the exact-private live tier; keep decoy subscriptions as the operational default where acceptable.
7. Keep server count generic for Dense. Three servers raise collusion tolerance from one to two colluding servers, but all three answers are still required; this is not one-server failure tolerance.
8. Benchmark a real Binary-Fuse/RAID-style tag-page layout before promoting it over packed cuckoo Dense.

## Reproduce

```text
cargo run -p pir-poc --release -- bench full
cargo run -p pir-poc --release -- bench-cold full
cargo run -p pir-poc --release -- bench-endpoints full
cargo run -p pir-poc --release -- bench-singlepass full
cargo run -p pir-poc --release -- bench-subscriptions full
```

Every benchmark correctness-checks recovered rows, pages, or subscription results. The POC is research code, not audited production cryptography.
