# PIR POC evidence status and benchmark requirements

The earlier version of this document put measurements from different physical layouts into one table. That made ratios between windowed Dense, packed tag pages, indexed decoys, and SinglePass look comparable when they were not. Those cross-workload ratios are withdrawn. Snapshot protocol ranking remains open until every option runs against the same populated immutable tables and query semantics.

## Current decision status

| Component | Status |
|---|---|
| Packed tag-page Dense XOR | Current stateless strict-private implementation candidate; needs the unified benchmark below |
| Public time-window routing | Supported feature; the disclosed window is an intentional privacy/performance choice |
| 100 indexed decoys | Lower-privacy baseline; must query the identical selected windows in the unified benchmark |
| SinglePass | Promising warm-query experiment; not yet compared on the packed tag-page layout or identical windows |
| Finite differences | Retained cold-query experiment; not selected for production |
| 4-wise Fuse retrieval over Dense XOR | Implemented and benchmarked on the identical populated page corpus; best server-side static layout so far, but roughly doubles phone upload versus packed cuckoo |
| 3-wise Fuse retrieval over Dense XOR | Implemented and benchmarked; not selected because its storage/upload/build-memory costs are higher and its timing advantage over 4-wise was not stable |
| RAID-PIR | Evaluated separately as a distribution protocol; not useful for the current three-server/two-colluder target because `r = k` leaves every server with the full table |
| Compact DPF subscriptions | Exact-private live experiment; separate from snapshot selection |

## What packed Dense means

Packed Dense is not a new PIR construction. It is ordinary Dense XOR PIR over a purpose-built immutable tag-page table:

1. All compact document locators for `(tag, page number)` are encoded into a fixed-size page with a fingerprint.
2. Four pages fit in one bucket row. The cuckoo builder places every page into one of two public candidate buckets at roughly 90% slot occupancy.
3. From the small public manifest, the client computes both candidate buckets but cannot know which one holds its page.
4. The client creates independent Dense XOR query shares for both candidates. Every server still scans the complete packed table for each candidate.
5. The client combines all server answers and accepts the bucket slot with the expected fingerprint.

"Packed" therefore reduces the number of rows that Dense must scan; it does not turn the scan into an indexed lookup. Two candidates also mean two Dense evaluations per page. The current benchmark uses two servers, while Dense share generation itself supports any `n >= 2`. All `n` answers are required.

## Fuse retrieval result

[`fuse_pages.rs`](src/fuse_pages.rs) uses the Binary Fuse peelable graph as an exact static retrieval table. A complete tag page is assigned across three or four cells so their XOR reconstructs it. All positions are selected in one Dense XOR query share; the client still needs only a constant-size manifest and all server answers.

The full `bench-fuse` run uses 4,194,304 documents, 1,048,576 tags, four 16-byte locators per tag, and the exact same 96-byte page corpus for every layout:

| Layout | Table/server | 2-server upload | 2-server download | 2-server wall p50 range | Summed server p50 range | Build range |
|---|---:|---:|---:|---:|---:|---:|
| Packed cuckoo | 106.67 MiB | 142.22 KiB | 1.50 KiB | 11.09–11.74 ms | 21.54–22.64 ms | 612.71–619.61 ms |
| Fuse-3 | 108.00 MiB | 288.00 KiB | 192 B | 9.44–10.53 ms | 18.03–20.45 ms | 1,514.70–1,567.14 ms |
| Fuse-4 | 103.50 MiB | 276.00 KiB | 192 B | 9.82–10.79 ms | 19.04–20.86 ms | 1,510.73–1,574.62 ms |

Across two repeated 31-sample passes, Fuse-4 reduced two-server wall time by 2.72–16.37% and summed server time by 3.15–15.93% relative to cuckoo. Deterministically it reduced expected XOR bytes by 51.49%, storage by 2.97%, and download by 87.5%, while increasing upload by 94.06%. Fuse-3/Fuse-4 timing order flipped between runs; Fuse-4 remains the useful implementation because its storage, upload, expected server bytes, and build memory are all lower. See [`FUSE_BENCHMARK.md`](FUSE_BENCHMARK.md) for the three-server data, build-memory accounting, method, and RAID-PIR analysis.

This closes the physical-layout comparison only. It does not make the older decoy and SinglePass numbers comparable: those paths still need the broader identical-window and transport benchmark described below. A real phone/network run is also required to decide whether Fuse-4's small server saving is worth its additional 137 KiB of two-server upload.

## Why the old snapshot numbers are not decision data

| Benchmark | What it actually measures | Missing for a fair comparison |
|---|---|---|
| `bench-cold` | Global synthetic tag pages; page zero; packed Dense and finite differences versus 100 public tag lookups | Identical public time windows, realistic cardinality/page distributions, and HTTP/network |
| `bench-endpoints` | Dense routing and serialization over fixed-capacity tables containing only one populated record | Real packed tag-page tables and matching decoy/SinglePass requests |
| `bench-singlepass` | SinglePass mechanics over a raw `N × row_size` synthetic database | Packed tag pages, selected windows, state transfer, and comparison with identical decoy results |
| `bench` | Dense kernel scaling over raw fixed-size rows | DefraDB tag-page semantics and matching alternatives |
| `bench-fuse` | Packed cuckoo, Fuse-3, and Fuse-4 over one identical populated tag-page corpus, with two and three replicated servers | Phone/network transport and the decoy/SinglePass paths |
| Chalamet measurements | A separate small-record experiment | The same data scale, layout, and phone implementation |

The endpoint and protocol benchmarks remain useful as correctness tests and scaling diagnostics. They must not be used to claim a global/window crossover or that one snapshot protocol is a particular multiple faster than another.

## Required snapshot benchmark

The next performance comparison must build one realistic immutable generation and derive global and coarse UTC-window tag-page tables from it. Every path must query the same selected windows, return the same padded locator pages, and include the same transport boundary:

- one visible indexed tag;
- 100 visible decoy tags;
- packed Dense with two and three servers;
- SinglePass with setup reported separately and amortized over query counts;
- finite differences if it remains within the storage/download budget.

Until that benchmark exists, time-window support is a functional capability, packed Dense is an implementation candidate, and SinglePass is a warm-query hypothesis—not a measured winner over decoys.

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
| 3-wise Fuse retrieval | Implemented, but uses more storage, upload, expected server bytes, and build memory than 4-wise; measured timing order flipped across repeated runs |
| RAID-PIR for 3 servers / 2-colluder privacy | With `k = 3, r = 3`, every server stores the full table; it requires a separate protocol and does not improve the current target deployment |
| Compact DPF for snapshot retrieval | Compact client key, but expanding it across the complete snapshot made server CPU substantially worse than Dense; the selected library is exactly two-party |
| ChalametPIR | Good single-server response and server timing at small scale, but the tested client matrix extrapolates far beyond phone memory |
| Path ORAM | Solves mutable access-sequence privacy with position maps, stash, path reads/writes, and reshuffling; much broader complexity than immutable tag retrieval |
| TEE plus ORAM | Adds hardware trust, attestation, deployment constraints, side-channel review, and still retains ORAM state/access-pattern work |
| Pairwise DPF on three servers | Unsafe shortcut: any colluding pair owns one complete DPF key pair and can recover the target |

## Proposed production shape

1. Build authenticated immutable generations from one DefraDB cutoff.
2. Publish a global table and coarse UTC window tables from the same generation.
3. Use the public-window endpoint when the client intentionally discloses a coarse range, without claiming a crossover until the unified benchmark exists.
4. Keep both packed cuckoo and Fuse-4 for the phone/network benchmark: Fuse-4 wins expected server bytes, storage, and response; cuckoo wins cold upload.
5. Evaluate SinglePass and 100 decoys on those identical packed window tables before defining cold/warm routing.
6. Offer Compact DPF as the exact-private live tier; keep decoy subscriptions as the operational default where acceptable.
7. Keep server count generic for Dense. Three servers raise collusion tolerance from one to two colluding servers, but all three answers are still required; this is not one-server failure tolerance.
8. Revisit RAID-PIR only if deployment grows beyond three servers while keeping a lower collusion threshold; do not combine it with the replicated-layout implementation.

## Reproduce

```text
cargo run -p pir-poc --release -- bench full
cargo run -p pir-poc --release -- bench-cold full
cargo run -p pir-poc --release -- bench-endpoints full
cargo run -p pir-poc --release -- bench-fuse full
cargo run -p pir-poc --release -- bench-singlepass full
cargo run -p pir-poc --release -- bench-subscriptions full
```

Every benchmark correctness-checks recovered rows, pages, or subscription results. The POC is research code, not audited production cryptography.
