# Fuse retrieval benchmark

## Result

Exact 3-wise and 4-wise Fuse retrieval is now implemented over the existing replicated Dense XOR protocol. The Fuse cells reconstruct the complete 96-byte tag page, not a probabilistic membership result. A client computes three or four public cell positions from the small manifest, puts all positions in one secret-shared multi-hot selector, and combines one page-sized answer from every server.

On the full populated workload, 4-wise Fuse is the useful variant. Across two repeated 31-sample full passes, it reduced two-server co-located wall time by 2.72–16.37% and summed server time by 3.15–15.93% versus packed cuckoo. Its deterministic costs are clearer: table storage fell 2.97%, expected XOR bytes/server fell 51.49%, and download fell 87.5%, while total upload rose 94.06% and offline layout-build time was about 150% higher. Fuse-3 and Fuse-4 timing order flipped between runs, but Fuse-4 has the smaller table, upload, expected server work, and peak build memory, so there is no reason to ship both.

This does not yet prove that Fuse-4 is the mobile winner. Its extra upload is 137 KiB for two servers, while its in-process server saving was only 0.30–1.92 ms across the repeated runs. A real phone/network benchmark is needed to decide whether to make Fuse-4 or packed cuckoo the default cold route.

## Workload and method

- Release build on an AMD Ryzen 7 3700X (8 cores/16 threads), under WSL with 7.7 GiB available memory.
- One synthetic but fully populated immutable window table: 4,194,304 documents, 1,048,576 distinct tags, four 16-byte document locators per tag, and one 96-byte page per tag.
- The encoded page corpus is constructed once. Cuckoo, Fuse-3, and Fuse-4 consume those identical page keys and bytes.
- Two public cuckoo candidates require two Dense evaluations. A Fuse lookup requires one Dense evaluation with three or four selected cells.
- Two and three server topologies are measured. Every co-located server has the existing persistent two-worker evaluator; servers contend for one memory bus.
- Every topology is warmed once, then measured 31 times with fresh cryptographic shares. Every recovered page is correctness checked.
- Timings exclude HTTP, TLS, serialization, and network latency.
- Peak build memory is deterministic algorithm-owned memory, including the common corpus, output table, and temporary vectors. It excludes allocator metadata, code, thread stacks, and runtime overhead. This is more comparable than process RSS across sequential builds.

The common encoded corpus took 426.99–428.90 ms to create and owns 179 MiB including page keys and Rust vector storage. The raw encoded page payload is 96 MiB.

## Full benchmark

| Layout | Private retrievals | Table/server | Build range | Peak tracked build | Expected XOR bytes/server | 2-server upload | 2-server download | 2-server wall p50 range | 2-server summed work p50 range | 3-server upload | 3-server download | 3-server wall p50 range | 3-server summed work p50 range |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Packed cuckoo | 2 candidates | 106.67 MiB | 612.71–619.61 ms | 303.45 MiB | 106.67 MiB | 142.22 KiB | 1.50 KiB | 11.09–11.74 ms | 21.54–22.64 ms | 213.33 KiB | 2.25 KiB | 11.38–12.28 ms | 33.10–35.43 ms |
| Fuse-3 | 3 cells in 1 selector | 108.00 MiB | 1,514.70–1,567.14 ms | 303.00 MiB | 54.00 MiB | 288.00 KiB | 192 B | 9.44–10.53 ms | 18.03–20.45 ms | 432.00 KiB | 288 B | 11.61–12.01 ms | 33.21–34.24 ms |
| Fuse-4 | 4 cells in 1 selector | 103.50 MiB | 1,510.73–1,574.62 ms | 298.50 MiB | 51.75 MiB | 276.00 KiB | 192 B | 9.82–10.79 ms | 19.04–20.86 ms | 414.00 KiB | 288 B | 11.27–11.65 ms | 32.01–33.50 ms |

Expected XOR bytes are lower for Fuse, but measured CPU time does not fall by the same factor. Fuse XORs roughly twice as many short 96-byte rows as cuckoo does 384-byte bucket rows; row-selection and loop overhead consume part of the memory-bandwidth saving. The Fuse-3/Fuse-4 order reversal and three-server p95s show visible co-location/frequency noise, so the repeated p50 ranges and deterministic byte counts are more useful than a single run.

Builds succeeded on their first deterministic seed for all three layouts. Fuse's additional build time is offline and acceptable for sealed immutable windows. Its peak tracked memory is not greater than cuckoo because materializing the final table, rather than peeling, is the peak at this page size.

## Practical selection

| Priority | Current choice | Reason |
|---|---|---|
| Minimum expected server bytes and response | Fuse-4 | 51.49% fewer expected XOR bytes than cuckoo, smallest table, 96-byte response/server |
| Minimum cold-phone upload | Packed cuckoo | Roughly half the upload; measured two-server wall penalty was only 0.30–1.92 ms |
| Fuse implementation | Fuse-4 only | Fuse-3 uses more storage/upload/build memory; its timing advantage was not stable across runs |
| More than three replicated servers | Either layout | Dense XOR sharing remains server-count neutral; all server answers are still required |

Both layouts need only constant-size public metadata (86 bytes for cuckoo and 97 bytes for Fuse in this report). Neither requires a client preload or per-tag map.

## RAID-PIR evaluation

[RAID-PIR](https://encrypto.de/papers/DHS14.pdf) is a separate database-distribution protocol, not another Fuse layout. For `k` servers and redundancy `r`, the paper stores and queries `r/k` of the database at each server and tolerates at most `r - 1` colluding servers.

Applied analytically to the 103.5 MiB Fuse-4 table:

| Servers `k` | Redundancy `r` | Max colluding | Table/server | Consequence |
|---:|---:|---:|---:|---|
| 3 | 2 | 1 | 69.0 MiB | Saves one third, but loses the desired privacy against two colluding servers |
| 3 | 3 | 2 | 103.5 MiB | Desired “one honest server” privacy, but no distribution saving |
| 4 | 2 | 1 | 51.75 MiB | Half-table servers with only one-colluder privacy |
| 4 | 3 | 2 | 77.63 MiB | First configuration that keeps two-colluder privacy and reduces each server's share |
| 5 | 3 | 2 | 62.10 MiB | More distribution for the same two-colluder threshold |

Therefore RAID-PIR is not worth implementing for the current two- or three-server “at least one behaves correctly” privacy target: that target requires `r = k`, which removes its per-server storage/work benefit. It becomes interesting only when there are more servers than the collusion threshold requires, such as `k = 4, r = 3`.

It is also not a drop-in striping mode for the current evaluator. Replicated Dense XOR assumes every server has identical rows; RAID-PIR changes query construction, row distribution, and answer reconstruction. The paper explicitly says its base constructions are not robust to server failures, although it discusses higher-layer recovery and accountability. It should remain a separate future experiment rather than complicating the first DefraDB integration.

## Implementation boundary

The POC changes only `tools/pir-poc`; no DefraDB storage or query code is touched. The common page encoder now feeds both layouts, the Dense share generator supports XOR selectors with any number of positions, and tests exercise exact Fuse recovery with 2, 3, and 5 servers.

Production follow-ups are deliberately separate:

- serialize, sign, and version the Fuse manifest and rows;
- authenticate the DefraDB cutoff used to build every immutable table;
- add the Fuse layout behind the existing sidecar/catalog interface;
- benchmark phone share generation and real cellular/Wi-Fi transport;
- pad continuation-page requests if result cardinality must remain private;
- replace or independently review this reference builder before treating it as production cryptography.

The geometry and sizing formulas follow the [Binary Fuse reference implementation](https://github.com/FastFilter/xor_singleheader) and its [paper](https://arxiv.org/abs/2201.01174). Using the peelable graph as an exact static function follows the same retrieval idea described by the [Fuse XORier Lookup Table](https://arxiv.org/abs/2312.13541) and used for key-value encoding by [ChalametPIR](https://github.com/itzmeanjan/ChalametPIR). The POC implementation is independent Rust code rather than a vendored library.

## Reproduce

```text
cargo run -p pir-poc --release -- bench-fuse quick
cargo run -p pir-poc --release -- bench-fuse full
```
