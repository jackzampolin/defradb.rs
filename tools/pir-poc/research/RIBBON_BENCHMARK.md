# Standard Ribbon retrieval benchmark

This benchmark evaluates Standard Ribbon as a physical static-function layout
over the POC's existing replicated Dense XOR protocol. It is not a new PIR
security construction: the client derives a multi-hot selector from the public
key and immutable manifest, and Dense XOR hides that selector from any `n - 1`
of `n` servers.

The implementation follows Algorithms 1--3 in
[Ribbon: Fast Succinct Static Retrieval and Approximate Membership](https://doi.org/10.1145/3785417):
incremental elimination of contiguous Boolean equations, deterministic
hash-seed restart on contradiction, bottom-up back substitution, and query by
the original width-64 equation. It stores complete 96-byte pages rather than
membership bits. The embedded 128-bit fingerprint rejects absent keys after
private retrieval.

## Identical quick workload

- 1,048,576 documents and 262,144 distinct tags.
- Four 16-byte locators per tag.
- 262,144 populated 96-byte encoded pages (24 MiB payload).
- One strict-global first-page lookup with no public time/hash partition.
- Fresh Dense shares for every sample; two and three co-located servers, two
  persistent workers/server, seven measured samples after warmup.
- All layouts return the same page and use the same encoded corpus.

The local timings below are one release run on the current WSL runner. The
deterministic byte/storage results are the stronger comparison; a longer full
run and separate hosts are needed before treating millisecond differences as
stable.

## Layout and two-server result

| Layout | Table/server | Public metadata | Selector weight | Total upload | Total download | Aggregate expected XOR bytes | Wall p50 | Sum-server p50 | Layout build |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| PtrHash exact Dense | 24.00 MiB | 98,534 B | 1 | 64.00 KiB | 192 B | 24.00 MiB | 3.15 ms | 5.66 ms | 132.00 ms |
| Standard Ribbon, w=64, epsilon=10% | 26.67 MiB | 154 B | 28 for sampled key | 71.13 KiB | 192 B | 26.67 MiB | 3.76 ms | 6.67 ms | 159.84 ms |
| Fuse-4 | 26.44 MiB | 97 B | 4 | 70.50 KiB | 192 B | 26.44 MiB | 3.19 ms | 5.85 ms | 364.13 ms |
| Packed cuckoo | 26.67 MiB | 86 B | two unit selectors | 35.56 KiB | 1,536 B | 53.33 MiB | 3.59 ms | 6.47 ms | 119.23 ms |

Standard Ribbon's random width-64 equation has expected weight 32.5 because
the first bit is forced to one; the sampled key had weight 28. Hamming weight
does not change Dense selector length or the expected half-table scan of each
random server share.

Relative to the alternatives on deterministic server-work accounting:

- MPHF exact Dense uses 11.14% fewer aggregate expected XOR bytes and 11.14%
  less upload, at the cost of a 98.5 KiB key-dependent public index.
- Fuse-4 uses 0.89% fewer aggregate expected XOR bytes and upload, and only
  four public cells. Standard Ribbon built 56.1% faster in this run.
- Standard Ribbon uses essentially the same stored bytes as packed cuckoo but
  halves its aggregate expected XOR bytes and response, while doubling upload.

## Three-server result

| Layout | Total upload | Total download | Aggregate expected XOR bytes | Wall p50 | Sum-server p50 |
|---|---:|---:|---:|---:|---:|
| PtrHash exact Dense | 96.00 KiB | 288 B | 36.00 MiB | 3.11 ms | 8.20 ms |
| Standard Ribbon | 106.69 KiB | 288 B | 40.01 MiB | 4.22 ms | 11.00 ms |
| Fuse-4 | 105.75 KiB | 288 B | 39.66 MiB | 3.37 ms | 9.06 ms |
| Packed cuckoo | 53.34 KiB | 2,304 B | 80.00 MiB | 4.09 ms | 11.13 ms |

Adding the third server raises aggregate work for every replicated layout. It
only strengthens privacy from one tolerated colluding server to two.

## Build and client properties

Standard Ribbon succeeded on its first deterministic seed. Its generation
digest commits to all dimensions, seed, and ordered solution rows; reversing
record input gives identical rows and generation. Tracked peak build memory was
77,222,712 bytes. This is explicit algorithm-owned memory, not process RSS.

The client needs no preload or per-key map: it hashes `tag || page` using the
manifest's seed and dimensions, creates one selector share per server, and
checks the privately recovered fingerprint. Its persistent layout state is the
154-byte generation manifest. The two-server upload was 72,834 bytes, safely
inside the POC's phone-friendly traffic class.

## BuRR feasibility record

[BuRR](https://github.com/lorenzhs/BuRR) is not Standard Ribbon with failed
equations ignored. It overloads the first layer, bumps deterministically
described chunks into recursive backyard layers, and routes each query using
public bump metadata. The official Apache-2.0 C++ implementation is a valuable
reference, but it exposes a value-returning `QueryRetrieval` API around
compile-time result widths; this POC needs the exact routed layer and equation
cells to construct a private Dense selector for 96-byte pages.

Therefore BuRR is recorded but not given a fabricated local timing. A faithful
experiment needs a reviewed adapter that:

1. exports the exact bump route and selected equation cells;
2. supports fixed-size page stripes and validates their XOR against the
   official `QueryRetrieval` result;
3. serializes/authenticates every layer's seed, dimensions, and bump metadata;
4. concatenates layers into one Dense address space so lookup remains one PIR
   evaluation; and
5. reports the key-dependent routing artifact separately from Standard
   Ribbon's constant metadata leakage profile.

The paper and official implementation report sub-1% retrieval overhead for
practical configurations, so BuRR could approach MPHF's server table size
without MPHF metadata. Whether its bump metadata is smaller and less revealing
than PtrHash, and whether its wider equation makes a material client cost,
remain measurements—not assumptions.

## Decision

Standard Ribbon is correct, compact, stateless, fits the preliminary
desktop-derived phone resource envelope (real-device performance is unverified), and is generic for
any replicated Dense server count. It does not advance to the current strict
cold default: MPHF exact Dense minimizes total server work when its public
index leakage is acceptable, while Fuse-4 has slightly lower strict
constant-metadata server work and a much smaller selector. Standard Ribbon is
retained as the simpler/faster-building constant-metadata fallback and as the
validated base needed for a faithful BuRR adapter.

Run the benchmark with:

```text
cargo run -p pir-poc --release -- bench-ribbon quick
```
