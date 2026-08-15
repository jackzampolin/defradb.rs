# Private-query POC: Dense XOR snapshots + Compact DPF subscriptions

This POC demonstrates private point and tag lookup over an immutable DefraDB export, plus private live-match notifications, while keeping the DefraDB integration surface small. DefraDB builds a deterministic snapshot; otherwise-independent replicas answer XOR query shares over that exact snapshot. Two Compact DPF servers evaluate live DefraDB update events without learning the subscribed bucket. A positive live notification triggers Dense XOR retrieval from a newly sealed snapshot.

This is a benchmarkable protocol spike, not audited production cryptography.

## Production-shaped design

- A lookup key is hashed to a fixed bucket. Tags with many values use deterministic lookup pages, so every page is another PIR query.
- The client creates `n` XOR shares of a unit vector. Any `n - 1` shares are uniformly random and reveal no bucket.
- Every server holds the same sealed snapshot and returns the XOR of the selected rows.
- The client needs all `n` answers. Three servers improve collusion resistance from one to two servers; they do **not** provide one-server failure tolerance.
- Server count is not hard-coded. The demo targets two and three servers, while share generation and combination accept any `n >= 2`.
- The HTTP service has a bounded request semaphore, batch limit, body limit, blocking isolation, and a configurable worker count. Two evaluator workers are the measured default for multi-million-row snapshots.

The data sizes are predictable:

| Buckets | 64-byte snapshot/replica | Query share/server | Total upload, 2 servers | Total upload, 3 servers |
|---:|---:|---:|---:|---:|
| 1,048,576 | 64 MiB | 128 KiB | 256 KiB | 384 KiB |
| 4,194,304 | 256 MiB | 512 KiB | 1 MiB | 1.5 MiB |

This is feasible for a phone because the client does not hold or scan the database. At four million buckets it generates and uploads 512 KiB per server and combines 64-byte answers. Network upload—not client RAM or answer processing—is the mobile-side cost to watch. Tags spanning many pages multiply that upload, so page sizing must be tuned from real Shinzo tag cardinalities.

## Live subscriptions

The `subscription-demo` command uses DefraDB's existing `EmbeddedNode::subscribe(EventName::Update)` API. It registers one persistent Compact DPF key with each of two servers, inserts a non-matching and a matching document, combines both servers' 16-byte result shares, and privately retrieves the matching payload through Dense XOR.

The division of responsibility is intentional:

1. A client hashes its target tag to a bucket and uploads one Compact DPF key to each of two non-colluding servers.
2. On every committed DefraDB update, each server maps the event tag to a bucket and point-evaluates every subscription assigned to it.
3. Each client receives one 16-byte share from each server for each event. Neither share reveals whether it matched; only the client can combine them.
4. A match is a hint, not the document. The client uses the existing Dense XOR path against an identical sealed snapshot to retrieve and verify the values.

Hash collisions can cause harmless false-positive notifications; the subsequent exact-key Dense XOR lookup rejects them. DefraDB's current event bus is live-only and bounded, so a dropped-event count must trigger snapshot resynchronization. The demo uses the known inserted tag to bridge an update event; a production sidecar must decode/project the indexed tag from the update block and publish snapshot cutoffs atomically.

Measured in-process in the release build:

| Buckets | Compact key/server | Point eval/server | 10,000 subscriptions/event/server | Dense 3-server key/server | Dense hot bit eval/server |
|---:|---:|---:|---:|---:|---:|
| 1,048,576 | 388 B | 0.48 us | 5.29–5.78 ms | 128 KiB | 1.01 ns |
| 4,194,304 | 422 B | 0.50–0.51 us | 5.69–6.01 ms | 512 KiB | 1.29–1.38 ns |
| 16,777,216 | 456 B | 0.55–0.56 us | 5.97–6.37 ms | 2 MiB | 1.51–1.54 ns |

Compact DPF scales logarithmically with the bucket domain but linearly with active subscriptions and event rate. At 4M buckets this host evaluates about 1.8–1.9 million subscription/event pairs per second on each server. A client key is only 422 bytes/server and client generation is about 1 us, making it much better suited to a phone than using Compact DPF to expand and scan an entire snapshot.

Dense subscription shares make the opposite trade: extremely cheap indexed event evaluation for very large persistent keys. At 4M buckets a three-server registration uploads 1.5 MiB once and takes 0.46 ms to generate on this desktop, but 10,000 subscriptions would occupy about 4.9 GiB **per server**. The reported 1.3 ns bit read is a hot-key lower bound, not a 5 GiB fanout benchmark. The compact representation needs only about 4 MiB/server for those same 10,000 subscriptions, so Compact DPF is the safer default when subscriber count is large; dense subscriptions remain useful for small populations or a deliberate memory-for-CPU tier.

### Two versus three servers for live matching

| Construction | Implemented | Privacy | Key/server at 4M | Event work | Important limitation |
|---|---|---|---:|---|---|
| 2-party Compact DPF | yes | private while the two servers do not collude | 422 B | O(log N) per subscription | both result shares required |
| 3-server dense XOR | yes | private if at least one of three servers is honest/non-colluding | 512 KiB | one indexed bit read | all three result shares required |
| True 3-party/threshold DPF | research only | can target stronger collusion/availability properties | construction-dependent | construction-dependent | different cryptography; no selected audited Rust implementation |

The code keeps registration/evaluation behind server-count-neutral concepts, but the selected Compact DPF primitive is exactly two-party. Three independent pairwise keys on AB, AC, and BC are **not** a safe shortcut: any colluding pair owns one complete DPF key pair and can reconstruct the subscribed point. Replicating one two-party share onto a third server also keeps one original party mandatory and weakens the collusion story. The measured dense three-server fallback is cryptographically simple and fast per event, but moves 512 KiB/server at 4M buckets for each subscription and is n-out-of-n for correctness.

## Measured comparison

Release profile, 64-byte rows, persistent two-worker pools per server. The two and three server cases below are co-located and contend for one memory bus; network, HTTP, and TLS are excluded. Separate production hosts should be benchmarked before using these latency values for capacity planning.

| Buckets | Public server | 2-server wall | 2-server summed work | 3-server wall | 3-server summed work |
|---:|---:|---:|---:|---:|---:|
| 1,048,576 | 0.00011 ms | 7.68 ms | 15.13 ms | 8.11 ms | 23.56 ms |
| 4,194,304 | 0.00011 ms | 34.05 ms | 67.85 ms | 29.96 ms | 89.44 ms |

The 4M three-server wall result being below the two-server result is benchmark noise and scheduling on a shared host, not a protocol speedup. Infrastructure work increases with every server.

At 1M buckets, batching amortizes scheduling but each logical Dense XOR query still scans its own share:

| Topology | Batch | Wall | Logical queries/s | Client share generation/item |
|---|---:|---:|---:|---:|
| 2 servers | 1 | 4.15 ms | 241 | 64 us |
| 2 servers | 8 | 28.94 ms | 276 | 64 us |
| 2 servers | 32 | 115.45 ms | 277 | 66 us |
| 3 servers | 1 | 4.14 ms | 241 | 112 us |
| 3 servers | 8 | 28.84 ms | 277 | 111 us |
| 3 servers | 32 | 106.26 ms | 301 | 115 us |

## Optimization result

The selected kernel walks only the set bits in a server-visible random share and XORs rows directly from the snapshot. Skipping zeros is safe for bucket privacy because an individual share—and any set of at most `n - 1` shares—is uniformly random independently of the target.

Focused 4M-bucket measurements on one server:

| Kernel | p50 | Relative to original masked scan | Extra persistent storage |
|---|---:|---:|---:|
| Original masked scan | 31.08 ms | 1.00x | none |
| Set-bit byte loop | 16.25 ms | 1.91x faster | none |
| Set-bit, 2 workers | 12.94 ms | 2.40x faster | none |
| Set-bit, 4 workers | 13.51 ms | 2.30x faster | none |
| Set-bit, 8 workers | 13.27 ms | 2.34x faster | none |

Two workers win at 4M because additional workers saturate memory bandwidth. The service therefore owns a persistent, explicitly sized pool instead of spawning threads per request or consuming every core.

### Experiments not selected

- **Persistent XOR-combination indexes:** tested with 2-4 row groups. At 1M buckets they used 2x-4x snapshot storage and took 12.1-14.7 ms versus 4.44 ms for the zero-bit scan. A 4M/group-4 index requires 1 GiB per replica. Fewer nominal reads lost to random access, cache misses, and prefetch failure.
- **Explicit AVX2:** 16.86 ms at 4M versus 16.25 ms for the portable byte loop. LLVM already vectorizes this small XOR well; the serving path stays portable.
- **On-the-fly Four Russians:** useful for a large batch on one thread, but the bounded pool of independent cache-friendly scans was faster in the measured 8- and 32-query batches. It remains in the research benchmark, not automatic serving behavior.
- **Compact DPF for snapshot retrieval:** retained for live point evaluation, but not selected for snapshot scans. Expanding a compact key across every row makes server cost the bottleneck, and the selected construction is specifically two-party.
- **ChalametPIR 0.8:** attractive single-server response/server costs, but the tested client layout extrapolates to about 7.8 GiB of public-matrix memory at 1M records. It is not a phone path without a new streaming/mobile implementation.
- **Path ORAM and TEE + ORAM:** solve access-sequence privacy for mutable storage, with client state, reshuffling, attestation, hardware trust, and side-channel work that are outside this immutable point-query POC.

## Run it

```text
cargo run -p pir-poc --release -- demo
cargo run -p pir-poc --release -- subscription-demo
cargo run -p pir-poc --release -- bench quick
cargo run -p pir-poc --release -- bench-opt quick
cargo run -p pir-poc --release -- bench-subscriptions full
cargo run -p pir-poc --release -- build INPUT.json SNAPSHOT_DIR COLLECTION KEY_FIELD VALUE_FIELD
cargo run -p pir-poc --release -- serve SNAPSHOT_DIR 127.0.0.1:8080
cargo run -p pir-poc --release -- query TAG http://server-a:8080 http://server-b:8080
```

For production, keep replicas under separate operators/failure domains, authenticate snapshot publication, require identical manifest IDs, use TLS, pin resource limits to the host, and benchmark separate machines over the intended mobile networks. Dense XOR privacy fails if every server colludes, and correctness currently requires every server response.

The Compact DPF implementation uses [`fss-rs`](https://github.com/myl7/fss), a small research library rather than audited production cryptography. The construction follows the two-party DPF line introduced by [Boyle, Gilboa, and Ishai](https://www.iacr.org/archive/eurocrypt2014/84410245/84410245.pdf). Multi-party alternatives exist in the literature, including [three-server information-theoretic DPF](https://drops.dagstuhl.de/entities/document/10.4230/LIPIcs.ITC.2022.17) and [general n-server IT-DPF](https://eprint.iacr.org/2023/625), but require a separate implementation, security review, and benchmark before adoption.
