# Private-query POC: private snapshots + Compact DPF subscriptions

This POC demonstrates private point and tag lookup over a deterministic DefraDB export, plus private live-match notifications, while keeping the DefraDB integration surface small. Snapshot experiments cover stateless, server-count-neutral Dense XOR; stateless, two-server finite-differences PIR; and stateful, two-server SinglePass PIR. Two Compact DPF servers evaluate live DefraDB update events without learning the subscribed bucket.

This is a benchmarkable protocol spike, not audited production cryptography.

For the evidence status and the requirements for a valid comparison across public lookup, 100 decoys, packed Dense, public windows, finite differences, SinglePass, and Chalamet, see [`COMPARISON.md`](COMPARISON.md).

## Production-shaped design

- The original format hashes a lookup key to one generously sized bucket. The cold-path experiment packs values into tag pages and uses two public cuckoo candidates, producing a roughly 90%-full table with only constant-size client metadata.
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

### Global and public-window endpoints

The sidecar exposes the two snapshot modes as separate endpoints over the same Dense XOR evaluator:

| Endpoint | Public to each server | Kept private | Server table scanned |
|---|---|---|---|
| `POST /v1/query/global` | global snapshot ID and batch size | tag/bucket | the global immutable snapshot |
| `POST /v1/query/windows` | selected coarse window IDs, each snapshot ID, and batch sizes | tag/bucket within every window | only the selected immutable window snapshots |

`GET /v1/catalog` advertises the global manifest and the available public window manifests. The client requires every PIR replica to return an identical catalog before generating shares. Window queries are batched in one request, but every window has its own manifest and can therefore use a smaller independently sized table. The original `GET /v1/manifest` and `POST /v1/query` remain global-only compatibility aliases.

`bench-endpoints` is a functional and scaling smoke test, not decision data. Its fixed-capacity global and window tables contain only one populated record, so its timings and apparent crossover must not be compared with the populated packed tag-page, decoy, or SinglePass experiments. A production crossover remains unknown until all paths query identical populated global/window tables and return identical padded results.

`serve` accepts either an original single snapshot or this catalog directory layout:

```text
CATALOG/
  global/
    manifest.json
    rows.bin
  windows/
    2026-W31/
      manifest.json
      rows.bin
    2026-W32/
      manifest.json
      rows.bin
```

Each directory is built with the existing `build` command: use the full export for `global` and a prefiltered export for each coarse window. The POC treats window IDs as opaque public identifiers; fixed UTC weeks are the recommended initial policy. It does not trust a client-supplied path, allows only bounded filesystem-safe IDs, verifies every snapshot content hash, enforces one catalog across replicas, rejects duplicate/unknown windows, and applies the service batch limit across the entire multi-window request.

## Cold tag lookup

[`tag_pages.rs`](src/tag_pages.rs) implements packed tag-page Dense XOR. "Packed" describes the table layout, not different PIR cryptography. A sealed snapshot groups all values for a tag into fixed-size pages of compact locators, stores four pages in each row, and places every page in one of two cuckoo candidate buckets at roughly 90% slot occupancy. A cold client needs only the small public layout description, computes both candidate buckets, privately retrieves both through ordinary Dense XOR, and accepts the page with the matching 128-bit fingerprint. No per-tag client map is required.

Two candidates double the number of PIR operations, but remove the much larger costs in the original `build_paged` format: sizing from document count rather than page count, repeating the page key for every value, eight reserved value slots in every bucket, and empty one-hash capacity. Tags exceeding one page use deterministic continuation pages. Querying the observed number of pages can leak result cardinality through request count unless the client pads it.

[`finite_differences.rs`](src/finite_differences.rs) implements the actual two-server information-theoretic construction from [Henzinger and Ragavan, EUROCRYPT 2026](https://eprint.iacr.org/2025/2008), following their [reference implementation](https://github.com/ahenzinger/finite-diffs-pir). Servers encode the packed bucket rows as a low-degree polynomial truth table. A cold client sends one 64-bit share per candidate to each server and holds no mutable state. Each server reads a sublinear translated cloud; the client XOR-reconstructs both candidate rows. The POC includes real preprocessing, querying, and recovery rather than only an analytical model.

`bench-cold` remains a correctness and protocol-scaling experiment. It uses one synthetic global tag-page snapshot and page-zero lookups; it does not give every alternative the same public time window, realistic cardinality distribution, or transport. Its historical timings are deliberately omitted from this overview and must not be used to select between packed Dense, decoys, and SinglePass.

The decoy baseline has weaker privacy regardless of performance: the server learns all 100 tags, repeated candidate sets can be intersected, and different decoy sets sent to two servers reveal the real tag through their intersection. Sending the same set to multiple servers adds availability, not query privacy.

Snapshot routing is intentionally undecided until the unified benchmark described in [`COMPARISON.md`](COMPARISON.md) exists. Packed tag-page Dense is the current stateless implementation candidate, not a measured production winner. Finite differences remains an experiment.

The new cold paths intentionally remain an in-memory sidecar/benchmark and do not alter DefraDB internals. The catalog endpoints extend the HTTP API while retaining the original global manifest/query routes as compatibility aliases. `bench-cold` performs correctness-checked private recovery for both exact protocols. Production integration should serialize and sign the cuckoo manifest and packed rows, authenticate the snapshot cutoff, expose bounded two-candidate batch endpoints, stream finite-differences responses, and use an independently reviewed implementation before treating either construction as production cryptography.

## SinglePass snapshot retrieval

[`single_pass.rs`](src/single_pass.rs) implements the two-server client-preprocessing construction from [Single Pass Client-Preprocessing PIR](https://www.usenix.org/conference/usenixsecurity24/presentation/lazzaretti). Setup creates `Q` random permutations and `N/Q` parity hints in exactly one logical database pass. An online query sends `Q` 32-bit indices to each server; each server copies only those `Q` rows. The client reconstructs the target and applies the paper's show-and-shuffle update to its permutations and hints.

This is the low-server-work mode:

- Privacy requires the two roles not to collude. Unlike Dense XOR, this construction is exactly two-server and is not server-count-neutral.
- Server 0 can generate the initial state and transfer it to the phone. The POC measures generation but not state serialization or network transfer.
- Client state is mutable and permits one in-flight query. If a request may have reached a server but cannot be completed, stale state must not be rolled back and reused.
- `Q` trades phone storage for online work. Smaller `Q` reads and transfers fewer rows but stores more parity hints. The forward and inverse permutations always consume `8N` bytes in this implementation.
- The current implementation is an in-memory protocol/demo and benchmark. Durable atomic state, signed update replay, HTTP/TLS endpoints, malicious-server correctness, and recovery are production follow-ups.

`bench-singlepass` validates the expected mechanism and scaling on raw fixed-size synthetic rows. It has not run against the packed tag-page tables or the same public windows as the decoy path, and its state transfer is not measured. It therefore does not establish that SinglePass beats 100 decoys for a Shinzo warm query. Query-page multiplication still applies to high-cardinality tags, and an ordinary follow-up document fetch would reveal the access; return useful encrypted data in the private page or privately retrieve subsequent content.

## Live subscriptions

The `subscription-demo` command uses DefraDB's existing `EmbeddedNode::subscribe(EventName::Update)` API. It registers one persistent Compact DPF key with each of two servers, inserts a non-matching and a matching document, combines both servers' 16-byte result shares, and privately retrieves the matching payload through Dense XOR.

The division of responsibility is intentional:

1. A client hashes its target tag to a bucket and uploads one Compact DPF key to each of two non-colluding servers.
2. On every committed DefraDB update, each server maps the event tag to a bucket and point-evaluates every subscription assigned to it.
3. Each client receives one 16-byte share from each server for each event. Neither share reveals whether it matched; only the client can combine them.
4. A match is a hint, not the document. The client uses Dense XOR or a synchronized SinglePass state against the identical data version to retrieve and verify the values.

Hash collisions can cause harmless false-positive notifications; the subsequent exact-key Dense XOR lookup rejects them. DefraDB's current event bus is live-only and bounded, so a dropped-event count must trigger snapshot resynchronization. The demo uses the known inserted tag to bridge an update event; a production sidecar must decode/project the indexed tag from the update block and publish snapshot cutoffs atomically.

Measured in-process in the release build:

| Buckets | Compact key/server | Point eval/server | 10,000 subscriptions/event/server | Dense 3-server key/server | Dense hot bit eval/server |
|---:|---:|---:|---:|---:|---:|
| 1,048,576 | 388 B | 0.48 us | 5.75–5.92 ms | 128 KiB | 1.04–1.05 ns |
| 4,194,304 | 422 B | 0.52–0.53 us | 5.67–6.02 ms | 512 KiB | 1.14–1.32 ns |
| 16,777,216 | 456 B | 0.57 us | 8.33–8.50 ms | 2 MiB | 1.48–1.52 ns |

Compact DPF scales logarithmically with the bucket domain but linearly with active subscriptions and event rate. At 4M buckets this host evaluates about 1.6–1.8 million subscription/event pairs per second on each server at 10,000-subscription fanout. A client key is only 422 bytes/server and client generation is about 1 us, making it much better suited to a phone than using Compact DPF to expand and scan an entire snapshot.

The same full run now compares Compact DPF with a production-shaped public decoy subscription index. Each decoy client registers 100 visible candidate buckets (one target and 99 decoys) with one server. The server builds an inverted index once, so an event performs one hash lookup; it does **not** test all 100 candidates for every client. Hit timings rotate over up to 65,536 indexed buckets and clone one internal subscriber handle. Miss timings rotate over absent buckets. At the 4M-bucket domain:

| Logical subscriptions | Compact DPF work/event, both servers | 100-candidate decoy hit | DPF / decoy server-work ratio | DPF encoded key state | Estimated decoy index | DPF output/event, both servers | Expected decoy notifications/uniform event |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 0.982 us | 16 ns | 61x | 844 B | 5.2 KiB | 32 B | 0.000024 |
| 1,000 | 0.936 ms | 32 ns | 29,243x | 824 KiB | 5.14 MiB | 31.25 KiB | 0.0238 |
| 10,000 | 11.69 ms | 106 ns | 110,282x | 8.05 MiB | 73.0 MiB | 312.5 KiB | 0.238 |

An indexed miss took 13–16 ns at 4M buckets. Decoy registration is also smaller on the wire: 400 bytes for 100 `u32` bucket candidates versus 844 bytes for the two DPF keys. The trade is server state—one million public index memberships for 10,000 decoy clients—and much weaker privacy. The server sees all candidates and exactly which candidate caused a notification; repeated sets, tag popularity, notification timing, and subsequent reads can identify the real interest. The uniform-event notification estimate is only a baseline: popular decoy tags can produce much more phone traffic.

For live subscriptions, 100 indexed candidates are therefore overwhelmingly cheaper in server CPU and event output when candidate-set privacy is acceptable. Compact DPF remains the exact-private choice: it hides the target bucket cryptographically, but pays a linear evaluation and result-share cost for every active subscriber on every event. The server-cost gap is large enough that this should be an explicit privacy tier rather than treating Compact DPF as the universal default.

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
| 1,048,576 | 0.00011 ms | 7.77 ms | 15.29 ms | 8.95 ms | 26.14 ms |
| 4,194,304 | 0.00011 ms | 28.22 ms | 56.15 ms | 32.74 ms | 97.79 ms |

Three servers improve collusion tolerance, not speed or availability: infrastructure work increases with every server, and the baseline still needs every answer for reconstruction.

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
cargo run -p pir-poc --release -- singlepass-demo
cargo run -p pir-poc --release -- subscription-demo
cargo run -p pir-poc --release -- bench quick
cargo run -p pir-poc --release -- bench-opt quick
cargo run -p pir-poc --release -- bench-cold full
cargo run -p pir-poc --release -- bench-endpoints full
cargo run -p pir-poc --release -- bench-singlepass full
cargo run -p pir-poc --release -- bench-subscriptions full
cargo run -p pir-poc --release -- build INPUT.json SNAPSHOT_DIR COLLECTION KEY_FIELD VALUE_FIELD
cargo run -p pir-poc --release -- serve SNAPSHOT_OR_CATALOG_DIR 127.0.0.1:8080
cargo run -p pir-poc --release -- query TAG http://server-a:8080 http://server-b:8080
cargo run -p pir-poc --release -- query-window TAG 2026-W31,2026-W32 http://server-a:8080 http://server-b:8080
```

For production, keep replicas under separate operators/failure domains, authenticate data versions, require identical manifest IDs, use TLS, pin resource limits to the host, and benchmark separate machines over the intended mobile networks. Dense XOR privacy fails if every server colludes, and correctness currently requires every server response. SinglePass privacy fails if its two roles collude; its client state additionally needs authenticated, atomic persistence and ordered update replay before production use.

The Compact DPF implementation uses [`fss-rs`](https://github.com/myl7/fss), a small research library rather than audited production cryptography. The construction follows the two-party DPF line introduced by [Boyle, Gilboa, and Ishai](https://www.iacr.org/archive/eurocrypt2014/84410245/84410245.pdf). Multi-party alternatives exist in the literature, including [three-server information-theoretic DPF](https://drops.dagstuhl.de/entities/document/10.4230/LIPIcs.ITC.2022.17) and [general n-server IT-DPF](https://eprint.iacr.org/2023/625), but require a separate implementation, security review, and benchmark before adoption.
