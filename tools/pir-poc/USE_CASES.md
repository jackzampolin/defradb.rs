# Selected PIR use cases

This is the authoritative POC decision record. Historical protocol exploration
is archived in `COMPARISON.md`, `EXPLORATION.md`, and `research/`.

The primary objective is minimum aggregate server work for a complete useful
result. Client CPU, upload, download, storage, build/update work, privacy and
availability remain separate metrics.

## Runtime architecture

The normal binary contains five commands and three use-case paths. It remains a
DefraDB/Shieldd sidecar: an exporter supplies committed generation records, the
POC builds immutable tables, and no DefraDB query planner or storage engine is
modified.

Strict and 100-decoy modes share one table:

```text
authenticated generation
        |
        +-- active nullifier witness table
        |       +-- Dense XOR selector shares -> one fixed witness
        |       `-- 100 visible ordinals -> 100 witnesses, process one
        |
        +-- encrypted tag projection
        |       +-- Dense XOR selector shares -> one fixed result row
        |       `-- 100 visible ordinals -> 100 result rows, process one
        |
        `-- Shinzo Compact-DPF registrations -> fixed event shares
```

The client ordinal format is a canonical, safe digest-to-ordinal directory. It
is larger than PtrHash metadata and leaks populated key digests to dictionary
attacks, but the default serving path performs no unsafe deserialization and is
stable across compiler versions. PtrHash remains a research optimization.

Every replica advertises the same operator-MACed manifest containing generation
height/root, table and directory digests, fixed result shapes and admission
limits. The client rejects divergent replicas before generating a selector.

Origin privacy is a separate layer. The POC can carry each replica share over
RFC 9458 Oblivious HTTP using a different relay/gateway path:

```text
                         opaque OHTTP request        PIR share
wallet -- HTTPS --> relay A -----------------> gateway/replica A
       `- HTTPS --> relay B -----------------> gateway/replica B
```

Each relay sees a wallet address and ciphertext but not the PIR route, selector
share or response. Each gateway sees one PIR share and a relay address but not
the wallet. Using one relay for both replicas would give that relay a stable
cross-share correlation point, so the selected topology uses independently
operated paths.

## Selected protocols and unified benchmark

Command:

```bash
cargo run -p pir-poc --release -- benchmark full
```

Artifact: `target/pir-poc-results/selected-use-cases-full-final.json`.
The direct-binary run completed in 15.26 seconds with 603,688 KiB peak RSS,
zero swap and exit status zero.

| Use case | Private protocol | Private server time | Private client time | 100-decoy server time | 100-decoy client time | Private download | Decoy download |
|---|---|---:|---:|---:|---:|---:|---:|
| Active nullifier -> 2,008 B Merkle witness | Two-server live radix + Dense path retrieval | 35.76 ms | 24.58 ms | 0.232 ms | 0.00014 ms | 71.6 KB | 200.8 KB |
| Tag over 1B documents, 0.01% match -> 100K encrypted results | Two-server exact-MPHF striped Dense XOR | 13,299.94 ms | 44.63 ms | 69.60 ms | 38.19 ms | 38.9 MB | 1.94 GB |
| Shinzo live wallet subscription | Two-server Compact DPF | 0.000732 ms | 0.000040 ms | 0.000021 ms | approximately 0 ms[^timer] | 252 B | 204 B |

[^timer]: Below the benchmark timer's useful resolution.

Private server time is aggregate elapsed work across both replicas. These are
in-process measurements and exclude HTTP, TLS, queues and network latency.

Strict/decoy speed ratios are not security-equivalent comparisons. Decoys leak
the candidate set, result cardinality, popularity and longitudinal
intersections.

### OHTTP transport benchmark

The unified benchmark also exercises the real RFC 9458 HPKE and Binary HTTP
implementation at representative per-replica payload sizes. These measurements
isolate origin-hiding cryptography and framing; they exclude PIR evaluation,
TCP, TLS, relay latency and queues. A release-mode quick run on the development
machine produced:

| Per-replica payload | Padding | Request wire | Response wire | Client codec + crypto p50 | Gateway codec + crypto p50 |
|---|---|---:|---:|---:|---:|
| Compact-DPF representative: 320 B request, 126 B response | None | 489 B | 195 B | 0.133 ms | 0.092 ms |
| Compact-DPF representative | Power of two | 567 B | 288 B | 0.120 ms | 0.089 ms |
| Compact-DPF representative | Fixed 1 KiB/1 KiB | 1,079 B | 1,056 B | 0.127 ms | 0.101 ms |
| Active-nullifier share: 541,241 B request, 35,816 B response | None | 541,412 B | 35,887 B | 0.423 ms | 0.357 ms |
| Active-nullifier share | Power of two | 1,048,631 B | 65,568 B | 0.813 ms | 0.849 ms |
| 1B-tag share: 1,250 B request, 19,428,008 B response | None | 1,419 B | 19,428,079 B | 49.79 ms | 77.99 ms |
| 1B-tag share | Power of two | 2,103 B | 33,554,464 B | 76.43 ms | 117.62 ms |

Unpadded OHTTP adds approximately 55 request bytes and 32 response bytes beyond
Binary HTTP. The active-nullifier transport row uses the full-scale 541 KB
selector share: two gateways add about 0.71 ms aggregate, roughly 2% of the
full-run 35.76 ms PIR work; total client codec/crypto adds about 0.85 ms to the
24.58 ms PIR client work. For the 1B-tag case, two gateways add about 156 ms,
roughly 1.2% of the full-run 13.30-second PIR traversal. Client codec/crypto
adds about 100 ms of aggregate CPU to the roughly 45 ms PIR combine/decrypt
work because it must authenticate two 19.4 MB encrypted OHTTP answers.
Compact DPF is the opposite: OHTTP dwarfs its microsecond evaluator, but still
costs only about a tenth of a millisecond per party before network latency.

Power-of-two padding is not the default recommendation for large result rows:
it inflated the 19.4 MB answer to 33.6 MB and increased gateway crypto time by
about 51%. Production should use route-specific fixed public result classes
close to the authenticated manifest sizes. A fixed class makes valid success
and application-error ciphertexts the same length; no padding and power-of-two
classes leak their respective size class.

## Protocol overview

### 1. Active-nullifier private retrieval

The active Shieldd generation is represented as one immutable base plus small,
authenticated delta levels. A private lookup has two fixed-schedule stages:

1. privately retrieve the linked predecessor leaf from a radix layout;
2. privately retrieve the sibling rows at every level of the quaternary Merkle
   path.

For every selector, the client generates random XOR shares whose XOR is the
real selector. Each replica sees only its random share, scans its local copy and
returns an answer share. XORing every answer reconstructs the fixed 2,008-byte
witness. The generation height and root are public, but the nullifier has
information-theoretic target privacy while at least one required replica does
not collude with the others.

### 2. Exact-MPHF striped Dense XOR

A public exact minimal-perfect hash function maps every populated tag to a
compact ordinal, avoiding an overprovisioned hash table. The tag's results are
stored in fixed continuation stripes. In the one-billion-document benchmark,
the target has 100,000 matches, each containing five encrypted fields in one
188-byte value, spread over 391 stripes of 256 values.

The client generates XOR shares of one Dense selector and reuses that selector
across the fixed stripe schedule. Each replica traverses the encrypted
projection and returns one answer share per stripe; the client XORs the shares
and authenticates/decrypts the 100,000 target ciphertexts. This gives
information-theoretic tag privacy under the same non-collusion assumption. The
result-size class and populated-key metadata are public in this POC.

The strict query scans the projection, but downloads only target results. The
100-decoy alternative performs cheap ordinary lookups but returns ten million
ciphertexts. The client authenticates/decrypts its 100,000 target values and
discards the other 9.9 million without AEAD work.

### 3. Compact-DPF live subscription

At registration, the client converts its target wallet bucket into two compact
function-share keys and sends one key to each replica. A single key is
pseudorandom and does not reveal the target. For every new event, each replica
evaluates its key at the event bucket and returns a fixed-size output share.
The client combines both shares: a matching event reconstructs the notification
and a miss reconstructs zero.

Unlike snapshot Dense PIR, Compact DPF does not scan historical document rows.
Its event cost is proportional to the subscriptions being evaluated. Privacy
is computational under the DPF construction and AES PRG, and the selected
implementation is exactly two-party.

## Adding a third server

| Private process | Can add a third server? | Effect |
|---|---|---|
| Active nullifier radix/path Dense XOR | Yes, without changing the table or cryptographic construction | Generate three XOR shares and combine three answers. Aggregate upload, response traffic and server work rise by about 50% relative to two servers. |
| Exact-MPHF striped Dense XOR | Yes, without changing MPHF, stripes or rows | The same `n`-server sharing works for any `n >= 2`; a three-server deployment again costs approximately 50% more aggregate work than two servers. |
| Compact DPF subscription | No, not as a drop-in change | The current DPF construction and wire format produce exactly two keys and require exactly two answers. Three-party support needs a multi-party FSS/DPF construction or a different live protocol. |

The Dense extension is flexible about server count, but it is `n`-of-`n`: all
configured answers are needed to reconstruct the result. A third server
strengthens the non-collusion assumption only if the deployment actually
provides another independent trust domain; it does not automatically improve
availability. Threshold reconstruction such as two-of-three would be a
separate construction, not merely another XOR share. The benchmark numbers in
this document use two replicas.

## 1. Active Shieldd nullifier generation

Shieldd uses a depth-20 quaternary indexed nullifier tree with sequential leaf
positions. A non-membership witness contains a linked predecessor leaf and 60
sibling hashes, encoded as one fixed 2,008-byte result.

The full benchmark starts with 1,048,576 ordinary nullifiers and applies one
maximum 32,768-nullifier block. The rejected flat sidecar rewrites every one of
4,096 radix rows plus changed node rows.

| Active block update | Flat padded layout | Implemented immutable delta |
|---|---:|---:|
| Build/update time | 194.19 ms | 78.56 ms p50 |
| Payload written/replica | 143,667,040 B | 11,865,610 B |
| Amplification over raw 32-byte inserts | 137.01x | 11.32x |
| Relative result | baseline | 12.11x fewer bytes; 2.47x faster construction |

The implemented layout has:

- one immutable base;
- up to eight geometric immutable delta levels;
- newest-wins linked-leaf mutations;
- copy-on-write node-coordinate overrides;
- a fixed nine-level predecessor schedule;
- a fixed `9 * 60` node-probe schedule, including empty levels;
- off-path construction followed by one generation-pinned `Arc` publication;
- height/root/body-digest binding and operator MAC;
- image corruption and stale-publication rejection.

Overflow merges geometrically and eventually creates a new immutable base.
Readers that pinned the old generation continue to see it until completion.

The endpoint serves an exact populated nullifier-to-witness projection for the
published current generation. The delta engine and benchmark cover live block
mutation/publication; direct authenticated block ingestion is intentionally not
an unauthenticated public HTTP endpoint.

Strict query versus decoys:

- Strict Dense baseline: 35.76 ms aggregate server, 24.58 ms client, 1,082,482 B
  upload and 71,632 B download.
- Decoys: 0.232 ms server and 200,800 B download.
- Strict used 154.38x more server elapsed and saved 2.80x download.
- The decoy client parses only the target witness and ignores the other 99.

The remaining nullifier query optimization is a tree-specific path PIR. The
ordinary Dense baseline sends many level-specific selectors; this is now a
clearly isolated optimization rather than another storage redesign.

## 2. Encrypted tag projection at one billion documents

The benchmark has 10,000 equal-cardinality tags and 100,000 matches for the
target (`0.01%`). Each result carries five encrypted 32-byte fields as a
188-byte AEAD-shaped value. One selector is reused over 391 fixed continuation
stripe planes.

The logical immutable projection is 194.28 GB per replica. The benchmark
executes every logical scan over one resident 496.88 MB representative plane;
it preserves XOR count and wire geometry without claiming deployed 194 GB
memory behavior.

- Strict Dense: 13.30 seconds server, 38.86 MB response.
- 100 decoys: 69.60 ms server, 1.943 GB response.
- Strict used 191.09x more server elapsed.
- Decoys downloaded 50x more data.
- Both decrypt only the target's 100,000 ciphertexts. The decoy client ignores
  9.9 million non-target ciphertexts without AEAD.

No Dense micro-optimization removes the full encrypted-projection traversal.
For this workload the useful policy is explicit:

- public immutable time/collection windows when their leakage is acceptable;
- 100 decoys when server work is the primary constraint;
- strict Dense when candidate-set leakage is unacceptable or 1.943 GB download
  is unreasonable.

Ciphertext projection data is effectively incompressible. Three replicas add
approximately 50% aggregate Dense work and are an availability/trust choice,
not a speed optimization.

## 3. Shinzo wallet-event subscription

The selected live protocol is two-party Compact DPF over a 65,536-bucket public
domain. Registration uploads 640 bytes total. Each event returns fixed match
shares plus a fixed encrypted capsule, regardless of match or miss.

The full run verified both a target event and a non-target event:

- Compact DPF aggregate server p50: 0.000732 ms.
- Client combine p50: 0.000040 ms.
- Fixed response: 252 bytes.
- Indexed 100-wallet candidate lookup: 0.000021 ms and 204 bytes.

The decoy index is faster but reveals all registered wallets and longitudinal
activity. Compact DPF's absolute work is already below expected transport,
event decoding and delivery overhead, so further cryptographic optimization is
not a POC priority.

Compact DPF is computationally private under its construction and AES PRG; it
is not information-theoretic and this implementation is exactly two-party.

## HTTP surface

The demo and `serve` command expose:

```text
GET  /v1/manifest
POST /v1/nullifier/private
POST /v1/nullifier/decoy
POST /v1/tag/private
POST /v1/tag/decoy
POST /v1/shinzo/register
POST /v1/shinzo/event
```

Private endpoints accept only one replica's opaque selector shares. Decoy
endpoints accept visible candidates. Shinzo registration accepts one Compact-DPF
key share for that party.

## OHTTP origin-hiding POC

The implementation uses X25519/HKDF-SHA256/AES-128-GCM HPKE and Known-Length
Binary HTTP. It includes:

- operator-MACed public gateway key documents bound to the PIR generation;
- current/previous receive-key rotation;
- a bounded replay cache checked before state-changing dispatch;
- a fixed-destination relay that strips client headers and records only
  aggregate request/byte counters;
- fixed gateway authority, method/path allow-list and header allow-list;
- bounded zero padding with none, power-of-two and fixed strategies;
- the same selected-service dispatcher as direct HTTP;
- two-path Dense XOR and Compact-DPF clients.
- a minimal anonymous-HTTP transport seam with direct and Tor-compatible
  `socks5h` backends.

Tests cover all three private protocols, ciphertext tampering, replay, key
rotation/expiry, malformed and oversized padding, authenticated metadata, and
equal fixed-size success/error responses. `pir-poc demo` exercises the complete
client -> relay -> gateway -> selected-service flow. It also reports cold setup
plus first-query/p50/p95 verified latency over 11 queries for one visible direct
lookup, direct Dense XOR and Dense XOR over OHTTP. Three environment variables
add a real Tor + OHTTP row: `PIR_POC_TOR_SOCKS_URL`,
`PIR_POC_TOR_RELAY_URLS`, and `PIR_POC_OHTTP_RELAY_BINDS`. When Tor is not
configured, the row is explicitly marked not run.

### Real Tor/onion benchmark

This run used the signed Tor Expert Bundle 15.0.20 (Tor 0.4.9.11), Windows 11,
an AMD Ryzen 7 3700X and Rust 1.94.0 on 2026-08-25. Two v3 onion services mapped
to the POC's two local OHTTP relays. The client used `socks5h` and separate
SOCKS-auth isolation tokens for the replica paths. Every reported query
reconstructed and verified the requested nullifier witness.

| Path | Setup (ms) | First query (ms) | p50 (ms) | p95 (ms) | Encrypted upload/query | Encrypted download/query |
|---|---:|---:|---:|---:|---:|---:|
| Visible direct HTTP | 5.35 | 6.49 | 4.96 | 6.49 | not instrumented | not instrumented |
| Two-server PIR, direct HTTP | 8.78 | 5.21 | 5.09 | 5.21 | not instrumented | not instrumented |
| Two-server PIR, OHTTP loopback | 8.26 | 5.73 | 5.90 | 15.14 | 8,302 B | 131,136 B |
| Two-server PIR, Tor + OHTTP, cold circuits | 5,330.55 | 1,160.26 | 1,258.54 | 1,374.52 | 8,302 B | 131,136 B |
| Two-server PIR, Tor + OHTTP, warm[^tor-warm] | 2,026.60 | 863.92 | 882.98 | 1,186.26 | 8,302 B | 131,136 B |

[^tor-warm]: Median of three run-level results; every run contains 11 verified queries. Individual warm p50 values were 867.56, 882.98 and 896.43 ms.

Fresh Tor bootstrap took about 8.1 seconds; restart with cached directory state
took about 3.9 seconds. During three warm runs Tor used 125.64 MiB working set,
96.53 MiB private memory, and 2.406 CPU-seconds over 38.884 wall-seconds (about
0.062 average CPU core). These are desktop measurements, not phone/battery
results.

The OHTTP byte counts are exact relay payload counters summed over both
replicas; they exclude Tor cell framing, circuit handshakes and TCP/TLS
overhead. Fixed envelopes make the OHTTP and Tor rows equal at the application
layer. The two onion services and both POC servers ran on this machine, so this
is a real Tor rendezvous-path measurement but not evidence of administrative
non-collusion or geographically remote provider latency.

The conclusion is narrow: Tor works without changing PIR/OHTTP and does not add
PIR server evaluation, but warm verified latency was about 150x loopback OHTTP
in this run. Keep OHTTP as the low-latency default and Tor/onion as an explicit
strong-origin mode until native mobile Arti is measured on a phone.

The OHTTP layer hides origin from a non-colluding relay/gateway pair; it does
not change PIR server work or its non-collusion requirement. It also does not
defeat a colluding relay and gateway, a global timing observer, a compromised
client, or result-size correlation outside the selected padding class. The POC
uses whole-message buffers. This is acceptable for the 36 KB nullifier response
and possible but memory-relevant for a 19.4 MB tag share; production should
measure peak mobile memory before enabling that result class.

## Production boundary

The POC now enforces:

- immutable versioned generation directories;
- fsync before publication and no overwrite of an existing generation;
- operator-MACed manifests obtained through an out-of-band 32-byte key;
- safe, size-bounded ordinal metadata;
- query, response, key, batch, table, metadata, transient-memory, in-flight and
  subscription limits;
- fixed result schedules and generation IDs on every request/response;
- duplicate Compact-DPF subscription rejection;
- client agreement across replicas;
- absent-key row fingerprint rejection.
- canonical Shieldd indexed-nullifier witness verification using the 20-level,
  4-ary Poseidon construction;
- AES-256-GCM projection authentication bound to generation, tag and slot;
- a direct/Tor origin-transport boundary that leaves PIR and OHTTP framing
  unchanged.
- fixed local relay binds plus independently addressed Tor relay URLs for real
  onion-service testing;
- per-replica SOCKS-auth circuit isolation and first/p50/p95/byte reporting.

Before production, replace the symmetric operator MAC with the deployment's
signature/key-distribution system, obtain the Shieldd root through consensus or
a light client rather than the serving replicas, mmap large tables, move the
canonical witness verifier into a shared Shieldd SDK crate, protect projection
keys in the wallet key hierarchy, and add operational metrics and authenticated
block-ingestion authorization.
For OHTTP specifically, terminate HTTPS on both hops, deploy relays and
gateways in independent trust/administrative domains, authenticate key
documents with the production signature chain, add abuse controls that do not
introduce stable wallet identifiers, define a rotation overlap in time rather
than only key count, and evaluate batching/jitter if spend-timing correlation
is in scope.
