# PIR use-case gallery

This document turns the selected PIR primitives into small application-shaped
POCs. It is a catalog, not a claim that nine separate protocols should be
maintained. Every snapshot fixture exercises the same `PrivateTable` and
replicated Dense XOR implementation so correctness is comparable, even where
the production recommendation is 100 visible decoys. The served live fixtures
use the same immediate two-party Compact-DPF implementation; the measured
production direction batches alerts into packed-presence Dense epochs. The
authoritative decision ladder and scale conditions are in
[`USE_CASES.md`](USE_CASES.md).

The separate [encrypted-search POC](ENCRYPTED_SEARCH.md) evaluates a faster
one-lookup tier with explicitly weaker search/access-pattern privacy.

Run every case or one product group:

```bash
cargo run -p pir-poc --release -- use-cases
cargo run -p pir-poc --release -- use-cases mizu
cargo run -p pir-poc --release -- use-cases shinzo
cargo run -p pir-poc --release -- use-cases defra
```

The JSON result includes the source/query shape, deliberately public metadata,
fixed wire sizes, server-count flexibility, correctness checks, production
projection and primary limitation. It benchmarks strict PIR and 100 visible
decoys on the same rows. Timings are medians of 31 in-process release-mode
operations and exclude HTTP, OHTTP, queues and artifact building.

## Implemented POCs

| Product | Use case | Why it is useful | POC shape | Selected protocol |
|---|---|---|---|---|
| Mizu / Shieldd | Wallet note recovery | A remote wallet retrieves only encrypted actions matching its proof-bound routing prefix instead of downloading every compact block | Public generation/window + private routing prefix -> four-slot encrypted-action page | Dense XOR, 2+ replicas |
| Mizu / Shieldd | Nullifier non-membership witness | A wallet obtains the path needed to prove its note remains unspent without identifying the nullifier to a provider | 32-byte nullifier -> fixed 2,008-byte witness | 100 plausible decoy path/index reads by default; Dense high-privacy tier |
| Mizu / Shieldd | Routing-tag alert | A wallet learns that an encrypted action for its routing tag appeared without registering the tag in plaintext | Routing-prefix event bucket -> private match/miss hint | Packed-presence Dense per public epoch; immediate Compact DPF fallback |
| Shinzo | Historical contract logs | A researcher or wallet hides which contract and event signature it is investigating | Public block window + private address/topic0 -> four-slot log page | Dense XOR, 2+ replicas |
| Shinzo | Private transaction receipt | A wallet retrieves public transaction/receipt/attestation data without disclosing which transaction matters to it | Transaction hash -> fixed receipt and provenance projection | 100 same-block decoys; one-block Dense high-privacy tier |
| Shinzo | Contract event alert | A wallet privately follows an address or topic in the live DefraDB log stream | Canonical address/topic bucket -> private match/miss hint | Packed-presence Dense per block/epoch; immediate Compact DPF fallback |
| DefraDB | Private document by ID | An application retrieves an authorized projection while hiding a high-entropy document ID | Collection generation + document ID -> fixed encrypted projection | Dense for bounded collection/generation; decoys when no bounded partition exists |
| DefraDB | Private secondary-index page | Equality queries can return many documents without exposing the indexed value | Collection/field/value/page -> four fixed result slots | Dense XOR, 2+ replicas |
| DefraDB | Private change feed | An application follows equality-filtered updates without sending the filter value to the provider | Collection/field/value event bucket -> private hint | Packed-presence Dense per public epoch; immediate Compact DPF fallback |

The fixtures contain 256 rows. They prove query generation, independent server
evaluation, answer combination, fixed-row decoding, absent-key rejection and
Compact-DPF match/miss behavior. They are intentionally too small to establish
production throughput; the scale benchmarks remain in `USE_CASES.md`.

## PIR versus 100 visible decoys

For snapshot cases, the strict result is aggregate work across two Dense-XOR
replicas. The decoy result is one server reading 100 keys from the identical
`PrivateTable`, with the real key at a known client position. It returns 100
fixed rows; the client decodes only its real row and ignores the other 99.
Decoy upload contains the actual variable-length fixture keys, matching the
default endpoint rather than assuming pre-shared ordinals.

| Use case | PIR server | Decoy server | PIR server delta | PIR client | Decoy client | Upload PIR / decoy | Download PIR / decoy |
|---|---:|---:|---:|---:|---:|---:|---:|
| Mizu wallet note recovery | 5.9 us | 28.4 us | 79% faster | 0.8 us | 4.2 us | 64 B / 3,103 B | 1,608 B / 80,400 B |
| Mizu nullifier witness | 13.1 us | 29.1 us | 55% faster | 1.0 us | 3.7 us | 64 B / 4,190 B | 4,064 B / 203,200 B |
| Shinzo historical logs | 4.3 us | 26.4 us | 84% faster | 1.1 us | 4.2 us | 64 B / 3,614 B | 1,096 B / 54,800 B |
| Shinzo transaction receipt | 2.3 us | 25.7 us | 91% faster | 0.7 us | 3.9 us | 64 B / 3,794 B | 368 B / 18,400 B |
| DefraDB document by ID | 2.9 us | 26.5 us | 89% faster | 0.8 us | 4.2 us | 64 B / 3,299 B | 560 B / 28,000 B |
| DefraDB secondary-index page | 4.2 us | 26.6 us | 84% faster | 0.8 us | 3.9 us | 64 B / 3,898 B | 1,096 B / 54,800 B |

This snapshot result is specific to the deliberately tiny 256-row tables. At
that size, scanning random Dense shares is cheaper than 100 directory searches,
allocations and row copies. Dense work grows with the table; indexed decoy work
remains approximately 100 point reads. The production-scale benchmarks in
`USE_CASES.md` therefore still show decoys winning raw server work for large
tables. The stable gallery result is the 50x decoy download: two answer shares
versus 100 complete rows.

Exact 1K/1M/1B protocol geometry and the executed 1K/1M encrypted-index results
are in [ENCRYPTED_SEARCH.md](ENCRYPTED_SEARCH.md). The gallery JSON also emits
the scale geometry under `scale_comparison`.

For live cases, Compact DPF evaluates one subscription on two servers. The
decoy baseline publicly registers 100 distinct `u32` buckets and uses a
one-server inverted index, not a linear scan. Per-event bytes below exclude the
same event ID/envelope both protocols would carry.

| Use case | DPF server/event | Decoy server/event | DPF slowdown | DPF client/event | Decoy client/event | One-time registration DPF / decoy | Response DPF / decoy |
|---|---:|---:|---:|---:|---:|---:|---:|
| Mizu routing-tag alert | 0.779 us | 0.0046 us | 169x | 0.0087 us | 0.0003 us | 640 B / 400 B | 32 B / 1 B |
| Shinzo contract alert | 0.782 us | 0.0040 us | 195x | 0.0088 us | 0.0003 us | 640 B / 400 B | 32 B / 1 B |
| DefraDB private change feed | 0.779 us | 0.0054 us | 144x | 0.0090 us | 0.0003 us | 640 B / 400 B | 32 B / 1 B |

That table is the currently implemented immediate per-event endpoint. The new
fixed-epoch research adapter changes the preferred live design. Events OR their
buckets into an 8 KiB public presence bitmap; at epoch close each registered
subscriber retrieves one private presence bit and privately fetches the padded
snapshot page only on a hit. At batch 512 on the RTX 2070 SUPER:

| Epoch protocol | Aggregate server/subscriber | One-time registration | Response/epoch | Servers |
|---|---:|---:|---:|---|
| Packed-presence Dense | **0.182 us** | 16,384 B | 2 B | 2, 3, or more |
| GPU DPF over 16-byte rows | 32.589 us | 4,160 B | 32 B | exactly 2 |
| 100 visible buckets | 0.206 us | 400 B | 1,600 B | 1 visible server |

The strict and visible elapsed numbers use different processors and do not
establish an intrinsic speedup, but they remove server latency as the reason to
prefer decoys for epoch-capable alerts. Packed Dense costs 8 KiB of retained
selector state/subscriber/server. If selectors do not remain GPU-resident, the
measured host-to-device transfer adds about 4.78 us/subscriber at batch 512.

For genuinely immediate delivery, indexed decoys remain decisively cheaper
than Compact DPF by revealing all 100 watched buckets, the exact event bucket,
popularity and longitudinal intersections. Compact DPF hides the target but
performs a cryptographic point evaluation for every active subscription on
both parties. The 0.78-us one-subscription cost is small in isolation, but total
server work grows with both event and subscription count.

Strict and decoy ratios are descriptive, not privacy-equivalent. A decoy
provider can intersect repeated candidate sets and may identify the real target
even when every individual request contains 100 plausible values.

Representative stable wire geometry from the strict gallery path:

| Shape | Aggregate upload | Aggregate response | Client metadata |
|---|---:|---:|---:|
| 256-row Dense query, two replicas | 64 B | Depends on fixed row: 368 B to 4,064 B | about 37.2 KB |
| 65,536-bucket packed-presence registration, two replicas | 16,384 B once | 2 B/epoch before framing | selector seed plus epoch cursor |
| Compact-DPF registration, two parties | 640 B once | 32 B per event | no populated-key directory |

## What should become production first

1. **Mizu wallet note recovery** is the strongest new product case. Shieldd
   already defines routing records and encrypted action payloads. The exporter
   can create immutable pages per public generation or block window, and the
   wallet trial-decrypts recovered values. A full routing-prefix domain is worth
   evaluating because the current compact ordinal directory permits dictionary
   recovery of populated low-bit prefixes.
2. **Shinzo historical logs** complements the live adapter already implemented.
   Public block windows materially bound server work while keeping the address
   and topic private. Deterministic continuation pages are needed for popular
   contracts.
3. **DefraDB private exact/index projections** should remain an optional serving
   index. An exporter reads committed state through ordinary collection/index
   APIs and builds immutable, authorization-equivalent artifacts. PIR must not
   bypass ACP or combine data visible to different reader classes.

Live hints are not replacements for snapshot retrieval. A match tells the
client to fetch and verify a fixed projection. Prefer packed presence at a
public block/epoch cadence; retain immediate DPF only where that delay is
unacceptable. Both need expiry, admission, durable cursors, batching and
fixed-cadence padded delivery.

## Other useful candidates

These do not need new cryptography and can be added as new projections after a
real product need is confirmed:

- Mizu issuer audit routing: asset/authority selector -> encrypted compliance
  records, with authorization-specific artifacts.
- Mizu current user/asset status witness: private address/asset pair -> fixed
  compliance-tree proof.
- Shinzo address transaction history: address + public block window -> padded
  transaction pages.
- Shinzo attestation lookup: block number/hash -> block signature and CID proof
  material.
- DefraDB private relation traversal: parent document ID + relation + page ->
  fixed child-ID/projection page.
- DefraDB coarse range query: public time partition + private equality prefix ->
  fixed result pages. Fully private arbitrary ranges need a different layout
  and should not be implied by the equality POC.

Counts, sums, arbitrary GraphQL filters, joins and writes are not represented as
PIR queries here. They either reveal result-dependent work, require specialized
private-computation protocols, or solve a different privacy problem. The simple
production boundary remains:

```text
committed and authorized DefraDB state
  -> deterministic immutable projection or event bucket
  -> isolated PIR sidecar
  -> client verification/decryption
```
