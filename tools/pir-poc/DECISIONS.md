# PIR decisions

**First ask whether a public time, block or collection filter can exclude data
that is irrelevant to the requested answer.** The same use case can fall into
either category: wallet catch-up can skip processed blocks; full recovery cannot.

Dense scan work is linear. Splitting the same full search into smaller requests
does **not** by itself reduce total work. Filters save work only when they let us
skip data; smaller requests otherwise just bound memory, latency and payloads.
Compare **complete-query server work**: for all-match queries, decoys must
return all matches for all 100 candidates, not just their first pages.
Many matches per candidate can make PIR relatively cheaper; unique-key
lookups keep the decoy workload small as the dataset grows.

Low/high size refers to the data the request must search, not total chain size.
The low-size column states the initial scope; larger scopes are not proven
failure thresholds. Timings apply only to the stated benchmark/model scope.

## 1. Requests that can use a public filter

Use Dense for bounded reads and packed-presence Dense for epoch alerts.
The client must know and accept the public scope, and it must contain the
complete requested answer.

| Use case | Public filter | Low-size recommendation / scope | High-size recommendation | What was compared? | Server-time ratio vs 100 decoys | What makes PIR competitive? |
|---|---|---|---|---|---|---|
| **Mizu routing-tag retrieval** | New or alert-named blocks | Dense; up to 320K pages / 32 blocks | Dense for bounded windows; otherwise unvalidated | One page, not all tag matches | **Complete query unknown.** Page estimate: ~52× `P` | Many matches per candidate prefix, with complete-group retrieval avoiding repeated full scans |
| **Shinzo historical logs** | Requested block/date range | Dense; up to 320K pages / 32 blocks | Dense for bounded windows; otherwise unvalidated | One page, not all log matches | **Complete query unknown.** Page estimate: ~38× `P` | Frequent candidate address/topics increase decoy output; efficient continuation retrieval is required |
| **Shinzo receipt** | Known inclusion block | Dense; up to 10K receipts/block | Dense for bounded blocks; larger blocks unvalidated | One receipt from a known block | **~0.43× projected** `P` | Knowing the block keeps the searched table small; no large-group advantage |
| **DefraDB document by ID** | Known collection/tenant/version | Dense; up to 1M rows, 256-byte projection | Dense for bounded partitions; otherwise unvalidated | One fixed document projection | **~61× projected** `P` | A small known partition; unique keys otherwise keep decoy output small |
| **DefraDB secondary index** | Requested range/partition | Dense; up to 1M four-value pages | Dense for bounded partitions; otherwise unvalidated | One page, not all index matches | **Complete query unknown.** Page estimate: ~118× `P` | A large matching fraction with efficient complete-result retrieval |
| **Mizu / Shinzo / DefraDB alerts** | New block/epoch only | Packed Dense; 65,536 buckets, batched subscribers | Same for longer history; higher subscriber capacity unvalidated | One presence result/subscriber/epoch; kernel only | **~0.88× measured** `M`; excludes transfers and hit retrieval | Tiny bitmap, large ready subscriber batch and GPU-resident selectors |

**Page figures are not complete-tag costs.** The three multi-match rows above
compare one private page with one page per candidate. Complete-query relative
costs remain unmeasured for those workloads; the expectations are conditional,
not established rankings against receipt retrieval.

A request-size cap is not an optimal window or a total-work saving. If the
required range spans several requests, charge **all** of them. Resume from the
last processed block rather than repeatedly querying overlapping history.

Epoch alerts suit ongoing monitoring when a block delay is acceptable. They
are not a replacement for historical reads or current-root witnesses. Frequent
hits still require payload retrieval; few ready subscribers lose GPU batching
efficiency. Alert costs must include registration, polling and hit retrieval.

## 2. Requests that cannot discard data using such a filter

Do not invent a time window that changes the answer. "No validated recommendation"
means the current evidence does not support a deployment choice. Decoys remain
a last-resort, weaker-privacy fallback, only if their full response fits budget.

| Use case | Why no time filter | Low-size recommendation / scope | High-size recommendation | What was compared? | Server-time ratio vs 100 decoys | What makes PIR competitive? |
|---|---|---|---|---|---|---|
| **Mizu active nullifier witness** | Old state contributes to current-root proofs | Dense; occasional proofs, ~1M populated leaves | Above ~1M: no validated recommendation | One witness at ~1M populated leaves | **~244× measured** `M` | Smaller active state; each candidate returns one witness, not a large matching group |
| **Global receipt/document lookup** | Target partition unknown | Dense; 256-row fixture validated | No validated recommendation | Only small fixtures; no larger application comparison | **Unknown at scale** | A small corpus; without a known partition, decoys retain their indexed-read advantage as it grows |
| **Global secondary index** | All matches required | Dense; bounded four-value pages (1M-page model) | 1B documents, 0.01% matches: no validated recommendation | All matches at 0.01% selectivity over logical 1B documents | **~97× logical-workload result** `L` | Higher match fractions increase complete decoy output too; tested candidates cover only 1% of the corpus |

## How to interpret the comparison

- Ratios use **aggregate server evaluation time**, not measured energy or
  CPU-seconds: 1× is equal elapsed time, below 1× is less, above 1× is more.
  Absolute server/client timings and percentages remain in BENCHMARKS.
- `P`: GPU projection versus the CPU decoy fixture control, **not a matched
  at-scale benchmark**. `M`: measured; the epoch row compares GPU kernels with
  a CPU control and excludes selector transfers. `L`: logical large-workload
  execution, not a fully resident billion-document deployment.
- Limits are initial targets/evidence boundaries. Public filters reveal scope;
  decoys additionally reveal candidates. Neither may be introduced silently.
- The receipt's projected win is not verified at the modeled scale; its
  smaller known-block table explains the estimate, not a different protocol.
  Packed-alert timings require a ready batch and GPU-resident selectors.

Exact workloads, client costs, traffic and formula:
[BENCHMARKS.md](BENCHMARKS.md#snapshot-costs).
Complete-result work model: [relative work](BENCHMARKS.md#complete-query-work-relative-to-decoys).

## Delivery

Snapshot and immediate-DPF demos run today. Packed epochs are benchmarked but
not yet served. Immediate DPF is reserved for a genuine sub-epoch requirement.
Integration remains an authorized export/event sidecar.

[Use-case descriptions](USE_CASES.md) · [Protocol mechanisms](PROTOCOLS.md) ·
[Integration contract](PRODUCTION.md) · [Remaining work](ROADMAP.md) ·
[Origin and timing privacy](PRIVACY.md)
