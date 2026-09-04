# PIR implementation roadmap

Implementation work supporting [DECISIONS.md](DECISIONS.md).
Integration contract: [PRODUCTION.md](PRODUCTION.md).

## Implementation work

| Priority | Work | What exists | Completion gate |
|---|---|---|---|
| 1 | Generic DefraDB export adapter | Authorized-query/event demos; bundled product fixtures | Deterministic export per query/reader class, pinned cutoff, no core query/storage changes |
| 2 | Stable binary artifacts | Authenticated manifests and immutable JSON-loaded tables | Bounded binary format, memory mapping, atomic publication and replica agreement |
| 3 | Packed-epoch alert service | Correctness-checked GPU benchmark; served live API still uses immediate DPF | CPU/GPU service integration, bounded registration memory, expiry, durable cursors and replay |
| 4 | Canonical Mizu witness integration | Verified witness endpoint plus separate active-tree update benchmark | Proof parity with Shieldd, current-root binding, predecessor/node retrieval and update tests |
| 5 | Production privacy/security | OHTTP/Tor demos, AEAD verification, admission limits | Independent operators, HTTPS, production signatures/root trust, anonymous admission and fixed traffic policy |

The served nullifier endpoint reads prebuilt witnesses; it does not execute the
radix/delta benchmark per request. A larger active tree is not production-ready
merely because the sparse coordinates or leaf indices are stable.

### Decisions the integration must enforce

| Concern | Required decision |
|---|---|
| Scope-matched comparisons | Benchmark identical corpora/filters and complete results: all matches for the real tag versus all matches for 100 candidates. Vary match fractions and skew; charge every continuation, padding and repeated scan. Report aggregate server ratios, not just page latency. |
| Growing active tree | Validate a root-correct bounded working set. A checkpoint/delta chain alone is not a size bound. Benchmark larger populations before extending the tested envelope. |
| Keyword metadata | The demo's public digest directory can reveal populated keys. Use an approved public directory or a non-enumerating layout; benchmark its actual padding/build cost. |
| Large result sets | Configure padded pages and request caps without dropping required results. Charge all continuation/window requests for full recovery; do not silently fall back to decoys. |
| Private two-stage retrieval | Prove that both stages hide the secret selection and reduce total work; returning a secret partition ID and fetching it publicly is not sufficient. |
| Immediate alerts | Keep DPF only where the latency requirement rules out epoch batching; test full event-times-subscriber load. |
| Durable live delivery | Fixed cadence, replay/gap policy and retention must survive restarts; hit-only follow-up traffic requires an explicit timing-leakage policy. |

## Research that could change the choice

Watchlist from the August 2026 research pass, not newly verified upstream
status. Re-evaluate only on identical result shape and phase accounting.

| Candidate advance | Evidence needed | Affected use cases |
|---|---|---|
| InsPIRe/VIA/OnionPIR improvements | Lower total server work or a worthwhile one-server trust/traffic tradeoff on the same corpus | Cold reads and single-operator deployments |
| Batched proof PIR, including Skirrt | A current-root witness with lower measured server/client/traffic costs | Active nullifier tree |
| Private sharding | Hide the selected partition without restoring a global scan | Unwindowed receipts, documents and logs |
| Immutable preprocessing | Lower lifetime work after charging preload, state and generation refresh | Repeated archival queries |
| Audited embedded Tor/Arti | Phone latency, memory and battery evidence | Origin hiding for all query types |

Sources retained from that research:
[Ethereum Reads PIR workstream](https://reads.ethereum.foundation/workstreams/pir/),
[May/June update](https://reads.ethereum.foundation/feed/update-may-june-2026/),
[Arti engineering report](https://reads.ethereum.foundation/feed/embedding-arti-in-the-browser/).
Local comparison methodology: [research/FULL_COMPARISON.md](research/FULL_COMPARISON.md).
