# Survey: `crates/db-search/`

## Purpose
Vector + hybrid retrieval for DefraDB. Four small files:
- `config.rs` — `EmbeddingClientConfig` (url/model/api_key builder).
- `embedding.rs` — `set_embedding` (populate doc vector fields on create/update) and
  `embed_text` (query-time embedding); both call an OpenAI-compatible `/embeddings`
  HTTP endpoint via `reqwest`.
- `dense_search.rs` — `hybrid_search_dense`: validates a request, renders two GraphQL
  queries (BM25-ordered, dense-ordered), executes them, and fuses the two rankings
  with reciprocal rank fusion (RRF).
- `lib.rs` — re-exports.

## State machines
None. No status/lifecycle enums, no concurrency, no replication, no persisted
protocol state. `RankedOrder` is a 2-variant tag, not a transition system. Control
flow is request -> embed -> query -> fuse -> response (linear, single-shot).

## Modelable candidates

| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| RRF fusion total-order | Lean | `fuse_rankings_rrf` comparator is a strict weak ordering (irreflexive, antisymmetric, transitive) so the sort is deterministic and the f64/NaN fallback cannot panic or reorder ties | no | low |
| RRF determinism | Lean | same (bm25, dense) candidate lists -> identical fused top-k regardless of HashMap iteration order | no | low |

Both are pure-function properties on a v1 ranking heuristic. They are nice-to-have
determinism checks, not system-correctness invariants; the comparator already chains
to a `doc_id` total-order tie-break, so the risk is low and integration/unit tests
cover observable behavior. Everything else — HTTP embedding IO, GraphQL string
rendering, JSON parsing, config fallback resolution, escaping — is plumbing covered
by unit tests in `embedding.rs` and integration FTS tests.

## Verdict
Plumbing crate. **Not model-worthy.** No TLA+ surface (no concurrency/distribution/
security state machine). The only Lean-shaped targets (RRF order/determinism) are
low-priority hygiene properties on a heuristic ranker, not load-bearing laws —
recommend deferring unless a fusion-ranking bug surfaces.
