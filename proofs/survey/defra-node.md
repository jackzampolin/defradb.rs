# Survey: `crates/defra-node/`

## Purpose
Reusable **embedded DefraDB node builder**. Wraps the library crates (`db`, `query`,
`p2p`, `acp`, `events`, `defra-http`, `db-merge`, `storage`) behind a `NodeBuilder` /
`EmbeddedNode` API so downstream binaries embed a node without duplicating wiring.
Responsibilities: construct the store + DB + query executor, optionally start the HTTP
server and the Iroh P2P stack, and spawn background tasks. Plus crate-local search
helpers (`dense_search`, `coding_search`, `search_chunks`) that just shape GraphQL
requests, ACP config plumbing (`node_acp`), and a large benchmark harness
(`benchmark_*`, ~4.2k lines) that is test/bench-only.

## State machines
- **P2P lifecycle** (`P2PLifecycle` / `P2PLifecycleInner::shutdown`): orchestrated
  start/abort ordering of transport, coordinator, and five background tasks. This is
  task-teardown ordering, not a protocol state machine.
- **Iroh retry loop** (`spawn_iroh_retry_loop`): polls `storage::stores::RetryInfo`
  backoff and calls `db::merge::retry_doc_via_transport`, flipping
  `p2p::ReplicatorStatus`. The replicator backfill/live/resume/backoff state machine
  itself lives in `p2p`/`storage`, not here.
- **Document ACP selection** (`node_acp.rs`): a two-arm match (Local vs SourceHub);
  no transitions.
All emergent protocol behavior (replication, convergence, ACP gating, KMS, integrity)
is delegated to other crates and modeled by their own slices.

## Candidates

| Name | Kind | Property | Already-modeled | Priority |
|------|------|----------|-----------------|----------|
| (none — builder/wiring) | none | — | — | — |

The behaviors one might be tempted to model here are all owned and already covered
elsewhere: replicator lifecycle/backoff (existing **replicator** slice), filtered
replication & convergence (**B3** / **convergence**), ACP gating on commits
(**commits** / **acp**), KMS, integrity. `defra-node` only instantiates and sequences
these; it adds no new invariant.

## Verdict
**Plumbing — not model-worthy.** This is the embedded-node assembly crate: builder
wiring, task lifecycle/shutdown ordering, search-request shaping, and a benchmark
harness. The retry loop and P2P lifecycle are glue over already-modeled components;
integration tests (`tests/`, `p2p_tests.rs`, the `--test p2p`/`backup`/`acp` suites)
cover the wiring. No TLA+ or Lean candidates.
