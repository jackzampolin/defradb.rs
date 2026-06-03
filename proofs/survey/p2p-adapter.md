# Survey: `crates/p2p-adapter` (defra-p2p-adapter)

## Purpose
HTTP-facing adapter layer. Implements the `defra_http::router::P2POperations` trait
(get/connect peers, add/remove replicators, collections, documents, republish, sync)
by delegating to the `p2p`, `db`, `db-merge`, and `blockstore` crates. One impl per
transport: `P2PAdapter` (libp2p) and `IrohP2PAdapter` (iroh), plus `VersionSyncer` /
`DocPusher` trait shims and replicator-status persistence helpers.

## Responsibilities
- Translate HTTP P2P requests into `p2p` handle calls + DB persistence.
- Persist/load replicator + p2p-document/collection lists via `storage::Peerstore`.
- Sign DocSync requests, push existing docs, sync lens versions / branchable commits.
- `ReplicatorPushOptionsState`: an `Arc<RwLock<>>` snapshot of SE keys.

## State machines
- **No adapter-local protocol state machine.** All real protocols live downstream:
  replicator status transitions in `p2p::ReplicatorInfo::set_status_if_changed_now`
  (modeled by Replicator slice); DAG fetch / convergence in `p2p::sync` + `db-merge`
  (Convergence / DagReplication); ACP gating in `acp`/`db` (Acp / Commits); signing in
  `p2p::signing` (Auth / Integrity).
- The only emergent logic is the `sync_documents` completion loop: poll the event bus
  for `MergeComplete`, count against `peers × docs`, bounded by a 30s overall + 3s idle
  timeout over <=3 attempts. This is **advisory** (returns `Ok` on timeout); it does not
  define convergence, which the Convergence/DagReplication slices already prove.

## Candidates

| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| (none) | — | — | — | — |

Everything correctness-critical is delegated and already covered by an existing slice:
Replicator (status lifecycle), Convergence + DagReplication/MC_S* (filtered replication,
eventual merge), Auth + Integrity (request signing/verification), Acp + Commits (access
gating). The adapter adds no new invariant.

## Verdict
**Plumbing — not model-worthy.** Pure HTTP↔p2p/db glue: argument validation, persistence
delegation, trait dispatch, and one best-effort advisory sync loop. Integration tests
(`--test p2p`, `--test p2p_iroh`) exercise the wiring; the protocols it drives are proven
in the existing TLA+/Lean slices. No candidates.
