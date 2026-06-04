# Survey: `crates/embedded/`

## Purpose
Node-assembly / wiring layer. `build_with_store` composes an `EmbeddedNode` from
components owned by other crates: transport (libp2p / iroh), `SyncCoordinator`,
merge handler, document-ACP + NAC, KMS transport, SE query transport, and the
replicator-retry loops. The crate is the embedded-node equivalent of the CLI's
`P2PSetup` — it owns no protocol, only the graph of `Arc<dyn Trait>` objects and
the spawn/shutdown lifecycle.

## State machines
- **Node lifecycle / shutdown ordering** (`node.rs` `ShutdownHandle`): coordinator
  shutdown -> abort background tasks -> transport shutdown -> clear identity store.
  Plain teardown; no concurrency invariant beyond ordering.
- **Replicator retry pass** (`node_tasks.rs` `run_{libp2p,iroh}_retry_pass`): per-peer
  `Active`/`Inactive` status + `RetryInfo` backoff `bump`/`is_due`, driven by a 2s
  ticker and an on-demand FFI trigger. This is orchestration of state owned by
  `defra-p2p-adapter` (`set_persisted_replicator_status`) and `storage` (`RetryInfo`).
- **Event fan-out** (`spawn_*_event_handler`): routes `TransportEvent`s, special-casing
  SE artifact push / SE query request/reply, else dispatches to the coordinator under a
  32-permit semaphore with inline-ordering for ordered events. Pure dispatch.
- **Recovery** (`node_recovery.rs`): on boot, re-subscribe restored replicators/docs
  from the peerstore. Idempotent replay of persisted state.

## Candidates
| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| Replicator retry/resume | TLA+ | dropped doc eventually re-pushed on reconnect; no permanent loss | yes — `Replicator.tla` (`INV_NoLoss`, resumable) + `Convergence` restart/resume | n/a |
| SE-artifact + retry convergence | TLA+ | re-push of doc block + SE artifact converges visible set | yes — convergence / commits / claim slices | n/a |
| Retry-status soundness | TLA+ | `Active`/`Inactive` tracks actual push success | covered indirectly by Replicator lifecycle; logic lives in adapter | low |

## Verdict
**Plumbing — not model-worthy.** Every behavior with proof-grade depth here
(replication delivery, convergence/resume, KMS distribution, ACP-on-commits dual
path, management auth, block integrity, CRDT merge laws) is delegated to crates
already covered by existing TLA+/Lean slices (Replicator, Convergence, Kms, Commits,
Auth, Integrity, Acp, CRDT-laws). The crate's own contribution is `Arc`-graph wiring,
teardown ordering, and idempotent recovery replay — covered by integration tests
(`tests/shutdown.rs`, `iroh_smoke.rs`, `se_owner.rs`, `bm25_smoke.rs`). No new model.
