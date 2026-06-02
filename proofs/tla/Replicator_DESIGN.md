# Replicator Lifecycle - TLA+ Design

Date: 2026-06-02. Branch: `feat/p2p-tla-replicator`.

## Brainstorm Outcome

The requested `superpowers:brainstorming` skill is not installed in this Codex
session's available skill list, and a local skill lookup did not find a usable
`brainstorming/SKILL.md`. I captured the design pass here instead.

The model should not re-prove CRDT convergence or the generic DAG-fetch substrate.
Those are covered by `DagReplication.tla`, `Convergence.tla`, and the Lean CRDT
proofs. This slice models the directional push lifecycle that sits above them:
registration creates a target-specific obligation, backfill enumerates existing
documents, live writes enqueue new document head histories, ordered PushLog sends
may stop mid-document on transport failure, and reconnect either does or does not
recompute the target gap.

The key abstraction choice is doc-grain scheduling with block-grain receipt:
`queue` and `inflight` track documents, while `received` and `merged` track the
target's durable blocks. A document is complete only when every block in its
transitive head history has parent-guarded merged at the target.

## Source-Grounded Facts

| Fact | Source |
|---|---|
| `ReplicatorInfo` is the persisted peer/collection configuration; `ReplicatorStatus` is active/inactive. | `crates/p2p/src/replicator.rs:35`, `crates/p2p/src/replicator.rs:129` |
| Existing-doc replay waits for the target connection before it begins. | `crates/db-merge/src/push_docs_transport.rs:49` |
| Backfill enumerates local document ids from the datastore, then loads the latest composite heads. | `crates/db-merge/src/push_docs_transport.rs:109`, `crates/db-merge/src/push_docs_transport.rs:157` |
| Backfill expands each head into an ordered transitive DAG block list. | `crates/db-merge/src/push_docs_transport.rs:167`, `crates/db-merge/src/push_docs_common.rs:8` |
| Existing-doc replay sends blocks sequentially per document and stops that document on peer rejection or connection-like send failure. | `crates/db-merge/src/push_docs_transport.rs:199`, `crates/db-merge/src/push_docs_transport.rs:222` |
| Retry recomputes a single document's latest heads and DAG blocks from storage rather than trusting a stale in-memory queue. | `crates/db-merge/src/push_docs_transport.rs:374`, `crates/db-merge/src/push_docs_transport.rs:416` |
| Live writes push the full document DAG to every matching replicator. | `crates/p2p/src/sync/coordinator/broadcast.rs:149`, `crates/p2p/src/sync/coordinator/broadcast.rs:179` |
| Ordered live pushes stop on timeout or connection-like transport error and report a per-doc failure for retry. | `crates/p2p/src/sync/coordinator/broadcast.rs:436`, `crates/p2p/src/sync/coordinator/broadcast.rs:453`, `crates/p2p/src/sync/coordinator/broadcast.rs:238` |

## TLA+ Abstraction

| Symbol | Meaning |
|---|---|
| `phase` | Lifecycle state: `Disconnected`, `Connecting`, `Backfill`, `Live`, or `Backoff`. |
| `connected` | Current target reachability. The adversary may disconnect/reconnect; liveness is conditional on eventual connectivity. |
| `knownDocs` | Documents the source currently has an obligation to deliver to the target. |
| `liveCreated` | Documents created after the replicator reaches `Live`. |
| `queue` | Documents waiting for an outbound push. |
| `inflight` | Documents whose ordered PushLog block send is active. |
| `received` | Blocks durably received by the target. |
| `merged` | Blocks parent-guarded merged by the target. |
| `Mode` | `"Naive"` drops in-flight docs and only performs the first backfill pass; `"Resumable"` recomputes `MissingDocs` on every reconnect/backfill pass. |

`RequiredBlocks(d)` is the transitive history of all configured heads for a
document. `TargetCompleteFor(d)` holds when that whole history has merged at the
target. Re-delivery is intentionally harmless: a block already in `received` or
`merged` is simply not received or merged again.

## Properties

| Property | Meaning | Expected verdict |
|---|---|---|
| `INV_BackfillComplete` | If connectivity eventually stays up, every pre-existing `InitialDocs` document eventually remains complete at the target. | GREEN in `MC_Replicator_Resumable_Green` |
| `INV_LiveDelivery` | If connectivity eventually stays up, every document created after `Live` eventually remains complete at the target. | GREEN in `MC_Replicator_Resumable_Green` |
| `INV_NoLoss` | If connectivity eventually stays up, every known document, including one dropped mid-push, eventually remains complete at the target. | RED in `MC_Replicator_Naive_Red`, GREEN in `MC_Replicator_Resumable_Green` |
| `INV_TargetMergeClosed` | Target merge never outruns durable receipt or parent availability. | GREEN safety invariant in both runs |

## Run Commands

Run from `proofs/tla`:

```bash
# RED: disconnect during the first backfill can strand docA forever.
./tools/tlc -metadir states/replicator_naive_red -config MC_Replicator_Naive_Red.cfg MC_Replicator_Naive_Red.tla

# GREEN: reconnect recomputes MissingDocs, so backfill, live delivery, and no-loss hold.
./tools/tlc -metadir states/replicator_resumable_green -config MC_Replicator_Resumable_Green.cfg MC_Replicator_Resumable_Green.tla
```

These commands are intentionally not wired into `run-all.sh`; `prompt.md` says the
integrator will add harness and README entries after this conflict-free slice lands.
