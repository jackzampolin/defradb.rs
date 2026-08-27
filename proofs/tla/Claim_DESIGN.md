# Multi-Instance Claim-Uniqueness - TLA+ Design

Date: 2026-06-02. Branch: `feat/p2p-tla-claim`.

This is the B3 follow-on model for same-DID `AgentRequest` claim races. It keeps
`proofs/tla/DagReplication.tla` read-only and models only the extra behavior needed
for CRDT-CAS claims, LWW convergence, and execution side effects.

## Grounding

`gents:` anchors are files in the consumer repository, `source-inc/gents`,
re-verified against its `main` branch on 2026-07-27. Bare `crates/...` paths are
this repository.

| Source | Fact used by the model |
|---|---|
| `gents:crates/gents/src/lifecycle/claim.rs:258` | Claim is an `update_AgentRequest` guarded by local `_docID`, `status = pending`, and `lifecycle_state = pending`. |
| `gents:crates/gents/src/lifecycle/claim.rs:264` | A successful claim writes `status = processing`, `lifecycle_state = claimed`, `claimed_at`, `backend_id`, and related execution metadata. |
| `gents:crates/gents/src/lifecycle/claim.rs:287` | If the local update returns no row but the local re-check sees `processing`, the lifecycle still treats the request as claimed. |
| `gents:crates/gents/src/lifecycle/claim.rs:306` | After claim, the local lifecycle state becomes `Claimed`. |
| `gents:crates/gents/src/lifecycle/claim.rs:69` and `transition.rs:463` | Execution begins from local `Claimed` and performs a later transition to processing/execution state. |
| `gents:crates/gents/src/watcher/query.rs:73` | The watcher selects `AgentRequest` rows for the local `agent_did` and `status in [pending, processing]`. |
| `gents:crates/gents/src/watcher.rs:102` | Each watcher instance carries one `agent_did`; multiple instances can use the same value. |
| `crates/db/src/merge/merge_handler/lww.rs:165` | LWW merge applies the highest-priority/tie-break delta and rejects lower-priority alternatives. |

## Brainstorming Outcome

The safety question has two different meanings:

1. Eventual claim-uniqueness: after all mutually reachable claim blocks converge,
   the merged CRDT view has one LWW claimer. This should hold for both unfiltered
   and DID-filtered replication when all instances sharing the request DID remain
   in one mutual replication set.

2. Execution-uniqueness: at most one instance ever starts work. This should fail
   under concurrent local CAS, because two same-DID instances can both read a
   local pending view, author claim blocks, and execute before either receives the
   other's claim block. Later LWW convergence chooses one winner but cannot undo
   the already-started work.

The filter is therefore claim-neutral in the correct same-DID partition: it
neither creates nor fixes the execution race. The dangerous case is a filter that
splits same-DID contenders. Then claim blocks do not converge, so even eventual
claim uniqueness fails.

## Model

`proofs/tla/Claim.tla` represents:

| Spec symbol | Meaning |
|---|---|
| `Instances` | Agent process instances. |
| `DidOf` | Instance DID. |
| `RequestDID` | The `agent_did` on the request document. |
| `Contenders` | Instances where `DidOf[i] = RequestDID`; only these can claim. |
| `claims[i]` | Instance `i` authored its claim block. |
| `seen[i]` | Claim blocks merged into instance `i`'s local view. |
| `ClaimRank[i]` | `claimed_at` plus deterministic tie-break, abstracted as a unique rank. |
| `LocalClaimer(i)` | LWW winner among `seen[i]`, or none if no claim block is visible. |
| `executed` | Historical set of instances that started work. |
| `ReplicationPeers` | The scenario's delivery relation: unfiltered, DID-filtered, or split same-DID. |

Actions:

- `Claim(i)`: enabled only when `i`'s local view is still pending (`seen[i] = {}`).
  It records the local claim block and immediately makes `i` consider itself the
  claimer.
- `Deliver(src, dst)`: moves a claim block along `ReplicationPeers`; weak fairness
  ensures enabled deliveries eventually occur.
- `Execute(i)`: enabled when `LocalClaimer(i) = i`; it records the irreversible
  fact that `i` started processing.

## TLC Runs

Run from `proofs/tla/`:

```bash
./tools/tlc -config MC_Claim_Unfiltered_Eventual.cfg MC_Claim_Common.tla
./tools/tlc -config MC_Claim_Filtered_Eventual.cfg MC_Claim_Common.tla
./tools/tlc -config MC_Claim_Unfiltered_Execution.cfg MC_Claim_Common.tla
./tools/tlc -config MC_Claim_Filtered_Execution.cfg MC_Claim_Common.tla
./tools/tlc -config MC_Claim_Split_Eventual.cfg MC_Claim_Common.tla
```

## Invariants, Verdicts, and Source Notes

| Property | Plain English | TLC verdict | Source note |
|---|---|---|---|
| `INV_EventualClaimUnique` | Eventually, all same-DID contenders have the same merged claim-block set and, if any claim exists, exactly one LWW claimer. | GREEN for unfiltered; GREEN for DID-filtered with one same-DID replication set; RED for split same-DID filtering. | `claim.rs` guarded claim write plus `lww.rs` conflict resolution. |
| `INV_ExecutionUnique` | At most one instance ever starts work. | RED for unfiltered; RED for DID-filtered with one same-DID replication set. | `claim.rs:306` local `Claimed` state and `claim.rs:69` `begin_execution` happen before remote claim convergence is guaranteed. |
| `INV_FilterNeutral` | If the same-DID contention set remains mutually replicating, the eventual claim-uniqueness verdict is preserved. | GREEN for unfiltered and DID-filtered common-set configs. | `watcher/query.rs:73` filters claimable rows by `agent_did`; the P2P filter must preserve the whole same-DID contention set. |

## Result

Filtering is claim-neutral under the required partition: same-DID instances must
remain mutually replicating. Execution-uniqueness is not guaranteed by CRDT-CAS;
that race exists with or without DID filtering. If a filter ever splits instances
that share the request DID, the system can lose even eventual claim-uniqueness
because the concurrent claim branches never converge to one LWW winner.
