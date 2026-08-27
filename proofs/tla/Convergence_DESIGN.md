# DAG Convergence - Delivery And Local Merge

**Date:** 2026-06-02 - **Branch:** `feat/p2p-tla-convergence`

This slice factors convergence into two proof obligations:

1. **TLA+ delivery:** under partitions, reconnects, bounded synced-CID eviction,
   and finite restarts, every authorized node eventually receives and merges the
   causal history of every accepted head.
2. **Lean local state machine:** once those deltas are delivered, local merge/apply
   is order-independent for the modeled CRDT state.

The tools are intentionally complementary. Lean does not model network delivery;
TLA+ does not prove byte/value merge algebra.

## Source-Grounded Facts

| Fact | Source |
|---|---|
| `DagSyncState` tracks in-memory `syncing`, bounded `synced`, and FIFO `synced_order`; default cap is `100_000`. | `crates/p2p/src/sync/dag_sync/state.rs:9`, `state.rs:13` |
| `synced` is explicitly a bounded hint: `false` may mean never synced or evicted. | `crates/p2p/src/sync/dag_sync/state.rs:87` |
| `start_sync` refuses CIDs already `syncing` or `synced`; `complete_sync` removes from `syncing`, inserts into `synced`, appends FIFO order, and evicts oldest entries over cap. | `crates/p2p/src/sync/dag_sync/state.rs:100`, `state.rs:115` |
| Restart creates fresh DAG sync state; `clear` wipes `syncing`, `synced`, and `synced_order`. | `crates/p2p/src/sync/dag_sync/sync.rs:32`, `crates/p2p/src/sync/dag_sync/state.rs:160` |
| PushLog stores a root as unmerged, walks all missing DAG links, emits merge only when complete, otherwise registers a pending DAG. | `crates/p2p/src/sync/manager/process/pushlog.rs:248`, `pushlog.rs:269`, `pushlog.rs:292`, `pushlog.rs:363` |
| Missing-link discovery walks the full reachable DAG and short-circuits already merged subtrees. | `crates/p2p/src/sync/manager/links.rs:56`, `links.rs:78` |
| Fetch tries CAR first, then selective block batches, and emits `DagReady` only when no missing links remain. | `crates/p2p/src/sync/coordinator/dag_fetcher.rs:56`, `dag_fetcher.rs:99`, `dag_fetcher.rs:164` |
| Pending DAG entries have TTL/capacity limits, so convergence requires eventual reannouncement/head rediscovery if pending state is dropped. | `crates/p2p/src/sync/manager/pending.rs:11`, `pending.rs:17`, `crates/p2p/src/sync/manager/process/pending_dag.rs:311` |
| DocSync/BranchableSync discover current heads from providers after reconnect. | `crates/p2p/src/sync/coordinator/event_handler/doc_sync.rs:152`, `branchable_sync.rs:62`, `crates/db/src/merge/head_provider.rs:38`, `head_provider.rs:194` |
| Composite merge recursively merges parent heads before the current block; durable merged sets skip duplicate replay. | `crates/db/src/merge/merge_handler/composite.rs:140`, `crates/db/src/merge/merge_handler/mod.rs:83` |

## TLA+ Abstraction

| Spec symbol | Meaning |
|---|---|
| `have[n]` | Durable local blockstore contents. |
| `merged[n]` | Durable local merge/accepted-state marker. |
| `wanted[n]` | In-memory roots learned through head discovery and pending fetch. Restart clears it. |
| `syncing[n]` | In-memory CIDs currently being fetched. |
| `synced[n]`, `syncedOrder[n]` | Bounded FIFO duplicate-work hint. Eviction never removes `have` or `merged`. |
| `connected` | Current directed peer reachability relation. |
| `Heads` | Accepted heads whose causal history must converge everywhere. |

The key liveness property is:

```tla
CONV_EventualConnectivity == EventuallyConnected => <>[]AllConverged
```

where `EventuallyConnected == <>[]FullyConnected`. The green theorem also assumes
fair head rediscovery (`HeadRediscovery = TRUE`), representing eventual DocSync or
BranchableSync after reconnect. The red config turns that off and shows why link
reconnect alone is insufficient.

## TLA+ Properties And Verdicts

| Run | Verdict | Meaning |
|---|---|---|
| `MC_Conv_Eventual.cfg` | GREEN | With fair head rediscovery, a node that starts partitioned eventually receives and merges every accepted head history. |
| `MC_Conv_RestartEviction.cfg` | GREEN | Same property with `MaxSynced = 1` and one arbitrary restart per node. FIFO eviction and restart clear only hints, not durable merge state. |
| `MC_Conv_NoHeadRediscovery.cfg` | RED | Counterexample: peers reconnect, but the empty node never learns missed heads, so convergence fails. |

Safety invariants:

- `INV_SyncedFifo`: the FIFO order exactly matches the bounded `synced` hint and
  never exceeds `MaxSynced`.
- `INV_DurableMerge`: `synced` and `merged` are subsets of durable `have`; hint
  eviction does not delete blockstore or merged state.

## Lean Local Model

Lean lives under `proofs/lean/` and models local merge/apply behavior:

- LWW is a join over a resolved total-order key derived from decoded priority and
  deterministic value/tombstone tie-break (`crates/crdt/src/lww.rs:187`,
  `lww.rs:200`, `lww.rs:218`).
- Int64 counter accumulation is wrapping addition, so it is commutative and
  associative, but raw counter merge is not idempotent
  (`crates/crdt/src/counter.rs:410`, `counter.rs:447`).
- Counter idempotency is modeled at the durable merged-CID/applied-set layer, which
  is the contract documented above the merge handler
  (`crates/db/src/merge/merge_handler/counter.rs:422`).
- Composite merge is componentwise over field-local state
  (`crates/crdt/src/composite.rs:512`).

The Lean README records `#print axioms` status. No theorem uses `sorry` or custom
axioms. Float32/Float64 counter laws are intentionally not claimed because IEEE-754
addition is not generally associative.

## Result

Convergence holds under eventual connectivity **plus fair head rediscovery** and
bounded synced-CID eviction (TLA+), given the local merge state machine is
order-independent for the modeled CRDT components (Lean).
