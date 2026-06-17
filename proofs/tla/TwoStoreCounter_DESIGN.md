# TwoStoreCounter — design & anchors

Models the counter materialization race found 2026-06-15 by the same-doc merge
storm (`proofs/tests/behavioral/bughunt.rs::bughunt_same_doc_merge_storm`): under
concurrent same-document increments, Rust loses an increment while the commit DAG
fully converges (identical `_commits` set on every node, divergent values 12/12/11).

## What it abstracts

| Model element | Code |
|---|---|
| `blob` (materialized value, queries read it; local writes RMW it) | the CBOR document blob written by `db` crate `doc_mutator` (local increments) and by `db-merge` composite materialization |
| `acc` (accumulation store) | the counter `value_key` in `crates/crdt/src/counter.rs` |
| `MergeReconcile` (`acc := blob`) | `Counter::reconcile_int64(datastore, blob_value)` in `crates/db-merge/src/merge_handler/counter.rs` (process_counter_delta ~L91, process_counter_delta_in_txn ~L202) |
| `MergeCommit` (`acc += δ; blob := acc`) | `counter.merge(+delta)` then blob re-materialization |
| `LocalApply` (`blob += 1`, no lock, no `acc`) | local increment via the `db` crate; in the Split (pre-#1021) abstraction it does NOT acquire the shared per-doc guard |
| `MergeRedeliver` (re-delivery; inline `Dedup` branch) | a delta delivered twice; `is_merged(cid)` merged-set guard in `counter.rs`/`composite.rs` (suppresses when `Dedup="On"`) |

## The knobs

The model has two orthogonal axes, each with a CONSTANT knob.

### `StoreMode` — the lost-update / two-store-split axis

- `StoreMode = "Split"` [RED]: the merge commits its stale reconciled snapshot+delta
  with no cross-store conflict check. A local increment that interleaves between a
  merge's reconcile and commit is overwritten → `INV_NoLoss` violated
  (`blob < doneCount`). This is the Rust two-store split: local writes touch the
  blob key, merges touch the accumulation key, so the storage layer's transactional
  conflict detection never fires between them.
- `StoreMode = "Unified"` [GREEN]: the merge's RMW is conflict-checked — it commits
  only if `blob` is unchanged since reconcile, otherwise re-reconciles against the
  fresh value and retries. This is Go's design (counter.go `incrementValue` RMW on a
  single `key.WithValueFlag()`, where a concurrent local write and merge conflict at
  commit and the loser retries) and equivalently a per-doc lock spanning both the
  local-write and merge paths.

### `Dedup` — the double-apply / idempotency axis (added by this PR)

Orthogonal to the store split: even with a unified store, a remote counter delta can
be DELIVERED twice (a field block arriving as its own PushLog head, then again via its
composite — upstream `sourcenetwork/defradb#4935`). Go's `coreblock.ProcessBlock`
applies the delta unconditionally; idempotency rests entirely on the blockstore
merged-set / `is_merged(cid)` guard. The `MergeRedeliver` action models the re-delivery
and branches INLINE on `Dedup` (it is a no-op transition, not a disabled action, so the
re-delivery path is exercised in BOTH settings):

- `Dedup = "On"` [GREEN]: the merged-set guard finds the block already merged and
  SUPPRESSES the re-apply — `MergeRedeliver` is a no-op (`UNCHANGED <<blob, acc>>`).
  This active no-op is what makes GREEN non-vacuous on the double-apply axis.
- `Dedup = "Off"` [RED]: no guard → the +1 is applied a second time (`blob`/`acc` climb)
  with no new increment → `blob > doneCount`, violating `INV_NoDoubleApply`. The Lean
  twin is `CounterReconcile.counter_not_idempotent`.

## Invariants

The real definitions in `TwoStoreCounter.tla`:

- `INV_NoLoss == blob >= doneCount` — no increment dropped (the **Split** axis breaks
  this: a clobbered local write makes `blob < doneCount`).
- `INV_NoDoubleApply == blob <= doneCount` — no increment counted twice (the **Dedup=Off**
  axis breaks this: a re-applied delta makes `blob > doneCount`).
- `INV_Exact == blob = doneCount` — the headline: the materialized value equals the
  number of committed increments (oracle independent of the stores). Equivalent to
  `INV_NoLoss /\ INV_NoDoubleApply`.

The GREEN config (`MC_TwoStoreCounter_Green.cfg`, `StoreMode="Unified"`, `Dedup="On"`)
checks `INV_Exact` and holds it over all reachable states. The RED configs each check the
one directional invariant their bug violates.

## Scenarios / configs / verdicts

| Config | Knobs | Invariant | Expected |
|---|---|---|---|
| `MC_TwoStoreCounter_Green` | Unified + On | `INV_Exact` | GREEN |
| `MC_TwoStoreCounter_Red_Split` | Split + On | `INV_NoLoss` | RED (`blob < doneCount`) |
| `MC_TwoStoreCounter_Red_DoubleApply` | Unified + Off | `INV_NoDoubleApply` | RED (`blob > doneCount`) |

## Fix status

**Landed (#1021).** Rust's counter value behaves as one store under concurrency: local
increments and merges serialize on the shared per-doc write guard
(`crates/db/src/doc_write_queue.rs`, taken by BOTH the local-write path and the db-merge
merge handler — see `MergeQueue.tla`), and reconcile is init-if-absent (PCounter
migrate-via-max), so a local write and a same-doc merge never interleave their store RMW.
The behavioral repro `bughunt_same_doc_merge_storm` (+ `_go` control) now passes. The
GREEN `Unified` mode here abstracts that fix as a conflict-checked RMW; the per-doc-lock
realization is checked directly in `MergeQueue.tla`'s `INV_NoLocalMergeInterleave`.
