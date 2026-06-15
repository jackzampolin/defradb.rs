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
| `LocalApply` (`blob += 1`, no lock, no `acc`) | local increment via the `db` crate, which does NOT acquire the `db-merge` `merge_queue` lock |

## The knob

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

## Invariant

`INV_NoLoss == blob = doneCount` — the materialized value equals the number of
committed increments (oracle independent of the stores). TLC: RED violates it
(counterexample `blob=1, doneCount=2`); GREEN holds it over all reachable states.

## Fix direction it validates

Make Rust's counter value behave as one store under concurrency — either local
increments and merges serialize on a shared per-doc primitive, or (Go-parity) the
accumulation store is authoritative and local increments apply through it so a
concurrent merge conflicts. Avoids a new hot-path lock if done via the existing SSI
conflict detection on a shared key. Not yet implemented; `bughunt_same_doc_merge_storm`
(+ `_go` control) is the behavioral repro to flip green once the fix lands.
