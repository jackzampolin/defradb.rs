/-!
# Counter reconciliation — regression guard for the same-doc counter convergence bug

A counter field's value is tracked in two places in the implementation:

* the **committed** blob — the materialized document, advanced by BOTH local
  increments and merges; and
* the **accumulation store** — the CRDT `value_key` the merge reads/writes,
  advanced ONLY by merges.

A *local* increment (`update_Tally`) pushes the committed blob ahead of the
store. When a remote delta is then merged, the store is incremented and the blob
is re-materialized FROM the store — so any local increment stranded in the blob
is silently dropped. This is the twin of the LWW priority bug
(`DefraConvergence.PriorityReconcile`): in both, Rust split Go's single value
store into a materialized layer + a CRDT layer, and local writes touch only the
materialized layer.

The bug was found by the conformance harness
(`proofs/tests/behavioral/bughunt.rs`) — concurrent `+45 / +45` over a live
connection converged to `node0=90, node1=45` instead of `90/90` — and fixed in
`crates/db-merge/src/merge_handler/counter.rs` by reconciling the store up to the
committed blob before every merge (`Counter::reconcile_int64`).

This models the fix and proves: (1) reconciled accumulation is exact
(`committed + d`); (2) two replicas applying concurrent local increments then
merging each other's delta converge; and (3) the previous *conditional* band-aid
(`seed only if the store is uninitialized`) produces the exact asymmetric
`90 / 45` divergence — so the unconditional reconcile is necessary, not
decorative.
-/

namespace DefraConvergence.CounterReconcile

/-- Merge a remote increment `d` into the accumulation `store`: counters always
    accumulate (idempotency is enforced upstream by the merged-set, modeled here
    by considering each delta once). Mirrors `crdt::counter::apply_delta`. -/
def mergeInto (store d : Nat) : Nat := store + d

/-- Reconcile the accumulation store up to the committed blob before merging.
    The blob is written after the store within each merge txn and only local
    increments push it ahead, so `store ≤ committed` and adopting `committed`
    captures exactly the pending local increments. Mirrors the unconditional
    `Counter::reconcile_int64`. -/
def reconcile (committed _store : Nat) : Nat := committed

/-- The buggy *conditional* band-aid: seed the store from the committed blob ONLY
    when the store is uninitialized. `init = false` ⇒ adopt committed (the creator
    node, whose local create bypassed the store); `init = true` ⇒ keep the stale
    store (a node that received the doc by replication, whose store was already
    initialized by the replicated create). Mirrors the removed
    `seed_if_uninitialized_int64`. -/
def seedIfUninit (init : Bool) (committed store : Nat) : Nat :=
  if init then store else committed

/-- A reconciled merge: catch the store up to the committed blob, then accumulate
    the remote delta. -/
def reconciledMerge (committed store d : Nat) : Nat :=
  mergeInto (reconcile committed store) d

/-- **Safety / exactness.** A reconciled merge yields exactly the committed value
    plus the remote delta — the local increment in `committed` is preserved and
    the remote delta is added on top, regardless of how far the store lagged. -/
theorem reconciledMerge_exact (committed store d : Nat) :
    reconciledMerge committed store d = committed + d := by
  unfold reconciledMerge mergeInto reconcile
  rfl

/-- **Convergence.** Two replicas share seed `s`; replica A applies a local
    increment `a`, replica B applies `b`. Each then merges the other's delta with
    reconcile (its store still holds only the seed `s`). Both reach `s + a + b`. -/
theorem reconciled_replicas_converge (s a b : Nat) :
    reconciledMerge (s + a) s b = reconciledMerge (s + b) s a := by
  unfold reconciledMerge mergeInto reconcile
  omega

/-- **Non-vacuity (divergence).** The previous conditional band-aid produces the
    exact asymmetric split the harness observed. With seed `s = 0` and both local
    increments `= 45`:

    * the **creator** (store uninitialized) adopts committed `45`, then `+ 45 = 90`;
    * the **receiver** (store initialized to `0` by the replicated create) keeps
      the stale store `0`, then `+ 45 = 45`.

    `90 ≠ 45` — the replicas diverge, and the receiver sits below the correct
    total `committed + d = 90`. The reconcile (which ignores `init`) is therefore
    necessary. -/
theorem seedIfUninit_can_diverge :
    ∃ (committed store d : Nat),
      mergeInto (seedIfUninit false committed store) d
        ≠ mergeInto (seedIfUninit true committed store) d ∧
      mergeInto (seedIfUninit true committed store) d < committed + d := by
  exact ⟨45, 0, 45, by decide, by decide⟩

end DefraConvergence.CounterReconcile
