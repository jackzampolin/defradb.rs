/-!
# Priority reconciliation — regression guard for the same-doc convergence bug

A field's LWW priority is tracked in two places in the implementation:

* the **committed** state — the headstore + materialized document, advanced by
  BOTH local writes and merges; and
* the **merge view** — the datastore LWW entry the merge reads, advanced ONLY by
  merges.

A *local* write (e.g. `update_User`) pushes `committed` ahead of `view`. If the
merge then resolves a re-walked ancestor delta against the stale `view`, the
field can drop BELOW its committed priority — and the two replicas diverge. This
is the bug found by the conformance harness
(`proofs/tests/behavioral/partition.rs`) and fixed in
`crates/db-merge/src/merge_handler/lww.rs` (`seed_lww_from_existing_doc`).

This models the fix: `reconcile` raises `view` to `committed` before merging, and
we prove that the merge can then never drop the field below its committed
priority. A negative theorem shows that WITHOUT reconcile a clobber is reachable
(the exact live witness), so the reconcile step is necessary, not decorative.
-/

namespace DefraConvergence.PriorityReconcile

/-- A field entry: a merge `priority` and a tie-break-ordered `value`. -/
structure Entry where
  priority : Nat
  value : Nat
deriving DecidableEq, Repr

/-- LWW merge against the current entry: a strictly-higher priority wins; on an
    equal priority the strictly-greater value wins; otherwise the current entry
    is kept. Mirrors `crdt::lww::Lww::set_value`. -/
def lwwMerge (cur d : Entry) : Entry :=
  if d.priority > cur.priority then d
  else if d.priority = cur.priority ∧ d.value > cur.value then d
  else cur

/-- The merge result never has a lower priority than the current entry. -/
theorem lwwMerge_priority_ge_cur (cur d : Entry) :
    cur.priority ≤ (lwwMerge cur d).priority := by
  unfold lwwMerge
  by_cases h1 : d.priority > cur.priority
  · rw [if_pos h1]; omega
  · rw [if_neg h1]
    by_cases h2 : d.priority = cur.priority ∧ d.value > cur.value
    · rw [if_pos h2]; obtain ⟨he, _⟩ := h2; omega
    · rw [if_neg h2]; omega

/-- Reconcile the merge view against the committed state: raise the view to the
    committed entry when the committed priority is ahead (a local write that
    bypassed the merge view). Mirrors `seed_lww_from_existing_doc`. -/
def reconcile (committed view : Entry) : Entry :=
  if committed.priority > view.priority then committed else view

/-- After reconcile, the view's priority is at least the committed priority. -/
theorem reconcile_priority_ge_committed (committed view : Entry) :
    committed.priority ≤ (reconcile committed view).priority := by
  unfold reconcile
  by_cases h : committed.priority > view.priority
  · rw [if_pos h]; omega
  · rw [if_neg h]; omega

/-- **Safety.** Merging any delta against the *reconciled* view never drops the
    field below its committed priority — so a re-walked older delta can no longer
    clobber a newer committed write, and the two replicas cannot disagree on the
    surviving priority. -/
theorem reconciled_merge_ge_committed (committed view d : Entry) :
    committed.priority ≤ (lwwMerge (reconcile committed view) d).priority := by
  have h1 := reconcile_priority_ge_committed committed view
  have h2 := lwwMerge_priority_ge_cur (reconcile committed view) d
  omega

/-- **Non-vacuity.** WITHOUT reconcile, the merge CAN drop the field below its
    committed priority — the bug. The witness mirrors the live repro: committed
    `(priority 2, "LA")`, stale view `(priority 1, "NYC")`, and a re-walked create
    delta `(priority 1, "NYC")`. The equal-priority tie keeps the stale NYC at
    priority 1, below the committed priority 2 (values encoded as Nats; only the
    priority drop matters here). -/
theorem unreconciled_merge_can_clobber :
    ∃ committed view d : Entry,
      view.priority < committed.priority ∧
      (lwwMerge view d).priority < committed.priority := by
  exact ⟨⟨2, 0⟩, ⟨1, 1⟩, ⟨1, 1⟩, by decide, by decide⟩

end DefraConvergence.PriorityReconcile
