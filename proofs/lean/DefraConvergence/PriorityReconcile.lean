import DefraConvergence.CrdtField

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

/-! ## Instantiating the generic CRDT-field core (`DefraConvergence.CrdtField`)

LWW is the state-based regime: its merge is the lexicographic max over
`(priority, value)` — commutative, associative, AND idempotent. Idempotence is
the extra law that makes it a join-semilattice, so (unlike the counter) LWW is
re-delivery-safe and needs NO dedup. -/

/-- `lwwMerge` is idempotent: merging an entry with itself is a no-op. This is the
    defining extra law of a state-based join — and the reason LWW (unlike the
    counter, `CounterReconcile.counter_not_idempotent`) needs NO dedup: by
    `CrdtField.dup_absorb` a re-delivered entry is absorbed. -/
theorem lwwMerge_idem (a : Entry) : lwwMerge a a = a := by
  obtain ⟨p, v⟩ := a
  unfold lwwMerge
  rw [if_neg (by omega : ¬ p > p)]
  rw [if_neg (by intro h; omega : ¬ (p = p ∧ v > v))]

/-- `lwwMerge` is commutative (it is the max under the total `(priority, value)`
    order; ties occur only at full equality). -/
theorem lwwMerge_comm (a b : Entry) : lwwMerge a b = lwwMerge b a := by
  obtain ⟨pa, va⟩ := a
  obtain ⟨pb, vb⟩ := b
  unfold lwwMerge
  rcases Nat.lt_trichotomy pa pb with h | h | h
  · rw [if_pos (by omega : pb > pa), if_neg (by omega : ¬ pa > pb),
        if_neg (by intro hh; omega : ¬ (pa = pb ∧ va > vb))]
  · subst h
    rcases Nat.lt_trichotomy va vb with hv | hv | hv
    · rw [if_neg (by omega : ¬ pa > pa), if_pos (by exact ⟨rfl, by omega⟩ : pa = pa ∧ vb > va),
          if_neg (by omega : ¬ pa > pa), if_neg (by intro hh; omega : ¬ (pa = pa ∧ va > vb))]
    · subst hv; rfl
    · rw [if_neg (by omega : ¬ pa > pa), if_neg (by intro hh; omega : ¬ (pa = pa ∧ vb > va)),
          if_neg (by omega : ¬ pa > pa), if_pos (by exact ⟨rfl, by omega⟩ : pa = pa ∧ va > vb)]
  · rw [if_neg (by omega : ¬ pb > pa), if_neg (by intro hh; omega : ¬ (pb = pa ∧ vb > va)),
        if_pos (by omega : pa > pb)]

/-- The lexicographic order `lwwMerge` selects under: priority first, then value. -/
def le (a b : Entry) : Prop :=
  a.priority < b.priority ∨ (a.priority = b.priority ∧ a.value ≤ b.value)

/-- The order is total. -/
theorem le_total (a b : Entry) : le a b ∨ le b a := by
  obtain ⟨pa, va⟩ := a; obtain ⟨pb, vb⟩ := b
  simp only [le]; omega

/-- The order is transitive. -/
theorem le_trans {a b c : Entry} (h1 : le a b) (h2 : le b c) : le a c := by
  obtain ⟨pa, va⟩ := a; obtain ⟨pb, vb⟩ := b; obtain ⟨pc, vc⟩ := c
  simp only [le] at h1 h2 ⊢; omega

/-- When `a ⊑ b`, the merge keeps `b` (the larger entry). -/
theorem lwwMerge_eq_of_le {a b : Entry} (h : le a b) : lwwMerge a b = b := by
  obtain ⟨pa, va⟩ := a; obtain ⟨pb, vb⟩ := b
  simp only [le] at h; unfold lwwMerge
  by_cases h1 : pb > pa
  · rw [if_pos h1]
  · rw [if_neg h1]
    by_cases h2 : pb = pa ∧ vb > va
    · rw [if_pos h2]
    · rw [if_neg h2]
      have hpe : pa = pb := by omega
      subst hpe
      have hve : va = vb := by omega
      rw [hve]

/-- When `b ⊑ a`, the merge keeps `a`. -/
theorem lwwMerge_eq_of_ge {a b : Entry} (h : le b a) : lwwMerge a b = a := by
  obtain ⟨pa, va⟩ := a; obtain ⟨pb, vb⟩ := b
  simp only [le] at h; unfold lwwMerge
  by_cases h1 : pb > pa
  · exfalso; omega
  · rw [if_neg h1]
    by_cases h2 : pb = pa ∧ vb > va
    · exfalso; obtain ⟨he, hv⟩ := h2; omega
    · rw [if_neg h2]

/-- `lwwMerge` is associative — it is the maximum under the total lex order, so
    folding three entries is order-independent regardless of association. -/
theorem lwwMerge_assoc (a b c : Entry) :
    lwwMerge (lwwMerge a b) c = lwwMerge a (lwwMerge b c) := by
  rcases le_total a b with hab | hab
  · rw [lwwMerge_eq_of_le hab]
    rcases le_total b c with hbc | hbc
    · rw [lwwMerge_eq_of_le hbc, lwwMerge_eq_of_le (le_trans hab hbc)]
    · rw [lwwMerge_eq_of_ge hbc, lwwMerge_eq_of_le hab]
  · rw [lwwMerge_eq_of_ge hab]
    rcases le_total b c with hbc | hbc
    · rw [lwwMerge_eq_of_le hbc]
    · rw [lwwMerge_eq_of_ge hbc, lwwMerge_eq_of_ge (le_trans hbc hab),
          lwwMerge_eq_of_ge hab]

/-- The LWW merge as a generic commutative-associative merge — LWW instantiates the
    same `CrdtField` core the counter does. -/
def lwwCM : CrdtField.CommMerge Entry where
  merge := lwwMerge
  comm := lwwMerge_comm
  assoc := lwwMerge_assoc

/-- **LWW is idempotent** — the defining extra law of a state-based join. By
    `CrdtField.dup_absorb`, re-delivering the same entry is harmless, so LWW (unlike
    the counter, `CounterReconcile.counter_not_idempotent`) needs NO dedup. -/
theorem lww_idempotent : CrdtField.Idempotent lwwCM := lwwMerge_idem

/-- **Re-delivery safety**, inherited from the generic core: applying the same entry
    twice equals applying it once (the counterpart to the counter's dedup obligation). -/
theorem lww_dup_safe (s a : Entry) :
    lwwCM.merge (lwwCM.merge s a) a = lwwCM.merge s a :=
  CrdtField.dup_absorb lwwCM lww_idempotent s a

/-- Two-replica LWW convergence is the generic order-independent fold at `lwwCM`. -/
theorem lww_two_converge (s a b : Entry) :
    lwwCM.merge (lwwCM.merge s a) b = lwwCM.merge (lwwCM.merge s b) a :=
  CrdtField.two_converge lwwCM s a b

/-- Three-replica (merge-storm) LWW convergence, likewise inherited — the same
    generic theorem the counter instantiates (`counter_three_converge`), so both
    fields expose the identical convergence set. -/
theorem lww_three_converge (s a b c : Entry) :
    lwwCM.merge (lwwCM.merge (lwwCM.merge s a) b) c
      = lwwCM.merge (lwwCM.merge (lwwCM.merge s c) b) a :=
  CrdtField.three_converge lwwCM s a b c

end DefraConvergence.PriorityReconcile
