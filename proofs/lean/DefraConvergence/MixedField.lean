import DefraConvergence.CounterReconcile
import DefraConvergence.PriorityReconcile

/-!
# Mixed-field convergence — product of LWW and Counter fields

The same document can carry fields from both CRDT regimes. The mixed-field
regression surface is not a new scalar merge law: it is the componentwise product
of the two worked `#1041` instances:

* an LWW field, which is state-based and idempotent; and
* a counter field, which is op-based and not idempotent.

This module proves the algebraic part of that product. If each replica applies the
same set of LWW states and counter deltas, any delivery order converges to the same
mixed document state. The product remains non-idempotent because the counter
component remains non-idempotent, so mixed documents inherit the counter's
exactly-once/dedup obligation.

The temporal/concurrency part is split across the component models and a
mixed-specific materialization model: `PriorityReconcile` covers LWW priority
reconciliation, `TwoStoreCounter` covers exactly-once counter RMW/dedup, and
`MixedFieldMaterialization.tla` proves that a field merge must materialize
componentwise rather than committing a stale whole-document snapshot.
-/

namespace DefraConvergence.MixedField

abbrev LwwEntry := PriorityReconcile.Entry
abbrev Mixed := LwwEntry × Int

/-- Componentwise merge for a same-document mixed field state. -/
def mixedMerge (cur delta : Mixed) : Mixed :=
  (PriorityReconcile.lwwCM.merge cur.1 delta.1,
   CounterReconcile.counterCM.merge cur.2 delta.2)

theorem mixedMerge_comm (a b : Mixed) : mixedMerge a b = mixedMerge b a := by
  cases a with
  | mk al ac =>
    cases b with
    | mk bl bc =>
      unfold mixedMerge
      simp only
      apply Prod.ext
      · exact PriorityReconcile.lwwCM.comm al bl
      · exact CounterReconcile.counterCM.comm ac bc

theorem mixedMerge_assoc (a b c : Mixed) :
    mixedMerge (mixedMerge a b) c = mixedMerge a (mixedMerge b c) := by
  cases a with
  | mk al ac =>
    cases b with
    | mk bl bc =>
      cases c with
      | mk cl cc =>
        unfold mixedMerge
        simp only
        apply Prod.ext
        · exact PriorityReconcile.lwwCM.assoc al bl cl
        · exact CounterReconcile.counterCM.assoc ac bc cc

/-- Mixed-field documents instantiate the generic CRDT field core by product. -/
def mixedCM : CrdtField.CommMerge Mixed where
  merge := mixedMerge
  comm := mixedMerge_comm
  assoc := mixedMerge_assoc

/-- Two-replica mixed-field convergence follows from the generic product merge. -/
theorem mixed_two_converge (s a b : Mixed) :
    mixedCM.merge (mixedCM.merge s a) b = mixedCM.merge (mixedCM.merge s b) a :=
  CrdtField.two_converge mixedCM s a b

/-- Three-replica mixed-field convergence follows from the generic product merge. -/
theorem mixed_three_converge (s a b c : Mixed) :
    mixedCM.merge (mixedCM.merge (mixedCM.merge s a) b) c
      = mixedCM.merge (mixedCM.merge (mixedCM.merge s c) b) a :=
  CrdtField.three_converge mixedCM s a b c

/-- Re-delivering the same mixed update is not safe in general: the counter
    component double-applies even though the LWW component absorbs duplicates. -/
theorem mixed_not_idempotent : ¬ CrdtField.Idempotent mixedCM := by
  intro h
  let lww0 : LwwEntry := ⟨0, 0⟩
  have hdup := congrArg Prod.snd (h (lww0, (1 : Int)))
  simp [CrdtField.Idempotent, mixedCM, mixedMerge, CounterReconcile.counterCM] at hdup

/-- The LWW component of a mixed document is still duplicate-safe; only the counter
    component imposes the product's dedup obligation. -/
theorem mixed_lww_component_dup_safe (s a : Mixed) :
    (mixedCM.merge (mixedCM.merge s a) a).1 = (mixedCM.merge s a).1 := by
  cases s with
  | mk sl sc =>
    cases a with
    | mk al ac =>
      simp [mixedCM, mixedMerge, PriorityReconcile.lww_dup_safe]

end DefraConvergence.MixedField
