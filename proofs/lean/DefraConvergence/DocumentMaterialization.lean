namespace DefraConvergence.DocumentMaterialization

/-!
# Document materialization status

Composite/document materialization keeps the deletion marker as a status component
separate from mutable field bytes. A later active field rematerialization may update
the retained bytes, but it must not clear the tombstone.
-/

inductive Status
  | active
  | deleted
deriving Repr, DecidableEq

namespace Status

/-- Deletion is absorbing over active document status. -/
def merge : Status → Status → Status
  | .deleted, _ => .deleted
  | _, .deleted => .deleted
  | .active, .active => .active

theorem merge_comm (a b : Status) : merge a b = merge b a := by
  cases a <;> cases b <;> rfl

theorem merge_assoc (a b c : Status) : merge (merge a b) c = merge a (merge b c) := by
  cases a <;> cases b <;> cases c <;> rfl

theorem merge_idem (a : Status) : merge a a = a := by
  cases a <;> rfl

theorem deleted_absorbs_active : merge .deleted .active = .deleted := by
  rfl

end Status

structure MaterializedDoc where
  status : Status
  age : Nat
deriving Repr, DecidableEq

def mergeDelete (doc : MaterializedDoc) : MaterializedDoc :=
  { doc with status := Status.merge doc.status .deleted }

def mergeActiveAge (doc : MaterializedDoc) (age : Nat) : MaterializedDoc :=
  { doc with status := Status.merge doc.status .active, age }

/-- An active field update after a delete may update bytes, but keeps the tombstone. -/
theorem active_age_after_delete_keeps_deleted (doc : MaterializedDoc) (age : Nat) :
    (mergeActiveAge (mergeDelete doc) age).status = Status.deleted := by
  cases doc with
  | mk status currentAge =>
    cases status <;> rfl

/-- Delete and active field rematerialization converge when status is componentwise. -/
theorem delete_active_age_converge (doc : MaterializedDoc) (age : Nat) :
    mergeActiveAge (mergeDelete doc) age = mergeDelete (mergeActiveAge doc age) := by
  cases doc with
  | mk status currentAge =>
    cases status <;> rfl

end DefraConvergence.DocumentMaterialization
