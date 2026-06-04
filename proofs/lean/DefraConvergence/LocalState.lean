import Std

namespace DefraConvergence

/-!
Local CRDT state-machine model for the convergence split:

* TLA+ models distributed delivery.
* Lean models local merge/apply order-independence once a node has received the
  relevant deltas.

Source correspondence:
* `crates/crdt/src/lww.rs`: LWW accepts the higher priority, with lexicographic
  bytes as the deterministic tie-break.
* `crates/crdt/src/counter.rs`: counter merge applies wrapping Int64 addition;
  the raw counter merge is not idempotent.
* `crates/db-merge/src/merge_handler/counter.rs`: counter idempotency is supplied
  by the durable merged-CID gate above the CRDT merge function.
* `crates/crdt/src/composite.rs`: composite merge is componentwise over fields.
-/

abbrev Rank := Nat

def lwwMerge (a b : Rank) : Rank := max a b

theorem lwwMerge_comm (a b : Rank) : lwwMerge a b = lwwMerge b a := by
  unfold lwwMerge
  exact Nat.max_comm a b

theorem lwwMerge_assoc (a b c : Rank) :
    lwwMerge (lwwMerge a b) c = lwwMerge a (lwwMerge b c) := by
  unfold lwwMerge
  exact Nat.max_assoc a b c

theorem lwwMerge_idem (a : Rank) : lwwMerge a a = a := by
  unfold lwwMerge
  exact Nat.max_eq_left (Nat.le_refl a)

/--
`resolvedKey` is the deterministic total-order key produced from the Rust LWW
comparison: decoded `u64` priority first, then lexicographic value/tombstone
tie-break. The byte-level varint codec is storage representation, not the order.
-/
structure LwwState where
  resolvedKey : Rank
deriving Repr, DecidableEq

def LwwState.merge (a b : LwwState) : LwwState :=
  { resolvedKey := lwwMerge a.resolvedKey b.resolvedKey }

theorem lwwState_merge_comm (a b : LwwState) :
    a.merge b = b.merge a := by
  cases a
  cases b
  simp [LwwState.merge, lwwMerge_comm]

theorem lwwState_merge_assoc (a b c : LwwState) :
    (a.merge b).merge c = a.merge (b.merge c) := by
  cases a
  cases b
  cases c
  simp [LwwState.merge, lwwMerge_assoc]

theorem lwwState_merge_idem (a : LwwState) : a.merge a = a := by
  cases a
  simp [LwwState.merge, lwwMerge_idem]

abbrev Word64 := Nat

def word64Modulus : Nat := 18446744073709551616

def word64Add (a b : Word64) : Word64 := (a + b) % word64Modulus

theorem word64Add_comm (a b : Word64) : word64Add a b = word64Add b a := by
  unfold word64Add
  rw [Nat.add_comm]

theorem word64Add_assoc (a b c : Word64) :
    word64Add (word64Add a b) c = word64Add a (word64Add b c) := by
  unfold word64Add
  calc
    ((a + b) % word64Modulus + c) % word64Modulus = (a + b + c) % word64Modulus := by
      rw [Nat.mod_add_mod]
    _ = (b + c + a) % word64Modulus := by
      congr 1
      ac_rfl
    _ = ((b + c) % word64Modulus + a) % word64Modulus := by
      rw [Nat.mod_add_mod]
    _ = (a + (b + c) % word64Modulus) % word64Modulus := by
      congr 1
      ac_rfl

theorem word64Add_not_idempotent : Not (word64Add 1 1 = 1) := by
  native_decide

/--
Float counters are NOT order-independent: IEEE-754 addition is not associative, so
two replicas applying the SAME set of float deltas in different merge orders can
converge to DIFFERENT values. The convergence proof above therefore covers only the
Int64 (`word64Add`) counter; this negative theorem makes the float exclusion
machine-evident. (Audit 2026-06-03: both Go and Rust ship `Float32/64` counter
fields with raw IEEE-754 `+` and no mitigation — a real latent divergence.)
-/
theorem float_add_not_assoc :
    (((0.1 + 0.2) + 0.3 : Float) == (0.1 + (0.2 + 0.3))) = false := by
  native_decide

/--
Abstract canonical representation of the durable merged-CID/applied-delta set.
The model uses `max` as the join over canonical set ranks; the important local
state-machine fact is that this layer is idempotent, unlike raw counter addition.
-/
structure AppliedSet where
  canonicalRank : Rank
deriving Repr, DecidableEq

def AppliedSet.merge (a b : AppliedSet) : AppliedSet :=
  { canonicalRank := max a.canonicalRank b.canonicalRank }

theorem appliedSet_merge_comm (a b : AppliedSet) :
    a.merge b = b.merge a := by
  cases a
  cases b
  simp [AppliedSet.merge, Nat.max_comm]

theorem appliedSet_merge_assoc (a b c : AppliedSet) :
    (a.merge b).merge c = a.merge (b.merge c) := by
  cases a
  cases b
  cases c
  simp [AppliedSet.merge, Nat.max_assoc]

theorem appliedSet_merge_idem (a : AppliedSet) : a.merge a = a := by
  cases a
  simp [AppliedSet.merge, Nat.max_eq_left]

structure CompositeState where
  lww : LwwState
  counterApplied : AppliedSet
deriving Repr, DecidableEq

def CompositeState.merge (a b : CompositeState) : CompositeState :=
  { lww := a.lww.merge b.lww,
    counterApplied := a.counterApplied.merge b.counterApplied }

theorem composite_merge_comm (a b : CompositeState) :
    a.merge b = b.merge a := by
  cases a
  cases b
  simp [CompositeState.merge, lwwState_merge_comm, appliedSet_merge_comm]

theorem composite_merge_assoc (a b c : CompositeState) :
    (a.merge b).merge c = a.merge (b.merge c) := by
  cases a
  cases b
  cases c
  simp [CompositeState.merge, lwwState_merge_assoc, appliedSet_merge_assoc]

theorem composite_merge_idem (a : CompositeState) : a.merge a = a := by
  cases a
  simp [CompositeState.merge, lwwState_merge_idem, appliedSet_merge_idem]

end DefraConvergence
