/-!
# Generic CRDT-field convergence core

The reusable algebraic backbone shared by every CRDT field. A field's
materialized value is produced by folding incoming states/deltas through a binary
`merge`. This module proves — ONCE — the convergence consequences of `merge`'s
algebraic laws; each field (counter, LWW, …) instantiates it by supplying its
`merge` and the laws it satisfies, and inherits the convergence theorems.

Two regimes, divided by idempotence — the crisp line the `#4935` counter
double-apply bug drew:

* **commutative + associative** ⇒ the fold is *order-independent*: replicas
  applying the same multiset of operations in any interleaving converge. EVERY
  field needs this.
* **+ idempotent** ⇒ the fold is also *duplicate-independent*: re-delivering a
  state is harmless, so the field needs NO dedup (state-based / join-semilattice
  fields, e.g. LWW — `PriorityReconcile`).
* **¬ idempotent** ⇒ a duplicate CHANGES the result, so the field MUST apply each
  operation exactly once (op-based fields, e.g. counters — `+` is not idempotent;
  `CounterReconcile`). Re-applying a counter delta double-counts: precisely the
  upstream double-apply bug `sourcenetwork/defradb#4935`.

This is the *algebraic* axis. The *concurrency* axis — that the real two-store
read-modify-write realizes this fold without losing or double-applying a delta
under interleaving — is modeled per field in `proofs/tla` (`TwoStoreCounter`, …).
-/

namespace DefraConvergence.CrdtField

/-- A commutative, associative binary merge over `V` — the minimum every CRDT
    field's merge satisfies. A field instantiates this with its operation and the
    proofs of the two laws. -/
structure CommMerge (V : Type) where
  merge : V → V → V
  comm : ∀ x y : V, merge x y = merge y x
  assoc : ∀ x y z : V, merge (merge x y) z = merge x (merge y z)

variable {V : Type}

/-- **Order-independence (adjacent swap).** Folding two incoming values into a
    state is independent of their order. This is the generator of full
    permutation-invariance; the 2- and 3-operation corollaries below are what the
    live-pair and 3-node merge-storm conformance topologies exercise. -/
theorem swap (m : CommMerge V) (s a b : V) :
    m.merge (m.merge s a) b = m.merge (m.merge s b) a := by
  rw [m.assoc, m.comm a b, ← m.assoc]

/-- **Two replicas converge.** A applies `a` then merges `b`; B applies `b` then
    merges `a`. Both reach the same state, with no assumption about which led. -/
theorem two_converge (m : CommMerge V) (s a b : V) :
    m.merge (m.merge s a) b = m.merge (m.merge s b) a :=
  swap m s a b

/-- **Three replicas converge** (full-mesh merge-storm topology): the fully
    reversed application order folds to the same state. -/
theorem three_converge (m : CommMerge V) (s a b c : V) :
    m.merge (m.merge (m.merge s a) b) c
      = m.merge (m.merge (m.merge s c) b) a := by
  rw [swap m (m.merge s a) b c, swap m s a c, swap m (m.merge s c) a b]

/-- An idempotent merge: re-merging a value with itself is a no-op. The defining
    extra law of state-based / join-semilattice CRDT fields. -/
def Idempotent (m : CommMerge V) : Prop := ∀ x : V, m.merge x x = x

/-- **Duplicate-independence (idempotent fields).** Applying the same incoming
    value twice equals applying it once — so re-delivery is harmless and an
    idempotent field needs NO dedup. -/
theorem dup_absorb (m : CommMerge V) (idem : Idempotent m) (s a : V) :
    m.merge (m.merge s a) a = m.merge s a := by
  rw [m.assoc, idem]

/-- **The dedup obligation (op-based fields).** A merge that is NOT idempotent has
    a value at which re-merging (a duplicate delivery) changes the result — so the
    field cannot be re-delivery-safe and MUST apply each operation exactly once.
    This is the algebraic root of `#4935`: a counter delta re-applied is a delta
    double-counted (`CounterReconcile.counter_not_idempotent` supplies the
    concrete `1 + 1 ≠ 1` witness). -/
theorem nonidem_has_dup_witness (m : CommMerge V) (hne : ¬ Idempotent m) :
    ∃ x : V, m.merge x x ≠ x :=
  Classical.byContradiction fun hcon =>
    hne (fun x => Classical.byContradiction fun hx => hcon ⟨x, hx⟩)

end DefraConvergence.CrdtField
