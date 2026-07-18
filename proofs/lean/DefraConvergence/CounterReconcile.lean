import DefraConvergence.CrdtField

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
connection converged to `node0=90, node1=45` instead of `90/90`.

The LANDED fix (#1021) removes the two-store split entirely: there is ONE
authoritative `value_key`, and BOTH local writes and merges read-modify-write it
by their delta — Go parity with `counter.go incrementValue`. The materialized
value queries read mirrors that single store, so there is no second store to
diverge. `Counter::reconcile_int64` is deliberately *init-if-absent* (seed the
store from the committed blob only when absent; for a PCounter, migrate a legacy
blob-only value via `max`) — NOT a value comparison and NOT a reconcile-from-blob.

This file models that landed design and proves the functional invariant it
guarantees: the value is an **order-independent fold of the applied-delta
multiset** (so any local-write/merge interleaving applying the same multiset
converges; `Int` deltas ⇒ PNCounter decrements covered, no value-magnitude
comparison). The concurrency axis — that the concurrent RMW realizes this fold
without losing or double-applying a delta — is proven by `proofs/tla/TwoStoreCounter`.

This also settles #1051's representation question: Rust's PNCounter is not a
separate product-state `(positive, negative)` algebra. `CType::PnCounter` enables
negative deltas on the same `Counter`, and both local writes and merges fold those
signed deltas into the same authoritative value. Mapping a product PN history to
its signed increments and decrements therefore refines directly to `counterCM`;
there is no additional implementation state for a separate product model to
describe.

The `## Instantiating the generic CRDT-field core` section then plugs this merge
into `DefraConvergence.CrdtField`, inheriting `{two,three}_converge`.

A SUPERSEDED block below preserves the earlier #1014 design (an unconditional
reconcile-from-blob, plus a conditional `seed-only-if-uninitialized` band-aid)
and the theorems showing why each was wrong, to motivate the single-store design.
-/

namespace DefraConvergence.CounterReconcile

/-- Merge a remote increment `d` into the accumulation `store`: counters always
    accumulate (idempotency is enforced upstream by the merged-set, modeled here
    by considering each delta once). Mirrors `crdt::counter::apply_delta`. -/
def mergeInto (store d : Nat) : Nat := store + d

/-- SUPERSEDED MODEL (#1014 design). Models the UNCONDITIONAL reconcile-from-blob
    `acc := committed` that the code USED to do. It is exact only *sequentially*
    (`store ≤ committed`); under concurrency it clobbers (proven RED by
    `proofs/tla/TwoStoreCounter`), so #1021 replaced it with the single-store design
    proven in the post-fix section below (`Counter::reconcile_int64` is now
    init-if-absent + PCounter migrate-via-max, NOT this). The `reconcile*` defs and
    `reconciledMerge_exact` / `reconciled_replicas_converge` / `seedIfUninit_can_diverge`
    theorems are retained to document why the conditional band-aid was wrong and to
    motivate the single-store theorems — they do NOT model current code. -/
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

/-!
## Post-fix design: a single authoritative store (order-safe)

The unconditional `reconcile` above (the #1014 fix) is exact *sequentially* — when
the blob is never read stale, i.e. `store ≤ committed`. But under **concurrent**
same-document merges and local writes the blob can be read stale, and reconciling
the store back down to it drops increments. That lost update is proven as a real
counterexample by the TLA model `proofs/tla/TwoStoreCounter` (RED `Split`
configuration): the commit DAG converges yet the materialized value is short.
Lean could not have caught it — a permutation/interleaving race is outside a
purely functional model, which is precisely why a green Lean proof and the bug
coexisted.

The fix removes the two-store split (Go parity: Go keeps one `value_key` that both
local writes and merges read-modify-write by their delta — `counter.go`
`incrementValue`). BOTH paths apply their **delta** to one authoritative store, and
the materialized value queries read mirrors that store — there is no second store
to diverge. This section proves the functional invariant the single-store design
guarantees: the value is an **order-independent fold of the applied-delta
multiset**. Paired with `TwoStoreCounter` (which proves the concurrent RMW realizes
this fold without losing or double-applying a delta), the two axes together cover
the bug. Deltas are `Int` so PNCounter decrements are covered — and there is no
value-magnitude comparison anywhere (that ambiguity is exactly what made a
`max(store, blob)` shortcut wrong for decrements).
-/

/-- Sum of a delta list (defined locally; this library deliberately depends only
    on core Lean, no Mathlib). -/
def sumList : List Int → Int
  | [] => 0
  | d :: ds => d + sumList ds

/-- The single authoritative value after applying `deltas` to `init`. The SAME
    operation is used by local writes and by merges (one value key, RMW by delta).
    Mirrors the post-fix `crdt::counter::apply_delta` on the authoritative store. -/
def applyAll (init : Int) (deltas : List Int) : Int :=
  init + sumList deltas

/-- The value queries read IS the store: there is no separate materialized blob
    that can diverge from the accumulation store. -/
def materialized (store : Int) : Int := store

/-- **No blob/acc divergence.** Single store, by construction. -/
theorem no_blob_acc_divergence (store : Int) : materialized store = store := rfl

/-- **Value = init + fold of the delta multiset.** -/
theorem applyAll_eq (init : Int) (deltas : List Int) :
    applyAll init deltas = init + sumList deltas := rfl

/-- `sumList` is invariant under swapping two adjacent deltas — the generator of
    all permutations — so the fold depends only on the MULTISET of deltas, not the
    order (or concurrent interleaving) in which they were applied. -/
theorem sumList_swap (a b : Int) (rest : List Int) :
    sumList (a :: b :: rest) = sumList (b :: a :: rest) := by
  simp only [sumList]; omega

/-- **Order-independence.** Applying the same deltas in a swapped order yields the
    same value — concurrent local-write/merge interleavings that apply the same
    multiset converge. -/
theorem applyAll_swap (init a b : Int) (rest : List Int) :
    applyAll init (a :: b :: rest) = applyAll init (b :: a :: rest) := by
  unfold applyAll; rw [sumList_swap]

/-- **Two replicas converge** (the live-pair case). Replica A applies local `a`
    then merges B's `b`; replica B applies `b` then merges `a`. Both reach
    `s + a + b`, with NO assumption that either store led the other — so it is
    robust to the concurrent stale-read interleaving the TLA model exhibits. -/
theorem singleStore_replicas_converge (s a b : Int) :
    applyAll s [a, b] = applyAll s [b, a] := by
  simp only [applyAll, sumList]; omega

/-- **Three replicas converge** (the merge-storm topology). Any order of three
    deltas yields the same total — the fold is independent of the 3-node mesh
    interleaving that exhibited the under-count. -/
theorem singleStore_three_converge (s a b c : Int) :
    applyAll s [a, b, c] = applyAll s [c, b, a] := by
  simp only [applyAll, sumList]; omega

/-- **PNCounter (decrement) safety.** The fold is correct for negative deltas:
    `+50` then `-30` converges to `+20` in either order — no clamping, no
    value-magnitude comparison. This is the PNCounter refinement theorem: the
    schema mode reuses the signed `Int` counter algebra rather than introducing a
    separate product-state merge. -/
theorem singleStore_pncounter_converges (s : Int) :
    applyAll s [50, -30] = applyAll s [-30, 50] ∧ applyAll 0 [50, -30] = 20 := by
  simp only [applyAll, sumList]; omega

/-! ## Instantiating the generic CRDT-field core (`DefraConvergence.CrdtField`)

The counter is the op-based regime: its merge is integer addition of deltas —
commutative and associative (so convergence is the generic order-independent
fold) but NOT idempotent (so it must dedup every delta exactly once). -/

/-- The counter merge as a generic commutative-associative merge: integer delta
    addition. (`Int` ⇒ PNCounter decrements are covered.) -/
def counterCM : CrdtField.CommMerge Int where
  merge := (· + ·)
  comm := Int.add_comm
  assoc := Int.add_assoc

/-- Two-replica counter convergence is the generic fold theorem at `counterCM` —
    no counter-specific reasoning. -/
theorem counter_two_converge (s a b : Int) :
    counterCM.merge (counterCM.merge s a) b = counterCM.merge (counterCM.merge s b) a :=
  CrdtField.two_converge counterCM s a b

/-- Three-replica (merge-storm) counter convergence, likewise inherited. -/
theorem counter_three_converge (s a b c : Int) :
    counterCM.merge (counterCM.merge (counterCM.merge s a) b) c
      = counterCM.merge (counterCM.merge (counterCM.merge s c) b) a :=
  CrdtField.three_converge counterCM s a b c

/-- **The counter is NOT idempotent** (`1 + 1 ≠ 1`). By
    `CrdtField.nonidem_has_dup_witness` this means a re-delivered delta changes the
    value — the counter MUST apply each delta exactly once. That dedup obligation
    is discharged by the blockstore merged-set / `is_merged` guard; its violation
    is the upstream double-apply bug `sourcenetwork/defradb#4935`. -/
theorem counter_not_idempotent : ¬ CrdtField.Idempotent counterCM := by
  intro h
  have h1 : (1 : Int) + 1 = 1 := h 1
  omega

end DefraConvergence.CounterReconcile
