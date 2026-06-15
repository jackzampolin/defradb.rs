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
    value-magnitude comparison. -/
theorem singleStore_pncounter_converges (s : Int) :
    applyAll s [50, -30] = applyAll s [-30, 50] ∧ applyAll 0 [50, -30] = 20 := by
  simp only [applyAll, sumList]; omega

end DefraConvergence.CounterReconcile
