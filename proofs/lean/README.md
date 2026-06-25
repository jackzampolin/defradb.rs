# Convergence Proofs

The proof split is intentional:

- TLA+ (`proofs/tla/Convergence.tla`) models distributed delivery: partitions,
  reconnects, head rediscovery, synced-CID eviction, and restart.
- Lean (`proofs/lean/`) models the local merge/apply state machine after delivery.

## Lean Theorems

Run:

```bash
cd proofs/lean
lake build
```

The model proves:

| Theorem | Meaning | Rust source |
|---|---|---|
| `lwwState_merge_comm`, `lwwState_merge_assoc`, `lwwState_merge_idem` | LWW is a join over the deterministic priority/value key. | `crates/crdt/src/lww.rs` |
| `word64Add_comm`, `word64Add_assoc` | Int64 counter accumulation is order-independent under wrapping addition. | `crates/crdt/src/counter.rs` |
| `word64Add_not_idempotent` | Raw counter merge is not idempotent. Duplicate suppression is not local to `Counter::merge`. | `crates/crdt/src/counter.rs` |
| `appliedSet_merge_*` | The durable merged-CID/applied set is the idempotent layer used for duplicate suppression. | `crates/db-merge/src/merge_handler/counter.rs` |
| `composite_merge_*` | Composite local state merges componentwise from the LWW and applied-set components. | `crates/crdt/src/composite.rs` |
| `CrdtField.swap` / `two_converge` / `three_converge` | Generic reusable core (`DefraConvergence/CrdtField.lean`): for ANY commutative-associative merge, the fold is order-independent (replicas applying the same multiset converge). | — (field-agnostic) |
| `CrdtField.dup_absorb` / `nonidem_has_dup_witness` | Idempotence is the dedup dividing line: idempotent ⇒ re-delivery-safe (no dedup); ¬idempotent ⇒ a duplicate changes the result (must dedup). | — |
| `CounterReconcile.counterCM` + `counter_not_idempotent`; `PriorityReconcile.lwwCM` + `lww_idempotent` | Both fields fully instantiate the core: the counter (op-based, `Int +`, **not** idempotent ⇒ must apply each delta once — the algebraic root of the #4935 double-apply) and LWW (state-based join, **idempotent** ⇒ re-delivery-safe). | `crates/crdt/src/{counter,lww}.rs` |
| `MixedField.mixedCM` + `mixed_two_converge` / `mixed_three_converge` + `mixed_not_idempotent` | Same-document `Counter × LWW` is a componentwise product of the two #1041 field instances. It converges when both components receive the same operations, but the product is still non-idempotent because the counter component still needs exactly-once dedup. The cross-field stale whole-document materialization hazard is modeled red/green by `MixedFieldMaterialization.tla`. | `crates/crdt/src/composite.rs`; `crates/crdt/src/{counter,lww}.rs` |
| `DocumentMaterialization.active_age_after_delete_keeps_deleted` / `delete_active_age_converge` | Document status is a component of materialization: active field rematerialization may update retained bytes, but cannot clear a tombstone. | `crates/db-merge/src/merge_handler/composite_persist.rs`; `crates/crdt/src/composite.rs` |

`#print axioms` status checked with Lean 4.18:

- `lwwState_merge_assoc`: `[propext]`
- `word64Add_assoc`: `[propext, Quot.sound]`
- `word64Add_not_idempotent`: `[Lean.ofReduceBool]`
- `composite_merge_assoc`: `[propext]`
- `reconciled_merge_ge_committed`: `[propext, Quot.sound]`
- `unreconciled_merge_can_clobber`: *(no axioms)*

Generic `CrdtField` core:

- `CrdtField.swap`: *(no axioms)*
- `CrdtField.two_converge`: *(no axioms)*
- `CrdtField.three_converge`: *(no axioms)*
- `CrdtField.dup_absorb`: *(no axioms)*
- `CrdtField.nonidem_has_dup_witness`: `[propext, Classical.choice, Quot.sound]`

Field instantiations:

- `CounterReconcile.counterCM`: `[propext]`
- `CounterReconcile.counter_not_idempotent`: `[propext, Quot.sound]`
- `CounterReconcile.counter_two_converge`: `[propext]`
- `CounterReconcile.counter_three_converge`: `[propext]`
- `PriorityReconcile.lwwCM`: `[propext, Classical.choice, Quot.sound]`
- `PriorityReconcile.lww_idempotent`: `[propext, Classical.choice, Quot.sound]`
- `PriorityReconcile.lww_dup_safe`: `[propext, Classical.choice, Quot.sound]`
- `PriorityReconcile.lww_two_converge`: `[propext, Classical.choice, Quot.sound]`
- `PriorityReconcile.lww_three_converge`: `[propext, Classical.choice, Quot.sound]`
- `MixedField.mixedCM`: `[propext, Classical.choice, Quot.sound]`
- `MixedField.mixed_two_converge`: `[propext, Classical.choice, Quot.sound]`
- `MixedField.mixed_three_converge`: `[propext, Classical.choice, Quot.sound]`
- `MixedField.mixed_not_idempotent`: `[propext, Classical.choice, Quot.sound]`
- `MixedField.mixed_lww_component_dup_safe`: `[propext, Classical.choice, Quot.sound]`
- `DocumentMaterialization.active_age_after_delete_keeps_deleted`: *(no axioms)*
- `DocumentMaterialization.delete_active_age_converge`: *(no axioms)*

`nonidem_has_dup_witness` is the generic core's only classical lemma (it pulls in
`Classical.choice` via `Classical.byContradiction`), and it is the trivial
existential dual of the `Idempotent` definition — it is NOT consumed by any field
convergence or dedup result. The counter results (`counterCM`,
`counter_not_idempotent`, `counter_*_converge`) are fully constructive
(`Classical.choice`-free). The LWW results also depend on `Classical.choice`, but
independently: it enters through the `lwwMerge` case analysis, not through
`nonidem_has_dup_witness`. The mixed-field product inherits that same footprint
from its LWW component.

`Classical.choice`, `propext`, and `Quot.sound` are the three built-in axioms of
Lean 4's core logic, not project-defined axioms. No theorem uses `sorry` or any
custom (project-defined) axiom. Float32/Float64 counter laws are not claimed here
because IEEE-754 addition is not generally associative.

## Adding a new CRDT field

This PR is the reference template. A new field's proof (composite, object,
PN-counter) follows the same recipe — the canonical exemplars are the counter
(op-based, `DefraConvergence/CounterReconcile.lean`), LWW (state-based,
`DefraConvergence/PriorityReconcile.lean`), and the first follow-up product
instance (`DefraConvergence/MixedField.lean`):

1. **Define your merge and prove its two laws.** Implement the field's binary
   merge and prove it commutative + associative, then package it:
   `def fooCM : CrdtField.CommMerge V := { merge := …, comm := …, assoc := … }`.
2. **Settle the idempotence axis.** A state-based join (componentwise / lexicographic
   max — like LWW) is idempotent: prove `CrdtField.Idempotent fooCM` and inherit
   `dup_absorb` (re-delivery safe, NO dedup). An op-based field (delta accumulation —
   like the counter) is NOT idempotent: prove `¬ CrdtField.Idempotent fooCM`, which
   makes the per-delta dedup obligation explicit (discharged upstream by the
   merged-set / `is_merged` guard).
3. **Derive convergence by instantiation only.** Get `foo_two_converge` from
   `CrdtField.two_converge fooCM` and `foo_three_converge` from
   `CrdtField.three_converge fooCM`. Both fields expose the IDENTICAL convergence
   set — no field-specific reasoning in these theorems. ("Implement `CommMerge`,
   plug in, inherit the same named theorems" is the copyable invariant.)
4. **Concurrency axis (TLA+).** Copy `proofs/tla/TwoStoreCounter.tla`'s structure:
   one `CONSTANT` knob per hazard, so each interleaving hazard is toggled
   independently and shown RED/GREEN.
5. **Behavioral conformance leg.** If the field is numeric/counter-like, reuse
   `run_counter_storm` (`proofs/tests/support.rs`) — its exact-sum oracle is the
   behavioral template for the COUNTER FAMILY. For a non-numeric field
   (LWW/composite/object) reuse only its connect/replicate/seed/round scaffolding
   and supply the field's own convergence predicate (a last-writer-wins assertion,
   componentwise equality, etc.) in place of the numeric exact-sum oracle.
6. **Code plug-point.** The new field's local-write handling plugs into
   `crates/db/src/auto_commit_mutator/helpers.rs` — `apply_local_counter_deltas`
   (update path) and `init_counter_stores_on_create` (create path) are the counter
   exemplars — reached through the single `write_local_update` / `write_local_create`
   chokepoint that ALL local-write mutators (auto-commit, batch, explicit-txn) call.
   Add the field-CRDT logic THERE (the shared chokepoint), NOT in the individual
   per-mutator files, so the single-store invariant stays enforced by construction.

`DefraConvergence/PriorityReconcile.lean` proves the invariant behind the
same-doc convergence bug this project found and fixed: a field's priority lives
in two stores (the headstore, advanced by local writes *and* merges; the
datastore LWW view, advanced only by merges). `reconciled_merge_ge_committed`
shows that re-seeding the view to the committed priority before merging
guarantees no delta can drop the field below its committed priority (no clobber);
`unreconciled_merge_can_clobber` is the constructive witness that, without the
reconcile, the bug is reachable — the live repro, encoded in Lean. The fix lives
in `crates/db-merge/src/merge_handler/lww.rs` (`seed_lww_from_existing_doc`).
