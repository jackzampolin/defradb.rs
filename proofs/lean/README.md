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

`#print axioms` status checked with Lean 4.18:

- `lwwState_merge_assoc`: `[propext]`
- `word64Add_assoc`: `[propext, Quot.sound]`
- `word64Add_not_idempotent`: `[Lean.ofReduceBool]`
- `composite_merge_assoc`: `[propext]`
- `reconciled_merge_ge_committed`: `[propext, Quot.sound]`
- `unreconciled_merge_can_clobber`: *(no axioms)*

No theorem uses `sorry` or custom axioms. Float32/Float64 counter laws are not
claimed here because IEEE-754 addition is not generally associative.

`DefraConvergence/PriorityReconcile.lean` proves the invariant behind the
same-doc convergence bug this project found and fixed: a field's priority lives
in two stores (the headstore, advanced by local writes *and* merges; the
datastore LWW view, advanced only by merges). `reconciled_merge_ge_committed`
shows that re-seeding the view to the committed priority before merging
guarantees no delta can drop the field below its committed priority (no clobber);
`unreconciled_merge_can_clobber` is the constructive witness that, without the
reconcile, the bug is reachable — the live repro, encoded in Lean. The fix lives
in `crates/db-merge/src/merge_handler/lww.rs` (`seed_lww_from_existing_doc`).
