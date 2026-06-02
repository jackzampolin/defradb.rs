# Convergence Proofs

The proof split is intentional:

- TLA+ (`specs/Convergence.tla`) models distributed delivery: partitions,
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

`#print axioms` status checked with Lean 4.18:

- `lwwState_merge_assoc`: `[propext]`
- `word64Add_assoc`: `[propext, Quot.sound]`
- `word64Add_not_idempotent`: `[Lean.ofReduceBool]`
- `composite_merge_assoc`: `[propext]`

No theorem uses `sorry` or custom axioms. Float32/Float64 counter laws are not
claimed here because IEEE-754 addition is not generally associative.
