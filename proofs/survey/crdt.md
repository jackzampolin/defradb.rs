# Survey: `crates/crdt/`

## Purpose
Delta-state CRDTs for DefraDB.rs field/document conflict resolution: LWW register,
Counter (single accumulated value, Int64 wrapping + Float32/64 IEEE-754), and
Composite (document-level coordination of field CRDTs). Plus priority varint codec
and the `Delta`/`ReplicatedData`/`MergeResult` traits. CRDTs are storage-less; they
operate on a borrowed `ReaderWriter` transaction.

## State machines
- **LWW merge** (`lww.rs`): on each delta compare `(priority, value-bytes)`. Higher
  priority wins; tie -> lexicographically greater value wins (empty/tombstone loses).
  Outcomes: `Applied | RejectedLowerPriority | RejectedTieBreak`.
- **Counter merge** (`counter.rs`): unconditional addition; idempotency is delegated
  upstream to the durable merged-CID gate (`db-merge`), not local.
- **Composite merge** (`composite.rs`): pre-validate all fields, then apply each field
  delta componentwise (LWW / Counter / Delete). Document status `ACTIVE(1)/DELETED(2)`.

## Candidates
| name | kind | property | already-modeled | priority |
|---|---|---|---|---|
| LWW merge is a join (comm/assoc/idem) | Lean | order-independent convergence over resolved key | yes (`lwwState_merge_*`) | — |
| Counter Int64 add laws | Lean | wrapping add comm/assoc; raw merge not idempotent | yes (`word64Add_*`) | — |
| Float counter non-convergence | Lean | IEEE-754 add not associative -> divergence | yes (`float_add_not_assoc`) | — |
| Composite componentwise merge | Lean | document state merges per-field as a product join | yes (`composite_merge_*`) | — |
| Applied-set idempotency layer | Lean | durable merged-CID set supplies idempotence | yes (`appliedSet_merge_*`) | — |
| LWW tie-break key is a total order | Lean | `(u64 priority, lex value-bytes)` is a deterministic total order, so the `resolvedKey:=max` abstraction is faithful | NO (abstracted as `Rank`, not derived) | low |

## Verdict
**Model-worthy — but essentially already covered.** This crate is the canonical Rust
source for the existing Lean convergence slice; every merge law that needs a proof
beyond integration tests is already proven there. The only uncovered sliver is that the
Lean model *assumes* the LWW `(priority, value-bytes)` comparison forms a total order
(modeled as `resolvedKey : Nat` with `merge = max`) rather than deriving it from the
byte-level lexicographic comparison. That gap is low priority: total order of
`(u64, &[u8])` lex is standard and the tie-break is exercised by tests. The priority
varint codec and trait plumbing are pure IO/glue — no model needed.
