# HeadSet: concurrent collection-head transitions

Family: `db-block-builder`. Companion Lean model: `proofs/lean/HeadSet/Core.lean`.

## The property

Two writers that append to the same collection without seeing each other must
both land, leaving two tips for a later merge to join. That is not a throughput
concern; it is what makes the head set a CRDT. A design in which one of them is
turned away has changed the data structure, not its speed.

## The mechanism

`crates/db/src/block/builder/collection.rs::write_collection_block` used to:

1. scan the head prefix, collecting `col_heads` and `old_head_keys`,
2. build a block whose parents are exactly `col_heads`,
3. `set` the new head key,
4. `delete` every observed old head key.

Step 4 was the subject. Two transactions that scanned before either committed
hold the same `old_head_keys` and issue the same delete. That is a write-write
conflict on one key.

Step 4 is gone. The tree now writes a supersede marker per parent
(`crates/db/src/block/heads.rs::record_supersedes`) and derives the head set on
read (`live_collection_heads`), with
`crates/db/src/merge/merge_handler/collection.rs` doing the same for a
replicated block.

The store regolith replaced carried a conflict tracker with an explicit
carve-out for this: `IterOptions::with_commutative_set` marked a prefix scan as
an observed-remove/add set transition, and the tracker then permitted
overlapping keys when both transactions marked the scan that way. regolith has
no such notion, so the carve-out had nothing behind it and the conflict was
real. The flag has since been removed rather than left as a no-op, because one
that silently grants nothing is a promise about serializability that no backend
keeps.

## Why relaxing isolation is not the fix

regolith validates a transaction's write set at commit under every isolation
level. Measured on this tree, the sibling-head test fails identically under
`ReadCommitted`, `SnapshotIsolation` and `Serializable`. The model therefore
takes no isolation parameter: a write-write overlap is refused at all three, and
the Lean companion states `Conflict` with no level argument for the same reason.

## The two configurations

**Theo: Reds here must stay red because that's how we prove that what Golang and previous code does is not correct by construction!!!!!**

**Even golang does eager delete, it is a possibility that fully commutative CRDT merge is not committed and rejected at txn layer. See below:**

| cfg | Strategy | Reclaim | verdict | why |
|---|---|---|---|---|
| `MC_HeadSet_Red_EagerDelete.cfg` | `EagerDelete` | `Together` | **RED** | Both writers write the seed's head key. `INV_NoWriteConflict` is violated: one writer aborts. |
| `MC_HeadSet_Green.cfg` | `Derived` | `Together` | **GREEN** | Every key a writer writes names that writer, so write sets are disjoint. Both commit; both are heads; the seed is not. Reclamation runs throughout and changes no answer. |
| `MC_HeadSet_Red_MarkersOnly.cfg` | `Derived` | `MarkersOnly` | **RED** | Reclamation that drops a head's markers but keeps its head key makes the superseded head read as live. `INV_HeadsExact` is violated. |

RED is the point of each pair. GREEN alone would not show that the derived
strategy is load-bearing rather than incidentally true in this configuration,
nor that "the head key and its markers leave together" is a requirement rather
than an implementation detail.

## The fix the GREEN configuration models

A writer stops deleting. It writes:

* its own head key, and
* one supersede marker per parent, naming **itself** as the superseder.

Both are functions of the writer's own block id, so two writers cannot collide.
The head set stops being maintained by deletion and becomes a query: a stored
head key is a head exactly when nothing supersedes it.

`INV_HeadsExact` is what keeps this a refactor rather than a redefinition. It
requires the head set a reader observes to equal the DAG's actual tips, computed
independently of either strategy as "stored blocks that no stored block names as
a parent". A derived head set that merely avoided conflicts while reporting the
wrong tips would fail it.

## Reclamation

Nothing deletes a superseded head key on the write path any more, so without a
sweep the headstore grows one key per mutation and every append scans all of
them. That is the real cost of the design on a device with a small memory
budget, and it is why `prune_superseded_heads` exists.

The sweep runs in a transaction of its own. That is not a detail: it writes keys
another sweep would also write, so it is the one path here that can conflict.
Folding it into an append would put the shared write back on the path that must
not have one. Losing the race costs nothing, because the head set is a function
of the markers and the next pass repeats the work.

`DB::maybe_prune_collection_heads` amortizes it over `HEAD_PRUNE_INTERVAL`
appends, which holds the backlog at a small constant per collection rather than
letting it track history length. The interval is a policy number chosen here,
not a parity constant: Go deletes inline and has nothing to reclaim.

`Prune` in the model covers the safety question, which is whether reclaiming
changes what a reader sees. `prune_preserves_derivedHeads` is the same statement
in Lean, and it carries `isSuperseded s b` as a hypothesis, so it says nothing
about sweeping a live head.

## What the model does not cover

* **Unbounded growth.** The model has a fixed finite block set, so it can show
  that reclaiming is safe but not that reclaiming keeps up. That the backlog
  stays bounded rests on the amortization interval and on every branchable
  mutation passing through
  `write_branchable_collection_block`; it is asserted by
  `crates/db/tests/block/heads.rs::a_bounded_pass_reports_what_it_left`, which
  is a convergence test rather than a proof.
* **Orphan markers.** A sweep that commits between another writer's scan and its
  commit leaves a marker whose parent has no head key. The sweep retains it on
  purpose, because a block replicated ahead of its parent is indistinguishable
  from an orphan. It is bounded by how often that interleaving happens, not by
  history length, and the GREEN model reaches the state and stays correct in it.
* **Merge-path siblings.** Siblings also arise from P2P replication applying
  remote blocks. That path goes through the merge handler, which performs the
  same transition, and convergence itself is covered by the convergence models.
