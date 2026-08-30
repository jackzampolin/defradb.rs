import Std

/-!
# HeadSet_DESIGN: concurrent collection-head transitions (db-block-builder)

Mathlib-free Lean model of the **head-set transition** DefraDB performs when it
appends a block to a collection's DAG, and of why the transition as written
cannot be run concurrently on regolith.

The companion TLA+ model (`proofs/tla/HeadSet.tla`) checks the temporal story:
two concurrent writers, one of which aborts under the current strategy. This
file carries the algebra the temporal model rests on, and in particular the two
facts a reader should not have to take on trust:

* two distinct writers have **disjoint write sets** under the derived strategy
  and **overlapping** ones under eager delete (`derived_writeSets_disjoint`,
  `eager_writeSets_overlap`). The second is the non-vacuity result: it is what
  makes the first worth having; and
* the derived strategy reaches the same head set by a different route, which is
  what makes it a refactor rather than a redefinition: appending a block makes
  it a head (`applyDerived_is_head`) and un-heads every parent it named
  (`applyDerived_parents_not_head`), which is exactly what the delete achieved.

## The exact mechanism modeled

`write_collection_block` (Rust anchor below) used to do:

  1. scan the collection's head prefix, collecting `col_heads` and
     `old_head_keys`;
  2. build a block whose parents are exactly `col_heads`;
  3. `set` the new block's head key;
  4. `delete` every key in `old_head_keys`.

Step 4 is the subject. Two transactions that scanned before either committed
both hold the same `old_head_keys`, so both issue the same delete. regolith
validates a transaction's write set at commit under every isolation level, so
the second transaction is refused.

The store this replaced had an explicit carve-out for the case, an
`IterOptions::with_commutative_set` flag its conflict tracker honoured, which
is why the behaviour changed rather than merely regressing in speed. The flag
is gone: regolith cannot honour it, and one that silently does nothing is a
promise of serializability semantics nothing keeps.

The derived strategy keeps steps 1 to 3 and drops step 4. In its place a writer
records, for each parent it observed, a marker naming **itself** as the block
that superseded that parent. Every key a writer writes is then a function of its
own block id, so no two writers can collide. The head set stops being maintained
and starts being computed: a stored block is a head exactly when nothing
supersedes it.

## Source anchors

The derived strategy is what the tree implements; the eager one is kept here
only so the model can show what it costs.

Rust (`crates/db`, this worktree):
* `src/block/heads.rs` `live_collection_heads` is `derivedHeads`. It walks the
  head prefix and the marker prefix together rather than testing each head, so
  the query is one pass over each range and holds neither.
* `src/block/heads.rs` `record_supersedes` is the `supersedes` half of
  `applyDerived`, and `src/block/builder/collection.rs:89-92` writes the head
  key, which is the other half.
* `src/block/builder/collection.rs:19` reads the heads, and `:46`
  `Block::new(payload, col_heads, links)` records them as the block's parents.
  Modeled by `Block.parents`, the relation the derived strategy reads instead
  of re-deriving by deletion.
* `src/block/heads.rs` `prune_superseded_heads` is `applyPrune`. It deletes a
  head key and the markers against it in one transaction, which is what
  `prune_markersOnly_resurrects` says it must.
* `src/merge/merge_handler/collection.rs` applies the same transition to a
  replicated block, so two peers pushing siblings do not collide either.

The eager loop the model proves defective is gone from the tree. Its shape is
preserved in `eagerWriteSet` and in
`proofs/tla/MC_HeadSet_Red_EagerDelete.cfg`.

Storage (`crates/storage`, this worktree):
* `src/corekv/types.rs` no longer carries `IterOptions::commutative_set`. It
  was a flag the removed backends honoured and regolith does not, and once
  nothing set it a flag promising overlap that no backend granted was worse
  than no flag. It is quoted below as history, not as an anchor.
* `src/backends/regolith/transaction.rs` `RegolithTxn::commit` maps
  `TransactionError::Conflict` to `Error::TxnConflict`. regolith validates the
  write set at every isolation level, which is why relaxing isolation does not
  help. `Conflict` is therefore modeled with no level parameter, and the TLA+
  companion checks the same three levels empirically.

## What is the proof's content (why it is NOT vacuous)

`applyDerived_parents_not_head` is the load-bearing result and it is not true by
construction: the two strategies reach the head set by different means, one by
removing a key and one by adding a relation that a query subtracts. They agree
only because a writer supersedes exactly the parents it recorded, which is
`supersedes_iff_parent`, and the proof goes through that lemma.

`applyDerived_is_head` carries its hypotheses honestly rather than assuming them
away: a block that named itself as a parent, or a store that already held a
marker against it, would not read back as a head, and those are stated as
`hself` and `hfresh` instead of being quietly excluded.

`eager_writeSets_overlap` is stated so the model cannot be read as proving
something about a strategy nobody uses. It exhibits the collision concretely:
given two writers that observed a common head, the intersection of their write
sets is inhabited. If someone "fixes" the conflict by changing `eagerWriteSet`
without changing what the code does, this theorem stops holding and says so.
-/

namespace HeadSet

/-- A block id. `Nat` stands in for a CID: all the model needs is decidable
equality and the ability to have distinct writers. -/
abbrev BlockId := Nat

/-- A key in the store. Keys are compared for equality and nothing else, which
is all a write-set conflict check looks at. -/
inductive Key where
  /-- `/heads/<collection>/<block>`: the block is a head candidate. -/
  | head (b : BlockId)
  /-- `/superseded/<parent>/<child>`: `child` names `parent` as a parent. -/
  | superseded (parent child : BlockId)
  deriving DecidableEq, Repr

/-- One block: its id and the parents it was built against. -/
structure Block where
  id : BlockId
  parents : List BlockId
  deriving Repr

/-- The store, as the two relations a head query needs. -/
structure Store where
  /-- Blocks with a stored head key. -/
  headKeys : List BlockId
  /-- `(parent, child)` pairs: `child` superseded `parent`. -/
  supersedes : List (BlockId × BlockId)
  deriving Repr

/-- Is `b` superseded by anything in the store? -/
def isSuperseded (s : Store) (b : BlockId) : Bool :=
  s.supersedes.any (fun p => p.1 == b)

/-- The derived head set: a stored head key is a head exactly when nothing
supersedes it. This is a query, not a maintained value. -/
def derivedHeads (s : Store) : List BlockId :=
  s.headKeys.filter (fun b => !isSuperseded s b)

/-- Applying one block under the derived strategy: add its head key, and one
marker per parent naming this block as the superseder. Nothing is removed. -/
def applyDerived (s : Store) (blk : Block) : Store :=
  { headKeys := blk.id :: s.headKeys
    supersedes := blk.parents.map (fun p => (p, blk.id)) ++ s.supersedes }

/-- Applying one block under the eager-delete strategy: add its head key and
remove the head key of every parent it observed. -/
def applyEager (s : Store) (blk : Block) : Store :=
  { headKeys := blk.id :: s.headKeys.filter (fun b => !blk.parents.contains b)
    supersedes := s.supersedes }

/-- The eager strategy's head set is simply whatever keys remain. -/
def eagerHeads (s : Store) : List BlockId := s.headKeys

/-! ## Write sets

What a transaction writes, which is exactly what regolith validates at commit.
-/

/-- Eager delete: the writer's own head key, plus a write to the head key of
every parent it observed. That second part is shared with any other writer that
observed the same parent. -/
def eagerWriteSet (blk : Block) : List Key :=
  Key.head blk.id :: blk.parents.map Key.head

/-- Derived: the writer's own head key, plus one marker per parent that names
the writer itself. Every key mentions `blk.id`. -/
def derivedWriteSet (blk : Block) : List Key :=
  Key.head blk.id :: blk.parents.map (fun p => Key.superseded p blk.id)

/-- Two transactions conflict when their write sets share a key. This is the
whole of regolith's commit check, and it takes no isolation level: see the note
on `eager_writeSets_overlap`. -/
def Conflict (a b : List Key) : Prop := ∃ k, k ∈ a ∧ k ∈ b

/-- The block a key is written by. Every key in a write set names its writer,
which is the property the disjointness result turns on. -/
def writerOf : Key -> BlockId
  | Key.head b => b
  | Key.superseded _ c => c

/-! ## Results -/

/-- Every key the derived strategy writes names the writing block. -/
theorem derived_key_writer (blk : Block) (k : Key) (hk : k ∈ derivedWriteSet blk) :
    writerOf k = blk.id := by
  unfold derivedWriteSet at hk
  rcases List.mem_cons.mp hk with h | h
  · subst h; rfl
  · obtain ⟨p, _, hp⟩ := List.mem_map.mp h
    subst hp; rfl

/-- **The fix.** Two writers with distinct block ids never write the same key,
whatever they observed. There is no carve-out and no tracker: the write sets
cannot intersect because every key names its writer. -/
theorem derived_writeSets_disjoint (a b : Block) (hne : a.id ≠ b.id) :
    ¬ Conflict (derivedWriteSet a) (derivedWriteSet b) := by
  rintro ⟨k, hka, hkb⟩
  apply hne
  rw [← derived_key_writer a k hka, derived_key_writer b k hkb]

/-- **Non-vacuity.** The strategy in the tree really does collide. Two writers
that observed a common head both write that head's key, so their write sets
intersect and regolith refuses the second. Relaxing the isolation level cannot
help, because this is a write-write overlap and regolith validates the write set
at every level.

If someone changes `eagerWriteSet` without changing what the code does, this
theorem stops holding and says so. -/
theorem eager_writeSets_overlap (a b : Block) (h : BlockId)
    (ha : h ∈ a.parents) (hb : h ∈ b.parents) :
    Conflict (eagerWriteSet a) (eagerWriteSet b) := by
  refine ⟨Key.head h, ?_, ?_⟩
  · exact List.mem_cons_of_mem _ (List.mem_map.mpr ⟨h, ha, rfl⟩)
  · exact List.mem_cons_of_mem _ (List.mem_map.mpr ⟨h, hb, rfl⟩)

/-- A marker is present exactly when the block recorded that parent, or the
store already held it. The derived relation is the parent relation written
down, not an independent structure. -/
theorem supersedes_iff_parent (s : Store) (blk : Block) (p : BlockId) :
    (p, blk.id) ∈ (applyDerived s blk).supersedes
      ↔ (p ∈ blk.parents ∨ (p, blk.id) ∈ s.supersedes) := by
  unfold applyDerived
  simp [List.mem_append, List.mem_map]

/-- **Parents stop being heads.** This is what replaces the delete: appending a
block un-heads every parent it named, without writing to those parents' keys.

The eager strategy achieved the same end by removing each parent's head key,
which is precisely the write that collides. -/
theorem applyDerived_parents_not_head (s : Store) (blk : Block) (p : BlockId)
    (hp : p ∈ blk.parents) :
    p ∉ derivedHeads (applyDerived s blk) := by
  intro hmem
  have hnot := (List.mem_filter.mp hmem).2
  have hsup : isSuperseded (applyDerived s blk) p = true := by
    unfold isSuperseded
    apply List.any_eq_true.mpr
    refine ⟨(p, blk.id), (supersedes_iff_parent s blk p).mpr (Or.inl hp), by simp⟩
  simp [hsup] at hnot

/-- **The new block is a head.** Appending it adds its head key, and nothing
supersedes it unless a later block names it, so it reads back as a tip. The
hypotheses are the two ways it could fail: the block naming itself as a parent,
or the store already holding a marker against it. -/
theorem applyDerived_is_head (s : Store) (blk : Block)
    (hself : blk.id ∉ blk.parents)
    (hfresh : ∀ q ∈ s.supersedes, q.1 ≠ blk.id) :
    blk.id ∈ derivedHeads (applyDerived s blk) := by
  apply List.mem_filter.mpr
  refine ⟨by simp [applyDerived], ?_⟩
  have hsup : isSuperseded (applyDerived s blk) blk.id = false := by
    unfold isSuperseded
    apply List.any_eq_false.mpr
    intro q hq
    have hne : q.1 ≠ blk.id := by
      unfold applyDerived at hq
      rcases List.mem_append.mp hq with h | h
      · obtain ⟨r, hr, hrq⟩ := List.mem_map.mp h
        subst hrq
        intro hcontra
        -- `q = (r, blk.id)`, so `hcontra : r = blk.id`; rewriting turns the
        -- parent membership of `r` into one for `blk.id`, which `hself` forbids.
        exact hself (hcontra ▸ hr)
      · exact hfresh q h
    simpa using hne
  simp [hsup]

/-! ## Reclamation

The derived strategy stops deleting, so a superseded head key stays until
something sweeps it. Without the sweep the headstore grows one key per mutation
and every append scans all of them, which is the cost the design actually has to
answer for on a small device.

The sweep is safe for a reason worth stating: it removes only keys the query
already ignores. `prune_preserves_derivedHeads` is that statement, and it is
conditional on `isSuperseded`, so a sweep that removed a live head would not
satisfy it.

`prune_markersOnly_resurrects` is the matching non-vacuity result: removing a
head's markers while leaving its head key makes it read as a head again. That is
why the two deletions belong to one transaction, and it is the case
`proofs/tla/MC_HeadSet_Red_MarkersOnly.cfg` fails on. -/

/-- Reclaim `b`: drop its head key and every marker against it. -/
def applyPrune (s : Store) (b : BlockId) : Store :=
  { headKeys := s.headKeys.filter (fun h => h != b)
    supersedes := s.supersedes.filter (fun q => q.1 != b) }

/-- Reclaim `b` by dropping only its markers. The defect the model checks. -/
def applyPruneMarkersOnly (s : Store) (b : BlockId) : Store :=
  { headKeys := s.headKeys
    supersedes := s.supersedes.filter (fun q => q.1 != b) }

/-- Reclaiming `b` does not change whether anything else is superseded: every
marker it removes names `b`, and no other head is asking about those. -/
theorem isSuperseded_prune (s : Store) (b h : BlockId) (hne : h ≠ b) :
    isSuperseded (applyPrune s b) h = isSuperseded s h := by
  unfold isSuperseded applyPrune
  rw [Bool.eq_iff_iff]
  simp only [List.any_eq_true, List.mem_filter, beq_iff_eq, bne_iff_ne, ne_eq]
  constructor
  · rintro ⟨q, ⟨hq, _⟩, heq⟩
    exact ⟨q, hq, heq⟩
  · rintro ⟨q, hq, heq⟩
    refine ⟨q, ⟨hq, ?_⟩, heq⟩
    intro hcontra
    exact hne (heq ▸ hcontra)

/-- **Reclamation is invisible.** Sweeping a superseded head key, together with
the markers against it, leaves the head set exactly as it was.

The hypothesis is the whole safety condition: `b` has to be superseded already.
Sweeping a live head would remove it from the answer, and this statement would
not hold of it. -/
theorem prune_preserves_derivedHeads (s : Store) (b : BlockId)
    (hsup : isSuperseded s b = true) (h : BlockId) :
    h ∈ derivedHeads (applyPrune s b) ↔ h ∈ derivedHeads s := by
  have hkey : ∀ x : BlockId, x ∈ (applyPrune s b).headKeys ↔ (x ∈ s.headKeys ∧ x ≠ b) := by
    intro x
    simp [applyPrune, List.mem_filter]
  unfold derivedHeads
  simp only [List.mem_filter, Bool.not_eq_true']
  constructor
  · rintro ⟨hmem, hnot⟩
    obtain ⟨hmem, hne⟩ := (hkey h).mp hmem
    exact ⟨hmem, by rwa [isSuperseded_prune s b h hne] at hnot⟩
  · rintro ⟨hmem, hnot⟩
    have hne : h ≠ b := by
      intro hcontra
      rw [hcontra, hsup] at hnot
      exact Bool.noConfusion hnot
    exact ⟨(hkey h).mpr ⟨hmem, hne⟩, by rwa [isSuperseded_prune s b h hne]⟩

/-- **Non-vacuity for reclamation.** Dropping a head's markers while keeping its
head key brings it back as a head. The two deletions are one transaction for
this reason and no other. -/
theorem prune_markersOnly_resurrects (s : Store) (b : BlockId)
    (hmem : b ∈ s.headKeys) :
    b ∈ derivedHeads (applyPruneMarkersOnly s b) := by
  apply List.mem_filter.mpr
  refine ⟨hmem, ?_⟩
  have hsup : isSuperseded (applyPruneMarkersOnly s b) b = false := by
    unfold isSuperseded applyPruneMarkersOnly
    apply List.any_eq_false.mpr
    intro q hq
    have := (List.mem_filter.mp hq).2
    simpa [bne_iff_ne, ne_eq] using this
  simp [hsup]

end HeadSet

/-! ## Axiom footprint

Printed so the foundations are visible rather than assumed. Nothing here should
reach beyond `propext`/`Classical.choice`, and nothing should be `sorryAx`.
-/

#print axioms HeadSet.derived_key_writer
#print axioms HeadSet.derived_writeSets_disjoint
#print axioms HeadSet.eager_writeSets_overlap
#print axioms HeadSet.supersedes_iff_parent
#print axioms HeadSet.applyDerived_parents_not_head
#print axioms HeadSet.applyDerived_is_head
#print axioms HeadSet.isSuperseded_prune
#print axioms HeadSet.prune_preserves_derivedHeads
#print axioms HeadSet.prune_markersOnly_resurrects
