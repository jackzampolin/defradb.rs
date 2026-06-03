import Std

/-!
# IndexMaintenance_DESIGN — Secondary-index maintenance consistency (db-index)

Mathlib-free Lean model of the **index-maintenance consistency** invariant for
DefraDB's secondary indexes: after `IndexManager::on_document_update(old, new)`
runs, the set of stored index-entry tuples for the document equals
`extract_index_values(new)` **exactly** — every tuple that should exist for `new`
exists (none missing), and no tuple that belonged to `old` but not `new` survives
(no stale tuples).

This is the "stale-entry freedom" nugget flagged in `proofs/survey/db-index.md`
(candidate `index-maintenance-consistency`, medium priority). A bug here is a
silent query-correctness failure: a stale index tuple makes a deleted/changed
value still match an equality/range filter, returning a document that no longer
has that value (or, conversely, a missing tuple hides a live document from its
index).

## The exact mechanism modeled

`on_document_update` (Rust anchor below) does, per index, guarded by
`old_value_sets != new_value_sets`:

  1. compute `old_set := extract_index_values(old)`  and
     `new_set := extract_index_values(new)`  (each a `Vec<Vec<NormalValue>>`,
     i.e. a list of value-tuples — the Cartesian product over composite/array
     fields);
  2. for every `t ∈ old_set`: `index.delete(docId, t)`  (remove the stored entry);
  3. for every `t ∈ new_set`: `index.save(docId, t)`    (write the stored entry).

So the strategy is **delete-the-whole-old-set, then save-the-whole-new-set** —
*not* a minimal delta. This matches Go exactly: `collectionSimpleIndex.Update`
is literally `Delete(oldDoc); Save(newDoc)` (anchors below).

We model the stored index as a **set of tuples** (the entries for one docId, one
index) via a characteristic function `Tuple → Bool` (extensional set equality is
then exactly the property we want). We model `delete`/`save` as the obvious
point operations and prove the composition `saveAll new ∘ deleteAll old` maps any
prior store to exactly `new` — *provided* the prior store agrees with `old` on the
complement of `new` (the real precondition: the store was consistent with the old
document before the update). Crucially the order matters: save-after-delete is
what makes "save wins" recover every `new` tuple even if it was just deleted.

## Source anchors

Rust (`crates/db-index`, this worktree):
* `src/index_manager/mod.rs:412` `on_document_update` — the headline op.
  - `:427-428` compute `old_value_sets` / `new_value_sets` via `extract_index_values`.
  - `:430` guard `if old_value_sets != new_value_sets` (the no-op-on-equal short circuit).
  - `:431-436` `for old_values in &old_value_sets { index.delete(.., old_values) }`  (deleteAll old).
  - `:437-442` `for new_values in &new_value_sets { index.save(.., new_values) }`    (saveAll new).
  Modeled by `onDocumentUpdate := saveAll new ∘ deleteAll old`, plus
  `onDocumentUpdateGuarded` for the `old = new` short-circuit branch.
* `src/index_manager/value_extraction.rs:31` `extract_index_values` →
  `Vec<Vec<NormalValue>>` (the Cartesian product `cartesian_product`, `:229`).
  Modeled abstractly as the function `extract : Doc → List Tuple` (its internal
  determinism/algebra is the *separate* `index-value-extraction-determinism`
  slice; here it is a black box — all we need is that update uses the same
  `extract` for both old and new, which the code does).
* `src/index_manager/mod.rs:331-336` (`bulk_index`) and `:362-369`
  (`on_document_create`) — `save` writes one entry per tuple; modeled by `save`.
* `src/index_manager/mod.rs:463-470` (`on_document_delete`) — `delete` removes one
  entry per tuple; modeled by `delete`.

Go (`origin/develop`, live upstream — fetched via
`git -C …/defradb show origin/develop:internal/db/...`):
* `internal/db/collection_index.go:59` `updateDocIndex` =
  `deleteIndexedDoc(oldDoc); addDocToIndex(newDoc)` — same delete-then-save shape.
* `internal/db/index.go:379` `collectionSimpleIndex.Update` =
  `index.Delete(oldDoc); index.Save(newDoc)`. This is the wire-parity contract the
  Rust `on_document_update` mirrors. `:364` `Save` sets the key; `:394` `Delete`
  removes the key.

## What is the proof's content (why it is NOT vacuous)

The positive results are **not** trivially true:

* `onDocumentUpdate_correct` needs the precondition `agreesOff new prior old`
  (the store agreed with `old` outside `new`'s tuples). Without delete, that
  precondition is *useless* — which is exactly what the negative theorem exploits.
* `onDocumentUpdate_no_stale` isolates the half that a buggy "save-only"
  maintenance violates.

The NEGATIVE theorem `buggySaveOnly_leaves_stale` exhibits a concrete two-tuple
witness (`old = {a}`, `new = {b}`, `a ≠ b`) where the buggy maintenance that
**only saves new and never deletes old** (`buggySaveOnly := saveAll new`) leaves
the stale tuple `a` in the store — so the resulting store is NOT equal to `new`.
This pins the positive theorems to a real property: the `deleteAll old` step is
load-bearing, not decorative. A second negative, `deleteOnly_drops_kept`, shows
the dual bug (delete-only) drops tuples that should remain. Together they bracket
the correct delete-then-save composition.

We also prove the **order** matters: `saveThenDelete_buggy` shows that doing the
two phases in the *wrong* order (save new, THEN delete old) is wrong whenever a
tuple is in BOTH old and new (`extract(old) ∩ extract(new) ≠ ∅`), because the
final delete removes a tuple that must survive. The real code saves *after*
deleting (mod.rs:431 before :437), which is the correct order; this theorem shows
the code's ordering is necessary, not incidental.

## Verify

    cd proofs/lean && lake env lean IndexMaintenance.lean   # compiles, no errors, no `sorry`
    # axioms of the headline theorems (expect: only standard `propext`/`Quot`-style, no slice axioms):
    #   #print axioms IndexMaintenance.onDocumentUpdate_correct
    #   #print axioms IndexMaintenance.onDocumentUpdate_no_stale
    #   #print axioms IndexMaintenance.onDocumentUpdate_none_missing
    #   #print axioms IndexMaintenance.buggySaveOnly_leaves_stale
    #   #print axioms IndexMaintenance.saveThenDelete_buggy

## Integrator

Add `lean_lib IndexMaintenance` to `proofs/lean/lakefile.lean` (this slice writes
no shared files).

## Boundary / honest reach

* **Single index, single docId.** `on_document_update` loops over `self.indexes`
  and keys every entry by `doc_id`; entries for distinct (index, docId) live in
  disjoint key ranges (`IndexDataStoreKey`), so per-(index,docId) correctness
  composes to the whole store. We model one such range.
* **`extract` is a black box.** Its determinism/Cartesian-product algebra is the
  separate `index-value-extraction-determinism` candidate. Here we rely only on
  the code using the *same* `extract` for `old` and `new` (it does: same method,
  same `index.description()`, same `schema` — mod.rs:427-428).
* **Storage is faithful.** We assume `save`/`delete` actually set/clear the
  characteristic bit (the `txn.Datastore().Set/Delete` boundary). Storage
  durability/serializability is the `storage` SSI slice, not this one.
* **Set semantics, not multiset.** An index key either exists or not; re-saving an
  existing tuple is idempotent (sets the same empty-valued key). Modeling the store
  as `Tuple → Bool` is therefore faithful to the keyspace, not an oversimplification.
-/

namespace IndexMaintenance

open List

/-! ## The store: a set of index-entry tuples for one (index, docId)

`Tuple` is opaque — it stands for one `Vec<NormalValue>` row of
`extract_index_values` (one Cartesian-product combination). All we need is
decidable equality, which `NormalValue`/`Vec` have in Rust (`PartialEq`/`Eq`). We
model the stored entry SET as its characteristic function `Store := Tuple → Bool`;
two stores are equal iff they have the same members (`storeEq`). -/

abbrev Store (Tuple : Type) := Tuple → Bool

/-- Membership of a tuple in the stored entry set. -/
def mem {Tuple : Type} (s : Store Tuple) (t : Tuple) : Bool := s t

/-- Extensional store equality: same membership for every tuple. This is exactly
"the stored entry set equals X" — the property statement. -/
def storeEq {Tuple : Type} (s₁ s₂ : Store Tuple) : Prop := ∀ t, s₁ t = s₂ t

/-- The empty store (no entries). -/
def emptyStore {Tuple : Type} : Store Tuple := fun _ => false

/-! ## The store as a function of a tuple-set `List Tuple`

`extract_index_values` returns a `Vec<Vec<NormalValue>>` (`List Tuple`). The store
"of a tuple list" marks exactly the listed tuples present. Membership reduces to
`List` membership, which keeps the proofs grounded in the actual `Vec` the code
iterates. -/

variable {Tuple : Type} [DecidableEq Tuple]

/-- `storeOf ts` is the store whose members are exactly the tuples in `ts`. -/
def storeOf (ts : List Tuple) : Store Tuple := fun t => decide (t ∈ ts)

@[simp] theorem mem_storeOf (ts : List Tuple) (t : Tuple) :
    storeOf ts t = decide (t ∈ ts) := rfl

/-! ## The point operations: `save` and `delete`

`index.save(docId, t)` writes the entry for tuple `t` (sets present);
`index.delete(docId, t)` removes it (sets absent). On the characteristic function
these are the obvious updates. -/

/-- Save one tuple: the entry for `t` becomes present; others unchanged. -/
def save (s : Store Tuple) (t : Tuple) : Store Tuple :=
  fun u => if u = t then true else s u

/-- Delete one tuple: the entry for `t` becomes absent; others unchanged. -/
def delete (s : Store Tuple) (t : Tuple) : Store Tuple :=
  fun u => if u = t then false else s u

@[simp] theorem save_self (s : Store Tuple) (t : Tuple) : save s t t = true := by
  simp [save]

@[simp] theorem delete_self (s : Store Tuple) (t : Tuple) : delete s t t = false := by
  simp [delete]

theorem save_other (s : Store Tuple) (t u : Tuple) (h : u ≠ t) : save s t u = s u := by
  simp [save, h]

theorem delete_other (s : Store Tuple) (t u : Tuple) (h : u ≠ t) : delete s t u = s u := by
  simp [delete, h]

/-! ## The bulk operations: `saveAll` and `deleteAll`

The `for new_values in &new_value_sets { save(..) }` and
`for old_values in &old_value_sets { delete(..) }` loops. Folding over the tuple
list. -/

/-- Save every tuple in `ts` (the `saveAll new` loop, mod.rs:437-442). -/
def saveAll (s : Store Tuple) (ts : List Tuple) : Store Tuple :=
  ts.foldl (fun acc t => save acc t) s

/-- Delete every tuple in `ts` (the `deleteAll old` loop, mod.rs:431-436). -/
def deleteAll (s : Store Tuple) (ts : List Tuple) : Store Tuple :=
  ts.foldl (fun acc t => delete acc t) s

/-- After `saveAll ts`, a tuple is present iff it was already present OR it is in
`ts`. This is the workhorse for "none missing". -/
theorem mem_saveAll (ts : List Tuple) : ∀ (s : Store Tuple) (u : Tuple),
    saveAll s ts u = (s u || decide (u ∈ ts)) := by
  induction ts with
  | nil => intro s u; simp [saveAll]
  | cons t rest ih =>
      intro s u
      simp only [saveAll, foldl_cons] at *
      rw [ih (save s t) u]
      by_cases h : u = t
      · subst h; simp [save]
      · simp [save, h, List.mem_cons]

/-- After `deleteAll ts`, a tuple is present iff it was already present AND it is
NOT in `ts`. The workhorse for "no stale". -/
theorem mem_deleteAll (ts : List Tuple) : ∀ (s : Store Tuple) (u : Tuple),
    deleteAll s ts u = (s u && !decide (u ∈ ts)) := by
  induction ts with
  | nil => intro s u; simp [deleteAll]
  | cons t rest ih =>
      intro s u
      simp only [deleteAll, foldl_cons] at *
      rw [ih (delete s t) u]
      by_cases h : u = t
      · subst h; simp [delete]
      · simp [delete, h, List.mem_cons]

/-! ## The maintenance operation

`onDocumentUpdate old new prior` = run `deleteAll old` then `saveAll new` on the
prior store. Anchors: mod.rs:431-436 (delete loop) then :437-442 (save loop). The
save-after-delete ORDER is exactly the code's. -/

/-- `on_document_update`'s storage effect (the `old ≠ new` branch). -/
def onDocumentUpdate (old new : List Tuple) (prior : Store Tuple) : Store Tuple :=
  saveAll (deleteAll prior old) new

/-- The guarded form including the `if old_value_sets != new_value_sets` short
circuit (mod.rs:430): on `old = new` the store is untouched. -/
def onDocumentUpdateGuarded (old new : List Tuple) (prior : Store Tuple) : Store Tuple :=
  if old = new then prior else onDocumentUpdate old new prior

/-- Pointwise membership after the maintenance op: present iff in `new`, OR
(was present and not in `old`). The `save` step makes "in new" win unconditionally
over the preceding `delete`. -/
theorem mem_onDocumentUpdate' (old new : List Tuple) (prior : Store Tuple) (u : Tuple) :
    onDocumentUpdate old new prior u
      = (decide (u ∈ new) || (prior u && !decide (u ∈ old))) := by
  simp only [onDocumentUpdate]
  rw [mem_saveAll, mem_deleteAll]
  cases h : decide (u ∈ new) <;> simp [h] <;> rw [Bool.or_comm]

/-! ## The precondition

Before the update, the store was consistent with the OLD document: it had exactly
`old`'s tuples. The real call site guarantees this — the entries currently stored
for `docId` are precisely `extract(old)` (they were written by the previous
create/update). We capture the *minimal* needed fact: the store agrees with `old`
on every tuple NOT in `new`. (On tuples in `new` we need nothing — `save` will
fix them regardless.) -/

/-- `prior` agrees with the old tuple-set `old` on the complement of `new`. -/
def agreesOff (new : List Tuple) (prior : Store Tuple) (old : List Tuple) : Prop :=
  ∀ u, u ∉ new → prior u = decide (u ∈ old)

/-- The strong (and realistic) precondition: the store was exactly `old`. -/
def storedExactlyOld (prior : Store Tuple) (old : List Tuple) : Prop :=
  storeEq prior (storeOf old)

theorem storedExactlyOld_agreesOff (new : List Tuple) (prior : Store Tuple)
    (old : List Tuple) (h : storedExactlyOld prior old) : agreesOff new prior old := by
  intro u _; exact h u

/-! ## HEADLINE: maintenance leaves exactly `extract(new)`

After `on_document_update`, the stored entry set equals `storeOf new` — no stale,
none missing. Requires only `agreesOff new prior old`. -/

theorem onDocumentUpdate_correct (old new : List Tuple) (prior : Store Tuple)
    (h : agreesOff new prior old) :
    storeEq (onDocumentUpdate old new prior) (storeOf new) := by
  intro u
  rw [mem_onDocumentUpdate']
  simp only [mem_storeOf]
  by_cases hn : u ∈ new
  · simp [hn]
  · -- u ∉ new: result = prior u && !old. By agreesOff, prior u = decide (u ∈ old).
    rw [h u hn]
    by_cases ho : u ∈ old <;> simp [hn, ho]

/-- The guarded version (with the `old = new` short circuit) is also correct,
under the realistic `storedExactlyOld` precondition. On the `old = new` branch the
store is already `storeOf old = storeOf new`, so leaving it untouched is correct;
on the other branch `onDocumentUpdate_correct` applies. -/
theorem onDocumentUpdateGuarded_correct (old new : List Tuple) (prior : Store Tuple)
    (h : storedExactlyOld prior old) :
    storeEq (onDocumentUpdateGuarded old new prior) (storeOf new) := by
  unfold onDocumentUpdateGuarded
  by_cases he : old = new
  · subst he; simpa [storeEq] using h
  · simp only [he, if_false]
    exact onDocumentUpdate_correct old new prior (storedExactlyOld_agreesOff new prior old h)

/-! ## The two property halves, named explicitly (matching the prompt) -/

/-- NO STALE: no tuple from `old` that is not in `new` survives. -/
theorem onDocumentUpdate_no_stale (old new : List Tuple) (prior : Store Tuple)
    (h : agreesOff new prior old) :
    ∀ u, u ∉ new → onDocumentUpdate old new prior u = false := by
  intro u hu
  have := onDocumentUpdate_correct old new prior h u
  rw [this]; simp [hu]

/-- NONE MISSING: every tuple that should exist for `new` exists. -/
theorem onDocumentUpdate_none_missing (old new : List Tuple) (prior : Store Tuple) :
    ∀ u, u ∈ new → onDocumentUpdate old new prior u = true := by
  intro u hu
  rw [mem_onDocumentUpdate']; simp [hu]

/-! ## NEGATIVE THEOREMS — the bug-or-it-didn't-happen oracle

These pin the positive results to a real property by exhibiting nearby-wrong
variants that FAIL it. -/

/-- The buggy "save-only" maintenance: it saves `new` but NEVER deletes `old`
(drops the mod.rs:431-436 loop). -/
def buggySaveOnly (_old new : List Tuple) (prior : Store Tuple) : Store Tuple :=
  saveAll prior new

/-- The buggy "delete-only" maintenance: deletes `old` but never saves `new`. -/
def buggyDeleteOnly (old _new : List Tuple) (prior : Store Tuple) : Store Tuple :=
  deleteAll prior old

/-- The wrong-order variant: save `new` first, THEN delete `old`
(swaps the order of mod.rs:431-436 and :437-442). -/
def saveThenDelete (old new : List Tuple) (prior : Store Tuple) : Store Tuple :=
  deleteAll (saveAll prior new) old

/-- NEGATIVE (the headline bug): a save-only maintenance leaves stale tuples.
Concrete witness over `Tuple := Nat`: `old = [0]`, `new = [1]`, prior store was
exactly `old`. After `buggySaveOnly`, tuple `0` (a stale tuple, ∈ old, ∉ new) is
STILL present, so the store is NOT equal to `storeOf new`. -/
theorem buggySaveOnly_leaves_stale :
    ∃ (old new : List Nat) (prior : Store Nat),
      agreesOff new prior old ∧
      ¬ storeEq (buggySaveOnly old new prior) (storeOf new) := by
  refine ⟨[0], [1], storeOf [0], ?_, ?_⟩
  · intro u _; rfl
  · intro hcontra
    -- evaluate at the stale tuple 0
    have h0 := hcontra 0
    simp [buggySaveOnly, mem_saveAll, storeOf] at h0

/-- NEGATIVE (dual bug): a delete-only maintenance drops tuples that must remain.
Witness: `old = [0]`, `new = [0]` (the value is unchanged for this index), prior =
exactly `old`. `buggyDeleteOnly` removes tuple `0`, which is in `new` — so it is
missing. (Note: the real guard `old != new` would skip this case; the point is
that the delete-without-resave shape is unsound on its own.) -/
theorem deleteOnly_drops_kept :
    ∃ (old new : List Nat) (prior : Store Nat),
      storedExactlyOld prior old ∧
      ¬ storeEq (buggyDeleteOnly old new prior) (storeOf new) := by
  refine ⟨[0], [0], storeOf [0], ?_, ?_⟩
  · intro u; rfl
  · intro hcontra
    have h0 := hcontra 0
    simp [buggyDeleteOnly, mem_deleteAll, storeOf] at h0

/-- NEGATIVE (order matters): doing save-then-delete (wrong order) is unsound
whenever a tuple is in BOTH `old` and `new` — the final delete removes a tuple
that must survive. Witness: `old = [0]`, `new = [0, 1]` (tuple 0 kept, tuple 1
added), prior = exactly `old`. `saveThenDelete` saves {0,1} then deletes {0},
leaving only {1} — but `new` needs both 0 and 1, so tuple 0 is missing. This
shows the code's save-AFTER-delete order (mod.rs: delete loop precedes save loop)
is necessary. -/
theorem saveThenDelete_buggy :
    ∃ (old new : List Nat) (prior : Store Nat),
      storedExactlyOld prior old ∧
      ¬ storeEq (saveThenDelete old new prior) (storeOf new) := by
  refine ⟨[0], [0, 1], storeOf [0], ?_, ?_⟩
  · intro u; rfl
  · intro hcontra
    -- tuple 0 ∈ new but is deleted last ⇒ absent in result, present in storeOf new
    have h0 := hcontra 0
    simp [saveThenDelete, mem_deleteAll, mem_saveAll, storeOf] at h0

/-- And the CORRECT order is sound on the SAME witness that breaks the wrong order,
demonstrating the two are genuinely distinguished (not both-wrong / both-right). -/
theorem correct_order_ok_on_witness :
    storeEq (onDocumentUpdate [0] [0, 1] (storeOf [0])) (storeOf ([0, 1] : List Nat)) := by
  apply onDocumentUpdate_correct
  intro u _; rfl

end IndexMaintenance
