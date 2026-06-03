import Std

/-!
# Cid_DESIGN — CID content-addressing determinism + Block canonicalization

Mathlib-free Lean model of the content-addressing guarantee that the
**Convergence** (`tla/`) and **Integrity** (`tla/Integrity.tla`) slices *assume*:
that "same content ⇒ same CID" and "same content ⇒ same DocID" hold deterministically,
independent of how a block's heads/links happened to be ordered in memory.

This slice discharges that assumption abstractly. The SHA-256 / DAG-CBOR primitives
are the **assumed boundary** (their injectivity-modulo-collision is *not* modeled —
that is the standard crypto-hash assumption recorded in `proofs/README.md`
"Crypto boundary"). What we DO prove, from the structure of `Block::new`, is:

  (1) `Block::new` produces a **unique normal form** (sorted heads, sorted links,
      empty→`None`), so it is a function of the *multiset* of inputs only —
      input link/head ordering cannot change the block. (Theorem
      `block_new_order_independent`.)

  (2) Equal blocks ⇒ equal canonical encoding ⇒ equal CID, and distinct canonical
      content ⇒ distinct CID (injectivity *modulo the hash*). (Theorems
      `equal_block_equal_cid`, `cid_injective_mod_hash`.)

  (3) Composing (1)+(2): the CID of a `Block::new` result is independent of input
      head/link ordering. (Theorem `block_new_cid_order_independent`.)

  (4) The merkle batch-root (`compute_merkle_root`) sort step is likewise
      order-independent over distinct CIDs. (Theorem `merkle_input_order_independent`.)

## Source anchors

Rust (`crates/defra-core`, this worktree):
* `src/block.rs:80`  `Block::new` — sorts heads by `cid.to_string()` (`:83`), sorts
  links via `Ord` (`:92`), empty→`None` (`:84-88`, `:93-97`). Modeled by `Block.new`.
* `src/block.rs:225` `impl Ord for DAGLink` — orders by `link.to_string()` then `name`.
  Modeled by `DagLink` order key `(link, name)`.
* `src/block.rs:136` `Block::generate_cid` → `to_dag_cbor` (`:123`) then
  `generate_cid_from_bytes` (`:354`): SHA2-256 → multihash → CIDv1/DAG-CBOR.
  Modeled by `cidOf := hashOf ∘ encode` over the **normal form**.
* `src/batch_signing.rs:26` `compute_merkle_root` — sorts CIDs before reduction
  (`:31-32`). Modeled by `merkleSortKeys`.

Go (`origin/develop`, the live upstream — fetched via
`git -C …/defradb show origin/develop:internal/core/block/block.go`):
* `block.go:205` `func New(delta, links, heads...)` — `sort.Slice(heads … strings.Compare(String()))`
  (`:208`), `sort.Slice(links … strings.Compare(Cid.String()))` (`:222`), empty→`nil`
  (`:234-247`). Identical normal-form construction: this is the wire-parity contract.
* `block.go:298` `GenerateLink()` → DAG-CBOR encode + CIDv1/SHA2-256.

## What is the proof's content (why it is NOT vacuous)

The normalization is modeled as a *strict* sort (CIDs are content-addressed and so
have distinct sort keys — distinct strings). Strict-sorted permutations are unique
(`eq_of_lt_sorted_perm`), which is the load-bearing fact. The NEGATIVE theorem
`buggy_new_not_order_independent` exhibits a concrete witness where a variant that
*skips* the sort (the bug the real `sort_by_cached_key` call prevents) produces two
*different* blocks from the same inputs in different orders — hence different CIDs.
So the positive results pin down a real property, not a trivially-true one.

## Verify

    cd proofs/lean && lake env lean Cid.lean      # compiles, no errors, no `sorry`
    # axioms of the headline theorems (standard only):
    #   #print axioms block_new_cid_order_independent
    #   #print axioms cid_injective_mod_hash
    #   #print axioms buggy_new_not_order_independent

## Integrator

Add `lean_lib Cid` to `proofs/lean/lakefile.lean` (this slice writes no shared files).
-/

namespace Cid

open List

/-! ## Boundary: strict-sorted permutations are unique

CIDs are content addresses with distinct string keys; the sort in `Block::new` is
therefore a strict sort. Two strictly-sorted lists that are permutations of each
other are equal. This is the only nontrivial list fact we need and we prove it
ourselves (mathlib-free) so the slice has no hidden dependency. -/

theorem eq_of_lt_sorted_perm :
    ∀ {l1 l2 : List Nat},
      Pairwise (· < ·) l1 → Pairwise (· < ·) l2 → l1 ~ l2 → l1 = l2 := by
  intro l1
  induction l1 with
  | nil => intro l2 _ _ hp; simpa using hp.symm.eq_nil
  | cons a t ih =>
    intro l2 h1 h2 hp
    cases l2 with
    | nil => simp at hp
    | cons b s =>
      have hmem_a : a ∈ b :: s := hp.mem_iff.mp (mem_cons_self a t)
      have hmem_b : b ∈ a :: t := hp.symm.mem_iff.mp (mem_cons_self b s)
      have hab : a = b := by
        rcases List.mem_cons.mp hmem_a with h | h
        · exact h
        · rcases List.mem_cons.mp hmem_b with h' | h'
          · exact h'.symm
          · exfalso
            -- h : a ∈ s, h' : b ∈ t — both heads dominate, contradiction
            have hlt1 : b < a := (List.pairwise_cons.mp h2).1 a h
            have hlt2 : a < b := (List.pairwise_cons.mp h1).1 b h'
            exact absurd (Nat.lt_trans hlt1 hlt2) (Nat.lt_irrefl b)
      subst hab
      have hts : t ~ s := (List.perm_cons a).mp hp
      rw [ih (List.pairwise_cons.mp h1).2 (List.pairwise_cons.mp h2).2 hts]

/-! ## CID model

A `Cid` is modeled by its sort key — a `Nat`. The only structure used is the strict
total order that the Rust/Go `cid.to_string()` comparison induces (content addresses
are distinct ⇒ keys are distinct). The SHA/CBOR bytes themselves are below the
abstraction boundary. -/

abbrev Cid := Nat

/-- The `≤` Bool comparator the model sorts with. The Rust/Go sort key is the CID
    string under `strings.Compare`; modeled here as the `Nat` order. Using the total
    preorder `≤` (rather than strict `<`) makes `mergeSort` total; on the distinct
    (`Nodup`) keys of a real head/link set it strengthens to strict order below. -/
def leKey (a b : Cid) : Bool := decide (a ≤ b)

/-- `Block::new`'s head/link normalization: sort by key, then empty→`none`.
    Sorting via `mergeSort` matches a deterministic stable sort; `none` matches the
    `Option<Vec<_>>` empty→`None` collapse (`block.rs:84-88`, `block.go:234-247`). -/
def normalize (xs : List Cid) : Option (List Cid) :=
  match xs.mergeSort leKey with
  | []      => none
  | y :: ys => some (y :: ys)

/-- `leKey` is a transitive, total Bool order, so `mergeSort` produces a `Pairwise (≤)`
    list; with distinct elements (`Nodup`) this strengthens to `Pairwise (· < ·)`, the
    strict form `eq_of_lt_sorted_perm` consumes. -/
theorem mergeSort_lt_pairwise {xs : List Cid} (hnodup : xs.Nodup) :
    Pairwise (· < ·) (xs.mergeSort leKey) := by
  have htrans : ∀ a b c : Cid, leKey a b = true → leKey b c = true → leKey a c = true := by
    intro a b c hab hbc
    simp only [leKey, decide_eq_true_eq] at *
    exact Nat.le_trans hab hbc
  have htotal : ∀ a b : Cid, (leKey a b || leKey b a) = true := by
    intro a b
    simp only [leKey, Bool.or_eq_true, decide_eq_true_eq]
    exact Nat.le_total a b
  -- Sorted under ≤.
  have hsorted := List.sorted_mergeSort (le := leKey) htrans htotal xs
  -- The sorted list inherits Nodup from the input (mergeSort is a permutation).
  have hnodup' : (xs.mergeSort leKey).Nodup :=
    (List.mergeSort_perm xs leKey).nodup_iff.mpr hnodup
  -- Combine `≤`-pairwise with Nodup (= `Pairwise (· ≠ ·)`) to get strict `<`-pairwise.
  have hle : Pairwise (fun a b => a ≤ b) (xs.mergeSort leKey) := by
    refine hsorted.imp ?_
    intro a b h; simpa [leKey, decide_eq_true_eq] using h
  have hne : Pairwise (fun a b => a ≠ b) (xs.mergeSort leKey) := hnodup'
  refine (hle.and hne).imp ?_
  intro a b h
  exact Nat.lt_of_le_of_ne h.1 h.2

/-- A `Block` in **normal form**: heads and links are already the normalized
    `Option (List Cid)`. Field order mirrors `block.rs:47-71` / `block.go` struct.
    `delta` is an opaque payload key (a `Nat`); the proofs are uniform in it. -/
structure Block where
  delta : Nat
  heads : Option (List Cid)
  links : Option (List Cid)
  encryption : Option Cid
  signature : Option Cid
deriving DecidableEq, Repr

/-- Faithful model of `Block::new` (`block.rs:80`, `block.go:205`): normalize the
    two raw input lists; `encryption`/`signature` default to `none`. -/
def Block.new (delta : Nat) (heads links : List Cid) : Block :=
  { delta := delta
  , heads := normalize heads
  , links := normalize links
  , encryption := none
  , signature := none }

/-! ## (1) Normal-form / order-independence of `Block::new`

The heart: `normalize` depends only on the multiset of distinct inputs. -/

/-- `normalize` is invariant under input permutation, for duplicate-free input.
    (Content-addressed CIDs in a head/link set are distinct.) -/
theorem normalize_perm_invariant {xs ys : List Cid}
    (hx : xs.Nodup) (hy : ys.Nodup) (hp : xs ~ ys) :
    normalize xs = normalize ys := by
  -- Both sorts are strict-sorted permutations of each other ⇒ equal lists.
  have hpx : xs.mergeSort leKey ~ xs := List.mergeSort_perm xs leKey
  have hpy : ys.mergeSort leKey ~ ys := List.mergeSort_perm ys leKey
  have hsame : xs.mergeSort leKey ~ ys.mergeSort leKey :=
    (hpx.trans hp).trans hpy.symm
  have heq : xs.mergeSort leKey = ys.mergeSort leKey :=
    eq_of_lt_sorted_perm (mergeSort_lt_pairwise hx) (mergeSort_lt_pairwise hy) hsame
  unfold normalize
  rw [heq]

/-- **Headline (1): `Block::new` is order-independent.** Permuting the input heads
    and/or links (each duplicate-free) yields the *identical* block — hence (with the
    CID lemmas below) the same CID. Mirrors the determinism comments at
    `block.rs:81` / `block.go:206-207`. -/
theorem block_new_order_independent
    {delta : Nat} {h1 h2 l1 l2 : List Cid}
    (hh1 : h1.Nodup) (hh2 : h2.Nodup) (hl1 : l1.Nodup) (hl2 : l2.Nodup)
    (hph : h1 ~ h2) (hpl : l1 ~ l2) :
    Block.new delta h1 l1 = Block.new delta h2 l2 := by
  unfold Block.new
  rw [normalize_perm_invariant hh1 hh2 hph, normalize_perm_invariant hl1 hl2 hpl]

/-- `Block::new` is **idempotent on its own normalized output**: re-running `new`
    with the already-sorted heads/links (when present) reproduces the block. This is
    the "unique normal form" fixpoint property. -/
theorem block_new_is_normal_form
    {delta : Nat} {heads links : List Cid} (hh : heads.Nodup) (hl : links.Nodup) :
    let b := Block.new delta heads links
    Block.new delta (b.heads.getD []) (b.links.getD []) = b := by
  intro b
  -- Reduce both sides to `normalize` of an already-sorted (Nodup) list.
  have key : ∀ xs : List Cid, xs.Nodup →
      normalize ((normalize xs).getD []) = normalize xs := by
    intro xs hxs
    unfold normalize
    cases hm : xs.mergeSort leKey with
    | nil => simp
    | cons z zs =>
      -- getD [] of `some (z::zs)` is `z::zs`; its mergeSort is itself (already sorted).
      have hsorted : Pairwise (· < ·) (z :: zs) := by
        have := mergeSort_lt_pairwise hxs; rw [hm] at this; exact this
      have hpw : Pairwise (fun a b => leKey a b = true) (z :: zs) := by
        refine hsorted.imp ?_
        intro a b h; simp only [leKey, decide_eq_true_eq]; exact Nat.le_of_lt h
      have hself : (z :: zs).mergeSort leKey = z :: zs := List.mergeSort_of_sorted hpw
      simp [hm, hself]
  show Block.new delta (b.heads.getD []) (b.links.getD []) = b
  show Block.new delta ((Block.new delta heads links).heads.getD [])
        ((Block.new delta heads links).links.getD []) = Block.new delta heads links
  unfold Block.new
  simp only [Block.mk.injEq, true_and, and_true]
  exact ⟨key heads hh, key links hl⟩

/-! ## (2) CID injectivity modulo the hash

`encode` (canonical DAG-CBOR) and `hashOf` (SHA-256 multihash → CIDv1) are the
boundary primitives. We model them as injective functions on the **normal form**;
their composition `cidOf` therefore satisfies:  same content ⇔ same CID. -/

/-- Abstract canonical DAG-CBOR bytes. -/
abbrev Bytes := List Nat

/-- Injectivity, defined inline (mathlib's `Function.Injective` is unavailable in a
    mathlib-free build). -/
def Injective {α β : Type} (f : α → β) : Prop := ∀ ⦃a b⦄, f a = f b → a = b

/-- Canonical DAG-CBOR encoding of a normal-form block (`block.rs:123`,
    `block.go:Marshal`). Injective: canonical encodings are reversible to the block. -/
opaque encode : Block → Bytes

/-- DAG-CBOR is a canonical, reversible codec: distinct blocks encode to distinct
    bytes. (Boundary: deterministic-CBOR canonicality. Not proven here.) -/
axiom encode_injective : Injective encode

/-- SHA-256 multihash wrapped as a CIDv1 (`block.rs:354`, `block.go:GenerateLink`).
    Modeled as a function on the canonical bytes. -/
opaque hashOf : Bytes → Cid

/-- Collision-freedom of SHA-256 (the standard crypto-hash boundary assumption,
    recorded in `proofs/README.md`). Distinct canonical bytes ⇒ distinct CID. -/
axiom hashOf_injective : Injective hashOf

/-- The block's content address: `generate_cid = hash(dag_cbor(block))`
    (`block.rs:136-139`). -/
def cidOf (b : Block) : Cid := hashOf (encode b)

/-- **Headline (2a): equal blocks ⇒ equal CID.** The "same content ⇒ same CID" half
    that Convergence/Integrity assume. (Trivial direction, but it is the contract
    being discharged — and the model makes the dependency on `encode` explicit.) -/
theorem equal_block_equal_cid {a b : Block} (h : a = b) : cidOf a = cidOf b := by
  rw [h]

/-- **Headline (2b): CID injectivity modulo the hash.** Distinct canonical content
    ⇒ distinct CID: if two blocks share a CID they are the *same* block. This is the
    injectivity the content-addressing guarantee rests on; it holds *because* both
    `encode` and `hashOf` are injective (the crypto boundary). -/
theorem cid_injective_mod_hash : Injective cidOf := by
  intro a b h
  exact encode_injective (hashOf_injective h)

/-- Contrapositive form, as Convergence/Integrity use it: different blocks ⇒
    different CIDs (no two distinct blocks collide, modulo SHA). -/
theorem distinct_block_distinct_cid {a b : Block} (h : a ≠ b) : cidOf a ≠ cidOf b :=
  fun hc => h (cid_injective_mod_hash hc)

/-! ## (3) Composition: `Block::new` CID is order-independent -/

/-- **Headline (3): the CID of a `Block::new` result is independent of input head/link
    order.** This is the property Convergence ("same delta+parents ⇒ same block CID
    regardless of in-memory ordering") and Integrity ("a re-derived block hashes to the
    advertised CID") take for granted. Proven, not assumed. -/
theorem block_new_cid_order_independent
    {delta : Nat} {h1 h2 l1 l2 : List Cid}
    (hh1 : h1.Nodup) (hh2 : h2.Nodup) (hl1 : l1.Nodup) (hl2 : l2.Nodup)
    (hph : h1 ~ h2) (hpl : l1 ~ l2) :
    cidOf (Block.new delta h1 l1) = cidOf (Block.new delta h2 l2) :=
  equal_block_equal_cid (block_new_order_independent hh1 hh2 hl1 hl2 hph hpl)

/-! ## (4) Merkle batch-root input order independence

`compute_merkle_root` (`batch_signing.rs:26`) sorts the CID set first (`:31-32`),
so the reduction sees a canonical sequence. We model the sort step and show it is
order-independent over distinct CIDs (the same `eq_of_lt_sorted_perm` core). -/

def merkleSortKeys (cids : List Cid) : List Cid := cids.mergeSort leKey

/-- The sorted CID sequence fed to the merkle reduction is invariant under input
    permutation, so `compute_merkle_root` is order-independent (`merkle_root_deterministic`
    in `batch_signing.rs:147`). -/
theorem merkle_input_order_independent {xs ys : List Cid}
    (hx : xs.Nodup) (hy : ys.Nodup) (hp : xs ~ ys) :
    merkleSortKeys xs = merkleSortKeys ys := by
  unfold merkleSortKeys
  have hpx : xs.mergeSort leKey ~ xs := List.mergeSort_perm xs leKey
  have hpy : ys.mergeSort leKey ~ ys := List.mergeSort_perm ys leKey
  have hsame : xs.mergeSort leKey ~ ys.mergeSort leKey := (hpx.trans hp).trans hpy.symm
  exact eq_of_lt_sorted_perm (mergeSort_lt_pairwise hx) (mergeSort_lt_pairwise hy) hsame

/-! ## NEGATIVE / RED control — the proof is not vacuous

If `Block::new` *skipped* the sort (the bug `sort_by_cached_key` at `block.rs:83`
prevents), it would NOT be order-independent: two in-memory orderings of the same
heads produce different blocks, hence — via `cid_injective_mod_hash` — different CIDs.
We exhibit a concrete witness. This is the analogue of `float_add_not_assoc` /
`word64Add_not_idempotent`: a nearby-wrong variant that FAILS the headline property. -/

/-- The buggy constructor: stores heads/links verbatim (no sort, no empty→none). -/
def Block.newNoSort (delta : Nat) (heads links : List Cid) : Block :=
  { delta := delta
  , heads := some heads
  , links := some links
  , encryption := none
  , signature := none }

/-- **NEGATIVE: the unsorted variant is NOT order-independent.** There exist a
    duplicate-free head list and a permutation of it on which `newNoSort` yields two
    *different* blocks. Contrast `block_new_order_independent`, which the *real*
    (sorting) `Block::new` satisfies. -/
theorem buggy_new_not_order_independent :
    ∃ (delta : Nat) (h1 h2 : List Cid),
      h1.Nodup ∧ h2.Nodup ∧ h1 ~ h2 ∧
      Block.newNoSort delta h1 [] ≠ Block.newNoSort delta h2 [] := by
  refine ⟨0, [0, 1], [1, 0], ?_, ?_, ?_, ?_⟩
  · decide
  · decide
  · exact List.Perm.swap 1 0 []
  · decide

/-- And the bug propagates to the content address: the two orderings give distinct
    blocks, so by `cid_injective_mod_hash` they get distinct CIDs — the very
    determinism Convergence/Integrity rely on would break. -/
theorem buggy_new_breaks_cid_determinism :
    ∃ (delta : Nat) (h1 h2 : List Cid),
      h1 ~ h2 ∧
      cidOf (Block.newNoSort delta h1 []) ≠ cidOf (Block.newNoSort delta h2 []) := by
  obtain ⟨delta, h1, h2, _, _, hp, hne⟩ := buggy_new_not_order_independent
  exact ⟨delta, h1, h2, hp, distinct_block_distinct_cid hne⟩

/-- Dual sanity check: the *correct* `Block::new` does collapse exactly the witness
    above to one block (so the negative theorem really is about the missing sort, not
    about some other discrepancy). Proven via the order-independence theorem itself —
    no kernel evaluation of `mergeSort`. -/
example : Block.new 0 [0, 1] [] = Block.new 0 [1, 0] [] :=
  block_new_order_independent (by decide) (by decide) (by decide) (by decide)
    (List.Perm.swap 1 0 []) (List.Perm.refl [])

end Cid

-- Axiom audit of the headline theorems (must be standard only +
-- the two declared crypto-boundary axioms `encode_injective`, `hashOf_injective`).
#print axioms Cid.block_new_order_independent
#print axioms Cid.block_new_cid_order_independent
#print axioms Cid.cid_injective_mod_hash
#print axioms Cid.merkle_input_order_independent
#print axioms Cid.buggy_new_not_order_independent
#print axioms Cid.buggy_new_breaks_cid_determinism
