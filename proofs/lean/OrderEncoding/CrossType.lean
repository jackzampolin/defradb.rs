import OrderEncoding.Markers

/-!
# OrderEncoding.CrossType — cross-type marker total order + descending inversion

## Cross-type ordering

Every encoded value is `marker :: body`. The markers occupy disjoint bands
(`OrderEncoding.Markers`), so when two encoded values have **different** markers the
comparison is decided by the marker alone — independent of either body. This is exactly
what makes a heterogeneous secondary index well-defined: a `null` key always sorts below
a `bytes` key below a `bool` key below an `int` key, regardless of the payloads.

We prove the general fact (`cross_type_decided_by_marker`) and instantiate the headline
chain `null < bytes < bool < int` (`cross_type_chain`).

## Descending inversion

The descending encoders are the ascending ones with order reversed. The two mechanisms
in the source are:
* flip the *value* before ascending-encoding (`encode_bool_descending(v)=asc(!v)`,
  bool.rs:14; `encode_varint_descending(v)=asc(!v)`, encode.rs:75-77), and
* ones-complement the payload bytes (`encode_bytes_descending`, bytes.rs:36-42:
  `ones_complement(buf[start+1..])`).

The unifying byte-level fact is: **applying ones-complement to each byte of two equal-length
keys reverses their lexicographic order** (`compl_inverts`). That is the reason every
"descending" variant inverts. We prove it for the byte level here; the per-value bool
inversion is already proved in `PerType` (`bool_desc_inverts`).
-/

namespace OrderEncoding

/-- A complete encoded key is `marker :: body`. -/
def keyed (marker : Nat) (body : Bytes) : Bytes := marker :: body

/-- **Cross-type comparison is decided by the marker** when markers differ.
    For any bodies `bx by`, if `mx < my` then `keyed mx bx <ₗ keyed my by`. -/
theorem cross_type_decided_by_marker {mx my : Nat} (h : mx < my) (bx by_ : Bytes) :
    keyed mx bx <ₗ keyed my by_ := by
  unfold keyed; exact Lex.head h

/-- The marker comparison is also *necessary* for cross-type ordering: if two keys with
    distinct markers order one way, it is because of the markers (the bodies are
    irrelevant). Stated as: distinct-marker order ↔ marker order. -/
theorem cross_type_iff_marker {mx my : Nat} (hne : mx ≠ my) (bx by_ : Bytes) :
    (keyed mx bx <ₗ keyed my by_) ↔ mx < my := by
  unfold keyed
  constructor
  · intro h
    cases h with
    | head h => exact h
    | cons _ => exact absurd rfl hne
  · intro h; exact Lex.head h

/-- The headline cross-type chain, instantiated at the real markers:
    `null < bytes < bool(false/true) < int`. Any bodies whatsoever. -/
theorem cross_type_chain (bn bb bf bt bi : Bytes) :
    keyed mNull bn <ₗ keyed mBytes bb
    ∧ keyed mBytes bb <ₗ keyed mFalse bf
    ∧ keyed mFalse bf <ₗ keyed mTrue bt
    ∧ keyed mTrue bt <ₗ keyed mIntZero bi := by
  refine ⟨?_, ?_, ?_, ?_⟩
  · exact cross_type_decided_by_marker (by decide) _ _
  · exact cross_type_decided_by_marker (by decide) _ _
  · exact cross_type_decided_by_marker (by decide) _ _
  · exact cross_type_decided_by_marker (by decide) _ _

/-! ## Descending: ones-complement inverts lexicographic order -/

/-- Per-byte ones-complement over the byte range `[0,255]` (`onesComplement`,
    bytes.rs:7-11 / Go bytes.go). Modeled as `255 - b` (valid for `b ≤ 255`). -/
def compl (b : Nat) : Nat := 255 - b

/-- Ones-complement of a key (each byte). -/
def complBytes : Bytes → Bytes
  | [] => []
  | (b :: bs) => compl b :: complBytes bs

/-- **Ones-complement inverts lex order** for equal-length, in-range keys:
    `complBytes xs <ₗ complBytes ys ↔ ys <ₗ xs`. The byte-level reason every
    descending variant reverses sort order. Proved by induction; requires the bytes
    to be `≤ 255` so that `255 - · ` is strictly antitone. -/
theorem compl_inverts : ∀ (xs ys : Bytes),
    (∀ b ∈ xs, b ≤ 255) → (∀ b ∈ ys, b ≤ 255) → xs.length = ys.length →
    (complBytes xs <ₗ complBytes ys ↔ ys <ₗ xs) := by
  intro xs
  induction xs with
  | nil =>
    intro ys _ _ hlen
    have hnil : ys = [] := List.length_eq_zero_iff.mp hlen.symm
    subst hnil
    exact ⟨fun h => (Lex.irrefl h).elim, fun h => (Lex.irrefl h).elim⟩
  | cons x xs ih =>
    intro ys hx hy hlen
    cases ys with
    | nil => exact absurd hlen (by simp)
    | cons y ys =>
      have hxb : x ≤ 255 := hx x (by simp)
      have hyb : y ≤ 255 := hy y (by simp)
      have hxs : ∀ b ∈ xs, b ≤ 255 := fun b hb => hx b (by simp [hb])
      have hys : ∀ b ∈ ys, b ≤ 255 := fun b hb => hy b (by simp [hb])
      have hlen' : xs.length = ys.length := by simpa using hlen
      simp only [complBytes]
      constructor
      · intro h
        rcases Lex.cons_inv h with hh | ⟨heq, ht⟩
        · -- compl x < compl y  ⇒  255-x < 255-y  ⇒  y < x
          have : y < x := by unfold compl at hh; omega
          exact Lex.head this
        · -- compl x = compl y ⇒ x = y; recurse on tails
          have hxy : x = y := by unfold compl at heq; omega
          subst hxy
          exact Lex.cons ((ih ys hxs hys hlen').mp ht)
      · intro h
        rcases Lex.cons_inv h with hh | ⟨heq, ht⟩
        · have : compl x < compl y := by unfold compl; omega
          exact Lex.head this
        · -- y = x ; recurse on tails
          subst heq
          exact Lex.cons ((ih ys hxs hys hlen').mpr ht)

end OrderEncoding
