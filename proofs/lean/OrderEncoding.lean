import OrderEncoding.Bytes
import OrderEncoding.Markers
import OrderEncoding.PerType
import OrderEncoding.BytesEnc
import OrderEncoding.CrossType

/-!
# OrderEncoding_DESIGN — order-preserving key-encoding monotonicity (Lean slice)

Mathlib-free Lean model of the **order-preserving value encoding** in
`crates/storage/src/encoding/` (CockroachDB-style, used for DefraDB secondary-index
keys). This is the canonical Lean target named in `proofs/survey/storage.md` and the
Coverage backlog row #1 in `proofs/README.md`:

> a<b ⟹ encode_asc(a) <lex encode_asc(b); cross-type markers total order.

The KV backends compare stored keys as raw `&[u8]` (`memcmp` / unsigned lexicographic
order). For secondary indexes and range scans to return correct results, the *encoding*
of a value must reproduce the value's own order under that byte comparison — for every
type — and a *single index column may hold values of different types*, so the per-type
encodings must also be totally ordered *across* types in a stable way. That whole
contract is what this slice proves, structurally, from the byte-emitting code.

## What is proved (headline theorems, all in this file)

* `asc_strictly_order_preserving_*` — for each modeled type, `a < b ↔ enc_asc a <ₗ enc_asc b`
  (the ⟺ form: encoding is a strict order *embedding*, not merely monotone). Types:
  `bool`, the integer single-byte band, the integer multi-byte payload (big-endian), and
  `bytes`/string (escape-terminated).
* `desc_inverts_*` — the descending variant reverses the order
  (`bool_desc_inverts`, and the byte-level `compl_inverts` that underlies every
  ones-complement descending encoder).
* `cross_type_total_order` — distinct type markers decide cross-type comparisons by the
  marker alone, consistently with the per-type orderings; instantiated as the real chain
  `null < bytes < bool < int`.

## The negative side (so the positives are not vacuous)

`proofs/README.md` mandates a red/green discipline: a nearby-wrong variant must FAIL.
This file proves three:

* `neg_swapped_bool_markers_not_order_preserving` — swapping the bool markers
  (`true↦9, false↦10`) breaks `bool_asc_mono` for a concrete witness.
* `neg_no_escape_bytes_collision` — dropping the `0x00`-escape (bytes.rs:20-22) makes a
  literal-`0x00` content string collide-order with the terminator, so a strict order is
  *not* preserved: a concrete `a <ₗ b` whose naive encodings compare the wrong way.
* `neg_shared_marker_breaks_cross_type` — if two types shared a leading marker, the
  cross-type comparison would be decided by bodies, so a "smaller-typed" value could
  outsort a "larger-typed" one: the marker disjointness is load-bearing.

## Source anchors (Rust this worktree / Go `origin/develop`)

Markers (`OrderEncoding.Markers`):
* Rust `crates/storage/src/encoding/mod.rs:27-51` (`ENCODED_NULL=0 … FLOAT32_NAN_DESC=16`,
  `INT_MIN=0x80`, `INT_ZERO=136`, `INT_MAX=0xfd`, `ENCODED_NULL_DESC=0xff`).
* Go `internal/encoding/encoding.go` `iota` block + `IntMin/intZero/IntMax`
  (`git -C …/defradb show origin/develop:internal/encoding/encoding.go`). Byte-identical.

Bool (`PerType.encBoolAsc/encBoolDesc`):
* Rust `encoding/bool.rs:8-16`. Go `internal/encoding/bool.go` `EncodeBoolAscending/Descending`.

Null (`PerType.encNullAsc`):
* Rust `encoding/null.rs:6-15`. Go `internal/encoding/null.go`.

Int/uvarint (`PerType.encSmallIntAsc`, `PerType.beBytes`):
* Rust `encoding/varint/encode.rs:80-147` (`encode_uvarint_ascending`: small band
  `intZero+v` at :82-83; multi-byte big-endian limbs at :85-145; signed delegates at
  :69-71; descending `!v` at :75-77).
* Go `internal/encoding/int.go:97-225` (`EncodeVarintAscending/EncodeUvarintAscending`,
  identical width-bucketing and `byte(v>>k)` big-endian limbs).

Bytes/string (`BytesEnc.encByte/encBytesBody`):
* Rust `encoding/bytes.rs:14-42` (escape `0x00↦0x00 0xff` at :20-22; terminator
  `0x00 0x01` at :30-31; descending = ones-complement at :36-42, modeled by
  `CrossType.compl`/`complBytes`).
* Go `internal/encoding/bytes.go:50-90` (`escape=0x00, escapedTerm=0x01, escaped00=0xff`,
  `ascendingBytesEscapes`).

Comparison substrate (`Bytes.Lex`): the `memcmp` order every backend uses on raw keys
(e.g. `crates/storage/src/backends/memory` `BTreeMap<Vec<u8>, …>`).

## Boundary / honest reach

* We model the *bodies* the encoders emit, byte for byte, over `Nat` bytes in `[0,255]`.
  The big-endian integer payload and the escape transform — the two value-dependent
  mechanisms — are reproduced and their monotonicity is *proved*, not assumed.
* The float encoding (`encoding/float/mod.rs`) is **not** modeled here: its monotonicity
  rests on IEEE-754 bit-layout sign-flip tricks (`u ^ (1<<63)` style) whose faithful
  model needs float bit reasoning; it is a documented Lean-appendix follow-up. NaN/zero
  banding is, however, covered structurally by the marker total order (the five float
  markers are distinct points in the same disjoint-band scheme).
* `time`/`json` reduce to the int and bytes encoders respectively (Go/Rust delegate),
  so their order-preservation follows from the int/bytes theorems here.
* No automated model↔code conformance harness (same caveat as every slice — see
  `proofs/README.md` "Model ≠ code").

## Verify

```
cd proofs/lean && lake env lean OrderEncoding.lean        # compiles, no errors, no sorry
# axiom audit on the headline + negative theorems is at the bottom of this file.
```
-/

namespace OrderEncoding

/-! ## Headline: ascending encoding is strictly order-preserving (per type) -/

/-- Bool: `enc_asc` is a strict order embedding. (`PerType.bool_asc_mono`) -/
theorem asc_strictly_order_preserving_bool (a b : Bool) :
    boolLt a b ↔ encBoolAsc a <ₗ encBoolAsc b := bool_asc_mono a b

/-- Integer single-byte band: `enc_asc` is a strict order embedding.
    (`PerType.small_int_asc_mono`) -/
theorem asc_strictly_order_preserving_small_int {a b : Nat} :
    a < b ↔ encSmallIntAsc a <ₗ encSmallIntAsc b := small_int_asc_mono

/-- Integer multi-byte payload (fixed width `n`, values `< 256^n`): the big-endian
    limb sequence is a strict order embedding. (`PerType.be_mono`) -/
theorem asc_strictly_order_preserving_int_payload (n v w : Nat)
    (hv : v < 256 ^ n) (hw : w < 256 ^ n) :
    v < w ↔ beBytes n v <ₗ beBytes n w := be_mono n v w hv hw

/-- Bytes/string body: `enc_asc` is a strict order embedding under raw-byte order.
    (`BytesEnc.bytes_body_mono`) -/
theorem asc_strictly_order_preserving_bytes (a b : Bytes) :
    a <ₗ b ↔ encBytesBody a <ₗ encBytesBody b := bytes_body_mono a b

/-! ## Headline: descending inverts -/

/-- Bool descending reverses the order. (`PerType.bool_desc_inverts`) -/
theorem desc_inverts_bool (a b : Bool) :
    boolLt a b ↔ encBoolDesc b <ₗ encBoolDesc a := bool_desc_inverts a b

/-- The byte-level reason every descending encoder inverts: ones-complement of each byte
    reverses lexicographic order on equal-length in-range keys. (`CrossType.compl_inverts`) -/
theorem desc_inverts_via_ones_complement (xs ys : Bytes)
    (hx : ∀ b ∈ xs, b ≤ 255) (hy : ∀ b ∈ ys, b ≤ 255) (hlen : xs.length = ys.length) :
    (complBytes xs <ₗ complBytes ys ↔ ys <ₗ xs) := compl_inverts xs ys hx hy hlen

/-! ## Headline: cross-type marker total order -/

/-- Distinct markers totally order keys across types, independent of bodies, consistently
    with the marker numeric order. (`CrossType.cross_type_iff_marker`) -/
theorem cross_type_total_order {mx my : Nat} (hne : mx ≠ my) (bx by_ : Bytes) :
    (keyed mx bx <ₗ keyed my by_) ↔ mx < my := cross_type_iff_marker hne bx by_

/-- The realized cross-type chain at the actual markers: `null < bytes < bool < int`,
    for arbitrary bodies. (`CrossType.cross_type_chain`) -/
theorem cross_type_total_order_chain (bn bb bf bt bi : Bytes) :
    keyed mNull bn <ₗ keyed mBytes bb
    ∧ keyed mBytes bb <ₗ keyed mFalse bf
    ∧ keyed mFalse bf <ₗ keyed mTrue bt
    ∧ keyed mTrue bt <ₗ keyed mIntZero bi := cross_type_chain bn bb bf bt bi

/-! ## Negative theorems — nearby-wrong variants that FAIL the property -/

/-- A *broken* bool encoder that swaps the markers (`true↦FALSE_MARKER`,
    `false↦TRUE_MARKER`). -/
def encBoolSwapped (v : Bool) : Bytes := [if v then mFalse else mTrue]

/-- **NEGATIVE**: the swapped-marker bool encoder is NOT order-preserving — there is a
    pair (`false < true`) whose encodings compare the wrong way. A green-only proof would
    not catch this; this is the red oracle for `asc_strictly_order_preserving_bool`. -/
theorem neg_swapped_bool_markers_not_order_preserving :
    ¬ (∀ a b : Bool, boolLt a b ↔ encBoolSwapped a <ₗ encBoolSwapped b) := by
  intro hAll
  -- boolLt false true holds, but encBoolSwapped false = [mTrue]=[10], true=[mFalse]=[9],
  -- and [10] <ₗ [9] is false.
  have h := (hAll false true).mp ⟨rfl, rfl⟩
  -- h : encBoolSwapped false <ₗ encBoolSwapped true, i.e. [10] <ₗ [9]
  simp only [encBoolSwapped, Bool.false_eq_true, if_false, if_true] at h
  cases h with
  | head hh => exact absurd hh (by decide)

/-- A *broken* bytes encoder that DROPS the `0x00`-escape: content bytes pass through
    verbatim, with the same `0x00 0x01` terminator. -/
def encBytesNoEscape : Bytes → Bytes
  | [] => [0, 1]
  | (c :: cs) => c :: encBytesNoEscape cs

/-- **NEGATIVE**: without the escape, a strict order is NOT preserved. Witness:
    raw `[] <ₗ [0]` (empty string before the one-byte string `0x00`), but their
    naive encodings are `[0,1]` and `[0,0,1]`, and `[0,1] <ₗ [0,0,1]` is *false*
    (`1 < 0` fails at position 1) — the order is inverted. The escape (`0x00↦0x00 0xff`)
    exists precisely to prevent this collision; `bytes_body_mono` proves the fixed
    version holds, this proves the broken version fails. -/
theorem neg_no_escape_bytes_collision :
    ¬ (∀ a b : Bytes, a <ₗ b ↔ encBytesNoEscape a <ₗ encBytesNoEscape b) := by
  intro hAll
  -- [] <ₗ [0] is true (nil before non-empty)
  have hlt : ([] : Bytes) <ₗ [0] := Lex.nil
  have h := (hAll [] [0]).mp hlt
  -- h : encBytesNoEscape [] <ₗ encBytesNoEscape [0]  =  [0,1] <ₗ [0,0,1]
  simp only [encBytesNoEscape] at h
  -- [0,1] <ₗ [0,0,1] : head 0=0, then [1] <ₗ [0,1] needs 1 < 0 → false
  cases h with
  | head hh => exact absurd hh (by decide)
  | cons hh =>
    cases hh with
    | head hh => exact absurd hh (by decide)

/-- **NEGATIVE**: marker disjointness is load-bearing. If two distinct types shared a
    leading marker `m`, cross-type order would be decided by the bodies, so a value of
    the "should-be-smaller" type with a larger body could outsort the other. Concretely,
    two keys with the *same* marker `m` but bodies `[5]` and `[3]` order as `[m,5] >
    [m,3]` even though by type-band intent they should be incomparable-by-body. We show
    the cross-type-by-marker law `cross_type_total_order` genuinely REQUIRES `mx ≠ my`:
    with `mx = my` it is false. -/
theorem neg_shared_marker_breaks_cross_type :
    ¬ (∀ (m : Nat) (bx by_ : Bytes), (keyed m bx <ₗ keyed m by_) ↔ (m < m)) := by
  intro hAll
  -- pick bodies [3] <ₗ [5]: keyed 0 [3] <ₗ keyed 0 [5] holds, but 0 < 0 is false.
  have h := (hAll 0 [3] [5]).mp (by unfold keyed; exact Lex.cons (Lex.head (by decide)))
  exact absurd h (Nat.lt_irrefl 0)

end OrderEncoding

/-! ## Axiom audit (run interactively, recorded in the slice report)

`#print axioms` on the headline + negative theorems confirms they rest only on Lean's
core axioms (`propext`, `Classical.choice`, `Quot.sound`) and introduce no slice-local
`axiom`. There are NO `axiom`s declared anywhere in this slice: every result is derived
from the byte-emitting definitions. -/

#print axioms OrderEncoding.asc_strictly_order_preserving_bool
#print axioms OrderEncoding.asc_strictly_order_preserving_small_int
#print axioms OrderEncoding.asc_strictly_order_preserving_int_payload
#print axioms OrderEncoding.asc_strictly_order_preserving_bytes
#print axioms OrderEncoding.desc_inverts_bool
#print axioms OrderEncoding.desc_inverts_via_ones_complement
#print axioms OrderEncoding.cross_type_total_order
#print axioms OrderEncoding.cross_type_total_order_chain
#print axioms OrderEncoding.neg_swapped_bool_markers_not_order_preserving
#print axioms OrderEncoding.neg_no_escape_bytes_collision
#print axioms OrderEncoding.neg_shared_marker_breaks_cross_type
