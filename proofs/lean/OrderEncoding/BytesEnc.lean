import OrderEncoding.Bytes

/-!
# OrderEncoding.BytesEnc — escape-based bytes/string encoding monotonicity

The string/`[]byte` encoder is the structurally trickiest order-preserving case
(`crates/storage/src/encoding/bytes.rs:14`, Go `internal/encoding/bytes.go:50`):

* prefix `BYTES_MARKER` (handled by the cross-type layer; omitted here — we model the
  *body*, i.e. the bytes after the marker);
* each input byte is copied verbatim **except** `0x00`, which is escaped to the pair
  `0x00 0xff` (`ESCAPE, ESCAPED_00`, bytes.rs:20-22 / Go bytes.go:87);
* the body is terminated by `0x00 0x01` (`ESCAPE, ESCAPED_TERM`, bytes.rs:30-31).

Why this is order-preserving: after the leading marker, the next byte of the encoded
form is either a content byte `c`, or — at end of input — the terminator byte `0x00`.
Because `0x00` (terminator lead) is **strictly below** every byte that a non-empty
continuation can present at that position *that is itself ≥ 0x00*, a string that is a
prefix of another sorts first. And content bytes copy through verbatim, so among equal
length they compare exactly as raw bytes.

The single subtlety the escape exists to defuse: a literal `0x00` content byte would
otherwise collide with the terminator's lead `0x00`. The escape rewrites content `0x00`
to `0x00 0xff`; since the terminator is `0x00 0x01` and `0x01 < 0xff`, *terminating*
(end of the shorter string) still sorts below *continuing with a literal `0x00`*. We
model the body transform exactly and prove monotonicity over the resulting `Lex` order.

We model bytes as `Nat` in `[0,255]`; the encoder's `0x00`-escape is the only
value-dependent branch, faithfully reproduced in `encByte`.
-/

namespace OrderEncoding

/-- Escape transform of a single content byte (bytes.rs:20-26 / bytes.go:84-90):
    `0x00 ↦ [0x00, 0xff]`, every other byte ↦ itself. -/
def encByte (c : Nat) : Bytes := if c = 0 then [0, 255] else [c]

/-- Encode the *body* of a bytes value: escape each byte, then append the terminator
    `0x00 0x01` (bytes.rs:14-32). The leading `BYTES_MARKER` is supplied by the
    cross-type layer, so it is intentionally not part of the body. -/
def encBytesBody : Bytes → Bytes
  | [] => [0, 1]
  | (c :: cs) => encByte c ++ encBytesBody cs

/-- **Bytes-encoding monotonicity** (the order-preserving law for strings/bytes):
    for all content strings `a b`, `a <ₗ b ↔ encBytesBody a <ₗ encBytesBody b`.

    Proof is by induction on `a`, casing on `b` and on whether the leading content
    bytes are `0x00` (the escape branch). The terminator/escape constants are chosen
    exactly so each case lands the right way; the proof would *fail* if the terminator
    second byte were ≥ the escape-continuation byte (see `BytesEnc` negative check
    in the barrel). -/
theorem bytes_body_mono : ∀ a b : Bytes, a <ₗ b ↔ encBytesBody a <ₗ encBytesBody b := by
  intro a
  induction a with
  | nil =>
    intro b
    cases b with
    | nil =>
      simp only [encBytesBody]
      exact ⟨fun h => (Lex.irrefl h).elim, fun h => (Lex.irrefl h).elim⟩
    | cons d ds =>
      -- [] <ₗ (d::ds) is always true; need encBytesBody [] = [0,1] <ₗ encBytesBody (d::ds).
      simp only [encBytesBody]
      constructor
      · intro _
        -- encByte d starts with either d (≠0) or 0; terminator starts with 0.
        unfold encByte
        by_cases hd : d = 0
        · subst hd
          simp only [if_pos rfl]
          -- [0,1] ++ ... vs (0::255::...) : heads 0=0, then 1 < 255
          exact Lex.cons (Lex.head (by decide))
        · simp only [if_neg hd]
          -- [0,1,...] vs (d :: ...) with d ≠ 0, so d ≥ 1 > 0 ⇒ head 0 < d
          exact Lex.head (Nat.pos_of_ne_zero hd)
      · intro _; exact Lex.nil
  | cons c cs ih =>
    intro b
    cases b with
    | nil =>
      -- (c::cs) <ₗ [] is false; encBytesBody (c::cs) <ₗ [0,1] must also be false.
      simp only [encBytesBody]
      constructor
      · intro h; cases h
      · intro h
        exfalso
        unfold encByte at h
        by_cases hc : c = 0
        · subst hc
          simp only [if_pos rfl] at h
          -- (0::255::...) vs (0::1::...) : head eq, then 255 < 1 is false
          cases h with
          | head h => exact absurd h (by decide)
          | cons h => cases h with
            | head h => exact absurd h (by decide)
        · simp only [if_neg hc] at h
          -- (c :: ...) vs (0 :: 1) with c ≠ 0 ⇒ head c < 0 impossible
          cases h with
          | head h => exact absurd h (Nat.not_lt_zero c)
          | cons h => exact absurd (Nat.pos_of_ne_zero hc) (by
              -- heads forced equal: c = 0, contradiction
              intro _; exact hc rfl)
    | cons d ds =>
      -- the main inductive case: compare leading content bytes c vs d.
      simp only [encBytesBody]
      by_cases hc : c = 0 <;> by_cases hd : d = 0
      · -- c = 0, d = 0 : both escape to 0::255::...; heads/2nd equal, recurse.
        subst hc; subst hd
        have e0 : encByte 0 = [0, 255] := rfl
        rw [e0]
        show ((0:Nat) :: cs) <ₗ (0 :: ds) ↔
          ([0, 255] ++ encBytesBody cs) <ₗ ([0, 255] ++ encBytesBody ds)
        rw [lex_cons_iff]
        -- [0,255]++X = 0::255::X
        show cs <ₗ ds ↔ ((0:Nat) :: 255 :: encBytesBody cs) <ₗ (0 :: 255 :: encBytesBody ds)
        rw [lex_cons_iff, lex_cons_iff]
        exact ih ds
      · -- c = 0, d ≠ 0 : raw head 0 < d ; encoded head 0 < d as well.
        subst hc
        simp only [encByte, if_pos rfl, if_neg hd, List.cons_append, List.nil_append]
        constructor
        · intro _; exact Lex.head (Nat.pos_of_ne_zero hd)
        · intro _; exact Lex.head (Nat.pos_of_ne_zero hd)
      · -- c ≠ 0, d = 0 : raw head c > 0 = d, so c::cs ≮ 0::ds ; encoded likewise.
        subst hd
        simp only [encByte, if_neg hc, if_pos rfl, List.cons_append, List.nil_append]
        constructor
        · intro h
          cases h with
          | head h => exact absurd h (Nat.not_lt_zero c)
          | cons h => exact absurd hc (by simp_all)
        · intro h
          cases h with
          | head h => exact absurd h (Nat.not_lt_zero c)
          | cons h => exact absurd hc (by simp_all)
      · -- c ≠ 0, d ≠ 0 : both copy through; compare c vs d, recurse on tie.
        simp only [encByte, if_neg hc, if_neg hd, List.cons_append, List.nil_append]
        constructor
        · intro h
          cases h with
          | head h => exact Lex.head h
          | cons h => exact Lex.cons ((ih ds).mp h)
        · intro h
          cases h with
          | head h => exact Lex.head h
          | cons h => exact Lex.cons ((ih ds).mpr h)

end OrderEncoding
