import Std

/-!
# OrderEncoding.Bytes — lexicographic byte-string order

The substrate for the whole slice: a byte is a `Nat` in `[0,255]` (we never need
the wrap-around algebra of `UInt8`; the encoders below only emit concrete bytes and
big-endian limbs, both of which we can reason about as `Nat`s in range), and an
encoded key is a `List Nat`. `Lex` is the standard dictionary order on `List Nat`
used by every KV backend when it compares raw keys (`redb`/`fjall`/`rocksdb` all
compare stored keys as `&[u8]` via the platform `memcmp`, i.e. unsigned-byte
lexicographic order).

`Lex` here is *exactly* `memcmp` order:
* a strict prefix is `<` its extensions (shorter-prefix-first),
* otherwise the first differing byte decides, by unsigned `Nat` `<`.

Anchors (the comparison this models):
* Rust: backends compare keys as byte slices; e.g. `crates/storage/src/backends/memory`
  uses a `BTreeMap<Vec<u8>, …>` whose `Ord` on `Vec<u8>` is `memcmp`-lex.
* The encoders under test live in `crates/storage/src/encoding/`.
-/

namespace OrderEncoding

/-- An encoded key: a list of bytes. We keep bytes as `Nat` (values in `0..255`). -/
abbrev Bytes := List Nat

/-- Strict lexicographic ("dictionary" / `memcmp`) order on byte strings.
    Mirrors how every KV backend orders raw `&[u8]` keys. -/
inductive Lex : Bytes → Bytes → Prop
  /-- The empty string precedes any non-empty string (shorter prefix sorts first). -/
  | nil  {b : Nat} {ys : Bytes} : Lex [] (b :: ys)
  /-- Equal heads: order decided by the tails. -/
  | cons {a : Nat} {xs ys : Bytes} (h : Lex xs ys) : Lex (a :: xs) (a :: ys)
  /-- First differing byte decides, by `Nat` `<` (unsigned byte order). -/
  | head {a b : Nat} {xs ys : Bytes} (h : a < b) : Lex (a :: xs) (b :: ys)

infix:50 " <ₗ " => Lex

/-- `memcmp`-lex is irreflexive. -/
theorem Lex.irrefl : ∀ {xs : Bytes}, ¬ (xs <ₗ xs) := by
  intro xs h
  induction xs with
  | nil => cases h
  | cons a xs ih =>
    cases h with
    | cons h => exact ih h
    | head h => exact (Nat.lt_irrefl a h)

/-- `memcmp`-lex is transitive. -/
theorem Lex.trans : ∀ {xs ys zs : Bytes}, xs <ₗ ys → ys <ₗ zs → xs <ₗ zs := by
  intro xs ys zs hxy hyz
  induction hxy generalizing zs with
  | nil =>
    cases hyz with
    | cons _ => exact Lex.nil
    | head _ => exact Lex.nil
  | @cons a xs ys _ ih =>
    cases hyz with
    | cons h => exact Lex.cons (ih h)
    | head h => exact Lex.head h
  | @head a b xs ys hab =>
    cases hyz with
    | cons _ => exact Lex.head hab
    | head h => exact Lex.head (Nat.lt_trans hab h)

/-- Two distinct single-byte heads order by the byte value, regardless of tails.
    This is the workhorse for cross-type marker ordering: distinct leading markers
    decide the comparison no matter what follows. -/
theorem lex_of_head_lt {a b : Nat} (h : a < b) (xs ys : Bytes) :
    (a :: xs) <ₗ (b :: ys) := Lex.head h

/-- Inversion for a comparison of two cons cells with (possibly distinct) heads:
    either the heads differ (`a < b`) or they are equal and the tails decide.
    This is the form used to relate big-endian limb order to `Nat` order without
    tripping dependent elimination on the equal-head constructor. -/
theorem Lex.cons_inv {a b : Nat} {xs ys : Bytes} (h : (a :: xs) <ₗ (b :: ys)) :
    a < b ∨ (a = b ∧ xs <ₗ ys) := by
  cases h with
  | cons h => exact Or.inr ⟨rfl, h⟩
  | head h => exact Or.inl h

/-- Builder: equal heads with ordered tails. -/
theorem Lex.cons_eq {a : Nat} {xs ys : Bytes} (h : xs <ₗ ys) :
    (a :: xs) <ₗ (a :: ys) := Lex.cons h

/-- Trichotomy on the heads: if heads are equal, the comparison reduces to the tails. -/
theorem lex_cons_iff {a : Nat} {xs ys : Bytes} :
    (a :: xs) <ₗ (a :: ys) ↔ xs <ₗ ys := by
  constructor
  · intro h
    cases h with
    | cons h => exact h
    | head h => exact absurd h (Nat.lt_irrefl a)
  · intro h; exact Lex.cons h

end OrderEncoding
