import OrderEncoding.Bytes

/-!
# OrderEncoding.Markers — the per-type leading marker bytes

The first byte of every encoded value is a *type marker*. These constants are
copied verbatim from the source (the `iota` block in Go / the `const` block in Rust);
their numeric *values* are what makes cross-type comparison a stable total order, so
we pin them exactly and let Lean check the inequalities by `decide` rather than
asserting them.

Anchors:
* Rust: `crates/storage/src/encoding/mod.rs:27-51`
  (`ENCODED_NULL = 0` … `FLOAT32_NAN_DESC = 16`, then the int range
  `INT_MIN = 0x80`, `INT_ZERO = 136`, `INT_MAX = 0xfd`, `ENCODED_NULL_DESC = 0xff`).
* Go: `git show origin/develop:internal/encoding/encoding.go` — the `iota` block
  `encodedNull = iota … float32NaNDesc` and the `IntMin/intZero/IntMax/encodedNullDesc`
  constants. Byte-for-byte identical: this is the wire-parity contract.
-/

namespace OrderEncoding

/-- `ENCODED_NULL` — mod.rs:27 / Go `encodedNull` (= 0). -/
def mNull : Nat := 0
/-- `BYTES_MARKER` — mod.rs:33 / Go `bytesMarker` (= 6). -/
def mBytes : Nat := 6
/-- `TIME_MARKER` — mod.rs:35 / Go `timeMarker` (= 8). -/
def mTime : Nat := 8
/-- `FALSE_MARKER` — mod.rs:36 / Go `falseMarker` (= 9). -/
def mFalse : Nat := 9
/-- `TRUE_MARKER` — mod.rs:37 / Go `trueMarker` (= 10). -/
def mTrue : Nat := 10
/-- `INT_MIN` — mod.rs:46 / Go `IntMin` (= 0x80 = 128): start of the integer marker range. -/
def mIntMin : Nat := 128
/-- `INT_ZERO` — mod.rs:48 / Go `intZero` (= 136): single-byte zero/small-int marker base. -/
def mIntZero : Nat := 136
/-- `INT_MAX` — mod.rs:50 / Go `IntMax` (= 0xfd = 253): end of the integer marker range. -/
def mIntMax : Nat := 253

/-!
## The cross-type total order on markers

The whole point of the marker bytes: because each type occupies a *disjoint* band of
leading-byte values, comparing two keys of different types is decided by the first byte
alone, consistently and transitively. We expose the exact ordering the source bakes in:

    null(0) < bytes(6) < time(8) < false(9)=bool < true(10) < int-band([128,253])

and check it with `decide` so the claim is grounded in the literal constant values,
not in a hand-asserted lemma.
-/

theorem markers_null_lt_bytes  : mNull  < mBytes  := by decide
theorem markers_bytes_lt_time  : mBytes  < mTime   := by decide
theorem markers_time_lt_false  : mTime   < mFalse  := by decide
theorem markers_false_lt_true  : mFalse  < mTrue   := by decide
/-- Bool's two markers are adjacent and below the integer band. -/
theorem markers_true_lt_intMin : mTrue   < mIntMin := by decide
/-- The integer single-byte band sits strictly inside `[IntMin, IntMax]`. -/
theorem markers_intMin_le_zero : mIntMin ≤ mIntZero := by decide
theorem markers_intZero_le_max : mIntZero ≤ mIntMax := by decide

/-- Sanity: the marker total order is consistent (a representative transitive chain).
    `decide` evaluates the literal constants, so this fails loudly if any constant drifts. -/
theorem markers_chain : mNull < mBytes ∧ mBytes < mTime ∧ mTime < mFalse
    ∧ mFalse < mTrue ∧ mTrue < mIntMin := by decide

end OrderEncoding
