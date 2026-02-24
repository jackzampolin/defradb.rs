# Composite Counter Missing Float64 Support

**Severity:** Medium
**Category:** Data Integrity / CRDT Correctness
**Status:** Open
**CRDT Type:** Composite (Counter path)

## Summary

The Composite CRDT's inline counter merge only handles Int64 values. If a Float64 counter field is processed via the Composite merge path, the f64 byte representation will be reinterpreted as i64 bits, producing silently incorrect values and permanent network divergence.

## Affected Files

- `crates/crdt/src/composite.rs` lines 287-327
- `crates/crdt/src/counter.rs` lines 412-468 (standalone handles both)

## Details

The standalone Counter correctly dispatches on `NumericKind`:

```rust
// counter.rs:412-468
match self.kind {
    NumericKind::Int64 => {
        let increment = delta.decode_int64()?;
        // ...wrapping_add...
    }
    NumericKind::Float64 => {
        let increment = delta.decode_float64()?;
        // ...finite checks, overflow rejection...
    }
}
```

The Composite hardcodes Int64 interpretation:

```rust
// composite.rs:317-318 - ALWAYS interprets as i64
let increment = i64::from_be_bytes(data[..8].try_into().unwrap());
let new_value = current.wrapping_add(increment);
```

**Example:** The f64 value `3.14` has byte representation `0x40091EB851EB851F`. Interpreted as i64, this becomes `4614253070214989087`. The counter would be incremented by ~4.6 quintillion instead of 3.14.

Additionally, the Composite's `FieldCrdtType` enum has no `NumericKind` discriminant:

```rust
enum FieldCrdtType {
    Lww,
    Counter,  // No NumericKind
}
```

**Impact:** Silent data corruption if Float64 counter fields exist and are merged via the Composite path. The Float64 overflow protections (NaN, infinity rejection) from the standalone Counter are also bypassed.

## Remediation

Add `NumericKind` to `FieldCrdtType::Counter` and implement proper dispatch:

```rust
enum FieldCrdtType {
    Lww,
    Counter { allow_decrement: bool, kind: NumericKind },
}
```

Implement Float64 handling in `apply_field_delta` with the same overflow checks as the standalone Counter.

## Test Gap

No test uses a Float64 counter through the Composite merge path.
