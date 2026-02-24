# Float Equality Uses f64::EPSILON Comparison in Queries and Indexes

**Severity:** Low
**Category:** Data Integrity / Query Correctness
**Status:** Confirmed — By Design
**Session:** 6 of 6

## Summary

Float equality comparisons in the query filter evaluator and index matcher use `(a - b).abs() < f64::EPSILON` rather than exact bit equality. This is standard practice for floating-point comparison but creates an inconsistency: two float values that are "equal" for queries may have different CBOR encodings and different CIDs, meaning the CRDT layer treats them as different values while the query layer treats them as equal.

## Affected Files

- `crates/query/src/mapper/filter/eval/operators.rs` line 20
- `crates/storage/src/index/matcher/mod.rs` lines 253-267
- `crates/storage/src/index/matcher/tests.rs` line 187

## Details

### Query Filter Evaluator

```rust
// operators.rs:20
(a - b).abs() < f64::EPSILON
```

### Index Matcher

```rust
// matcher/mod.rs:253-267
(NormalValue::Float64(a), NormalValue::Float64(b)) => (a - b).abs() < f64::EPSILON,
(NormalValue::Float32(a), NormalValue::Float32(b)) => (a - b).abs() < f32::EPSILON,
```

### Inconsistency with CRDT Layer

The LWW CRDT uses lexicographic byte comparison for tie-breaking:

```rust
// lww.rs:218
if data <= &current_value[..] {
    return Ok(MergeResult::RejectedTieBreak);
}
```

Two float values within EPSILON of each other have different byte representations, so the CRDT treats them as different. But a query filter `WHERE score == 1.0` would match both `1.0` and `1.0 + 5e-16`.

### Practical Impact

This inconsistency is unlikely to cause user-visible bugs because:
1. Float EPSILON is ~2.2e-16, far below any meaningful application precision
2. The CRDT divergence risk (Finding 03) produces differences at this scale
3. Users should not rely on exact float equality for queries

## Remediation

No change needed. This is standard float comparison practice. Document that float equality in queries uses epsilon tolerance.

## Test Gap

`test_float_equality_epsilon` in matcher tests covers the basic case. No test exercises the inconsistency between CRDT tie-breaking and query-level equality.
