# Counter Nonce Idempotency: Verified Clean

**Severity:** None (Green)
**Category:** CRDT Correctness
**Status:** Verified
**CRDT Type:** Counter

## Summary

The Counter CRDT's nonce-based idempotency mechanism is correct. Duplicate deltas are properly detected and skipped. The nonce scoping prevents cross-field and cross-document collisions.

## Verified Properties

### 1. Duplicate Detection

The `has_nonce` / `mark_nonce` pattern correctly prevents double-application:

```rust
// counter.rs:406-408
if !is_create && self.has_nonce(rw, delta.nonce).await? {
    return Ok(MergeResult::SkippedAlreadyApplied { nonce: delta.nonce });
}
```

- First application: nonce not found, delta applied, nonce marked
- Second application: nonce found, delta skipped with `SkippedAlreadyApplied`

**Verified by:** `test_counter_idempotency`, `test_counter_nonce_replay_protection` (10x replay)

### 2. Commutativity

Counter deltas are commutative because addition is commutative and nonce-based idempotency ensures each delta is applied exactly once regardless of order.

**Verified by:** `test_counter_commutativity` (proptest), `test_counter_full_convergence` (6 permutations)

### 3. Nonce Scoping

Nonces are scoped per-field-per-document via the key prefix:

```
/data/{schema_version}/{doc_id}/{field_name}/nonces/{nonce_bytes}
```

This prevents nonce collisions between:
- Different fields on the same document
- Same field on different documents
- Same field across different schema versions

### 4. First-Wins Semantics

If the same nonce arrives with a different increment value (adversarial scenario), the first application wins. Subsequent arrivals with the same nonce are skipped regardless of their increment value. The nonce check only examines key existence, not the stored value.

### 5. Int64 Overflow Handling

Wrapping arithmetic (`wrapping_add`) matches Go DefraDB behavior. Verified with property tests at boundary values:

- `test_counter_overflow_wrapping` (proptest, values near i64::MAX)
- `test_counter_underflow_wrapping` (proptest, values near i64::MIN)

### 6. Float64 Overflow Handling

NaN and infinity values are rejected at both delta construction and merge time:

- Constructor: `new_float64` checks `increment.is_finite()` (line 99)
- Merge: `apply_delta` checks `increment.is_finite()` (line 430), `current.is_finite()` (line 448), and `result.is_finite()` (line 460)

**Verified by:** `test_counter_float64_nan_rejected_by_constructor`, `test_counter_float64_overflow`

### 7. Numeric Kind Mismatch

Sending a Float64 delta to an Int64 counter (or vice versa) is caught and returns an error:

```rust
if delta.kind() != self.kind {
    return Err(Error::MergeError(...));
}
```

**Verified by:** `test_counter_numeric_kind_mismatch`

## Conclusion

The standalone Counter CRDT is correct. Nonce-based idempotency is sound, overflow handling matches Go behavior, and the property test suite provides strong coverage for commutativity and convergence.
