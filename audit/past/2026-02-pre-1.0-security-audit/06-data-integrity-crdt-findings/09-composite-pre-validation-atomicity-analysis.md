# Composite Pre-Validation and Atomicity Analysis

**Severity:** Informational
**Category:** CRDT Correctness / Atomicity
**Status:** Verified (with caveats)
**CRDT Type:** Composite

## Summary

The Composite CRDT's pre-validation phase correctly catches type mismatches, unknown fields, and malformed data before any storage writes. Post-validation application failures rely on transaction rollback by the caller. The design is sound given proper transaction usage, but HashMap iteration order makes reasoning about partial failure states more complex.

## Verified Properties

### 1. Pre-Validation Phase (Lines 406-439)

The pre-validation checks:
- All field names exist in `field_managers` (rejects unknown fields)
- Field delta types match registered CRDT types (rejects LWW delta for Counter field)
- Counter data is exactly 8 bytes (validates data format)
- Delete deltas are allowed for both LWW and Counter fields

These checks run BEFORE any storage writes, providing fail-fast semantics.

### 2. Application Phase (Lines 442-460)

After validation, fields are applied in HashMap iteration order (non-deterministic but consistent within a single Rust process):

```rust
for (field_name, field_delta) in &composite_delta.field_deltas {
    let result = self
        .apply_field_delta(rw, field_name, field_delta, ctx.is_create)
        .await?;
}
```

The `?` operator propagates errors immediately, potentially leaving some fields applied and others not.

### 3. Transaction-Based Atomicity

The `ReplicatedData` trait documentation states:
> "The caller is responsible for committing or rolling back the transaction. Multiple merge calls can share the same transaction for atomic updates."

If `merge()` returns `Err`, the caller should drop (not commit) the transaction, causing a rollback. This provides atomicity at the storage layer, not the CRDT layer.

### 4. Potential Failure Modes Post-Validation

After pre-validation passes, `apply_field_delta` can still fail due to:
- **Storage I/O errors** (transient) - handled by transaction rollback
- **Priority decode errors** (corrupted stored priority) - indicates data corruption, not a delta issue

Both are non-functional failures that should trigger transaction rollback.

### 5. HashMap Iteration Order

The `field_deltas` HashMap does not guarantee iteration order. This means:
- Field A may be applied before Field B on one node and vice versa on another
- This is NOT a convergence issue because all fields should succeed (pre-validated) and the final state is the same regardless of application order
- In error scenarios, different fields may be in "applied" vs "not-yet-applied" state at the point of failure, but transaction rollback handles this

### 6. Empty Delta Handling

An empty CompositeDelta (no field deltas) returns `MergeResult::Applied`:

```rust
if any_applied || composite_delta.field_deltas.is_empty() {
    Ok(MergeResult::Applied)
}
```

This is correct - an empty delta is a valid no-op.

### 7. All-Rejected Handling

If all fields are rejected (lower priority or tie-break), the composite returns `MergeResult::RejectedTieBreak`. This is a reasonable choice since there's no single rejection reason to report.

## Caveats

1. **Transaction discipline required**: If the caller commits after `merge()` returns `Err`, partial application occurs. This is a caller bug, not a CRDT bug.
2. **HashMap order non-determinism**: Makes debugging harder but doesn't affect correctness.
3. **Missing Float64 and allow_decrement**: See findings 01 and 02.

## Conclusion

The Composite's pre-validation + transaction-based atomicity design is sound. The primary risks are in the counter-specific gaps documented in separate findings, not in the atomicity mechanism itself.
