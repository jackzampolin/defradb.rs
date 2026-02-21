# Property Test Coverage Gaps

**Severity:** Low
**Category:** Test Gap
**Status:** Open
**CRDT Type:** All

## Summary

The property-based test suite covers core LWW and Counter properties well but has significant gaps in Composite coverage, Float64 convergence, delete-path convergence, and adversarial input ranges.

## Affected Files

- `crates/crdt/tests/property_tests.rs`

## Details

### Covered Properties (Green)

| Property | LWW Int | Counter Int64 | Counter Float64 | PCounter | Composite |
|----------|---------|---------------|-----------------|----------|-----------|
| Commutativity (2-delta) | Y | Y | Y | Y | **N** |
| Idempotence | Y | Y | Y | - | **N** |
| Determinism | Y | - | - | - | **N** |
| Full convergence (3-delta, 6 permutations) | Y | Y | **N** | Y | **N** |
| Overflow/underflow wrapping | Y | Y | - | - | **N** |

### Missing Test Categories

**1. Float64 3+ delta convergence:**
No property test verifies that 3 or more Float64 counter deltas converge across all permutations. Only 2-delta commutativity is tested. See finding `03-float64-counter-non-associative-divergence.md`.

**2. Composite CRDT property tests:**
Zero property tests for CompositeDAG. The existing `test_composite_multi_field_atomicity` in property_tests.rs creates a delta with no fields and only checks it doesn't panic - it doesn't test convergence.

**3. LWW delete convergence:**
`test_lww_delete_then_write` tests delete-write sequences but does NOT test commutativity of deletes: no test verifies that `merge(write, delete)` == `merge(delete, write)` with the same priorities. The unit tests in `lww_tests.rs` cover specific scenarios but not random values.

**4. Adversarial priority ranges:**
Property test priority ranges are `0..1000`. The extreme values `u64::MAX`, `u64::MAX - 1`, and `0` are tested in unit tests but not in property tests. Property tests should include the full `u64` range to catch edge cases with varint encoding.

**5. Mixed CRDT convergence:**
No test sends the same set of deltas (LWW + Counter) through Composite on multiple replicas in different orders and verifies byte-exact convergence.

**6. Duplicate nonce with different value:**
No test verifies that a nonce reused with a different increment value is correctly rejected (first-wins semantics).

## Remediation

Add the following property tests:

```rust
// 1. Float64 full convergence (3 deltas, 6 permutations)
fn test_float64_counter_full_convergence(...)

// 2. Composite convergence with LWW + Counter fields
fn test_composite_convergence(...)

// 3. LWW delete commutativity
fn test_lww_delete_commutativity(write_data, priority_write, priority_delete)

// 4. Full priority range
fn test_lww_commutativity_full_range(p1 in 0..u64::MAX, p2 in 0..u64::MAX, ...)

// 5. Nonce conflict
fn test_counter_nonce_conflict(nonce, inc1, inc2)
```
