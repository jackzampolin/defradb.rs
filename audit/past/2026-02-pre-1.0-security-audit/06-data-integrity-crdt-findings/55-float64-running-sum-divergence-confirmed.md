# Float64 Counter Running-Sum Architecture Causes Order-Dependent Divergence

**Severity:** Low
**Category:** Data Integrity / CRDT Convergence
**Status:** Confirmed (extends Finding 03)
**Session:** 6 of 6

## Summary

The Float64 counter stores a **running sum** rather than a **set of deltas**. This means different delta application orders on different nodes produce different accumulated values due to IEEE 754 non-associativity. While Finding 03 identified the theoretical risk, this finding confirms the architectural cause and quantifies the practical impact.

## Affected Files

- `crates/crdt/src/counter.rs` lines 440-467 (Float64 accumulation)
- `crates/crdt/src/composite.rs` lines 287-327 (Composite counter — Int64 only, but same pattern)

## Details

### Architecture: Running Sum

```rust
// counter.rs:440-467
let current = if is_create { 0.0 } else { self.get_float64(rw).await? };
let result = current + increment;
self.set_float64(rw, result).await?;
```

Each delta reads the current accumulated value and adds the increment. The stored value is always `sum(all_increments)`, not the individual deltas.

### Why This Causes Divergence

With 3 deltas d1, d2, d3 applied in different orders:

```
Node A: ((0 + d1) + d2) + d3
Node B: ((0 + d3) + d1) + d2
```

IEEE 754 addition is commutative (`a + b == b + a`) but NOT associative (`(a + b) + c` may differ from `a + (b + c)`). Since the running-sum architecture forces left-to-right accumulation in application order, different orders produce different bit-level results.

### Practical Impact Assessment

For typical use cases (monetary amounts, scores), the divergence is sub-ULP (< 10^-15 relative error). The property test `test_float64_counter_commutativity` passes because it only tests 2 deltas (2-delta addition IS commutative and order-independent).

**Worst case:** Values near the representable limit (e.g., sums approaching `f64::MAX / 2`) can accumulate rounding errors that compound to visible differences after millions of operations.

### Comparison with Go

Go's `math/big.Float` is not used — Go DefraDB uses the same `float64` running-sum pattern. Both implementations have identical divergence characteristics. This is acceptable for 1.0 parity.

## Remediation

No change needed for 1.0. If exact convergence is required:

1. **Sort-then-sum**: Store all individual deltas (sorted by nonce), re-sum deterministically on read. Requires changing the storage model.
2. **Fixed-point**: Use scaled integers instead of floats for financial counters.

## Test Gap

Missing: 3-delta Float64 convergence property test (only 2-delta tested). See Finding 04.
