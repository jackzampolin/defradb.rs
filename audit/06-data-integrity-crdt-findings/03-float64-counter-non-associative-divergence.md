# Float64 Counter Non-Associative Divergence Risk

**Severity:** Low
**Category:** Data Integrity / CRDT Convergence
**Status:** Open (by design, needs documentation)
**CRDT Type:** Counter (Float64)

## Summary

IEEE 754 f64 addition is commutative (`a + b == b + a`) but NOT associative (`(a + b) + c != a + (b + c)` in general). When three or more Float64 counter deltas are applied in different orders on different nodes, the running accumulation can produce different bit-level results, causing permanent byte-level divergence between replicas.

## Affected Files

- `crates/crdt/src/counter.rs` lines 426-468 (Float64 path)

## Details

The Counter accumulates Float64 values by reading the current stored value and adding the increment:

```rust
let current = self.get_float64(rw).await?;
let result = current + increment;
```

With three deltas applied in different orders:

```
Node 1: ((0 + d1) + d2) + d3
Node 2: ((0 + d2) + d3) + d1
```

Due to IEEE 754 rounding, `(d1 + d2) + d3` may differ from `d1 + (d2 + d3)` at the ULP (Unit in the Last Place) level.

**Concrete example:**
```
d1 = 1e-16, d2 = 1.0, d3 = -1.0

Node 1: ((0 + 1e-16) + 1.0) + (-1.0) = 0.0
         (1e-16 absorbed into 1.0, then subtracted back)

Node 2: ((0 + 1.0) + (-1.0)) + 1e-16 = 1e-16
         (1.0 - 1.0 = 0.0, then 0.0 + 1e-16 = 1e-16)
```

The two nodes store different byte patterns (`0.0` vs `1e-16`), and no future operation can reconcile them since counters have no priority-based conflict resolution.

**Practical impact:** For typical application values (monetary amounts, scores), the difference is negligible (< 1 ULP). However, for a CRDT system that guarantees bit-exact convergence across the network, any divergence is a protocol violation.

## Remediation

Options (in order of complexity):

1. **Document as known limitation** - Float64 counters may diverge at ULP precision. Acceptable for most applications.
2. **Fixed-point arithmetic** - Store Float64 counters as scaled integers (e.g., cents instead of dollars). Eliminates the problem entirely.
3. **Kahan summation** - Use compensated summation to reduce rounding error, but this doesn't guarantee bit-exact results across orderings.
4. **Sort-then-sum** - Always accumulate deltas in a deterministic order (e.g., sorted by nonce). Requires storing all individual deltas, not just the running total.

Go DefraDB likely has the same issue. Verify whether Go's behavior matches before deciding on a fix.

## Test Gap

- No 3+ delta convergence property test for Float64 counters (`test_float64_counter_commutativity` only tests 2 deltas)
- The Int64 equivalent (`test_counter_full_convergence`) tests 6 permutations of 3 deltas, but no Float64 equivalent exists
