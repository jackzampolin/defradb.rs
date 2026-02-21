# LWW Tie-Breaking Correctness: Verified Clean

**Severity:** None (Green)
**Category:** CRDT Correctness
**Status:** Verified
**CRDT Type:** LWW

## Summary

The LWW tie-breaking logic is mathematically correct and satisfies all three required CRDT properties: commutativity, associativity, and idempotency.

## Verified Properties

### 1. Commutativity: merge(A, B) == merge(B, A)

The LWW merge function selects the value with the highest priority. On priority tie, the lexicographically greatest byte value wins. Since `max()` is commutative, so is the merge.

**Verified at:** `lww.rs:200-226`
**Property test:** `test_lww_commutativity` (proptest, random priorities and values)
**Full convergence:** `test_lww_full_convergence` (all 6 permutations of 3 deltas)

### 2. Idempotency: merge(A, A) == A

When the same delta is applied twice:
- Priority comparison: `incoming == current` (Equal)
- Tie-break: `data <= current_value` is true (since data == current_value)
- Result: `RejectedTieBreak` - no state change

**Verified at:** `lww.rs:218` (`data <= &current_value[..]`)
**Property test:** `test_lww_idempotence`

### 3. Associativity: merge(merge(A,B),C) == merge(A,merge(B,C))

The merge function computes `max(priority, current_priority)` with lexicographic tie-breaking. The result is always the single "winning" value. Since the winner is independent of application order, the function is associative.

**Verified via:** `test_lww_full_convergence` (all 6 orderings converge)

### 4. Delete Semantics

- Delete is represented as empty data (tombstone)
- Delete has a priority like any other write
- On priority tie: empty data < any non-empty data lexicographically, so writes beat deletes
- Higher-priority deletes correctly override lower-priority writes
- Lower-priority deletes cannot incorrectly override higher-priority writes

**Verified at:** `lww.rs:230-234` (empty data triggers delete in storage)
**Unit tests:** `test_lww_deletion`, `test_lww_empty_data_tie_breaking`, `test_lww_deletion_resurrection_with_priority`

### 5. Comparison Basis

The comparison uses Rust's `<=` on `&[u8]` slices, which performs lexicographic byte comparison. This is deterministic, platform-independent, and does not depend on value encoding or interpretation. The comparison operates on the raw serialized bytes, not the logical value.

### 6. Edge Cases Verified

| Edge Case | Behavior | Test |
|-----------|----------|------|
| Priority 0 | Works normally | `test_lww_priority_zero` |
| Priority u64::MAX | Works normally | `test_lww_priority_max` |
| Empty vs non-empty on tie | Non-empty wins | `test_lww_empty_data_tie_breaking` |
| 1MB / 10MB payloads | Correctly stored and compared | `test_lww_large_payload` |
| Identical values on tie | Rejected (idempotent) | `test_lww_idempotence` |

## Conclusion

The LWW implementation is mathematically sound. The `<=` operator (line 218) is the correct choice: it means "incoming must be STRICTLY greater to win," which gives a deterministic, unique winner for any set of concurrent values.
