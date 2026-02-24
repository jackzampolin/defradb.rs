# Partition Healing Convergence — DAG Ordering Ensures Correctness

**Severity:** Informational (Verified Clean)
**Category:** Data Integrity / CRDT Convergence
**Status:** Verified
**Session:** 6 of 6

## Summary

When two network partitions reconnect, the DAG merge order may differ between nodes. Convergence holds because:

1. **LWW fields**: Priority-based + lexicographic tie-breaking is order-independent
2. **Counter fields**: Nonce-based idempotency ensures each increment applied exactly once regardless of order
3. **Composite deltas**: Process heads recursively (oldest-first), and CRDT commutativity guarantees the final state is independent of merge order

## Affected Files

- `crates/db/src/merge_handler/composite.rs` lines 91-161 (recursive head processing)
- `crates/crdt/src/lww.rs` lines 194-247 (LWW merge — order-independent)
- `crates/crdt/src/counter.rs` lines 390-481 (Counter merge — nonce idempotent)
- `crates/crdt/tests/property_tests.rs` (6-permutation convergence tests)

## Details

### Partition Scenario

```
Partition A:  create → update1 → update2 → delete → create2
Partition B:  create → update3 → update4
```

After reconnection:

1. Both partitions exchange their DAG blocks
2. Each node processes the other partition's blocks via `process_composite_delta`
3. Recursive head traversal ensures parents are processed before children
4. The final state depends ONLY on the set of all blocks, not their processing order

### LWW Convergence Proof

Property tests verify this for all 6 permutations of 3 deltas:

```rust
// property_tests.rs — test_lww_full_convergence
let permutations: Vec<Vec<&LwwDelta>> = vec![
    vec![&delta1, &delta2, &delta3],
    vec![&delta1, &delta3, &delta2],
    vec![&delta2, &delta1, &delta3],
    vec![&delta2, &delta3, &delta1],
    vec![&delta3, &delta1, &delta2],
    vec![&delta3, &delta2, &delta1],
];
// All 6 produce identical results
```

### Counter Convergence Proof

Same 6-permutation test exists for counters (`test_counter_full_convergence`). Nonce idempotency ensures each increment is counted exactly once regardless of delivery order.

### Delete + Re-Create

When partition A deletes then re-creates a document:
- The delete block has priority N
- The re-create block has priority N+1 (higher)
- On partition B, when both blocks arrive, the higher-priority create wins
- This is deterministic: priority is derived from DAG height, not wall-clock time

### Edge Case: Both Partitions Delete Same Document

- Both produce delete blocks with different priorities (different DAG heights)
- The higher-priority delete wins, but both produce the same final state (deleted)
- If one partition also re-creates after delete, the re-create has the highest priority and wins globally

## Security Assessment

The partition healing design is fundamentally sound. The only risk is Finding 03 (Float64 non-associativity), which can cause sub-ULP divergence for float counters — this is a known limitation, not a partition-healing bug.

## Test Gap

No integration test exercises partition healing with delete + re-create sequences. The property tests cover CRDT-level convergence but not the full merge handler path with recursive head processing.
