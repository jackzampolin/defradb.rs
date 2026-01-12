# CRDT Test Parity Analysis

## Current Rust Test Coverage

### Unit Tests (15 tests)
**LWW Register:**
- ✅ test_lww_higher_priority_wins
- ✅ test_lww_lower_priority_ignored
- ✅ test_lww_same_priority_lexicographic
- ✅ test_lww_deletion

**Counter:**
- ✅ test_counter_increment
- ✅ test_counter_idempotency (nonce-based)
- ✅ test_counter_decrement_not_allowed

**Priority:**
- ✅ test_priority_small_value
- ✅ test_priority_large_value
- ✅ test_priority_typical_timestamp
- ✅ test_invalid_priority
- ✅ test_incomplete_varint

**Composite:**
- ✅ test_composite_multiple_fields

**Misc:**
- ✅ test_priority_roundtrip
- ✅ test_context_creation

### Property Tests (5 tests)
- ✅ test_lww_commutativity (order independence)
- ✅ test_lww_idempotence (repeated merges)
- ✅ test_lww_multi_replica_convergence (3+ replicas)
- ✅ test_counter_commutativity
- ✅ test_counter_idempotence

## Go DefraDB Test Coverage

### Unit Tests (3 tests)
- baseCRDT valueKey formatting
- baseCRDT priorityKey formatting
- baseCRDT set/get priority

### Integration Tests (~15 tests)
**PCounter (increment-only):**
- Negative increment error
- Positive increment
- Overflow behavior (rolls to min int64)
- Float32/Float64 support
- Float overflow (no-op or inf)

**PNCounter (increment + decrement):**
- Positive increment
- Overflow behavior
- Float support
- Decrement overflow (negative inf)
- Insignificant value handling

## Missing Tests in Rust

### High Priority
- ❌ Overflow behavior (saturating_add but should test explicitly)
- ❌ Float64 counter support (have Int64 only)
- ❌ Float32 counter support
- ❌ Overflow edge cases (max values)
- ❌ Insignificant value handling for floats

### Medium Priority
- ❌ Full composite document test with multiple CRDTs
- ❌ Schema version mismatch errors
- ❌ Field name mismatch errors
- ❌ Document ID mismatch errors

### Low Priority (Integration-level)
- Network replication tests (requires P2P)
- Multi-node conflict resolution
- DAG sync tests

## Recommendations

1. **Add Float support to Counter:**
   - NumericKind::Float32
   - NumericKind::Float64
   - Handle special cases (NaN, Inf)

2. **Add overflow tests:**
   - Int64 max + increment
   - Float64 max + increment
   - Verify behavior matches Go (saturating vs wrapping)

3. **Add error case tests:**
   - Schema version mismatch
   - Field name mismatch
   - Type mismatches

4. **Integration tests (later):**
   - Multi-store coordination
   - Blockstore integration
   - P2P replication
