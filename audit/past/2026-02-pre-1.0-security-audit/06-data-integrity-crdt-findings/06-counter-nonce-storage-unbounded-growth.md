# Counter Nonce Storage Unbounded Growth

**Severity:** Low
**Category:** Resource Exhaustion / Storage Leak
**Status:** Open (documented in code)
**CRDT Type:** Counter

## Summary

Counter nonces are stored permanently and never garbage collected. Each counter increment/decrement operation consumes one 9-byte storage entry (8-byte nonce key + 1-byte marker value) that persists forever. For high-throughput counters, this represents unbounded storage growth.

## Affected Files

- `crates/crdt/src/counter.rs` lines 293-308
- `crates/crdt/src/composite.rs` lines 274-276, 322-324

## Details

The code already documents this trade-off:

```rust
/// Note: Nonces are stored permanently and never garbage collected in this implementation.
/// For production use, consider implementing nonce garbage collection strategies:
///
/// 1. Time-based: Remove nonces older than a configurable retention period
/// 2. CID-based: Track nonces per DAG block and remove when blocks are pruned
/// 3. Hybrid: Combine time-based with causality tracking
///
/// Nonce storage grows unbounded without GC, which could become a storage leak for
/// high-throughput counters.
```

**Storage per nonce:**
- Key: `{value_key}/nonces/{8-byte nonce}` (~40-60 bytes with key overhead)
- Value: `[1]` (1 byte)
- Total: ~50-70 bytes per nonce entry

**Growth rate for a busy counter:**
- 100 increments/second = 100 nonces/second
- ~5 KB/second, ~430 MB/day, ~12 GB/month per counter field

**Impact:** Not an immediate concern for typical usage patterns. Becomes significant for counters with high write frequency (e.g., real-time metrics, page view counters).

## Remediation

Implement one of the GC strategies documented in the code. The safest approach is CID-based: once a DAG block is finalized and all peers have acknowledged it, the nonces from that block can be pruned.

For 1.0, this can be deferred unless high-throughput counter use cases are expected.

## Test Gap

No test measures nonce storage growth or verifies GC behavior (since GC is not implemented).
