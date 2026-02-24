# Nonce Storage Cost Quantified — P2P Amplification Vector

**Severity:** Medium
**Category:** Resource Exhaustion / Denial of Service
**Status:** Confirmed (extends Finding 06)
**Session:** 6 of 6

## Summary

Finding 06 identified unbounded nonce growth. This finding quantifies the attack surface: a malicious peer can force nonce accumulation on a target node by replicating counter increments. Each increment costs the attacker ~100 bytes of P2P bandwidth but permanently consumes ~50-70 bytes of storage on the target that can never be reclaimed.

## Affected Files

- `crates/crdt/src/counter.rs` lines 280-308 (nonce storage)
- `crates/db/src/merge_handler/counter.rs` (P2P counter merge)
- `crates/db/src/merge_handler/composite.rs` (composite with counter fields)

## Details

### Nonce Storage Key Format

```
/data/{schema_version_id}/{doc_id}/{field_name}/nonces/{8-byte-nonce}
```

For a typical document:
- schema_version_id: ~52 bytes (CID string)
- doc_id: ~52 bytes (CID-based DocID)
- field_name: ~10 bytes
- nonce: 8 bytes (fixed)
- Key prefix overhead: ~20 bytes (`/data/`, `/nonces/`, separators)
- Total key: ~142 bytes
- Value: 1 byte (`[1]`)
- Storage metadata overhead: ~20-50 bytes (depends on backend)

**Total per nonce: ~160-200 bytes**

### Attack Cost Analysis

| Metric | Value |
|--------|-------|
| Storage per nonce | ~180 bytes |
| P2P bandwidth per increment | ~200 bytes (CBOR-encoded counter block) |
| Amplification ratio | ~0.9x (no amplification — 1:1) |
| Storage per million nonces | ~180 MB |
| Time to generate 1M nonces at 100/sec | ~2.8 hours |

While the amplification ratio is approximately 1:1, the key asymmetry is **permanence**: the attacker's bandwidth cost is transient, but the target's storage cost is permanent (no GC).

### Query Performance Impact

Counter value reads (`get_int64`/`get_float64`) access a single key — O(1) regardless of nonce count. Nonce checks (`has_nonce`) are also single-key lookups — O(1). The nonce accumulation does NOT degrade query performance.

The impact is purely storage: disk usage grows monotonically for counter fields under sustained write load.

### Practical Threshold

For a single counter field incremented 100 times/second:
- 1 day: ~1.5 GB
- 1 month: ~45 GB
- 1 year: ~540 GB

For typical usage (counters incremented a few times per minute), the growth is negligible (~5 MB/year).

## Remediation

Priority: Low for 1.0, Medium for production deployments.

1. **Per-counter nonce budget**: Reject increments when nonce count exceeds a configurable limit (e.g., 1 million per field). This breaks CRDT semantics but prevents unbounded growth.
2. **Nonce compaction**: Periodically compact nonces into a "confirmed up to nonce N" marker, discarding individual nonce entries older than N.
3. **Monitoring**: Add a metric for nonce storage size per collection, so operators can detect anomalous growth.

## Test Gap

No test measures nonce storage growth over time. No test verifies behavior at extreme nonce counts (1M+).
