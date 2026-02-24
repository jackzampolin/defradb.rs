# Write Skew Possible — Documented Trade-Off

**Severity:** Informational
**Category:** Isolation Level / Consistency
**Status:** Confirmed — Accepted Trade-Off

## Summary

The transaction system implements snapshot isolation with write-write conflict detection, but does NOT detect read-write conflicts (write skew). Two transactions can read overlapping data, write to disjoint keys based on stale reads, and both commit successfully. This is a documented trade-off — the ConflictTracker only checks whether the same KEY was written by concurrent transactions, not whether the same key was READ.

## Affected Files

- `crates/storage/src/backends/shared.rs:217-257` (ConflictTracker — write-only tracking)

## Details

### What Write Skew Looks Like

```
Initial state: balance_A = 100, balance_B = 100
Constraint: balance_A + balance_B >= 100

TxnX reads balance_A=100, balance_B=100 → total=200, ok
TxnY reads balance_A=100, balance_B=100 → total=200, ok

TxnX writes balance_A = 0 (withdraws 100 from A)
TxnY writes balance_B = 0 (withdraws 100 from B)

Both commit successfully (different keys written)
Result: balance_A=0, balance_B=0, total=0 — constraint violated
```

### ConflictTracker Only Tracks Writes

```rust
// shared.rs:232-240
for (commit_ver, keys) in committed.iter() {
    if *commit_ver > read_version {
        for key in &write_keys {
            if keys.contains(*key) {  // Only checks write keys
                return Err(Error::TxnConflict);
            }
        }
    }
}
```

The tracker has no record of what keys a transaction READ, only what it WROTE.

### Where Write Skew Could Matter in DefraDB

1. **Index-document consistency**: Not affected — both document and index updates happen within the SAME transaction, so there's no cross-transaction write skew between them.

2. **ACP policy mutations**: Two transactions could read overlapping policy state and make conflicting permission changes. This is unlikely in practice because ACP policy changes are rare administrative operations.

3. **Counter CRDTs**: Not affected — counters use additive deltas, not read-then-write patterns.

4. **Collection definitions**: Two concurrent `add_schema` operations could read the same schema state and create conflicting definitions. Mitigated by the rarity of concurrent DDL.

5. **Merge handler**: Concurrent merges of the same document read current CRDT state and apply deltas. CRDT idempotency and commutativity handle this correctly without needing serializable isolation.

### Why This Is Acceptable

- DefraDB's CRDT architecture inherently handles concurrent modifications via merge semantics
- The Go DefraDB (reference implementation) has the same isolation level
- Serializable isolation would significantly reduce throughput
- The `max_txn_retries` configuration suggests the system is designed for optimistic concurrency with retries

## Remediation

No action needed — this is an accepted design choice. Document the isolation level clearly for application developers who build on DefraDB.

## Test Gap

- No test explicitly demonstrating write skew behavior
- No test verifying that write skew does NOT cause data corruption in the CRDT layer
