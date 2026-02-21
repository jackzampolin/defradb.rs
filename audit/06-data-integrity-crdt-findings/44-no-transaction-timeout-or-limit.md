# No Transaction Timeout or Concurrent Transaction Limit

**Severity:** Medium
**Category:** Resource Exhaustion / DoS
**Status:** Confirmed

## Summary

There is no maximum duration for storage-level transactions and no limit on the number of concurrent open transactions. A malicious or buggy client can:
1. Open transactions via the HTTP API and never commit/discard them
2. Each open transaction holds a storage snapshot, preventing compaction in persistent backends
3. Each open transaction contributes to ConflictTracker memory growth
4. The `active_txn_count` grows unboundedly

While `cleanup_stale_transactions()` exists at the registry level (for HTTP-exposed transactions), there is no automatic invocation and no protection at the storage level.

## Affected Files

- `crates/storage/src/backends/redb/store.rs:280-327` (no limit check in `new_txn`)
- `crates/storage/src/backends/fjall/store.rs:152-192`
- `crates/storage/src/backends/rocksdb/store.rs:147-163`
- `crates/storage/src/backends/memory/store.rs:47-69`
- `crates/db/src/txn_registry.rs:323-349` (begin has no limit)
- `crates/db/src/txn_registry.rs:182-235` (cleanup exists but not auto-scheduled)

## Details

### No Storage-Level Transaction Limit

```rust
// redb/store.rs:280-291
async fn new_txn(&self, readonly: bool) -> Result<Box<dyn Txn>> {
    {
        let closed = self.closed.read().await;
        if *closed {
            return Err(Error::DBClosed);
        }
        self.active_txn_count.fetch_add(1, Ordering::SeqCst);
        // No check: if active_txn_count > MAX_CONCURRENT_TXNS
    }
    // ...create transaction...
}
```

### No Transaction Timeout

Transactions live as long as the holder keeps them. There is no background reaper at the storage level. The `close_timeout` only applies when the store is being shut down.

### Cleanup Exists But Is Not Automatic

```rust
// txn_registry.rs:182
pub async fn cleanup_stale_transactions(&self, max_age: Duration) -> Result<CleanupResult>
```

This method exists and works correctly, but it must be called explicitly. There is no background task that periodically invokes it. The HTTP server does not schedule cleanup.

### Impact

1. **Snapshot retention**: In redb and rocksdb, open snapshots prevent the database from reclaiming space from compacted/rewritten data. A long-lived snapshot can cause disk usage to grow without bound.

2. **Memory growth**: Each transaction has a pending writes BTreeMap and the memory backend clones the entire store for each snapshot.

3. **ConflictTracker bloat**: The ConflictTracker's `committed_writes` vector grows with each committed transaction. The 1000-entry GC only triggers during commit, not during idle periods.

4. **Close timeout failure**: The store's `close()` method waits for `active_txn_count` to reach zero with a timeout. Leaked transactions can prevent clean shutdown.

### HTTP Attack Vector

```
POST /api/v0/tx         → creates a transaction (returns tx_id)
# attacker never calls POST /api/v0/tx/{id}/commit or /discard
# repeat 10,000 times
```

Each transaction holds a storage snapshot and is tracked in the registry's HashMap.

## Remediation

1. **Add a concurrent transaction limit** at the store level:
```rust
const MAX_CONCURRENT_TXNS: usize = 1000;
if self.active_txn_count.load(Ordering::SeqCst) >= MAX_CONCURRENT_TXNS {
    return Err(Error::TooManyTransactions);
}
```

2. **Schedule automatic cleanup** in the HTTP server startup:
```rust
tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(60));
    loop {
        interval.tick().await;
        registry.cleanup_stale_transactions(Duration::from_secs(300)).await;
    }
});
```

3. **Consider per-transaction timeouts** using `tokio::time::timeout` around the entire transaction lifecycle.

## Test Gap

- No test for behavior when many transactions are opened without commit/discard
- No test for resource consumption under transaction leak scenarios
- No test that `cleanup_stale_transactions` is called periodically in production configurations
