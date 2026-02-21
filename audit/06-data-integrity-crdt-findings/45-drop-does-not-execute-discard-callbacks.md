# Transaction Drop Does Not Execute Discard Callbacks

**Severity:** Low
**Category:** Resource Management / Safety
**Status:** Confirmed — By Design

## Summary

When a transaction is dropped without explicit `commit()` or `discard()`, the Drop implementation decrements the active transaction count and logs a warning, but does NOT execute registered `on_discard` callbacks. Any cleanup logic registered via `on_discard` or `on_discard_async` is silently skipped. The warning message mentions skipped callbacks but does not execute them.

## Affected Files

- `crates/storage/src/backends/redb/transaction.rs:68-97` (Drop impl)
- `crates/storage/src/backends/fjall/transaction.rs:32-58`
- `crates/storage/src/backends/rocksdb/transaction.rs:74-100`
- `crates/storage/src/backends/memory/transaction.rs` (no Drop impl — relies on Rust's default)

## Details

### Drop Implementation (redb example)

```rust
impl Drop for RedbTxn {
    fn drop(&mut self) {
        self.active_txn_count.fetch_sub(1, Ordering::SeqCst);

        let was_committed = *self.committed.lock();
        let was_discarded = *self.discarded.lock();
        if !was_committed && !was_discarded {
            let total_skipped =
                self.callbacks.counts().on_discard + self.callbacks.counts().on_discard_async;

            if total_skipped > 0 {
                tracing::warn!(
                    skipped_callbacks = total_skipped,
                    "Transaction dropped without commit() or discard() - \
                     {} registered discard callback(s) were NOT executed.",
                    total_skipped
                );
            }
        }
    }
}
```

The Drop counts the discard callbacks but only logs — it does not call `CallbackManager::execute_callbacks()`.

### Why This Is By Design

Executing callbacks in Drop is risky because:
1. Drop runs during stack unwinding (panics) — triggering more code can cause nested panics
2. Async callbacks cannot be awaited in a synchronous Drop
3. Callback panics in Drop could abort the process

### Impact

If code paths exist where transactions are dropped without explicit commit/discard:
- Resource cleanup registered as discard callbacks will not run
- Event notifications won't fire
- The warning log is the only indication

### Memory Backend Missing Drop

The memory backend's `MemoryTxn` does not implement Drop at all. Dropped transactions:
- Don't decrement any active count (there is no `active_txn_count` on MemoryStore)
- Don't log any warning
- Don't notify about lost pending changes

This is less concerning because the memory backend is for testing only, but it means behavior differs from the other backends.

## Remediation

1. **Accept current behavior** and ensure all code paths call commit/discard explicitly. Audit callers to verify.

2. **Add Drop to memory backend** for consistency with other backends:
```rust
impl Drop for MemoryTxn {
    fn drop(&mut self) {
        let was_committed = *self.committed.lock();
        let was_discarded = *self.discarded.lock();
        if !was_committed && !was_discarded {
            if !self.pending.lock().is_empty() {
                tracing::warn!("MemoryTxn dropped without commit/discard");
            }
        }
    }
}
```

3. **Consider executing sync discard callbacks in Drop** with catch_unwind protection. Only sync callbacks (not async) can safely run in Drop.

## Test Gap

- No test that verifies Drop behavior when a transaction with registered callbacks is dropped
- No test for memory backend's missing Drop behavior
- No test that the warning is actually emitted on implicit drop
