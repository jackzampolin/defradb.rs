# Callback Panic Safety Verified

**Severity:** Informational
**Category:** Safety / Robustness
**Status:** Verified — Correct

## Summary

Transaction lifecycle callbacks (on_success, on_error, on_discard) are wrapped in `std::panic::catch_unwind` for sync callbacks and `AssertUnwindSafe(...).catch_unwind()` for async callbacks. A panicking callback does not prevent other callbacks from executing, does not corrupt transaction state, and does not affect the commit/discard return value. The commit is already durable before success callbacks run.

## Affected Files

- `crates/storage/src/backends/shared.rs:122-151` (CallbackManager execution)

## Details

### Sync Callback Protection

```rust
pub(crate) fn execute_callbacks(callbacks: Vec<TxnCallback>) {
    for (i, callback) in callbacks.into_iter().enumerate() {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback));
        if let Err(panic_info) = result {
            tracing::error!(
                callback_index = i,
                panic = ?panic_info,
                "Transaction callback panicked - continuing with remaining callbacks"
            );
        }
    }
}
```

### Async Callback Protection

```rust
pub(crate) async fn execute_async_callbacks(callbacks: Vec<AsyncTxnCallback>) {
    use futures::FutureExt;
    for (i, callback) in callbacks.into_iter().enumerate() {
        let future = callback();
        let result = std::panic::AssertUnwindSafe(future).catch_unwind().await;
        if let Err(panic_info) = result {
            tracing::error!(
                callback_index = i,
                panic = ?panic_info,
                "Async callback panicked during execution"
            );
        }
    }
}
```

### Execution Timing

| Event | Callbacks | When |
|-------|-----------|------|
| Commit success | on_success, on_success_async | After storage commit succeeds |
| Commit failure | on_error, on_error_async | Before returning Err |
| Discard | on_discard (sync) | Immediately during discard |
| Discard | on_discard_async | Spawned in background via `tokio::spawn` |

### Async Discard Caveat

Async discard callbacks are spawned in a background task:
```rust
tokio::spawn(async move {
    CallbackManager::execute_async_callbacks(on_discard_async).await;
});
```

These may not complete if the process exits. This is logged with a warning and documented in the trait definition.

## Remediation

None needed. Callback panic safety is correctly implemented.

## Test Gap

The redb callback test suite (`backends/redb/tests/callbacks.rs`) tests basic callback execution. Consider adding a test that registers a panicking callback and verifies other callbacks still execute.
