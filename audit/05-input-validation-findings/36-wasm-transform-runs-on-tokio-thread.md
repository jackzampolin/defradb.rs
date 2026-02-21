# 36: WASM Transform Execution Blocks Tokio Worker Thread

| Field    | Value |
|----------|-------|
| Severity | MEDIUM |
| Category | Denial of Service |
| Status   | Confirmed |

## Summary

The WASM transform execution in `execute_batch_transform()` runs synchronous wasmtime calls directly on the calling async context. The pipeline processor is spawned via `tokio::spawn()`, and its `run()` method calls `transform_to_target()` which calls `apply_transform()`. The `transform()` and `inverse()` methods create async streams, but the actual WASM execution inside those streams (`execute_batch_transform()`) uses synchronous wasmtime `TypedFunc::call()` which blocks the tokio worker thread. A long-running or infinite-loop WASM module would block one of tokio's limited worker threads, degrading all async operations on the node.

## Affected Files

- `crates/lens/src/wasm.rs:414-639` — `execute_batch_transform()` runs synchronous code
- `crates/lens/src/wasm.rs:554-563` — Synchronous `transform_fn.call()` in a loop
- `crates/lens/src/pipeline.rs:89` — `tokio::spawn(pipeline.run())`
- `crates/lens/src/pipeline.rs:176-189` — PipelineProcessor::run() calls transform in async context

## Details

### The Blocking Call Chain

1. `pipeline.rs:89`: `tokio::spawn(pipeline.run())` — runs on tokio thread pool
2. `pipeline.rs:178`: `transform_to_target()` — async function
3. `pipeline.rs:378-384`: `store.transform()` / `store.inverse()` — returns async stream
4. `wasm.rs:263-311`: Stream unfold calls `execute_batch_transform()`
5. `wasm.rs:554-563`: **Synchronous** `transform_fn.call(&mut store, ())` in a loop

The `TypedFunc::call()` is a synchronous wasmtime call that executes WASM instructions. Without fuel metering (finding 31), this can run for an arbitrary duration, blocking the tokio worker thread.

### Impact

tokio uses a thread pool with a default of `num_cpus` worker threads. If a WASM transform blocks one thread for an extended period (or indefinitely with an infinite loop), the node loses 1/N of its async processing capacity. Multiple concurrent lens operations could block all worker threads, making the node completely unresponsive to HTTP requests, P2P operations, and all other async work.

## Remediation

1. **Use `spawn_blocking()`**: Move WASM execution to the blocking thread pool:
   ```rust
   let result = tokio::task::spawn_blocking(move || {
       execute_batch_transform(&engine, &module, input_docs, arguments, inverse)
   }).await??;
   ```

2. **Dedicated thread pool**: Create a separate thread pool for WASM execution to isolate it from tokio workers.

3. **Combined with fuel metering** (finding 31): Even on a blocking thread, fuel metering ensures the WASM execution terminates.

## Test Gap

No test verifies that WASM execution does not block the tokio runtime. No test measures the impact of a slow WASM module on concurrent HTTP request latency.
