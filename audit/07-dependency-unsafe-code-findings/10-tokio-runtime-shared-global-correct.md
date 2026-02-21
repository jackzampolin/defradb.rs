# 10: Tokio Runtime — Single Global, Correctly Shared

| Field | Value |
|-------|-------|
| **Severity** | GREEN |
| **Category** | Async Runtime / Thread Safety |
| **Status** | Verified |

## Summary

The FFI crate uses a single global Tokio runtime (`OnceLock<Runtime>`) shared across all nodes and FFI calls. This is the correct design for bridging sync FFI to async Rust. The runtime is multi-threaded, initialized once via `defra_init()`, and accessed safely from multiple goroutines.

## Verified Properties

### 1. Single initialization, safe concurrent access

```rust
// crates/ffi/src/runtime.rs
pub static RUNTIME: OnceLock<Runtime> = OnceLock::new();

pub fn init_runtime() -> bool {
    if RUNTIME.get().is_some() { return true; }
    // ...
    RUNTIME.get_or_init(|| rt);
}
```

`OnceLock` guarantees the runtime is initialized exactly once, even with concurrent `defra_init()` calls. The `get()` check before `get_or_init()` is an optimization to avoid the build attempt on subsequent calls.

### 2. Multi-threaded runtime

```rust
tokio::runtime::Builder::new_multi_thread()
    .enable_all()
    .build()
```

This creates a runtime with worker threads matching CPU cores. All `block_on()` calls from FFI functions will execute async work on these worker threads.

### 3. block_on() from FFI is correct

Every FFI function calls `rt.block_on(async { ... })`. Since FFI functions are called from Go goroutines (which run on OS threads), and `block_on()` blocks the calling thread until the future completes, this correctly bridges sync→async.

### 4. No nested block_on()

The FFI functions use `block_on()` at the top level, and the async code inside uses `.await`. As long as no code inside the async block calls `block_on()` again, there's no risk of the "cannot start a runtime from within a runtime" panic.

Exception: `query/exec.rs:152` uses `spawn_blocking` + `block_on` for thread-local propagation:
```rust
let handle = tokio::runtime::Handle::current();
tokio::task::spawn_blocking(move || {
    defra_core::signing::set_signing_config(config);
    handle.block_on(async { runner.execute(request).await })
})
```

This is safe because `spawn_blocking` runs on a dedicated thread pool (not a tokio worker), and `handle.block_on()` from a non-tokio thread is allowed.

## Residual Risk

- If a future library dependency internally calls `Runtime::block_on()` from within an async context, it would panic. This is mitigated by the standard Rust ecosystem convention of never blocking inside async code.
- The runtime is never shut down (it lives for the process lifetime). This is correct for FFI — the Go process controls the lifetime.

## Test Gap

- `test_runtime_init` verifies initialization
- `test_runtime_handle` verifies async execution
- `test_runtime_init_idempotent` verifies multiple init calls
- Coverage is adequate
