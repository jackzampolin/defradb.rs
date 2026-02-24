# 31: WASM Sandbox Has No Memory, CPU, or Syscall Restrictions

| Field    | Value |
|----------|-------|
| Severity | HIGH |
| Category | Sandbox Escape / Resource Exhaustion |
| Status   | Confirmed |

## Summary

The wasmtime WASM runtime in `crates/lens/src/wasm.rs` is configured with `Engine::default()` — no memory limits, no fuel metering, no execution timeout, and no WASI capabilities are configured (which is actually safe). A malicious or buggy WASM module can: (1) allocate unbounded memory inside the sandbox, (2) execute an infinite loop blocking the host thread/task indefinitely, and (3) produce unbounded output by calling `transform()` in a loop. The absence of resource limits means a single malicious lens transform can DoS the entire node.

## Affected Files

- `crates/lens/src/wasm.rs:58-59` — `Engine::default()`, no `Config`
- `crates/lens/src/wasm.rs:421` — `WasmStore::new(engine, ...)`, no `Store::limiter()`
- `crates/lens/src/wasm.rs:559-563` — unbounded `loop { transform_fn.call() }` with no iteration cap
- `crates/lens/src/wasm.rs:271` — `docs.collect().await` collects all input docs into memory
- `crates/lens/src/pipeline.rs:89` — `tokio::spawn(pipeline.run())` — WASM runs on tokio thread pool
- `crates/lens/Cargo.toml:41` — `wasmtime = { version = "27" }`

## Details

### No Memory Limits

```rust
pub fn new() -> Result<Self> {
    let engine = Engine::default(); // No Config — default limits
    Ok(Self { engine, modules: ..., configs: ... })
}
```

wasmtime's `Engine::default()` creates an engine with no custom `Config`. The `Store` is created without a `StoreLimiter`:

```rust
let mut store = WasmStore::new(engine, BatchHostState::new(input_docs));
```

Without `store.limiter(...)`, a WASM module can:
- Call `memory.grow()` unlimited times
- Allocate gigabytes of linear memory
- The host process will OOM-kill

wasmtime 27 supports `StoreLimitsBuilder` for configuring memory limits. This is not used.

### No CPU/Instruction Limits (No Fuel Metering)

wasmtime supports fuel metering via `Config::consume_fuel(true)` and `Store::set_fuel()`. Neither is configured.

Without fuel metering, a WASM module can execute an infinite loop:

```wasm
(func $transform (loop br 0))
```

This blocks the calling thread indefinitely. Since the pipeline processor runs via `tokio::spawn()` (`pipeline.rs:89`), this would block a tokio worker thread, degrading all async processing on the node.

### No Execution Timeout

wasmtime supports `Store::set_epoch_deadline()` for wall-clock timeouts. Not configured.

There is also no `tokio::time::timeout()` wrapper around the WASM execution call in `execute_batch_transform()`.

### Unbounded Output Loop

```rust
loop {
    let result_offset = transform_fn.call(store.as_context_mut(), ())
        .map_err(|e| ...)?;
    if result_offset == 0 { break; }
    // ... parse output doc ...
    output_docs.push(doc);
}
```

A malicious WASM module's `transform` function can return valid documents indefinitely (never returning EOS marker 127), causing unbounded memory growth in `output_docs`.

### Unbounded Input Collection

```rust
let input_docs: Vec<LensDoc> = docs.collect().await;
```

All input documents are collected into a `Vec` before WASM execution. For a large collection, this could use significant memory. Combined with the transform output amplification, memory usage is multiplied.

### WASI Capabilities: Not Granted (Safe)

Notably, no WASI capabilities are configured. The `WasiCtxBuilder` is not used anywhere. The WASM modules are instantiated without WASI imports — they only get a custom `lens::next` import function. This means WASM modules **cannot** access the filesystem, network, environment variables, stdin/stdout, or any other host resource. This is the correct security posture.

### WASM Module Validation: Handled by wasmtime (Safe)

wasmtime validates WASM module binaries during `Module::from_file()` and `Module::new()`. Invalid WASM will be rejected. However, there is no size limit on module binaries — a multi-gigabyte `.wasm` file would be read into memory and compiled.

### wasmtime Version

The dependency specifies `wasmtime = { version = "27" }`. wasmtime 27 (released late 2024) has no known critical CVEs. The Bytecode Alliance maintains an active security advisory process.

## Remediation

1. **Memory limits**: Configure `StoreLimitsBuilder` with a maximum memory (e.g., 64MB):
   ```rust
   use wasmtime::{Config, StoreLimits, StoreLimitsBuilder};
   let limits = StoreLimitsBuilder::new()
       .memory_size(64 * 1024 * 1024) // 64MB
       .build();
   let mut store = WasmStore::new(&engine, (host_state, limits));
   store.limiter(|(_, limiter)| limiter);
   ```

2. **Fuel metering**: Enable fuel consumption to prevent infinite loops:
   ```rust
   let mut config = Config::new();
   config.consume_fuel(true);
   // Before each transform call:
   store.set_fuel(1_000_000)?; // ~1M instructions
   ```

3. **Output cap**: Add an iteration limit to the transform output loop:
   ```rust
   const MAX_OUTPUT_DOCS: usize = 10_000;
   if output_docs.len() >= MAX_OUTPUT_DOCS {
       return Err(Error::WasmExecution("output limit exceeded"));
   }
   ```

4. **Execution timeout**: Wrap WASM execution in `tokio::time::timeout()`:
   ```rust
   tokio::time::timeout(Duration::from_secs(30), async {
       execute_batch_transform(...)
   }).await??
   ```

5. **Module size limit**: Reject WASM module files larger than a configured maximum (e.g., 10MB) before passing to `Module::from_file()`.

6. **Dedicated thread pool**: Execute WASM transforms on `tokio::task::spawn_blocking()` or a dedicated thread pool to prevent blocking async workers.

## Test Gap

No tests exercise WASM resource limits. No test verifies behavior with a module that allocates excessive memory or runs an infinite loop. No test checks the output document count limit. The existing tests (`test_wasm_store_creation`, `test_invalid_lens_config`) only verify basic construction.
