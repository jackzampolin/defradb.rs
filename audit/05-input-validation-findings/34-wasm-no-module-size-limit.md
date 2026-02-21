# 34: No Size Limit on WASM Module Binaries

| Field    | Value |
|----------|-------|
| Severity | LOW |
| Category | Resource Exhaustion |
| Status   | Confirmed |

## Summary

The lens WASM module loader accepts modules of arbitrary size via both the file path (`Module::from_file()`) and inline bytes (`Module::new()`) routes. A multi-gigabyte WASM file would be read entirely into memory and compiled by wasmtime, consuming significant CPU and memory during compilation even before execution begins. The HTTP endpoint `POST /api/v0/lens/set` and `POST /api/v0/lens` accept lens configurations that can reference arbitrarily large module files or include arbitrarily large base64-encoded module bytes.

## Affected Files

- `crates/lens/src/wasm.rs:69-89` — `load_module()` with no size check
- `crates/lens/src/config.rs:77` — `module: Option<Vec<u8>>` with no size validation
- `crates/http/src/handlers/lens.rs:45-66` — HTTP handler passes config through

## Details

### File Path Route

```rust
fn load_module(&self, lens: &LensModule) -> Result<Module> {
    if let Some(ref path_str) = lens.path {
        let clean_path = path_str.strip_prefix("file://").unwrap_or(path_str);
        let path = Path::new(clean_path);
        Module::from_file(&self.engine, path) // No size check
    }
}
```

`Module::from_file()` reads the entire file into memory then compiles it. No size check before reading.

### Inline Bytes Route

```rust
else if let Some(ref bytes) = lens.module {
    Module::new(&self.engine, bytes) // bytes already in memory, no size limit
}
```

The `LensModule.module` field is `Option<Vec<u8>>` deserialized from base64-encoded JSON. Combined with finding 01 (no HTTP body size limit), a request could contain a very large base64-encoded WASM module.

### Compilation Cost

wasmtime's compilation of a WASM module is CPU-intensive. A 100MB WASM file could take significant time and memory to compile, effectively DoS-ing the node during compilation even if the module itself is well-behaved.

## Remediation

1. **File size check**: Before `Module::from_file()`, check `std::fs::metadata(path).len()` against a configured maximum (e.g., 10MB)
2. **Inline bytes check**: Validate `lens.module.as_ref().map(|b| b.len())` against the same limit
3. **HTTP body size limit**: This is partially addressed by finding 01 (no HTTP body size limit) — fixing that would also bound inline module size

## Test Gap

No test sends a large WASM module (or a large file path) and verifies rejection.
