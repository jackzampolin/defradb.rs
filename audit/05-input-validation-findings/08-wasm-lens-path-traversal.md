# 08: WASM Lens Module Path Traversal via `file://` Prefix

| Field    | Value |
|----------|-------|
| Severity | MEDIUM |
| Category | Path Traversal |
| Status   | Confirmed |

## Summary

The WASM lens module loader in `crates/lens/src/wasm.rs` and `crates/ffi/src/lens.rs` strips the `file://` URL prefix and passes the remaining string directly to `Module::from_file()` / `std::fs::read()` with **no path traversal validation**. An attacker who can influence a lens module path (via schema migration, HTTP API, or P2P replication) could read arbitrary files from the node's filesystem.

## Affected Files

- `crates/lens/src/wasm.rs:70-78` (WASM transform store)
- `crates/ffi/src/lens.rs:27-31` (FFI lens loader)
- `crates/lens/src/config.rs:60-64` (`LensModule.path` — String with no validation)

## Details

### Vulnerable Code (wasm.rs)

```rust
fn load_module(&self, lens: &LensModule) -> Result<Module> {
    if let Some(ref path_str) = lens.path {
        // Strip file:// URL scheme if present (Go sends paths as file:// URLs)
        let clean_path = path_str.strip_prefix("file://").unwrap_or(path_str);
        let path = Path::new(clean_path);
        Module::from_file(&self.engine, path).map_err(|e| { ... })
    }
    ...
}
```

### Vulnerable Code (ffi/lens.rs)

```rust
fn read_wasm_bytes(module: &LensModule) -> Result<Vec<u8>, String> {
    if let Some(ref path_str) = module.path {
        let clean_path = path_str.strip_prefix("file://").unwrap_or(path_str);
        std::fs::read(clean_path)
            .map_err(|e| format!("failed to read WASM file {}: {}", clean_path, e))
    }
    ...
}
```

### Attack Vectors

1. **HTTP API** (`POST /api/v0/lens/set` and `POST /api/v0/lens`): A remote client sends a lens configuration with `"Path": "file://../../../etc/passwd"`. The HTTP handler in `crates/http/src/handlers/lens.rs` accepts the configuration as a JSON string and passes it through to `set_migration()` which eventually calls `load_module()`. **No path validation occurs at any layer.**

2. **FFI from Go**: Go passes `lens_add(node, '{"Path": "file://../../../etc/passwd"}')` through the C FFI boundary. The `read_wasm_bytes()` function strips `file://` and reads the arbitrary path.

3. **P2P Schema Replication**: If a peer's schema migration includes a lens with a file path, and that schema is replicated, the receiving node may attempt to load the WASM module from its local filesystem using the attacker-controlled path. The `LensModule.path` is a plain `String` field deserialized from JSON with no validation.

### What the Path Can Do

- `file://../../../etc/passwd` → reads `/etc/passwd`
- `file:///etc/shadow` → reads `/etc/shadow` (if process has permission)
- `file:///proc/self/environ` → reads environment variables (Linux)
- The file content is passed to `Module::from_file()` which will fail WASM validation, but the **file is still read into memory**. In the FFI path (`std::fs::read()`), the bytes are fully returned.

### Missing Controls

- No `..` component rejection
- No `fs::canonicalize()` to resolve symlinks
- No confinement to a specific directory
- No allowlist of paths
- `LensModule.path` accepts arbitrary strings with no structural validation

## Remediation

1. **Validate lens paths**: Reject paths containing `..` components after stripping `file://`.
2. **Canonicalize**: Use `std::fs::canonicalize()` and verify the resolved path is within an allowed directory.
3. **Restrict to data directory**: Lens WASM modules should only be loadable from the node's data directory or a configured lens module directory.
4. **Validate at deserialization**: Add validation to `LensModule` deserialization to reject paths with traversal components.
5. **P2P defense**: When receiving lens configurations via P2P, require module bytes (not file paths). File paths should only be accepted from local CLI/config.

## Test Gap

No tests verify that path traversal is rejected in lens module paths. The test in `crates/lens/src/wasm.rs` only checks invalid configs (no path and no bytes), not malicious paths. No integration test sends a lens path through the HTTP API.
