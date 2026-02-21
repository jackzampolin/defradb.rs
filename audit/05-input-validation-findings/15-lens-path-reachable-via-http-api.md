# 15: Lens WASM Path Traversal Reachable via HTTP API

| Field    | Value |
|----------|-------|
| Severity | HIGH |
| Category | Remote Path Traversal / Arbitrary File Read |
| Status   | Confirmed |

## Summary

The lens WASM module path traversal vulnerability (finding 08) is reachable by **remote attackers** through the HTTP API. The `POST /api/v0/lens/set` and `POST /api/v0/lens` endpoints accept lens configurations containing a `Path` field that flows unvalidated to the WASM module loader. A remote attacker can read arbitrary files from the server's filesystem.

## Attack Chain

```
Remote Attacker → HTTP POST /api/v0/lens/set → handlers/lens.rs:set_migration()
  → LensOperations.set_migration() → LensAdapter → DB.set_migration()
  → LensConfig parsed with Path field → WasmTransformStore.add()
  → load_module() → strip_prefix("file://") → Module::from_file(arbitrary_path)
```

## Affected Files

- `crates/http/src/handlers/lens.rs:45-66` (HTTP entry point)
- `crates/http/src/handlers/lens.rs:96-122` (add_lens entry point)
- `crates/http/src/router/routes.rs:68,171` (route registration)
- `crates/cli/src/lens_adapter.rs:25-56` (adapter that bridges HTTP to DB)
- `crates/db/src/migration/set_migration.rs:32+` (stores config, triggers load)
- `crates/lens/src/wasm.rs:69-89` (actual filesystem read)

## Details

### HTTP Request

```
POST /api/v0/lens/set
Content-Type: text/plain

{
  "SourceSchemaVersionID": "bafyrei_v1",
  "DestinationSchemaVersionID": "bafyrei_v2",
  "Lenses": [{
    "Path": "file://../../../etc/passwd"
  }]
}
```

### What Happens

1. The HTTP handler receives the body as a raw string
2. `LensAdapter::set_migration()` parses it as `LensConfig`
3. `DB::set_migration()` calls `WasmTransformStore::add()` which calls `load_module()`
4. `load_module()` strips `file://`, creates `Path::new("../../../etc/passwd")`
5. `Module::from_file()` reads the file from disk

### Information Disclosure

Even though the file content won't be valid WASM, the **error message** reveals whether the file exists and may include file content in some error paths:
- File not found → "failed to load WASM from ../../../etc/passwd: file not found"
- File exists but invalid → "failed to load WASM from ../../../etc/passwd: invalid module"

In the FFI path (`std::fs::read()`), the file bytes are fully read before validation.

### Authentication Check

The HTTP lens endpoints require `MigrationSet` or `LensCreate` NAC permission. However:
- NAC is **optional** and disabled by default
- When NAC is disabled, the endpoint is accessible to any client that can reach the HTTP API
- Even with NAC enabled, any authenticated user with `MigrationSet` permission can exploit this

### P2P Amplification

If a schema migration is replicated via P2P, the receiving node may attempt to load the WASM module from the path specified by the originating node. This means:
- Node A sends a schema with lens path `file:///etc/shadow`
- Node B receives the schema via P2P replication
- Node B attempts to read `/etc/shadow` from its own filesystem

## Remediation

1. **Immediate**: In the HTTP lens handlers, reject any configuration where `Path` contains `..`, starts with `/`, or uses the `file://` scheme. Only accept embedded module bytes (`Module` field) via HTTP.
2. **Defense in depth**: In `WasmTransformStore::load_module()`, validate the path after stripping `file://`:
   ```rust
   let clean_path = path_str.strip_prefix("file://").unwrap_or(path_str);
   let canonical = std::fs::canonicalize(clean_path)?;
   if !canonical.starts_with(&allowed_wasm_dir) {
       return Err(Error::InvalidConfig("WASM module path outside allowed directory"));
   }
   ```
3. **P2P defense**: When deserializing lens configs from P2P messages, strip the `path` field entirely. Only accept `module` bytes from P2P peers.

## Test Gap

- No integration test sends a lens path through the HTTP API
- No test verifies that `..` paths are rejected
- No test covers the P2P replication of lens configs with file paths
- No test verifies NAC enforcement on the lens endpoints
