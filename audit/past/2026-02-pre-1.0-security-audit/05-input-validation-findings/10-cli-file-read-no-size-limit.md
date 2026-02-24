# 10: CLI File Reading Operations Have No Size Limit

| Field    | Value |
|----------|-------|
| Severity | LOW |
| Category | Resource Exhaustion |
| Status   | Confirmed |

## Summary

Multiple CLI commands use `std::fs::read_to_string()` on user-specified file paths with no file size limit. While these paths come from CLI arguments (so the user already has local access), reading unbounded files could cause OOM in the DefraDB process. More critically, paths pointing to special files (`/dev/zero`, `/dev/urandom`, named pipes) could cause the process to hang or consume unlimited memory.

## Affected Files

- `crates/cli/src/commands/client/backup.rs:94` — `fs::read_to_string(&self.file)` for import
- `crates/cli/src/commands/client/schema.rs:63` — `fs::read_to_string(path)` for schema files
- `crates/cli/src/commands/client/query.rs:69` — `fs::read_to_string(path)` for query files
- `crates/cli/src/commands/client/mod.rs:66` — `get_data_from_args()` shared utility
- `crates/cli/src/commands/client/view.rs:68,79` — query and SDL files for views
- `crates/cli/src/commands/client/lens.rs:85` — via `get_data_from_args()`
- `crates/cli/src/commands/sdl.rs:64` — SDL input files
- `crates/cli/src/commands/identity.rs:211` — JWK import from file
- `crates/ffi/src/backup/import.rs:38` — FFI import path

## Details

### The Pattern

Every CLI file-reading operation follows the same pattern:

```rust
let content = std::fs::read_to_string(path).map_err(|e| Error::ReadFile { ... })?;
```

This reads the entire file into a `String` in memory with no bounds.

### Risk Scenarios

1. **Large file OOM**: `defra client backup import giant-100gb-file.json` would attempt to allocate 100GB.
2. **Device file hang**: `defra client query --file /dev/urandom` would read forever.
3. **Named pipe block**: `defra client schema add --file /tmp/named-pipe` would block until the pipe writer sends data.
4. **Symlink to device**: A symlink from `schema.graphql` → `/dev/zero` would silently redirect.

### Why This Is LOW Severity

All these paths come from CLI arguments, meaning the user already has local system access. The user is effectively attacking their own process. However, the robustness issue means an operator mistake (wrong path) could crash the node rather than failing gracefully.

## Remediation

1. **File size limit**: Check file size with `std::fs::metadata()` before reading. Reasonable limits: schema files (1MB), query files (1MB), backup files (1GB), JWK files (10KB).
2. **Regular file check**: Verify the path is a regular file (not a device, directory, pipe, or socket) using `metadata().file_type().is_file()`.
3. **Shared utility**: The `get_data_from_args()` function in `crates/cli/src/commands/client/mod.rs` is a natural place to add these checks, but not all callers use it.

## Test Gap

No tests verify behavior with large files, device files, or named pipes as input paths.
