# 09: FFI Backup Export Writes to Arbitrary Filesystem Path

| Field    | Value |
|----------|-------|
| Severity | MEDIUM |
| Category | Path Traversal / Arbitrary File Write |
| Status   | Confirmed |

## Summary

The FFI `basic_export` function in `crates/ffi/src/backup/export.rs` writes backup data to an arbitrary filesystem path provided by the caller. The path comes from a JSON config's `filepath` field with no validation, sanitization, or directory confinement. Similarly, `basic_import` reads from an arbitrary path. The CLI backup commands (`backup.rs`) have the same pattern but are lower risk since the user directly controls the path.

## Affected Files

- `crates/ffi/src/backup/export.rs:28-59` (FFI export — arbitrary `fs::write`)
- `crates/ffi/src/backup/import.rs:26-49` (FFI import — arbitrary `fs::read_to_string`)
- `crates/cli/src/commands/client/backup.rs:81-84` (CLI export — `fs::write` on user PathBuf)
- `crates/cli/src/commands/client/backup.rs:94-97` (CLI import — `fs::read_to_string` on user PathBuf)

## Details

### FFI Export (Highest Risk)

```rust
pub unsafe extern "C" fn basic_export(node_ptr: usize, config_json: *const c_char) -> FfiResult {
    let config: BackupConfig = serde_json::from_str(&config_str)?;
    // ...
    let temp_path = format!("{}.temp", config.filepath);
    fs::write(&temp_path, &json_output)?;     // Arbitrary write
    fs::rename(&temp_path, &config.filepath)?; // Atomic rename
}
```

The `filepath` field is an arbitrary string from the FFI caller (Go). Possible attacks:
- `filepath: "/etc/cron.d/malicious"` — write a cron job
- `filepath: "/home/user/.ssh/authorized_keys"` — inject SSH keys
- `filepath: "/tmp/existing-file"` — overwrite any file the process can write

### FFI Import (Moderate Risk)

```rust
pub unsafe extern "C" fn basic_import(node_ptr: usize, filepath: *const c_char) -> FfiResult {
    let content = fs::read_to_string(&path_str)?;
    // Parses as JSON — if not valid JSON, returns error with error message
    // Error message may include partial file contents
}
```

- `filepath: "/etc/shadow"` — attempt to parse as JSON, error message may leak contents
- `filepath: "/dev/zero"` — `read_to_string` on device file could hang or OOM
- `filepath: "/proc/self/maps"` — read process memory layout

### CLI Backup (Lower Risk)

The CLI paths come from command-line arguments, so the user already has filesystem access. However:
- No file size limit on `fs::read_to_string` during import
- No symlink resolution — a symlink could redirect reads/writes

### HTTP Backup Handler (SAFE)

The HTTP handler in `crates/http/src/handlers/backup.rs` is correctly designed:
- Export returns data in the response body (no filesystem write)
- Import reads data from the request body (no filesystem read)
- When Go-format `filepath` field is present, it is **explicitly rejected** with a clear error message
- Import data is capped at 100MB (`MAX_IMPORT_SIZE`)

## Remediation

1. **FFI path validation**: Validate that the filepath does not contain `..`, is not a symlink to outside the data directory, and is not a special file (`/dev/*`, `/proc/*`).
2. **FFI path confinement**: Consider restricting FFI backup paths to the node's data directory or a configured backup directory.
3. **File size limit on CLI import**: Add a size check before `fs::read_to_string` to prevent OOM from large files.
4. **Temp file safety**: The `format!("{}.temp", config.filepath)` pattern in export could fail if the original path has no parent directory or lands in a restricted location. Consider using `tempfile` crate.

## Test Gap

- No test attempts to export to a path outside the temp directory
- No test verifies rejection of `..` components or symlinks in backup paths
- No test checks behavior with special files (`/dev/null`, `/dev/zero`)
- HTTP handler's `filepath` rejection is not tested (only structural tests exist)
