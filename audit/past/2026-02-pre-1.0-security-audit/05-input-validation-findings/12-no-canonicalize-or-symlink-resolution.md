# 12: No `canonicalize()` or Symlink Resolution on Any User-Controlled Path

| Field    | Value |
|----------|-------|
| Severity | LOW |
| Category | Symlink Following / TOCTOU |
| Status   | Confirmed |

## Summary

The codebase uses `std::fs::canonicalize()` **only** in test/tooling code (integration-test workspace root resolution). No production code path resolves symlinks or normalizes paths before performing filesystem operations on user-controlled paths. While Rust's stdlib handles most path safety, symlinks could redirect reads/writes to unexpected locations.

## Affected Files

All files that use `fs::read_to_string`, `fs::write`, or `fs::read` on user-controlled paths:

- `crates/lens/src/wasm.rs:74` — `Module::from_file` on lens path
- `crates/ffi/src/lens.rs:29` — `fs::read` on lens path
- `crates/ffi/src/backup/export.rs:51` — `fs::write` on backup path
- `crates/ffi/src/backup/import.rs:38` — `fs::read_to_string` on backup path
- `crates/cli/src/commands/client/backup.rs:81,94` — CLI backup export/import
- All CLI `fs::read_to_string` calls on `--file` arguments

## Details

### What `canonicalize()` Does

`std::fs::canonicalize()` resolves a path to its absolute, symlink-free form. Without it:

```
/tmp/backup.json → (symlink) → /etc/cron.d/evil
```

A `fs::write("/tmp/backup.json", data)` would follow the symlink and write to `/etc/cron.d/evil`.

### Where `canonicalize()` IS Used

Only in test/tooling code for workspace root:
```rust
// tools/integration-test/src/lib.rs
.join("../..").canonicalize().expect("failed to canonicalize workspace root")
```

### TOCTOU Risk

Even with `canonicalize()`, a time-of-check-time-of-use race could occur:
1. Path is canonicalized and verified
2. Between check and open, symlink is created
3. Open follows the new symlink

However, this requires local filesystem access by an attacker, making it a lower-priority concern.

### Practical Impact

- **CLI**: The user already controls the filesystem. Symlink attacks are user-against-self.
- **FFI**: Go controls the path. If Go passes a symlinked path, Rust follows it. This is acceptable if Go validates the path.
- **Lens paths via HTTP**: This is the highest risk. If a remote attacker sends a lens path that the operator has symlinked (unlikely but possible), the symlink is followed.

## Remediation

1. **FFI backup paths**: Use `canonicalize()` to resolve symlinks, then verify the resolved path is within an allowed directory.
2. **Lens module paths**: Canonicalize after stripping `file://`, verify within allowed WASM module directory.
3. **Config paths**: Already use `resolve_paths()` for relative→absolute conversion, but this does not resolve symlinks.
4. **Consider `O_NOFOLLOW`**: For highest security, open files with `O_NOFOLLOW` flag to reject symlinks entirely. Rust's `std::fs` does not expose this directly, but `std::os::unix::fs::OpenOptionsExt` does.

## Test Gap

- No tests verify symlink handling for any user-controlled path
- No tests create symlinks and verify they are rejected or resolved correctly
- The `canonicalize()` calls in tests are for workspace root, not for testing symlink defense
