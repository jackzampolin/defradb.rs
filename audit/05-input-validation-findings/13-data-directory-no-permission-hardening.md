# 13: Data Directory Created Without Permission Hardening

| Field    | Value |
|----------|-------|
| Severity | LOW |
| Category | File Permissions |
| Status   | Confirmed |

## Summary

The node's data directory is created using `std::fs::create_dir_all()` with default permissions (typically 0755 on Unix). No explicit permission setting restricts the data directory to the running user. The config file is written with default permissions. While the storage backends (redb, fjall, rocksdb) may set their own file permissions, the directory structure itself is world-readable.

## Affected Files

- `crates/cli/src/config/mod.rs:232` — `fs::create_dir_all(&self.rootdir)` (data directory creation)
- `crates/cli/src/config/mod.rs:241` — `fs::write(&config_path, yaml)` (config file write)
- `crates/keyring/src/file.rs:117` — `fs::write(&path, cipher)` (encrypted key write)

## Details

### Data Directory Creation

```rust
// crates/cli/src/config/mod.rs
if !self.rootdir.exists() {
    fs::create_dir_all(&self.rootdir).map_err(|e| Error::CreateDirectory { ... })?;
}
```

Default `create_dir_all` uses the process's umask (typically 0022), resulting in mode 0755 — world-readable, owner-writable. This means:
- Other users on the system can list directory contents
- Other users can read the config file (may contain API addresses, ACP settings)
- Other users can browse the data directory structure

### Config File

```rust
fs::write(&config_path, yaml).map_err(|e| Error::WriteConfig { ... })?;
```

Written with default permissions (typically 0644) — world-readable. Contains API configuration but not secrets.

### Keyring Files

The `FileKeyring` writes encrypted key files with default permissions. While the key data is encrypted, the filenames reveal which keys exist.

### What Should Be Done

Go DefraDB creates the data directory with 0700 permissions:
```go
os.MkdirAll(rootDir, 0700)
```

## Remediation

1. **Set directory permissions**: After `create_dir_all`, set permissions to 0700 using `std::os::unix::fs::PermissionsExt`:
   ```rust
   #[cfg(unix)]
   {
       use std::os::unix::fs::PermissionsExt;
       std::fs::set_permissions(&self.rootdir, std::fs::Permissions::from_mode(0o700))?;
   }
   ```
2. **Set config file permissions**: Write with 0600 permissions.
3. **Keyring directory**: Ensure the keyring directory is 0700.

## Test Gap

- No test verifies directory permissions after creation
- No test checks config file permissions
- No cross-platform permission test exists
