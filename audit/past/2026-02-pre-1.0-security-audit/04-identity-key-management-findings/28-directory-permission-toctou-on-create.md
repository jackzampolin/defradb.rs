# Directory Permissions: TOCTOU Between create_dir_all and set_permissions

- **Severity**: Low
- **Category**: File Permission Race
- **Status**: Open (accepted risk)

## Summary

Both FileKeyring and SystemdCredsKeyring create directories with `fs::create_dir_all()` followed by `fs::set_permissions(0o700)`. The default permissions from `create_dir_all` are affected by the process umask (typically `0o022`, resulting in `0o755`). There is a brief window between directory creation and permission restriction where another user on the system could access the directory.

## Affected Files

- `crates/keyring/src/file.rs:46-53` — `create_dir_all` then `set_permissions`
- `crates/keyring/src/systemd_creds.rs:33-37` — same pattern

## Details

```rust
// file.rs:46-53
fs::create_dir_all(&dir)?;

#[cfg(unix)]
{
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))?;
}
```

**Race window**: Between `create_dir_all()` (creates with umask-modified permissions, typically `0o755`) and `set_permissions()` (restricts to `0o700`), another process could:
1. List the directory contents
2. Access any files already present (none on first creation, but relevant on restarts)

**Mitigating factors**:
1. On first run, the directory is empty — there's nothing to access.
2. On subsequent runs, `create_dir_all()` is a no-op (directory exists) and `set_permissions()` only tightens permissions.
3. Key files are created with `OpenOptions::mode(0o600)` (atomic on Unix), so individual key files are properly protected from creation.
4. The practical risk requires a local attacker actively monitoring for new directory creation.

**Key file creation is correct**: The `set()` method uses `OpenOptions::mode(0o600)`:

```rust
// file.rs:105-111
use std::os::unix::fs::OpenOptionsExt;
let mut file = std::fs::OpenOptions::new()
    .write(true)
    .create(true)
    .truncate(true)
    .mode(0o600)   // atomic — set at open(2) time, not chmod after
    .open(&path)?;
```

This is the correct approach — the mode bits are set in the `open(2)` syscall, so there is no TOCTOU race for individual key files. The `umask` *does* affect `OpenOptions::mode()`, but typical umask values (0o022) only remove group/other write bits, which are already absent in 0o600.

## Remediation

1. Set umask to `0o077` before `create_dir_all()` and restore after, or use a platform-specific mkdir with explicit mode.
2. Alternatively, accept this as a low-risk known limitation — the key files themselves are atomically protected.
3. Consider using `DirBuilder::new().mode(0o700).recursive(true).create(&dir)` on Unix, which sets the mode on creation (though it still goes through umask).

## Test Gap

- Existing tests verify directory permissions after creation (line 387 of integration_tests.rs), confirming the `set_permissions` call works.
- No test for the race window between `create_dir_all` and `set_permissions`.
