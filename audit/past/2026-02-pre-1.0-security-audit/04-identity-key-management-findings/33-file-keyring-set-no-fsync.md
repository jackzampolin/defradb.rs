# FileKeyring set(): No fsync After Writing Key File

- **Severity**: Low
- **Category**: Data Durability
- **Status**: Open

## Summary

FileKeyring's `set()` method writes encrypted key data but does not call `sync_all()` / `fsync()` after writing. A system crash between the `write_all()` and the OS flushing the page cache could result in a zero-length or partially-written key file. The SystemdCredsKeyring `set()` method correctly calls `file.sync_all()` after writing.

## Affected Files

- `crates/keyring/src/file.rs:98-121` — no sync_all after write
- `crates/keyring/src/systemd_creds.rs:121-136` — correctly calls sync_all (line 133)

## Details

```rust
// file.rs:106-112 (FileKeyring — no fsync)
let mut file = std::fs::OpenOptions::new()
    .write(true)
    .create(true)
    .truncate(true)
    .mode(0o600)
    .open(&path)?;
std::io::Write::write_all(&mut file, &cipher)?;
// <-- missing file.sync_all()? here
```

```rust
// systemd_creds.rs:126-133 (SystemdCredsKeyring — correct)
let mut file = fs::OpenOptions::new()
    .write(true)
    .create(true)
    .truncate(true)
    .mode(0o600)
    .open(&path)?;
file.write_all(&encrypted)?;
file.sync_all()?;  // <-- correct
```

**Impact**: On crash, the key file could be:
- Truncated to zero bytes (truncate happened, write didn't flush)
- Partially written

The next `get()` would return `Error::Decryption` (invalid JWE format), which is a safe failure mode — but the key is lost and must be regenerated.

## Remediation

Add `file.sync_all()?` after `write_all()` in `FileKeyring::set()` for consistency with SystemdCredsKeyring.

## Test Gap

- No test for crash recovery / durability of written keys.
