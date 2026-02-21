# Secure Deletion: No fsync Between Zero-Fill and Unlink

- **Severity**: Low
- **Category**: Secure Deletion
- **Status**: Open

## Summary

FileKeyring's `delete()` method zero-fills the file before unlinking, but does not call `fsync()` / `sync_all()` between writing zeros and removing the file. This means the zeros may only reach the page cache, and the original ciphertext could survive on disk if the system crashes or the filesystem journals the unlink before flushing the zero-write. Additionally, the zero-write result is silently discarded.

## Affected Files

- `crates/keyring/src/file.rs:135-151` — `FileKeyring::delete()`

## Details

```rust
// file.rs:135-151
fn delete(&self, name: &str) -> Result<()> {
    let path = self.key_path(name)?;
    // Overwrite file contents with zeros before unlinking.
    // Defense-in-depth: prevents trivial recovery from filesystem journals.
    // (SSD wear-leveling may retain old blocks regardless.)
    if let Ok(metadata) = fs::metadata(&path) {
        let zeros = vec![0u8; metadata.len() as usize];
        let _ = fs::write(&path, &zeros);  // Result discarded
    }
    fs::remove_file(&path).map_err(|e| {
        // ...
    })
}
```

**Issues**:

1. **No fsync**: `fs::write()` goes through the page cache. Without `sync_all()`, the OS may reorder the unlink ahead of the zero-write flush. On crash, the original ciphertext could still be on disk.

2. **Result discarded**: `let _ = fs::write(...)` ignores write failures (e.g., read-only filesystem, disk full). The file is then unlinked with original ciphertext intact on disk (journal/snapshot).

3. **CoW filesystems**: On APFS (macOS default), btrfs, and ZFS, writing zeros creates a *new* block — the original data block is retained until garbage collection. The existing comment acknowledges SSD wear-leveling but not CoW filesystems.

**Comparison with SystemdCreds**: The SystemdCredsKeyring `delete()` does *not* zero-fill at all — it just unlinks. This is arguably correct because systemd-creds encrypts with TPM-bound keys, making the ciphertext useless without the TPM. However, the asymmetry is worth noting.

**Comparison with SystemdCreds `set()`**: SystemdCredsKeyring `set()` *does* call `file.sync_all()` after writing (line 133). FileKeyring `set()` does not, creating an inconsistency.

## Remediation

1. Open the file, write zeros, call `sync_all()`, then `drop()` + `remove_file()`:
   ```rust
   if let Ok(metadata) = fs::metadata(&path) {
       if let Ok(mut file) = fs::OpenOptions::new().write(true).open(&path) {
           let zeros = vec![0u8; metadata.len() as usize];
           let _ = file.write_all(&zeros);
           let _ = file.sync_all();
       }
   }
   ```
2. Log a warning if the zero-write fails rather than silently ignoring it.
3. Add `sync_all()` to `FileKeyring::set()` for consistency with SystemdCredsKeyring.
4. Document the CoW filesystem limitation (APFS, btrfs, ZFS) — zeroing is best-effort on these filesystems.

## Test Gap

- No test verifies that file content is zeroed before deletion.
- No test checks that fsync is called.
- No test for delete failure handling (e.g., read-only directory).
