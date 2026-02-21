# SystemdCreds: No Secure Deletion of .cred Files

- **Severity**: Low
- **Category**: Secure Deletion
- **Status**: Open

## Summary

The SystemdCredsKeyring `delete()` method removes `.cred` files with a simple `fs::remove_file()` without zero-filling the content first. While the FileKeyring implements zero-before-unlink as defense-in-depth, the SystemdCreds backend does not.

## Affected Files

- `crates/keyring/src/systemd_creds.rs:150-159` — `delete()` uses bare `remove_file()`

## Details

```rust
// systemd_creds.rs:150-159
fn delete(&self, name: &str) -> Result<()> {
    let path = self.key_path(name)?;
    fs::remove_file(&path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            Error::NotFound(name.to_string())
        } else {
            Error::Io(e)
        }
    })
}
```

**Mitigating factors**: systemd-creds encrypts data with a key derived from the TPM and/or machine-specific secrets. The `.cred` file contents are useless without the TPM/machine key, making the ciphertext recovery from disk less of a concern than with password-based encryption (FileKeyring). This is arguably the correct design choice — the threat model for systemd-creds assumes the encryption is the protection, not file deletion.

**Inconsistency**: The two file-based backends (FileKeyring and SystemdCredsKeyring) have different deletion behaviors. This asymmetry should be documented.

## Remediation

1. Document that SystemdCreds relies on TPM-bound encryption rather than secure file deletion.
2. Optionally add zero-fill for consistency, though it provides minimal additional security.

## Test Gap

- No test verifies deletion behavior for either backend.
