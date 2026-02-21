# keyring export Prints Raw Key Hex to stdout

- **Severity**: Low
- **Category**: Credential Safety
- **Status**: Confirmed — Expected Behavior

## Summary

The `keyring export <name>` command prints the raw key bytes as hex to stdout. The `--raw` flag outputs raw binary bytes. This is the expected and necessary behavior for an export command, but should be used with caution as the key material will appear in terminal scrollback, piped files, and potentially logs.

## Affected Files

- `crates/cli/src/commands/keyring_cmd.rs:176-190` (ExportArgs::execute)

## Details

```rust
// keyring_cmd.rs:183-188
if self.raw {
    io::stdout().write_all(&key)?;   // raw binary
} else {
    println!("{}", hex_encode(&key)); // hex string
}
```

This is functionally equivalent to Go's `defradb keyring export` behavior. There is no `--file` option to write directly to a file with restricted permissions.

## Remediation

Consider adding `--file` option that writes with mode 0600:
```rust
#[arg(long)]
pub file: Option<PathBuf>,

// In execute:
if let Some(ref path) = self.file {
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .write(true).create(true).truncate(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(&key)?;
} else { ... }
```

**Accept as-is** — this is standard CLI tool behavior for key export.

## Test Gap

None needed — export printing is straightforward.
