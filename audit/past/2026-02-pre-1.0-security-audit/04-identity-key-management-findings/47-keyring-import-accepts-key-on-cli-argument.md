# keyring import Accepts Key as CLI Argument

- **Severity**: Medium
- **Category**: Credential Safety
- **Status**: Confirmed

## Summary

The `keyring import <name> <hex-key>` command accepts the private key as a positional CLI argument, making it visible in the process table, shell history, and audit logs. This parallels the Go CLI behavior.

## Affected Files

- `crates/cli/src/commands/keyring_cmd.rs:196-205` (ImportArgs definition)
- `crates/cli/src/commands/keyring_cmd.rs:210-221` (execute)

## Details

```rust
// keyring_cmd.rs:196-205
pub struct ImportArgs {
    /// Name for the imported key
    pub name: String,
    /// Hex-encoded private key (Go-compatible positional argument)
    pub key_hex: Option<String>,   // ← private key on command line
    /// Read hex-encoded key from stdin (Rust extension)
    #[arg(long)]
    pub stdin: bool,               // ← safe alternative
}
```

Usage:
```bash
# Unsafe — key visible in ps/history
defradb keyring import my-key a1b2c3d4e5f6...

# Safe — key read from stdin
echo "a1b2c3d4e5f6..." | defradb keyring import my-key --stdin
```

The `--stdin` flag is a Rust extension not present in Go CLI. The positional argument matches Go CLI behavior.

**Mitigating factor**: The `--stdin` alternative exists for secure usage. The `identity import` command only supports `--stdin` and `--file` (no positional key argument), which is the safer design.

## Remediation

Add a deprecation or security warning when positional key_hex is used:

```rust
if self.key_hex.is_some() {
    eprintln!("WARNING: Passing keys as arguments is visible in process listings. \
               Consider using --stdin instead.");
}
```

## Test Gap

No test for stdin-based import. Integration tests use the positional argument form.
