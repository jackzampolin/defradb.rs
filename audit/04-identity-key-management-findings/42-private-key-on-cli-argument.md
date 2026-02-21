# Private Key Passed as CLI Argument Visible in Process Table

- **Severity**: Medium
- **Category**: Credential Safety
- **Status**: Confirmed

## Summary

The `--identity` / `-i` flag passes a hex-encoded private key as a command-line argument. On Unix systems, command-line arguments are visible to all users via `ps aux`, `/proc/<pid>/cmdline`, and system audit logs. This exposes the private key to any user with access to the machine's process table.

## Affected Files

- `crates/cli/src/commands/client/mod.rs:80-82` (identity CLI arg definition)
- `crates/cli/src/commands/client/mod.rs:141-142` (identity arg usage)

## Details

```rust
// client/mod.rs:80-82
/// Hex formatted private key used to authenticate with ACP
#[arg(long, short = 'i', global = true)]
pub identity: Option<String>,
```

Usage example:
```bash
defradb client query -i a3b2c1d4e5f6... '{ Users { name } }'
```

This private key is then visible in:
1. `ps aux` output to all local users
2. `/proc/<pid>/cmdline` on Linux
3. Shell history (bash, zsh)
4. System audit logs (auditd, macOS Unified Logging)

The `--identity-name` flag (loading from keyring) is the safe alternative:
```rust
// client/mod.rs:85-86
#[arg(long, global = true)]
pub identity_name: Option<String>,
```

**Mitigating factor**: The `--identity-name` alternative exists and uses the keyring, keeping the key out of the process table. This is a Go compatibility feature — Go's CLI also accepts hex keys directly.

## Remediation

1. Add a deprecation warning when `--identity` is used:
   ```rust
   if self.identity.is_some() {
       eprintln!("WARNING: --identity passes the private key on the command line, \
                  which is visible in process listings. Consider using --identity-name \
                  to load the key from the keyring instead.");
   }
   ```

2. Consider accepting the key via environment variable (`DEFRA_IDENTITY`) as an intermediate option — visible in `/proc/<pid>/environ` but not in `ps aux`.

3. Consider accepting the key via stdin for scripted usage.

## Test Gap

No test validates that `--identity-name` and `--identity` produce equivalent auth tokens for the same key.
