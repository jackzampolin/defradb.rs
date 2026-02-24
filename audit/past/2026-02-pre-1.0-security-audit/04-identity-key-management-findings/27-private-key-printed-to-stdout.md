# CLI: Private Key Material Printed to stdout

- **Severity**: Medium
- **Category**: Credential Leakage
- **Status**: Open

## Summary

The `identity new` command (without `--name`) prints the raw private key in hex to stdout. The `keyring export` command also prints key material to stdout. Terminal scrollback buffers, shell history files, and pipe destinations can retain this sensitive material indefinitely. The private key hex string is stored in a non-zeroizing `String`.

## Affected Files

- `crates/cli/src/commands/identity.rs:162-178` — prints `PrivateKey` hex to stdout
- `crates/cli/src/commands/keyring_cmd.rs:176-190` — export prints key to stdout

## Details

### identity new (without --name)

```rust
// identity.rs:162-178
let private_key_hex = hex::encode(&raw_bytes);  // String, not zeroized
// ...
println!("Private key: {}", private_key_hex);   // text mode
// or in JSON mode:
"PrivateKey": private_key_hex,
```

When `--name` is provided, the key is stored in the keyring and only the DID is printed (safe). Without `--name`, the full private key is printed to stdout — this is the Go-compatible behavior for ephemeral key generation.

### keyring export

```rust
// keyring_cmd.rs:176-190
let key = keyring.get(&self.name).map_err(...)?;  // plain Vec<u8>
if self.raw {
    io::stdout().write_all(&key)?;  // raw bytes to stdout
} else {
    println!("{}", hex_encode(&key));  // hex to stdout
}
```

### keyring import — key as CLI argument

```rust
// keyring_cmd.rs:200
pub key_hex: Option<String>,  // CLI argument, visible in `ps`
```

The `keyring import` command accepts the private key as a positional CLI argument (`key_hex`). This is visible in process listings (`ps aux`), `/proc/PID/cmdline`, and shell history.

**Mitigating factors**:
1. Go DefraDB has the same behavior — this is compatibility-required.
2. `identity new --name <name>` is the recommended path and does NOT print the key.
3. The `--stdin` flag for import avoids the process listing exposure.

## Remediation

1. Add a warning when printing private keys to stdout (e.g., "WARNING: private key material follows — do not share").
2. Consider `--output-file` option for export to avoid terminal scrollback.
3. Document that `keyring import <name> <hex>` exposes the key in process listings; recommend `--stdin` instead.
4. Zeroize the `private_key_hex` string and `key` Vec after use.

## Test Gap

- No test verifies that `--stdin` mode is recommended over positional argument.
- No test for key material zeroization after CLI operations.
