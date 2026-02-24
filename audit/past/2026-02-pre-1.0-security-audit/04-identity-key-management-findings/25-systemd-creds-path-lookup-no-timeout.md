# SystemdCreds: PATH-Based Lookup and No Subprocess Timeout

- **Severity**: Low
- **Category**: Subprocess Security
- **Status**: Open

## Summary

The SystemdCredsKeyring invokes `systemd-creds` via `Command::new("systemd-creds")`, which resolves the binary through the `PATH` environment variable. An attacker with write access to a directory earlier in `PATH` could inject a malicious binary. Additionally, the subprocess has no timeout — a hung `systemd-creds` process will block the calling thread indefinitely.

## Affected Files

- `crates/keyring/src/systemd_creds.rs:57` — `Command::new("systemd-creds")`
- `crates/keyring/src/systemd_creds.rs:193` — availability check also uses PATH
- `crates/keyring/src/systemd_creds.rs:73` — `wait_with_output()` blocks indefinitely

## Details

### PATH injection

```rust
// systemd_creds.rs:57
let mut child = Command::new("systemd-creds")
```

If an attacker can modify `PATH` or place a binary named `systemd-creds` in a directory that precedes `/usr/bin` in `PATH`, they can intercept key material passed via stdin.

**Mitigating factors**:
1. The attacker would need write access to a PATH directory, which typically requires the same user or root — at which point they could read the key files directly.
2. systemd-creds is Linux-only and `PATH` injection is a general Linux concern, not specific to this codebase.
3. The systemd project itself uses `PATH` for its own tools.

### No subprocess timeout

```rust
// systemd_creds.rs:73
let output = child
    .wait_with_output()
    .map_err(|e| make_err(format!("systemd-creds {} failed: {}", operation, e)))?;
```

`wait_with_output()` blocks until the child process exits. If `systemd-creds` hangs (e.g., waiting for TPM interaction, or a buggy version), the calling thread blocks forever.

### Positive: stderr is captured, not leaked

```rust
// systemd_creds.rs:60-61
.stdout(Stdio::piped())
.stderr(Stdio::piped())
```

Both stdout and stderr are piped, preventing accidental leakage to parent stderr. The stderr content is only included in error messages and does not contain key material (systemd-creds writes keys to stdout, errors to stderr).

### Positive: stdin pipe is properly closed

```rust
// systemd_creds.rs:65-71
let write_result = {
    let mut stdin = child.stdin.take().ok_or_else(|| ...)?;
    stdin.write_all(input)
};
// stdin dropped here (pipe closed), child can read EOF
```

The stdin pipe is dropped (closed) before `wait_with_output()`, which is correct — the child process sees EOF and can proceed.

## Remediation

1. **PATH**: Consider using an absolute path (`/usr/bin/systemd-creds`) or at minimum document the PATH dependency. However, this reduces portability (different distros may install to different paths).
2. **Timeout**: Use `tokio::time::timeout` or spawn a watchdog thread that kills the child after a reasonable duration (e.g., 30 seconds). Note: the keyring is currently sync, so a simple `std::thread` + `child.kill()` approach would work.

## Test Gap

- No test for `systemd-creds` not found in PATH (covered by `systemd_creds_available()` check, but not tested).
- No test for subprocess timeout/hang behavior.
- All systemd-creds tests are `#[ignore]` and require Linux with systemd installed.
