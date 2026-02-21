# Session 3 Summary: Keyring Backends Deep Dive

## Scope

Deep-dive into all three keyring backends — FileKeyring (JWE-encrypted at-rest storage), SystemKeyring (OS-native), and SystemdCreds (systemd-creds) — examining key derivation, file permissions, secure deletion, credential leakage, and backend-specific vulnerabilities.

## Files Audited

| File | Lines | Focus |
|------|-------|-------|
| `crates/keyring/src/file.rs` | 1-170 | FileKeyring: PBKDF2, JWE, permissions, secure delete |
| `crates/keyring/src/keyring.rs` | 1-31 | Keyring trait definition |
| `crates/keyring/src/system.rs` | 1-82 | SystemKeyring: OS-native, base64 encoding |
| `crates/keyring/src/systemd_creds.rs` | 1-207 | SystemdCreds: subprocess, pipes, permissions |
| `crates/keyring/src/lib.rs` | 1-96 | Module structure, env var loading |
| `crates/keyring/src/key_name.rs` | 1-148 | Key name validation |
| `crates/keyring/src/signer.rs` | 1-141 | KeyHandle, on-demand key fetching |
| `crates/keyring/src/error.rs` | 1-38 | Error types |
| `crates/keyring/Cargo.toml` | 1-38 | Dependencies |
| `crates/keyring/tests/file_tests.rs` | 1-128 | FileKeyring unit tests |
| `crates/keyring/tests/integration_tests.rs` | 1-414 | Integration tests |
| `crates/keyring/tests/signer_tests.rs` | 1-125 | KeyHandle tests |
| `crates/keyring/tests/system_tests.rs` | 1-105 | SystemKeyring tests (ignored) |
| `crates/keyring/tests/systemd_creds_tests.rs` | 1-119 | SystemdCreds tests (ignored) |
| `crates/cli/src/commands/keyring_cmd.rs` | 1-284 | CLI keyring commands |
| `crates/cli/src/commands/identity.rs` | 1-413 | CLI identity commands |
| `crates/cli/src/commands/mod.rs` | 1-107 | Backend selection logic |
| `crates/cli/src/commands/start/p2p.rs` | 1-152 | Peer key initialization |
| `crates/cli/src/commands/start/node.rs` | 1-198 | Node startup |

## Findings Summary

### Issues Found (7)

| # | Finding | Severity | Category |
|---|---------|----------|----------|
| 21 | PBKDF2 iteration count (10k) below modern recommendations | Medium | Key Derivation |
| 22 | No fsync between zero-fill and unlink in delete() | Low | Secure Deletion |
| 23 | load_secret_from_env returns plain Vec, not Zeroizing | Medium | Key Material Handling |
| 24 | Keyring::get() returns plain Vec — key material not zeroized | Medium | Key Material Handling |
| 25 | systemd-creds PATH-based lookup, no subprocess timeout | Low | Subprocess Security |
| 26 | SystemdCreds delete() has no secure file deletion | Low | Secure Deletion |
| 27 | Private key material printed to stdout (CLI export/identity new) | Medium | Credential Leakage |

### Lower Priority Issues (3)

| # | Finding | Severity | Category |
|---|---------|----------|----------|
| 28 | Directory permission TOCTOU between create_dir_all and set_permissions | Low | File Permission Race |
| 29 | No file locking for concurrent multi-process access | Low | Concurrency |
| 33 | FileKeyring set() missing fsync (inconsistent with SystemdCreds) | Low | Data Durability |

### Verified Clean (3)

| # | Finding | Status |
|---|---------|--------|
| 30 | JWE construction via josekit is sound, salt unique per key | Green |
| 31 | base64 STANDARD encoding for SystemKeyring is correct | Green |
| 32 | KeyName validation prevents path traversal | Green |

## Architectural Observations

### Strengths

1. **Password zeroization**: FileKeyring correctly uses `Zeroizing<Vec<u8>>` for the stored password.
2. **On-demand key fetching**: KeyHandle fetches keys from keyring on each use, minimizing memory exposure window.
3. **Atomic file permissions**: Key files created with `OpenOptions::mode(0o600)` — no TOCTOU for file creation.
4. **JWE library**: Uses established `josekit` crate, not hand-rolled crypto.
5. **Path traversal prevention**: KeyName validation is thorough and consistently applied.
6. **Error handling**: Errors are typed (not string-only) and don't leak key material.
7. **Go compatibility**: JWE format, iteration count, salt length all match Go DefraDB.
8. **Graceful fallback**: System keyring falls back to systemd-creds on Linux without D-Bus.

### Weaknesses

1. **Zeroization gap**: Password is zeroized, but the much more valuable *private key material* returned by `get()` is not. This is the most significant finding.
2. **PBKDF2 iterations**: 10k is the weakest point for offline attacks against stolen key files, constrained by Go compatibility.
3. **Secure deletion is best-effort**: Zero-fill before unlink is defense-in-depth but doesn't work on CoW filesystems and lacks fsync.
4. **CLI credential exposure**: Private keys visible in stdout and process arguments — inherent to CLI key management but worth documenting.

## Recommended Priority

1. **Highest**: Change `Keyring::get()` to return `Zeroizing<Vec<u8>>` (finding 24) — this protects all private key material across all backends and callers with a single type change.
2. **High**: Wrap `load_secret_from_env()` return in `Zeroizing` (finding 23).
3. **Medium**: Coordinate with Go DefraDB on increasing PBKDF2 iterations (finding 21).
4. **Low**: Add fsync to FileKeyring set() and delete() (findings 22, 33).
5. **Low**: Add subprocess timeout to systemd-creds (finding 25).

## Test Coverage Assessment

The keyring crate has good test coverage for functional correctness:
- FileKeyring: set/get/delete/list, wrong password, corruption, truncation, binary data, path traversal, permissions
- SystemKeyring: all operations (requires OS keyring, tests are `#[ignore]`)
- SystemdCredsKeyring: all operations (requires systemd, tests are `#[ignore]`)
- KeyHandle: creation, verification, wrong length, missing key

**Missing test areas**:
- Zeroization verification (password and key material)
- Concurrent write safety
- Crash recovery / durability
- Secure deletion verification (zeros on disk)
- Subprocess timeout behavior
