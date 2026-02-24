# Keyring Secret from Environment Not Wrapped in Zeroizing

- **Severity**: Medium
- **Category**: Key Material Handling
- **Status**: Open

## Summary

The `load_secret_from_env()` function returns a plain `Vec<u8>` rather than `Zeroizing<Vec<u8>>`. This means the password bytes persist in memory until the allocator reuses the heap page, potentially surviving for the entire process lifetime. The secret passes through at least one intermediate `String` (from `env::var()`) that is also not zeroized. Similarly, `FileKeyring::open()` accepts `impl Into<Vec<u8>>`, meaning callers can pass non-zeroizing types.

## Affected Files

- `crates/keyring/src/lib.rs:43-46` — `load_secret_from_env()` returns `Vec<u8>`
- `crates/keyring/src/lib.rs:52-55` — `open_file_keyring()` passes plain `Vec<u8>`
- `crates/keyring/src/file.rs:44` — `FileKeyring::open()` accepts `impl Into<Vec<u8>>`
- `crates/cli/src/commands/mod.rs:38-41` — `open_keyring()` passes secret without zeroizing

## Details

```rust
// lib.rs:43-46
pub fn load_secret_from_env() -> Result<Vec<u8>> {
    std::env::var(KEYRING_SECRET_ENV)
        .map(|s| s.into_bytes())  // String `s` is dropped without zeroizing
        .map_err(|_| Error::SecretNotSet)
}
```

The `env::var()` call returns a `String`. When `.into_bytes()` is called, the `String`'s buffer is moved into the `Vec<u8>`, so the original allocation is not copied — but there's no guarantee the runtime hasn't made intermediate copies (e.g., during the env var lookup itself via libc `getenv`).

The `FileKeyring` struct correctly wraps its stored password in `Zeroizing<Vec<u8>>`:

```rust
// file.rs:36-37
password: Zeroizing<Vec<u8>>,
```

But the caller chain from env var → `open_file_keyring()` → `FileKeyring::open()` has the secret living as a plain `Vec<u8>` on the stack/heap across function boundaries before it gets wrapped in `Zeroizing`.

**In the CLI path** (`commands/mod.rs`):
```rust
let secret = keyring::load_secret_from_env()
    .map_err(|e| Error::Keyring(e.to_string()))?;  // plain Vec<u8>
let kr = keyring::FileKeyring::open(&path, secret)  // moved into open()
    .map_err(|e| Error::Keyring(e.to_string()))?;
```

The `secret` `Vec<u8>` is moved into `open()`, which wraps it in `Zeroizing`. However, if the `open()` call fails, the `secret` is dropped without zeroization (moved into `open()` but `open()` may fail before wrapping it).

## Remediation

1. Change `load_secret_from_env()` to return `Zeroizing<Vec<u8>>`:
   ```rust
   pub fn load_secret_from_env() -> Result<Zeroizing<Vec<u8>>> {
       std::env::var(KEYRING_SECRET_ENV)
           .map(|s| Zeroizing::new(s.into_bytes()))
           .map_err(|_| Error::SecretNotSet)
   }
   ```
2. Change `FileKeyring::open()` signature to accept `Zeroizing<Vec<u8>>` directly, making it impossible to accidentally pass a non-zeroizing password.
3. Note: the libc `getenv()` copy is unavoidable in Rust's `env::var()` — this is a best-effort mitigation.

## Test Gap

- No test verifies that the secret is zeroized after `FileKeyring` is dropped.
- No test for the error path (secret not zeroized on `open()` failure).
