# FileKeyring: No File Locking for Concurrent Access

- **Severity**: Low
- **Category**: Concurrency
- **Status**: Open

## Summary

FileKeyring has no file-level locking. Two processes (or threads via separate FileKeyring instances) writing to the same key simultaneously can produce corrupted ciphertext. The integration test `test_file_keyring_concurrent_access` only tests concurrent *reads*, not concurrent writes.

## Affected Files

- `crates/keyring/src/file.rs:98-121` — `set()` with no locking
- `crates/keyring/tests/integration_tests.rs:79-119` — concurrent test is read-only

## Details

The `set()` method performs truncate-and-write without any advisory or mandatory file locks:

```rust
// file.rs:106-112
let mut file = std::fs::OpenOptions::new()
    .write(true)
    .create(true)
    .truncate(true)   // truncates existing content
    .mode(0o600)
    .open(&path)?;
std::io::Write::write_all(&mut file, &cipher)?;
```

If two processes call `set("peer-key", ...)` simultaneously:
1. Both open and truncate the file
2. Both write their ciphertext
3. The result may be the second write overlapping the first, producing a corrupt JWE token

**Mitigating factors**:
1. In practice, keyring writes happen during setup (key generation) or import — not during normal operation.
2. DefraDB typically runs as a single process with a single keyring instance.
3. Concurrent reads are safe because each read is an independent `fs::read()`.

**The concurrent test only reads**:
```rust
// integration_tests.rs:94-118
// Spawn multiple threads reading concurrently
let handles: Vec<_> = (0..5).map(|thread_id| {
    thread::spawn(move || {
        let keyring = FileKeyring::open(&path, password).unwrap();
        for i in 0..10 {
            let data = keyring.get(&key_name).unwrap();
            // ...
        }
    })
}).collect();
```

## Remediation

1. For single-process use, no change needed — Rust's type system prevents shared mutable access to the FileKeyring within a single process.
2. For multi-process safety, consider advisory file locking (`flock()` or `fcntl()`) during write operations.
3. Document that FileKeyring is not safe for concurrent multi-process writes.

## Test Gap

- No test for concurrent write scenarios.
- No test for multi-process access patterns.
