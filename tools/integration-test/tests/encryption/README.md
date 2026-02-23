# encryption/ — Searchable Encryption tests

```
cargo test -p integration-test --test encryption
```

## Files

| File | Tests | What it covers |
|------|-------|----------------|
| `block_verify.rs` | 2 | Encrypted block structure validation |
| `acp.rs` | 2 | ACP interaction with encrypted documents |
| `index.rs` | 2 | Encrypted index operations |
| `stubs.rs` | 4 | SE key rotation, field-level key isolation |

**10 tests, 0 ignored.** All pass on both Go and Rust nodes.
