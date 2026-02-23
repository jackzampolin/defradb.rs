# backup/ — Backup, restore, dump, and purge tests

```
cargo test -p integration-test --test backup
```

## Files

| File | Tests | What it covers |
|------|-------|----------------|
| `restore.rs` | 2 | Full and partial backup export/import, CID stability |
| `dump.rs` | 2 | Database dump endpoint (requires dev mode) |
| `purge.rs` | 4 | Purge in dev mode succeeds, purge without dev mode fails |

**7 active tests, 1 ignored.**

### Ignored

- `go_dump` — Go's dump endpoint has a CID parsing bug ("invalid cid: trailing bytes in data buffer passed to cid Cast"). Rust dump works.
