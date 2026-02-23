# basic/ — Core functionality tests

```
cargo test -p integration-test --test basic
```

## Files

| File | Tests | What it covers |
|------|-------|----------------|
| `smoke.rs` | 2 | Binary version output, single-node CRUD lifecycle |
| `document_lifecycle.rs` | 2 | Create/update/delete documents (Go + Rust) |
| `collection_management.rs` | 2 | Schema deployment, collection listing |
| `multi_collection.rs` | 2 | Multiple collections in a single node |
| `transactions.rs` | 2 | Transaction commit/rollback |
| `truncate_parallel.rs` | 2 | Concurrent truncate operations |

**14 tests, 0 ignored.** All pass on both Go and Rust nodes.
