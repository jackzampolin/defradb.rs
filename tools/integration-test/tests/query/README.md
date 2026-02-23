# query/ — Query engine and related feature tests

```
cargo test -p integration-test --test query
```

## Files

| File | Tests | What it covers |
|------|-------|----------------|
| `view.rs` | 2 | GraphQL view queries |
| `lens.rs` | 4 | Lens API management (list, reload, add, set) |
| `lens_persistence.rs` | 1 | Lens config survives node restart (Rust-only, redb) |
| `sdl_generate.rs` | 3 | CLI `sdl generate` command (file output, stdout, no-overwrite) |
| `index_management.rs` | 2 | Index create/drop/list |
| `explain_nested.rs` | 2 | Explain output for nested queries |
| `subscription_docid.rs` | 6 | GraphQL subscriptions with docID filtering |
| `stubs.rs` | 4 | Depth/width limits, timeout under load |

**21 active tests, 3 ignored.**

### Ignored

| Test | Reason |
|------|--------|
| `go_query_depth_width_limit` | Go does not implement query depth/width limits |
| `rust_query_timeout_under_load` | Blocked: TestCluster doesn't expose `--query-timeout` |
| `go_query_timeout_under_load` | Same as above |

### Note on lens tests

The lens tests exercise the management API (list, reload, add, set) — they do
not run WASM lens transformations. Future tests that apply lens migrations to
query results will need lens WASM binaries built and available.
