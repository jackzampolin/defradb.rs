# No Pin-Based Self-Referential Structs

**Severity**: Informational
**Category**: Unsafe Code — Pin Analysis
**Status**: Clean — no Pin-based self-references

## Summary

A search for `Pin`, `Unpin`, `!Unpin`, `pin_mut`, and `pin_project` across all crates found zero uses of Pin for self-referential struct pinning. All `Pin` usage is limited to `Pin<Box<dyn Future>>` for async trait return types — standard async Rust patterns with no safety implications.

## Details

### Pin Usage Found

All instances are `Pin<Box<dyn Future<...>>>` used in type aliases for async callbacks and trait methods:

| File | Usage |
|------|-------|
| `crates/datastore/src/txn.rs:18-20` | `AsyncCallback = Box<dyn FnOnce() -> Pin<Box<dyn Future>>>` |
| `crates/storage/src/corekv/traits.rs:45-52` | `AsyncTxnCallback` type alias |
| `crates/zanzibar/src/thread_bounds.rs:31-34` | `MaybeBoxFuture<'a, T>` type alias |
| `crates/defra-core/src/thread_bounds.rs:37-41` | `MaybeBoxFuture<'a, T>` type alias |
| `crates/lens/src/store.rs:42-45` | `LensDocStream` type alias |
| Various HTTP/P2P files | `-> Pin<Box<dyn Future + Send + 'async_trait>>` (async_trait expansion) |

### Unpin Usage Found

Only in `crates/p2p/src/codec.rs:43-231` where `AsyncRead + Unpin + Send` and `AsyncWrite + Unpin + Send` bounds are used for stream codec functions. This is standard — `Unpin` here means the stream can be polled without pinning, not that any self-referential struct is involved.

### No Self-Referential Pin Patterns

- No `pin_project` or `pin_project_lite` macros
- No `!Unpin` marker implementations
- No `Pin<&mut Self>` method signatures
- No `self: Pin<&mut Self>` methods
- No `ouroboros`, `self_cell`, or `rental` crate dependencies

### Implications

The two self-referential patterns in the codebase (OwnedSnapshot and FetcherWrapper) use unsafe transmute/raw pointers rather than Pin. This is because Pin doesn't help with their specific lifetime problems — Pin prevents moving after pinning, but these patterns need lifetime extension (OwnedSnapshot) or lifetime erasure (FetcherWrapper), which Pin doesn't provide.

## Remediation

None needed. The absence of Pin-based self-references simplifies the safety audit.
