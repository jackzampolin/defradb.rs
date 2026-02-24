# Manual `unsafe impl Send/Sync` Inventory

**Severity**: Medium
**Category**: Unsafe Code — Thread Safety Assertions
**Status**: Both impls are sound

## Summary

There are exactly two manual `unsafe impl Send` / `unsafe impl Sync` pairs outside the FFI crate. Both are justified but warrant ongoing attention during refactors.

## Inventory

### 1. OwnedSnapshot (storage crate)

**File**: `crates/storage/src/backends/rocksdb/transaction.rs:28-29`

```rust
unsafe impl Send for OwnedSnapshot {}
unsafe impl Sync for OwnedSnapshot {}
```

**Why needed**: The struct contains `SnapshotWithThreadMode<'static, ...>` where the `'static` was created via transmute. The real `SnapshotWithThreadMode<'a, OptimisticTransactionDB>` implements Send+Sync, but transmuting to `'static` causes the compiler to lose this knowledge. The manual impls restore what the compiler can't infer.

**Sound?**: Yes. `SnapshotWithThreadMode` is Send+Sync for `MultiThreaded` DB types (which `OptimisticTransactionDB` uses). The `Arc<DB>` is also Send+Sync. No new thread-unsafety is introduced.

### 2. FetcherWrapper (query crate)

**File**: `crates/query/src/runner/fetcher.rs:72-73`

```rust
unsafe impl Send for FetcherWrapper {}
unsafe impl Sync for FetcherWrapper {}
```

**Why needed**: The struct contains `*const ()` raw pointers (the decomposed fat pointer). Raw pointers are `!Send` and `!Sync` by default because the compiler can't verify what they point to.

**Sound?**: Yes, with caveats. The pointers point to data that implements `DocFetcher: MaybeSendSync` (which resolves to `Send + Sync` on non-WASM targets). The data behind the pointers is thread-safe. However, this assumes:
1. The pointed-to data is alive (lifetime not enforced at compile time)
2. The data was originally Send+Sync (enforced by the trait bound on DocFetcher)

## No Other Manual Send/Sync

Confirmed: no other `unsafe impl Send` or `unsafe impl Sync` exists outside the FFI crate in the entire codebase.

## Remediation

- **OwnedSnapshot**: Sound, low risk. Consider documenting why auto-impl fails.
- **FetcherWrapper**: Sound but fragile. Adding a lifetime parameter would eliminate the need for manual Send/Sync (the compiler could auto-derive them from `&'a dyn DocFetcher`).

## Test Gap

- No test verifies that these types can be sent across threads without data races.
- The integration test suite exercises multi-threaded scenarios (p2p, HTTP server) which implicitly validates these impls.
