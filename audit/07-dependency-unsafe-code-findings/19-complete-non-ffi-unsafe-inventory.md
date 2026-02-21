# Complete Non-FFI Unsafe Code Inventory

**Severity**: Informational
**Category**: Unsafe Code — Comprehensive Inventory
**Status**: Audited — 3 unsafe sites outside FFI

## Summary

Outside the FFI crate (`crates/ffi/`), there are exactly **3 locations** with unsafe code in the codebase, totaling **7 unsafe blocks** and **4 unsafe impl** declarations. All have been individually audited and found sound.

## Complete Inventory

### 1. OwnedSnapshot — RocksDB Lifetime Transmute

**File**: `crates/storage/src/backends/rocksdb/transaction.rs`

| Line | Unsafe Type | Purpose |
|------|-------------|---------|
| 28 | `unsafe impl Send for OwnedSnapshot` | Manual thread-safety assertion |
| 29 | `unsafe impl Sync for OwnedSnapshot` | Manual thread-safety assertion |
| 35-41 | `unsafe { std::mem::transmute::<...>(...) }` | Extend snapshot lifetime to 'static |

**Verdict**: Sound. See Finding #12 for full analysis.

### 2. FetcherWrapper — Fat Pointer Decomposition

**File**: `crates/query/src/runner/fetcher.rs`

| Line | Unsafe Type | Purpose |
|------|-------------|---------|
| 47 | `unsafe { std::mem::transmute::<*const dyn DocFetcher, (*const (), *const ())>(...) }` | Decompose fat pointer |
| 57-61 | `unsafe { std::mem::transmute::<(*const (), *const ()), *const dyn DocFetcher>(...) }` | Reconstruct fat pointer |
| 64 | `unsafe { &*ptr }` | Dereference reconstructed pointer |
| 72 | `unsafe impl Send for FetcherWrapper` | Manual thread-safety assertion |
| 73 | `unsafe impl Sync for FetcherWrapper` | Manual thread-safety assertion |

**Verdict**: Sound with caveats. See Finding #13 for full analysis.

### 3. Word "unsafe" in Non-FFI Code (Not Actual Unsafe Blocks)

The following are NOT unsafe code — they use "unsafe" as a domain term in the ACP policy transition logic:

| File | Line | Context |
|------|------|---------|
| `crates/db/src/error.rs:74` | `"unsafe policy transition blocked: {0}"` — error message string |
| `crates/db/src/collection_acp.rs:275-357` | `warn_on_unsafe_policy_transition()`, `block_unsafe_policy_transition()` — safe functions with "unsafe" in their name |
| `crates/db/src/txn_registry.rs:76` | Comment: "operation would be unsafe" |

These are safe Rust code that happens to use the word "unsafe" in identifiers/comments.

## Backend-by-Backend Unsafe Summary

| Backend | Unsafe blocks | Unsafe impls | Total |
|---------|--------------|--------------|-------|
| RocksDB | 1 (transmute) | 2 (Send+Sync) | 3 |
| Redb | 0 | 0 | 0 |
| Fjall | 0 | 0 | 0 |
| Memory | 0 | 0 | 0 |

| Non-storage crate | Unsafe blocks | Unsafe impls | Total |
|-------------------|--------------|--------------|-------|
| Query (FetcherWrapper) | 3 | 2 | 5 |

**Grand total (non-FFI)**: 4 unsafe blocks + 4 unsafe impls = 8 unsafe items across 2 files.

## Assessment

For a database engine with 4 storage backends and a query engine, having only 8 unsafe items is remarkably clean. The materialized iterator design is the key architectural choice that minimizes unsafe — by copying data out of storage-specific types into owned Vecs, the iterators avoid the lifetime complexity that would otherwise require unsafe.

## Miri Testing Feasibility

| Unsafe site | Miri testable? | Reason |
|-------------|---------------|--------|
| OwnedSnapshot transmute | No | RocksDB is C++ FFI |
| FetcherWrapper transmute | Partially | The transmute itself is pure Rust, but testing requires async runtime |
| FetcherWrapper deref | Partially | Same as above |

A targeted Miri test could verify the fat pointer round-trip (transmute decompose → recompose → deref) using a simple test trait instead of DocFetcher, avoiding the async/FFI dependencies.
