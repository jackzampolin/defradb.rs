# Session 2 Summary: Storage Backend Unsafe Code Audit

**Session**: 2 of 5 — Dependency & Unsafe Code Stream
**Date**: 2026-02-21
**Focus**: Non-FFI unsafe code deep-dive: OwnedSnapshot transmute, iterator lifetimes, FetcherWrapper

## Key Findings

### Critical: None

### Medium Severity (2)

1. **Finding #12 — RocksDB OwnedSnapshot Lifetime Transmute**: Uses `mem::transmute` to extend a snapshot's lifetime to `'static` for a self-referential struct. Sound because the `Arc<DB>` guarantees the DB outlives the snapshot. The `_db` field is an Arc, so drop order doesn't matter. No compiler enforcement of the invariant — correctness relies on manual reasoning.

2. **Finding #13 — FetcherWrapper Fat Pointer Transmute**: Decomposes a `*const dyn DocFetcher` fat pointer into raw `(*const (), *const ())` pointers via transmute, erasing the lifetime. Sound because all usage sites create and consume the wrapper within a single async function scope. However, there is no compile-time enforcement — a future refactor could accidentally create a dangling pointer. The fat pointer layout `(data, vtable)` is de facto stable but not formally guaranteed.

### Low Severity (1)

3. **Finding #17 — RocksDB Crate v0.22.0**: Current version, no known CVEs. The crate is inherently unsafe (C++ FFI wrapper) but provides safe Rust APIs. DefraDB uses the safe APIs except for the OwnedSnapshot transmute.

### Informational (4)

4. **Finding #14 — Iterator Lifetime Safety**: ALL backends use materialized iterators (data copied into owned Vecs at creation). No iterator holds a reference to any transaction or snapshot. This architectural decision eliminates the entire category of iterator-lifetime bugs. Memory trade-off: all matching data is loaded upfront.

5. **Finding #15 — Send/Sync Inventory**: Exactly 4 manual `unsafe impl Send/Sync` declarations outside FFI. Both pairs (OwnedSnapshot, FetcherWrapper) are sound — they restore auto-trait impls lost due to transmute/'static lifetimes and raw pointers.

6. **Finding #16 — Memory Backend Clean**: Zero unsafe code. Serves as a safe reference implementation with the same transaction, conflict detection, and iterator patterns as on-disk backends.

7. **Finding #18 — No Pin Usage**: No Pin-based self-referential patterns anywhere. All `Pin` usage is standard `Pin<Box<dyn Future>>` for async traits.

8. **Finding #19 — Complete Inventory**: 8 total unsafe items (4 blocks + 4 impls) across 2 files, outside the FFI crate. For a database engine, this is remarkably clean.

## Architecture Assessment

The most important finding is **architectural**: the materialized iterator design. By copying data out of storage-specific types into owned `Vec`s at iterator creation time, the codebase avoids the most common source of unsafe code in database engines — iterator-lifetime management. This is an explicit memory-for-safety trade-off that significantly reduces the unsafe surface area.

The two remaining unsafe sites (OwnedSnapshot, FetcherWrapper) both solve the same fundamental problem: Rust's borrow checker cannot express self-referential structs or lifetime erasure. Both use well-documented patterns (Arc-held lifetime extension, fat pointer decomposition) with sound safety arguments. Neither is ideal — the `self_cell` crate would be safer for OwnedSnapshot, and a lifetime parameter would be safer for FetcherWrapper — but both are currently correct.

## Remediation Priority

| Finding | Priority | Action |
|---------|----------|--------|
| #13 FetcherWrapper | Medium | Add lifetime parameter `FetcherWrapper<'a>` to get compiler enforcement |
| #12 OwnedSnapshot | Low | Consider `self_cell` crate for type-level safety |
| #17 RocksDB version | Monitor | Keep updated, watch RustSec advisories |
| Others | None | Informational, no action needed |

## Files Audited

| File | Lines | Unsafe? |
|------|-------|---------|
| `storage/backends/rocksdb/transaction.rs` | 471 | Yes — OwnedSnapshot transmute |
| `storage/backends/rocksdb/store.rs` | 234 | No |
| `storage/backends/rocksdb/iterator.rs` | 161 | No |
| `storage/backends/rocksdb/config.rs` | 336 | No |
| `storage/backends/rocksdb/mod.rs` | 12 | No |
| `storage/backends/redb/transaction.rs` | 499 | No |
| `storage/backends/redb/iterator.rs` | 187 | No |
| `storage/backends/fjall/transaction.rs` | 350 | No |
| `storage/backends/fjall/iterator.rs` | 161 | No |
| `storage/backends/memory/transaction.rs` | 295 | No |
| `storage/backends/memory/iterator.rs` | 143 | No |
| `query/runner/fetcher.rs` | 173 | Yes — FetcherWrapper transmute |
| `query/runner/query/nested.rs` | 225 | No (uses FetcherWrapper) |
| `query/runner/query/aggregate.rs` | 263+ | No (uses FetcherWrapper) |
| `query/runner/explain/execute.rs` | 251+ | No (uses FetcherWrapper) |
| `storage/Cargo.toml` | 75 | N/A — rocksdb v0.22 |
| `Cargo.lock` | N/A | N/A — rocksdb v0.22.0 |

## Grep Searches Executed

- `unsafe` across all non-FFI Rust files → 8 real unsafe items found
- `transmute|transmute_copy` → 3 transmute sites (1 OwnedSnapshot + 2 FetcherWrapper)
- `unsafe impl Send|unsafe impl Sync` → 4 impls (2 OwnedSnapshot + 2 FetcherWrapper)
- `from_raw|into_raw|as_ptr|as_mut_ptr|*const|*mut` → Only in FFI crate
- `Pin<|Unpin|!Unpin|pin_mut|pin_project` → Only async trait returns
- `FetcherWrapper|fetcher.*unsafe` → 3 usage sites + 1 definition
- `ouroboros|self_cell|rental` → None found
- `Drop.*impl|impl.*Drop|fn drop` → All backends have proper Drop impls
- `rocksdb` version in Cargo.toml/Cargo.lock → v0.22.0
