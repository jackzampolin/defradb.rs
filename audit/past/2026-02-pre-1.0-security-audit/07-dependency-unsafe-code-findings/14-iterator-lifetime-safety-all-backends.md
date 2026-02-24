# Iterator Lifetime Safety Across Storage Backends

**Severity**: Low (Informational)
**Category**: Unsafe Code — Iterator Safety
**Status**: Sound — all iterators are materialized

## Summary

All four storage backends (redb, fjall, rocksdb, memory) use the same design pattern: iterators **materialize all matching data into owned `Vec<(Vec<u8>, Vec<u8>)>`** at creation time. No iterator holds a reference to the transaction, snapshot, or any external state. This eliminates the entire class of iterator-lifetime-related unsafety.

## Affected Files

- `crates/storage/src/backends/rocksdb/iterator.rs` — `RocksDbMergingIterator`
- `crates/storage/src/backends/redb/iterator.rs` — `MergingIterator`
- `crates/storage/src/backends/fjall/iterator.rs` — `MergingIterator`
- `crates/storage/src/backends/memory/iterator.rs` — `MemoryIterator`

## Details

### Pattern (identical across all backends)

Iterator creation in the transaction's `iterator()` method:

1. Read all matching key-value pairs from the snapshot/read-transaction into a `Vec<(Vec<u8>, Vec<u8>)>`
2. Read all matching pending changes into a `Vec<(Vec<u8>, Option<Vec<u8>>)>`
3. Pass both owned Vecs to the iterator constructor
4. The iterator merges them on-demand during `next()` calls

### Why This Is Safe

The iterators contain only owned data:
```rust
pub(crate) struct RocksDbMergingIterator {
    snapshot_items: Vec<(Vec<u8>, Vec<u8>)>,   // owned
    snapshot_pos: usize,
    pending_items: Vec<(Vec<u8>, Option<Vec<u8>>)>,  // owned
    pending_pos: usize,
    reverse: bool,
    keys_only: bool,
    closed: bool,
}
```

No references. No lifetimes. No raw pointers. No unsafe.

### Trade-off

This design trades **memory** for **safety**:
- All matching data is copied into memory at iterator creation time
- The iterator is independent of the transaction — dropping the transaction doesn't invalidate it
- For large result sets, this could use significant memory
- But it completely eliminates use-after-free risks

### Comparison with typical RocksDB usage

In typical RocksDB applications, iterators hold a reference to the snapshot and read lazily. This requires careful lifetime management (the snapshot must outlive the iterator). By materializing all data upfront, this codebase avoids that complexity entirely.

### Backend-by-backend unsafe audit

| Backend | Iterator file | Unsafe code | References held |
|---------|--------------|-------------|-----------------|
| rocksdb | `RocksDbMergingIterator` | None | None — all owned |
| redb | `MergingIterator` | None | None — all owned |
| fjall | `MergingIterator` | None | None — all owned |
| memory | `MemoryIterator` | None | None — all owned |

### Transaction commit/rollback with active iterators

Since iterators hold materialized snapshots, committing or discarding a transaction has no effect on active iterators. The iterator continues to see the data as it was at creation time. This matches snapshot isolation semantics.

## Remediation

None needed. The materialized iterator pattern is a clean, safe design.

## Test Gap

- The backends all share a comprehensive test suite via `test_suite/` macros
- Iterator behavior is tested: `iterator_basic.rs`, `iterator_edge_cases.rs`, `iterator_reverse.rs`, `iterator_seek.rs`
- No test specifically verifies that iterators survive transaction drops (they do, by design)
