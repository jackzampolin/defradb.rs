# 07: Handle Registry Design — Sound, No ABA Problem

| Field | Value |
|-------|-------|
| **Severity** | GREEN |
| **Category** | Handle Validity / Registry Safety |
| **Status** | Verified |

## Summary

The handle registry design is sound for its purpose. Monotonically incrementing handles avoid the ABA problem (freed handle reuse). The `HashMap<usize, NodeState>` with `parking_lot::RwLock` provides correct concurrent access. Invalid and nonexistent handles are properly rejected.

## Verified Properties

### 1. Handle 0 is reserved as invalid

```rust
// crates/ffi/src/state/registry.rs:24
next_handle: AtomicUsize::new(1), // Start at 1, 0 is invalid
```

All result types use `node_ptr: 0` or `subscription_handle: 0` to indicate error. Tests verify this:
```rust
// node.rs:403 - test_node_close_invalid_handle
let result = node_close(0);
assert_eq!(result.status, 1);
```

### 2. No ABA problem — monotonic IDs

Handles are never recycled. Even after `remove()`, the counter keeps incrementing. A stale Go handle for freed node 5 will always get `None` from `HashMap::get(&5)` because the entry was removed. No new node will ever get handle 5 again (the counter has moved past it).

### 3. RwLock provides correct concurrent access

- `insert()`: acquires **write** lock, inserts, releases
- `get()`: acquires **read** lock, runs closure, releases (multiple concurrent reads OK)
- `get_mut()`: acquires **write** lock, runs closure, releases (exclusive access)
- `remove()`: acquires **write** lock, removes, releases

Using `parking_lot::RwLock` (not `std::sync::RwLock`) avoids poisoning — if a thread panics while holding the lock, the lock is not permanently poisoned.

### 4. Closure-based API prevents dangling references

```rust
pub fn get<F, R>(&self, handle: NodeHandle, f: F) -> Option<R>
where F: FnOnce(&NodeState) -> R {
    let nodes = self.nodes.read();
    nodes.get(&handle).map(f)
}
```

The lock is held for the duration of the closure. Callers clone `Arc<...>` inside the closure, which keeps the data alive after the lock is released. This prevents returning references that outlive the lock.

### 5. Invalid handle lookup returns None

All registry `get()` calls propagate `None` as an error:
```rust
// helpers.rs:24-28
pub fn get_node_database(node_ptr: usize) -> Result<Arc<FfiDatabase>, FfiResult> {
    NODES.get(node_ptr, |state| state.database.clone())
        .ok_or_else(|| FfiResult::error(ERR_INVALID_NODE_HANDLE))
}
```

`usize::MAX`, 0, 999999 — all return "invalid node handle" error.

## Residual Risk

See Finding 02 (handle counter overflow) for the edge case where the counter wraps. In practice, this is not a concern for 64-bit systems.

## Test Gap

- `test_node_close_invalid_handle` covers handle 0
- `test_node_close_nonexistent_handle` covers handle 999999
- `test_multiple_nodes` verifies distinct handles
- `test_node_lifecycle` verifies double-close returns error
- Coverage is adequate
