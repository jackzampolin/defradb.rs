# 02: Handle Counter Wraps to Zero (Invalid Sentinel) on Overflow

| Field | Value |
|-------|-------|
| **Severity** | LOW |
| **Category** | Integer Overflow / Handle Validity |
| **Status** | Open |

## Summary

The handle allocation counter (`next_handle: AtomicUsize`) starts at 1 (reserving 0 as the invalid sentinel) and uses `fetch_add(1, Ordering::SeqCst)` which wraps on overflow. On 64-bit systems this is practically unreachable (~18 quintillion operations), but on 32-bit targets (wasm32, some embedded), wrapping to 0 after ~4 billion allocations is feasible for a long-running node. After wrap, handle 0 would be returned (invalid) and the next handle (1) could collide with an existing live handle.

## Affected Files

- `crates/ffi/src/state/registry.rs:30` — `NodeRegistry::insert()`
- `crates/ffi/src/state/registry.rs:108` — `SubscriptionRegistry::insert()`
- `crates/ffi/src/state/registry.rs:169` — `GraphQLSubscriptionRegistry::insert()`

## Details

```rust
// crates/ffi/src/state/registry.rs:29-33
pub fn insert(&self, state: NodeState) -> NodeHandle {
    let handle = self.next_handle.fetch_add(1, Ordering::SeqCst);
    let mut nodes = self.nodes.write();
    nodes.insert(handle, state);
    handle
}
```

`AtomicUsize::fetch_add` wraps on overflow (Rust guarantees two's complement wrapping for atomics). The sequence is:

1. Counter at `usize::MAX`
2. `fetch_add(1)` → returns `usize::MAX`, counter wraps to `0`
3. Handle `usize::MAX` is valid (inserted into HashMap)
4. Next `fetch_add(1)` → returns `0`, counter becomes `1`
5. Handle `0` is returned to Go — but `0` is the invalid sentinel in `NewNodeResult`
6. Go may treat `node_ptr = 0` as an error
7. Next `fetch_add(1)` → returns `1`, which could collide with a live handle from early in the process

### Risk Assessment

On 64-bit: practically unreachable (would require ~2^64 node/subscription creations).
On 32-bit: feasible after ~4 billion operations in a long-running process. Subscription handles are created and destroyed frequently, making this more likely for the subscription registry.

## Remediation

Option A — Checked addition with error:

```rust
pub fn insert(&self, state: NodeState) -> Result<NodeHandle, &'static str> {
    let handle = self.next_handle.fetch_add(1, Ordering::SeqCst);
    if handle == 0 || handle == usize::MAX {
        return Err("handle space exhausted");
    }
    // ...
}
```

Option B — Use `checked_add` via CAS loop:

```rust
loop {
    let current = self.next_handle.load(Ordering::SeqCst);
    let next = current.checked_add(1).ok_or("handle exhausted")?;
    if self.next_handle.compare_exchange(
        current, next, Ordering::SeqCst, Ordering::Relaxed
    ).is_ok() {
        return Ok(current);
    }
}
```

## Test Gap

- No test verifies behavior when handle counter approaches `usize::MAX`
- No test checks that handle 0 is never returned to Go
- The unit test `test_registry_handles` only checks emptiness, not handle values
