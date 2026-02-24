# No Handle Lifecycle Stress Testing

- **Severity:** MEDIUM
- **Category:** Test Coverage / Resource Management
- **Status:** Confirmed — no stress tests for handle registry

## Summary

The `NodeRegistry` and `SubscriptionRegistry` use `AtomicUsize` counters starting at 1 that monotonically increment. No test verifies behavior under rapid create/destroy cycles, high handle counts, or concurrent access patterns. The handle counter will wrap around after `usize::MAX` operations (Finding 02 in Session 1), but this is never tested.

## Affected Files

- `crates/ffi/src/state/registry.rs:24` — `next_handle: AtomicUsize::new(1)`
- `crates/ffi/src/state/registry.rs:30` — `fetch_add(1, Ordering::SeqCst)` — no overflow check
- `crates/ffi/src/node.rs:25-312` — `new_node` / `node_close` lifecycle
- `crates/ffi/src/subscription/` — subscription handle lifecycle

## Details

### Missing Stress Tests

| Test Scenario | Status | Risk |
|---------------|--------|------|
| Rapid create+destroy 1000 nodes | NOT TESTED | Memory leak detection, registry cleanup |
| Create maximum handles (e.g., 10000) | NOT TESTED | Memory pressure, HashMap growth |
| Use handle after node_close | Partially tested (returns error) | Registry correctly returns None |
| Concurrent create from multiple threads | NOT TESTED | RwLock correctness under contention |
| Concurrent create+close from multiple threads | NOT TESTED | Race between insert and remove |
| Subscription handle lifecycle | NOT TESTED | `remove_for_node` correctness |
| Handle counter wrapping | NOT TESTED | `fetch_add` wraps, could reuse existing handle |

### Handle Counter Wrapping (Cross-ref Finding 02)

```rust
let handle = self.next_handle.fetch_add(1, Ordering::SeqCst);
```

After `usize::MAX` increments (~18.4 quintillion on 64-bit), the counter wraps to 0, then 1. Since handle 0 is invalid but handle 1 was the first handle issued, a wrap-around could produce a handle that collides with an active node if that original node was never closed. The `HashMap::insert` would silently overwrite the old entry.

In practice, `usize::MAX` is unreachable in normal operation. But in a long-running node with automated testing or malicious rapid handle creation, a 32-bit system would wrap after ~4 billion handles.

### Registry Test Coverage

The only registry test is:
```rust
fn test_registry_handles() {
    let registry = nodes();
    assert!(registry.is_empty() || !registry.is_empty()); // tautology
}
```

This test asserts nothing meaningful.

## Remediation

1. **Add lifecycle stress test** that creates and destroys 1000 nodes, verifying `registry.len() == 0` at the end
2. **Add concurrent stress test** using `std::thread::spawn` with multiple threads creating/closing nodes
3. **Add subscription lifecycle test** that creates subscriptions, closes the node, and verifies subscriptions are cleaned up
4. **Replace tautological test** with meaningful assertions

## Test Gap

The handle registry is a critical security component (all FFI operations go through it), yet has effectively zero test coverage beyond the implicit testing from `node.rs::test_node_lifecycle`.
