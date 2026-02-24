# 04: Race Between `node_close` and Concurrent Operations

| Field | Value |
|-------|-------|
| **Severity** | MEDIUM |
| **Category** | Concurrency / Use-After-Close |
| **Status** | Open |

## Summary

There is a TOCTOU (time-of-check-time-of-use) window between when an FFI function validates a node handle and when it uses the cloned state. If `node_close()` runs concurrently on another goroutine, the operation may execute against a closed database. Thanks to `Arc`, this is not a memory safety issue (no use-after-free), but the closed database may panic or return unexpected errors, and without `catch_unwind` (Finding 00), any panic is UB.

## Affected Files

- `crates/ffi/src/node.rs:322-368` — `node_close()`
- `crates/ffi/src/helpers.rs:24-28` — `get_node_database()`
- `crates/ffi/src/helpers.rs:31-35` — `get_node_runner()`
- All FFI functions that call `get_node_database()` or `get_node_runner()`

## Details

### The race window

```
Thread A (exec_request)             Thread B (node_close)
─────────────────────────           ─────────────────────
1. get_node_runner(node_ptr)
   → acquires read lock
   → clones Arc<QueryRunner>
   → releases read lock
                                    2. NODES.remove(node_ptr)
                                       → acquires write lock
                                       → removes NodeState
                                       → releases write lock
                                    3. state.database.close()
                                       → closes database connections
4. runner.execute(request)
   → uses database via Arc
   → database is CLOSED
   → may panic or error
```

### Why this matters

The `Arc<dyn QueryExecutor>` holds a reference to the database via internal `Arc<DB>`. Even after `node_close()` removes the state from the registry and calls `database.close()`, the runner still holds its `Arc<DB>`. The database is "logically closed" but the Rust object is still alive (Arc ref count > 0).

Depending on how `DB::close()` is implemented:
- If it sets an internal flag and subsequent operations return `Err`: safe, returns error to Go
- If it drops internal resources and subsequent operations `unwrap()` on closed state: **panic** → UB (per Finding 00)
- If it closes file handles and subsequent operations get IO errors: safe-ish, returns error

### node_close itself has the same issue

```rust
// crates/ffi/src/node.rs:330-333
let removed_subs = SUBSCRIPTIONS.remove_for_node(node_ptr);
for sub_state in removed_subs {
    NODES.get(node_ptr, |state| {           // ← node may already be removed
        state.event_bus.unsubscribe(sub_state.subscription.id());
    });
}
```

After `SUBSCRIPTIONS.remove_for_node()`, the code tries to `NODES.get()` again. If another thread's `node_close` already removed it, this `get()` returns `None` and the `unsubscribe` is silently skipped. This is not UB but means subscriptions may not be properly cleaned up.

## Remediation

### Option A — Hold write lock during close

```rust
pub extern "C" fn node_close(node_ptr: usize) -> FfiResult {
    // Atomically remove + get state. No window for concurrent access.
    let state = match NODES.remove(node_ptr) {
        Some(state) => state,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };
    // Clean up subscriptions using the removed state directly
    // (not via NODES.get)
    state.event_bus.close();
    // ...
}
```

This is already partially done (line 345), but the subscription cleanup on lines 330-342 happens BEFORE the remove.

### Option B — Catch_unwind (prerequisite: Finding 00)

With panic safety in place, a panic from using a closed database would be caught and converted to an error rather than being UB.

### Option C — Per-node "closing" flag

Add an `AtomicBool` to `NodeState` that is set before close begins. Operations check this flag after acquiring the Arc.

## Test Gap

- No test exercises concurrent `node_close` + `exec_request` on the same handle
- No test verifies behavior when operations hit a closed database
- Add a test that spawns two threads: one closing, one querying, and verify no crash
