# Finding: DAC Bypass Thread-Local Flag Safety Concerns

**Stream**: 02 - Access Control Policy
**Severity**: LOW
**Category**: Defense in Depth
**Status**: CONFIRMED - MITIGATED BUT FRAGILE
**Session**: S1 - DAC Implementation Review

## Summary

The DAC bypass flag is a `thread_local! { RefCell<bool> }` that grants unrestricted read access when set to `true`. While the current code paths correctly gate its activation behind NAC permission checks, the flag is never explicitly cleared after query execution, relying on being overwritten by the next request. A panic during query execution could leave the thread's bypass flag set, granting subsequent requests admin-level read access.

## Affected Files

| File | Line | Issue |
|------|------|-------|
| `crates/defra-core/src/dac_bypass.rs` | 10-24 | Thread-local flag definition |
| `crates/http/src/query_context.rs` | 37-44 | Set before query, no cleanup after |
| `crates/query/src/plan/permission_filter.rs` | 89-91 | Checked in hot path for every document |
| `crates/acp/src/nac/dac_bypass.rs` | 13-29 | `should_bypass_dac()` correctly gates activation |

## Details

### The Flag

```rust
// crates/defra-core/src/dac_bypass.rs
thread_local! {
    static DAC_BYPASS: RefCell<bool> = const { RefCell::new(false) };
}
```

### How It's Set (HTTP Path)

```rust
// crates/http/src/query_context.rs:37-44
tokio::task::spawn_blocking(move || {
    defra_core::signing::set_signing_config(signing_config);
    defra_core::batch_signing::set_batch_session_key(batch_session_key);
    defra_core::dac_bypass::set_dac_bypass(dac_bypass);  // SET
    handle.block_on(async { executor.execute(request).await })
    // NO CLEANUP — flag remains set on this thread
})
```

### The Concern

1. `spawn_blocking` pins execution to a single OS thread (correct for thread-local)
2. The flag is set BEFORE query execution
3. If `executor.execute(request).await` panics, the thread returns to the pool with `dac_bypass == true`
4. The next `spawn_blocking` task on the same thread inherits the bypass
5. While `spawn_blocking` catches panics via `expect()`, the thread pool thread itself survives

### Mitigations Already Present

1. **Correct activation gate**: `should_bypass_dac()` only returns `true` when NAC is enabled AND the identity has `NodePermission::DacBypass` — this is properly implemented
2. **Each request sets the flag**: Normal requests set `dac_bypass = false`, overwriting any stale `true`
3. **Fast path skips spawn_blocking**: When NAC is `None`, execution goes direct (line 27-29), so the thread-local is never touched

### Why This Is LOW, Not Higher

The window of vulnerability requires:
1. NAC to be enabled AND an admin to make a request (sets bypass=true)
2. That request to panic during execution (rare)
3. The next request on the same thread to be from a non-admin user
4. That next request to NOT go through `resolve_dac_bypass()` (which would correctly set it to false)

In practice, every request through the HTTP layer calls `resolve_dac_bypass()` which overwrites the flag. The only risk is if a panic occurs between `set_dac_bypass(true)` and the next `set_dac_bypass(false)` on the same thread, AND the recovery path doesn't reset the flag.

## Remediation

### Option A: Add explicit cleanup (minimal fix)

Use a drop guard to clear the flag on panic:

```rust
struct DacBypassGuard;
impl Drop for DacBypassGuard {
    fn drop(&mut self) {
        defra_core::dac_bypass::set_dac_bypass(false);
    }
}
```

### Option B: Pass bypass as function parameter

Instead of a thread-local, pass the bypass flag through the execution context. This eliminates the shared mutable state entirely.

## Test Gap

No test verifies DAC bypass behavior after a panic in the query execution path.
