# 00: No `catch_unwind` — Panics in FFI Are Undefined Behavior

| Field | Value |
|-------|-------|
| **Severity** | CRITICAL |
| **Category** | Panic Safety / Undefined Behavior |
| **Status** | Open |

## Summary

None of the 84 `pub unsafe extern "C"` FFI entry points are wrapped in `std::panic::catch_unwind()`. A Rust panic inside any FFI function will unwind across the C/Go boundary, which is **undefined behavior** per the Rust reference. This can corrupt the Go process stack, cause segfaults, or silently corrupt memory.

## Affected Files

- `crates/ffi/src/**/*.rs` — all 84 FFI entry points
- Verified by: `grep -r "catch_unwind\|AssertUnwindSafe" crates/ffi/` → zero matches

## Details

Every FFI function follows this pattern — naked entry with no unwind guard:

```rust
// crates/ffi/src/query/exec.rs:39
#[no_mangle]
pub unsafe extern "C" fn exec_request(
    node_ptr: usize,
    identity_did: *const c_char,
    request_query: *const c_char,
    // ...
) -> FfiResult {
    let rt = try_ffi!(get_rt());
    let query_str = try_ffi!(require_c_str(request_query, "request_query"));
    // ... no catch_unwind wrapper ...
}
```

### Panic sources reachable from FFI

1. **`block_on()` inside `block_on()`**: If any code path in the database/query engine calls `tokio::runtime::Runtime::block_on()` while already inside a `block_on()` (which every FFI function does), tokio panics: "Cannot start a runtime from within a runtime."

2. **`unwrap()` on fallible operations**: `sanitize_to_cstring` uses `CString::new("error").unwrap()` as its ultimate fallback (types.rs:206). While `"error"` has no null bytes, any code path reaching an unexpected state could hit an `unwrap()` in downstream crates.

3. **Integer arithmetic**: Although the handle counter wraps safely, other arithmetic in deep crate code may use checked operations that panic on overflow in debug mode.

4. **Serialization/deserialization**: `serde_json::to_string()` shouldn't panic, but custom `Serialize` implementations could.

5. **Index out of bounds**: Any `vec[i]` or slice indexing in the database/query engine.

### Impact

When Rust unwinds through a C frame (CGO):
- The C stack frames are not unwound correctly (no destructors, no cleanup)
- Go's goroutine stack gets corrupted
- Behavior is completely unpredictable: SIGSEGV, silent corruption, data loss

## Remediation

Wrap every FFI entry point in `catch_unwind`:

```rust
#[no_mangle]
pub unsafe extern "C" fn exec_request(/* ... */) -> FfiResult {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        exec_request_inner(/* ... */)
    })) {
        Ok(result) => result,
        Err(panic) => {
            let msg = panic
                .downcast_ref::<String>()
                .map(|s| s.as_str())
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("unknown panic in FFI");
            FfiResult::error(format!("internal error (panic): {}", msg))
        }
    }
}
```

Or create a macro:

```rust
macro_rules! ffi_entry {
    ($body:expr) => {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| $body)) {
            Ok(result) => result,
            Err(panic) => {
                let msg = /* extract message */;
                FfiResult::error(format!("internal error: {}", msg))
            }
        }
    };
}
```

Apply to all 84 entry points. This is the single highest-impact fix for FFI safety.

## Test Gap

- No test attempts to trigger a panic inside an FFI function
- No test verifies that panics are caught and converted to errors
- Add tests that force panics (e.g., pass data that triggers `unwrap()` on `None`) and verify `FfiResult.status == 1` is returned instead of a crash
