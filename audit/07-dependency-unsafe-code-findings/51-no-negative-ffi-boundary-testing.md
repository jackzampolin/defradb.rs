# No Negative Testing at FFI Boundary

- **Severity:** HIGH
- **Category:** Test Coverage / FFI Safety
- **Status:** Confirmed — no tests pass invalid inputs to FFI functions

## Summary

None of the 84 FFI entry points are tested with adversarial inputs: NULL pointers, invalid handles, empty strings, strings with embedded nulls, double-frees, or out-of-order lifecycle calls. The Rust unit tests in the FFI crate test the happy path and a few edge cases (null pointer to `defra_free_string`, invalid handle to `node_close`) but do not systematically exercise error paths.

## Affected Files

- All 84 FFI entry points in `crates/ffi/src/`
- `crates/ffi/src/types.rs:231` — `defra_free_string` (null check exists, double-free not tested)
- `crates/ffi/src/helpers.rs:19` — `require_c_str` (null check exists, never tested with non-UTF-8)
- `crates/ffi/src/state/registry.rs` — handle registry (never tested with concurrent access patterns)

## Details

### Missing Negative Test Categories

| Category | Example | Tested? | Risk |
|----------|---------|---------|------|
| NULL pointer to string param | `exec_request(node, NULL, NULL, NULL, NULL, NULL)` | NO | Undefined behavior if `require_c_str` path missed |
| Invalid handle (0) | `exec_request(0, ...)` | Partial — `node_close(0)` tested | Returns error cleanly |
| Invalid handle (usize::MAX) | `exec_request(usize::MAX, ...)` | NO | Should return error |
| Freed handle | Create node, close, then `exec_request(handle, ...)` | NO | Use-after-free if registry allows |
| Empty string | `add_schema(node, "", "")` | NO | Depends on downstream validation |
| String with embedded null | `add_schema(node, "hello\0world", ...)` | YES (types.rs unit test) | Null bytes replaced with U+FFFD |
| Non-UTF-8 bytes | Raw 0x80-0xFF byte sequences | NO | `to_string_lossy` handles, but untested |
| Double free of CString | `defra_free_string(ptr); defra_free_string(ptr)` | NO | **Undefined behavior** |
| Wrong-type handle | Pass subscription handle as node handle | NO | Registry lookup returns None → error |
| Concurrent operations | Multiple threads calling FFI simultaneously | NO | Registry uses RwLock, should be safe |
| Call before init | `exec_request(...)` before `defra_init()` | NO | Returns "runtime not initialized" |
| Call after close | `exec_request(node, ...)` after `node_close(node)` | NO | Should return error |

### Existing Edge Case Tests (Minimal)

The FFI crate has some edge case tests:

1. `node.rs:test_node_close_invalid_handle` — `node_close(0)` returns error
2. `node.rs:test_node_close_nonexistent_handle` — `node_close(999999)` returns error
3. `node.rs:test_node_lifecycle` — creates, uses, closes; double-close returns error
4. `types.rs:test_defra_free_string_null_ptr` — `defra_free_string(null)` is safe
5. `types.rs:test_ffi_result_success_with_null_bytes` — embedded nulls replaced
6. `types.rs:test_c_str_to_string_null_ptr` — null pointer returns None

### Missing Critical Tests

1. **Double-free of CString** — `defra_free_string` has a null check but no double-free guard. After `CString::from_raw(ptr)` frees the memory, calling it again is undefined behavior. No test verifies this crashes or is handled.

2. **NULL pointer to every function** — Only `defra_free_string` and `c_str_to_string` are tested with null. The 72 `unsafe extern "C"` functions take `*const c_char` parameters that could be null. Each uses `require_c_str` which returns an error on null, but this is never tested for most functions.

3. **Concurrent handle access** — The `NodeRegistry` uses `parking_lot::RwLock` which should be safe, but no test exercises concurrent reads/writes to verify there are no deadlocks or race conditions.

## Remediation

Add a systematic negative test module in `crates/ffi/src/`:

```rust
#[cfg(test)]
mod negative_tests {
    // For each FFI function, test:
    // 1. NULL pointer for every *const c_char parameter
    // 2. Invalid handle (0, usize::MAX)
    // 3. Handle after node_close
    // 4. Call before defra_init
}
```

Priority order:
1. **P0:** Double-free test for `defra_free_string` (memory corruption)
2. **P0:** NULL pointer to `exec_request`, `add_schema` (most common functions)
3. **P1:** Use-after-close for all functions taking `node_ptr`
4. **P2:** Non-UTF-8 input to string parameters
5. **P2:** Concurrent access patterns

## Test Gap

72 unsafe FFI functions × 4 negative test categories = ~288 missing negative tests. The most critical subset is NULL pointer handling for the 10 most commonly called functions.
