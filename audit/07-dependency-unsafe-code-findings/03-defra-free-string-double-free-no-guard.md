# 03: `defra_free_string` Has No Double-Free Protection

| Field | Value |
|-------|-------|
| **Severity** | LOW |
| **Category** | Memory Safety / Double-Free |
| **Status** | Open (by design — caller responsibility) |

## Summary

`defra_free_string()` checks for null but has no mechanism to detect or prevent double-free. If the Go caller frees the same pointer twice, `CString::from_raw()` will attempt to deallocate already-freed memory, causing undefined behavior (heap corruption, crash, or silent data corruption).

## Affected Files

- `crates/ffi/src/types.rs:231-235` — `defra_free_string()`

## Details

```rust
// crates/ffi/src/types.rs:231-234
pub unsafe extern "C" fn defra_free_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        drop(CString::from_raw(ptr));
    }
}
```

### The contract

The FFI design follows the standard C pattern: caller takes ownership of returned pointers and must free them exactly once. This is documented in the header comments. However, Go's garbage collector and CGO bridge create scenarios where double-free is more likely than in pure C:

1. **Deferred cleanup**: Go code often uses `defer C.defra_free_string(ptr)`. If the same pointer is assigned to multiple variables, both defers will fire.
2. **Error handling**: If Go code frees the error string in a cleanup path AND in the normal path, double-free occurs.
3. **Shared results**: If an `FfiResult` is copied (value semantics in Go), both copies' cleanup code may free the same pointer.

### Why this is LOW severity

- This is standard FFI practice — Rust's `CString::from_raw` has the same contract as C's `free()`
- The Go FFI wrapper should be the single point of responsibility for freeing
- Adding protection (e.g., a "freed pointers" set) would add per-call overhead for a condition that indicates a Go-side bug

### Cross-reference with defra.h

The header clearly documents the ownership contract:
```c
// defra.h:36-40
char *error;   // Caller must free with `defra_free_string`.
char *value;   // Caller must free with `defra_free_string`.
```

## Remediation

This is acceptable as-is for production use. For defense-in-depth:

Option A — Debug-mode poisoning (zero-cost in release):

```rust
pub unsafe extern "C" fn defra_free_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        #[cfg(debug_assertions)]
        {
            // Write a sentinel to detect double-free in tests
            *ptr = 0xDE as c_char;
        }
        drop(CString::from_raw(ptr));
    }
}
```

Option B — Document explicitly in Go wrapper comments that each pointer must be freed exactly once.

## Test Gap

- `test_defra_free_string_null_ptr` tests null safety (good)
- No test attempts double-free (would be UB, so can't test safely without a custom allocator)
- Consider adding a Miri-based test that verifies single-free correctness
