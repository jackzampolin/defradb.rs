# 08: CString Ownership & Sanitization — Sound Design

| Field | Value |
|-------|-------|
| **Severity** | GREEN |
| **Category** | String Safety / Ownership Transfer |
| **Status** | Verified |

## Summary

The CString ownership model is correctly implemented. `sanitize_to_cstring()` properly handles embedded null bytes (replacing with U+FFFD). `defra_free_string()` correctly reconstructs and drops the CString. All FFI result types consistently use `into_raw()` for ownership transfer and document the free requirement.

## Verified Properties

### 1. sanitize_to_cstring handles all edge cases

```rust
pub fn sanitize_to_cstring(value: impl Into<String>, fallback: &str) -> CString {
    let s = value.into();
    match CString::new(s.clone()) {
        Ok(cstring) => cstring,                      // Happy path
        Err(_) => {
            let sanitized = s.replace('\0', "\u{FFFD}"); // Replace null bytes
            CString::new(sanitized).unwrap_or_else(|_| {
                CString::new(fallback).unwrap_or_else(|_| {
                    CString::new("error").unwrap()    // Final fallback (no nulls)
                })
            })
        }
    }
}
```

The three-level fallback chain ensures a CString is always produced:
1. Try the original string
2. Replace null bytes with U+FFFD and retry
3. Use the provided fallback string
4. Use literal "error" (guaranteed null-free)

Tests verify this:
- `test_ffi_result_success_with_null_bytes` — verifies U+FFFD replacement
- `test_ffi_result_error_with_null_bytes` — same for error messages

### 2. Ownership transfer via into_raw()

Every CString crosses the FFI boundary via `.into_raw()`:
```rust
pub fn success(value: impl Into<String>) -> Self {
    Self {
        status: 0,
        error: ptr::null_mut(),
        value: sanitize_to_cstring(value, "{}").into_raw(), // Ownership to C
    }
}
```

`into_raw()` consumes the CString without calling its destructor, transferring ownership to the caller. This is the correct pattern — the Go side must call `defra_free_string()` to reclaim the memory.

### 3. defra_free_string correctly reverses the transfer

```rust
pub unsafe extern "C" fn defra_free_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        drop(CString::from_raw(ptr)); // Reconstruct and drop
    }
}
```

`CString::from_raw()` reconstructs the CString from the raw pointer, and `drop()` deallocates it using Rust's allocator. The null check prevents UB on null pointers.

### 4. Consistent documentation

All result type fields document the free requirement:
```rust
/// Error message (null on success). Caller must free with `defra_free_string`.
pub error: *mut c_char,
/// JSON value (null on error). Caller must free with `defra_free_string`.
pub value: *mut c_char,
```

The C header preserves this documentation.

### 5. to_string_lossy for incoming strings

```rust
pub unsafe fn c_str_to_string(ptr: *const c_char) -> Option<String> {
    // ...
    Some(CStr::from_ptr(ptr).to_string_lossy().into_owned())
}
```

Using `to_string_lossy()` instead of `to_str()` means invalid UTF-8 from Go is replaced with U+FFFD rather than causing an error. This is defensive and correct for an FFI boundary.

## Residual Risk

See Finding 03 for double-free concerns (caller responsibility).

## Test Gap

- Good coverage: null byte handling, null pointer free, basic result construction
- Could add: test with very large strings, test with UTF-8 multi-byte sequences
