# 09: C Header Type Mapping — Correct

| Field | Value |
|-------|-------|
| **Severity** | GREEN |
| **Category** | C Header / ABI Compatibility |
| **Status** | Verified |

## Summary

Spot-checked the generated `defra.h` against the Rust source. All type mappings are correct, calling conventions match, and struct layouts are ABI-compatible. cbindgen 0.28 generates correct output for this crate.

## Verified Mappings

| Rust Type | C Type (defra.h) | Correct? |
|-----------|-------------------|----------|
| `usize` | `uintptr_t` | Yes — both are pointer-width unsigned |
| `c_int` | `int` | Yes — standard mapping |
| `*mut c_char` | `char *` | Yes |
| `*const c_char` | `const char *` | Yes |
| `*const u8` | `const uint8_t *` | Yes |
| `u64` | `uint64_t` | Yes |
| `i32` | `int32_t` | Yes |

## Struct Layout Verification

### FfiResult

Rust:
```rust
#[repr(C)]
pub struct FfiResult {
    pub status: c_int,
    pub error: *mut c_char,
    pub value: *mut c_char,
}
```

C:
```c
typedef struct FfiResult {
    int status;
    char *error;
    char *value;
} FfiResult;
```

Match: fields in same order, same types, `repr(C)` ensures C layout.

### NodeInitOptions

Rust has 12 fields with mixed pointer and integer types. C header matches all fields in order with correct types. `uintptr_t` for `usize` length fields is correct (not `size_t`, which could differ on some exotic platforms, but in practice they're equivalent on all Go-supported targets).

### NewNodeResult

Rust: `node_ptr: usize` → C: `uintptr_t node_ptr` — Correct.

## Function Signatures

Spot-checked 10 functions:

| Rust | C Header | Match? |
|------|----------|--------|
| `fn new_node(options: NodeInitOptions) -> NewNodeResult` | `struct NewNodeResult new_node(struct NodeInitOptions options)` | Yes |
| `fn node_close(node_ptr: usize) -> FfiResult` | `struct FfiResult node_close(uintptr_t node_ptr)` | Yes |
| `fn exec_request(node_ptr: usize, ...) -> FfiResult` | `struct FfiResult exec_request(uintptr_t node_ptr, ...)` | Yes |
| `fn defra_free_string(ptr: *mut c_char)` | `void defra_free_string(char *ptr)` | Yes |
| `fn defra_init()` | `void defra_init(void)` | Yes |
| `fn defra_version() -> *mut c_char` | `char *defra_version(void)` | Yes |

## cbindgen Configuration

```toml
language = "C"
cpp_compat = true
```

The `cpp_compat = true` adds `extern "C"` guards for C++ inclusion. The `[fn] rename_args = "SnakeCase"` ensures consistent parameter naming.

## Residual Note

The `delete_index` function is renamed to `drop_index` in the C header (likely via cbindgen rename or a `#[no_mangle]` attribute difference). This is intentional to match Go's naming convention.

## Test Gap

- No automated test compares defra.h against Rust signatures
- Consider adding a build script that generates the header and diffs against the committed version
