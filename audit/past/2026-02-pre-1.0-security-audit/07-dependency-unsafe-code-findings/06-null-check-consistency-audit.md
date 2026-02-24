# 06: Null Pointer Check Consistency Audit — Consistent Pattern

| Field | Value |
|-------|-------|
| **Severity** | GREEN |
| **Category** | Null Pointer Safety |
| **Status** | Verified |

## Summary

Audited 20+ representative FFI functions across all modules for null pointer check consistency. The codebase uses a consistent two-tier pattern: `require_c_str()` for mandatory string parameters and `c_str_to_string()` for optional ones. Both check for null before calling `CStr::from_ptr()`. No function dereferences a raw char pointer without a null check.

## Verified Pattern

### Mandatory parameters — `require_c_str()`

```rust
// crates/ffi/src/helpers.rs:19-21
pub unsafe fn require_c_str(ptr: *const c_char, name: &str) -> Result<String, FfiResult> {
    c_str_to_string(ptr).ok_or_else(|| FfiResult::error(format!("{} is null", name)))
}
```

Used consistently across all modules:
- `exec_request`: `require_c_str(request_query, "request_query")`
- `commit_txn`: `require_c_str(txn_id, "txn_id")`
- `add_schema`: `require_c_str(schema_sdl, "schema_sdl")`
- `lens_add`: `require_c_str(lens_json, "lens_json")`
- `basic_import`: `require_c_str(filepath, "filepath")`

### Optional parameters — `c_str_to_string()`

```rust
// crates/ffi/src/types.rs:217-222
pub unsafe fn c_str_to_string(ptr: *const c_char) -> Option<String> {
    if ptr.is_null() {
        None
    } else {
        Some(CStr::from_ptr(ptr).to_string_lossy().into_owned())
    }
}
```

Used for nullable DID, operation name, variables, etc.:
- `exec_request`: `c_str_to_string(identity_did)`, `c_str_to_string(operation_name)`, etc.
- `batch_start`: `c_str_to_string(identity_did)`, `c_str_to_string(session_id)`

### Byte pointer parameters

For `*const u8` parameters (key bytes), null checks are done inline:
- `se_key.rs:23`: `if key_ptr.is_null() || key_len == 0 { return error }`
- `node.rs:106`: `if !options.signing_private_key.is_null() && options.signing_private_key_len > 0`

### Functions sampled

| Function | Module | Mandatory params | Optional params | Null checks |
|----------|--------|-----------------|-----------------|-------------|
| `exec_request` | query/exec | `request_query` | `identity_did`, `operation_name`, `variables`, `batch_session_id` | All checked |
| `commit_txn` | txn/lifecycle | `txn_id` | — | Checked |
| `add_schema` | schema | `schema_sdl` | `identity_did` | All checked |
| `lens_add` | lens | `lens_json` | — | Checked |
| `basic_import` | backup/import | `filepath` | — | Checked |
| `batch_start` | batch | — | `identity_did`, `session_id` | All checked |
| `block_verify_signature` | block | `public_key`, `block_cid` | `key_type`, `identity_did` | All checked |
| `delete_collection` | collection/write | `name` | `identity_did` | All checked |
| `set_se_encryption_key` | se_key | `key_ptr` | — | Checked |
| `p2p_add_collections` | p2p/collections | `collections_json` | `identity_did` | All checked |

## Residual Risk

Null checks prevent null dereferences but cannot detect **dangling pointers** (freed memory, wrong allocator, stack-allocated buffer that went out of scope). This is a fundamental limitation of FFI — the Rust side cannot validate pointer validity beyond null. The safety contract requires the Go caller to ensure pointers remain valid for the duration of the FFI call.

## Test Gap

- Several modules have tests for null parameter handling (good)
- `test_lens_add_null_json` specifically tests null pointer for lens_json
- `test_c_str_to_string_null_ptr` tests the core utility function
- Coverage is adequate for this check
