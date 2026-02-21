# 05: Functions Dereferencing Raw Pointers Not Marked `unsafe`

| Field | Value |
|-------|-------|
| **Severity** | LOW |
| **Category** | API Consistency / Safety Annotation |
| **Status** | Open |

## Summary

Several FFI functions accept raw pointers (via struct fields or direct parameters) and dereference them internally using `unsafe` blocks, but the function signatures themselves are not marked `unsafe extern "C"`. This is technically correct Rust (the `unsafe` blocks handle the invariants), but it creates an inconsistency in the FFI surface and means cbindgen may generate headers that don't distinguish these from truly safe functions.

## Affected Files

- `crates/ffi/src/node.rs:25` — `new_node(options: NodeInitOptions)` — dereferences `options.db_path`, `options.signing_private_key`, etc.
- `crates/ffi/src/node.rs:322` — `node_close(node_ptr: usize)` — no raw pointer params but paired with unsafe `new_node`
- `crates/ffi/src/txn/lifecycle.rs:22` — `begin_txn(node_ptr, readonly)` — no raw pointers
- `crates/ffi/src/lib.rs:146` — `defra_init()` — no raw pointers
- `crates/ffi/src/lib.rs:158` — `defra_version()` — no raw pointers, returns `*mut c_char`
- `crates/ffi/src/subscription/create.rs:62` — `create_merge_complete_subscription(node_ptr)` — no raw pointers
- `crates/ffi/src/p2p/push.rs:31` — `p2p_retry_replicators(node_ptr)` — no raw pointers

## Details

### The inconsistency

Of the 84 FFI entry points, most are marked `unsafe extern "C"`. But at least 7 are just `extern "C"`. The split:

```
unsafe extern "C":  ~77 functions (take raw pointers directly)
extern "C":         ~7 functions (either no raw pointers, or pointers in structs)
```

`new_node` is the most notable because `NodeInitOptions` contains 7 raw pointer fields that are dereferenced inside:

```rust
// NOT marked unsafe, but dereferences raw pointers from options
pub extern "C" fn new_node(options: NodeInitOptions) -> NewNodeResult {
    // ...
    let backend_name = unsafe { c_str_to_string(options.datastore_backend) };
    // ...
    let key_bytes = unsafe {
        std::slice::from_raw_parts(
            options.signing_private_key,
            options.signing_private_key_len,
        )
    };
}
```

### Impact

- The generated C header shows `new_node` with the same calling convention as safe functions
- A C/Go caller has no signal from the header that `NodeInitOptions` fields must satisfy safety invariants
- The Rust compiler allows calling `new_node` from safe Rust code even though it internally relies on raw pointer validity

### Why this is LOW

- All FFI functions are inherently unsafe from the caller's perspective (C has no safety guarantees)
- The `unsafe` blocks inside correctly delineate where raw pointer dereferences occur
- The header documentation describes the safety requirements regardless of the `unsafe` keyword
- `begin_txn`, `defra_init`, etc. genuinely don't need `unsafe` (they take no raw pointers)

## Remediation

Mark `new_node` and `new_node_with_p2p` as `unsafe extern "C"` for consistency with the rest of the FFI surface:

```rust
#[no_mangle]
pub unsafe extern "C" fn new_node(options: NodeInitOptions) -> NewNodeResult {
```

This doesn't change runtime behavior but signals to Rust callers and auditors that the function has safety preconditions.

## Test Gap

N/A — this is a documentation/annotation issue, not a runtime behavior issue.
