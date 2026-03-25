# Unsafe & Verification Audit Findings

## Summary
- Total findings: 14
- Critical: 2 | High: 3 | Medium: 5 | Low: 4

### Scope

Audited all 25 crates in the workspace. Unsafe code was found in exactly 3 crates:

| Crate | Unsafe blocks | Unsafe impls | extern "C" fns |
|-------|---------------|--------------|----------------|
| `query` | 3 | 2 | 0 |
| `storage` (rocksdb backend) | 1 | 2 | 0 |
| `ffi` | ~60 | 0 | ~85 |

The remaining 22 crates (`acp`, `blockstore`, `cli`, `crdt`, `crypto`, `datastore`, `db`, `defra-core`, `defra-version`, `document`, `events`, `http`, `identity`, `keyring`, `lens`, `orbis`, `p2p`, `pg-compat`, `schema`, `sourcehub`, `wasm`, `zanzibar`) contain zero `unsafe` code. This is an excellent architectural outcome for a systems database.

## Findings

### Finding 1
- **severity:** critical
- **category:** unsound
- **crate:** query
- **file:** crates/query/src/runner/fetcher.rs
- **line:** 28-73
- **pattern:** transmute-fat-pointer-layout
- **description:** `FetcherWrapper` uses `std::mem::transmute` to decompose and reconstruct trait object fat pointers (`*const dyn DocFetcher` to/from `(*const (), *const ())`). The layout of fat pointers is **not guaranteed by the Rust reference or any RFC**. While it works on all current targets, this is technically relying on an implementation detail. The comment on line 28-30 acknowledges this: "relies on the standard fat pointer layout... which is stable in practice but not formally guaranteed." This makes the code fragile to compiler changes. Additionally, the `get_fetcher()` method on line 55-65 dereferences a raw pointer with a lifetime that cannot be statically verified -- if the original reference is dropped before the wrapper, this is use-after-free.
- **training_ref:** rust-patterns-book ch12 "Common UB Pitfalls" -- "Invalid enum value / transmute... Almost always wrong"
- **suggested_fix:** Replace the transmute-based fat pointer decomposition with `std::ptr::metadata` and `std::ptr::from_raw_parts` once the `ptr_metadata` feature stabilizes. In the interim, consider using `std::raw::TraitObject` behind a cfg gate, or restructure to pass `Arc<dyn DocFetcher>` directly (the planner already requires `Arc<dyn DocFetcher>`). If the reference-based approach must remain, add a `PhantomData<&'a dyn DocFetcher>` with a proper lifetime parameter to make the lifetime constraint compile-time enforced.

### Finding 2
- **severity:** critical
- **category:** unsound
- **crate:** storage
- **file:** crates/storage/src/backends/rocksdb/transaction.rs
- **line:** 32-42
- **pattern:** transmute-lifetime-extension
- **description:** `OwnedSnapshot::new()` uses `std::mem::transmute` to extend the lifetime of a `SnapshotWithThreadMode<'_, DB>` to `SnapshotWithThreadMode<'static, DB>`. The safety argument is that the `Arc<DB>` stored alongside it keeps the DB alive. However, if `OwnedSnapshot` fields are reordered (e.g., by a future refactor moving `snapshot` before `_db`), Rust's drop order (fields drop in declaration order) would drop the snapshot while the DB is still alive, which is correct -- but if `_db` were moved after `snapshot`, the snapshot's destructor could access freed DB memory. The current field order is safe, but the invariant is fragile and not enforced by the type system.
- **training_ref:** rust-patterns-book ch12 "Common UB Pitfalls" -- "Dangling pointer: Dereference after drop()"
- **suggested_fix:** Add a comment `// IMPORTANT: _db MUST be declared before snapshot to ensure correct drop order.` and consider using `ManuallyDrop<SnapshotWithThreadMode<'static, DB>>` with an explicit `Drop` impl that drops in the correct order. Alternatively, use a `Pin` or an ouroboros-style self-referential struct.

### Finding 3
- **severity:** high
- **category:** anti-pattern
- **crate:** ffi
- **file:** crates/ffi/src/mobile.rs
- **line:** 597-624
- **pattern:** missing-catch-unwind-on-extern-c
- **description:** Three `extern "C"` functions -- `defra_mobile_peer_info` (line 597), `defra_mobile_connect` (line 608), and `defra_mobile_notify_network_change` (line 618) -- execute code before delegating to inner FFI functions that have `ffi_entry!`. The code between the outer `extern "C"` boundary and the inner call (specifically `default_identity_cstring()` on lines 598, 609, 619) is NOT wrapped in `catch_unwind`. If `default_identity_cstring` panics (e.g., a `NODES.get` callback panics), the panic will unwind across the FFI boundary, which is undefined behavior.
- **training_ref:** rust-patterns-book ch12 "FFI Patterns" -- "Calling Rust from C" and engineering-book ch5 "Miri, Valgrind, and Sanitizers" -- the decision tree mandates `catch_unwind` at every FFI boundary
- **suggested_fix:** Wrap each function body in `ffi_entry! { ... }`. For example: `pub extern "C" fn defra_mobile_peer_info(node_ptr: usize) -> FfiResult { ffi_entry! { let identity = ... } }`.

### Finding 4
- **severity:** high
- **category:** anti-pattern
- **crate:** ffi
- **file:** crates/ffi/src/lib.rs
- **line:** 191-209
- **pattern:** missing-catch-unwind-on-extern-c
- **description:** `defra_init()` (line 191) and `defra_version()` (line 203) are `extern "C"` functions without `ffi_entry!`. While `defra_init` is extremely unlikely to panic (it calls `init_runtime()` which handles errors internally and stores to an atomic), `defra_version` calls `CString::new(...).unwrap_or_else(...)` which theoretically cannot panic but still lacks the safety net. The `defra_init` function is more concerning because `init_runtime()` calls `tokio::runtime::Builder::new_multi_thread().enable_all().build()` which could theoretically panic in exotic conditions.
- **training_ref:** rust-patterns-book ch12 "FFI Patterns" -- every exported `extern "C"` function should use `catch_unwind`
- **suggested_fix:** Wrap both in `ffi_entry!`. For `defra_init()` which returns `void`, add a return type (e.g., `FfiResult`) or use a specialized panic-catching wrapper that ignores the return value.

### Finding 5
- **severity:** high
- **category:** anti-pattern
- **crate:** ffi
- **file:** crates/ffi/src/mobile.rs
- **line:** 458-459
- **pattern:** missing-catch-unwind-on-extern-c
- **description:** `defra_mobile_close_node` is `extern "C"` and directly delegates to `node_close(node_ptr)` without `ffi_entry!`. While `node_close` itself uses `ffi_entry!`, this is a coincidence of implementation -- the function signature at the `extern "C"` boundary is the contract that matters. If `node_close`'s implementation were ever changed to not use `ffi_entry!`, this would silently become unsound.
- **training_ref:** rust-patterns-book ch12 "FFI Patterns"
- **suggested_fix:** Wrap in `ffi_entry!` for defense-in-depth: `pub extern "C" fn defra_mobile_close_node(node_ptr: usize) -> FfiResult { ffi_entry! { node_close(node_ptr) } }`.

### Finding 6
- **severity:** medium
- **category:** improvement
- **crate:** ffi
- **file:** crates/ffi/src/acp/identity.rs
- **line:** 306
- **pattern:** missing-safety-comment-from-raw-parts
- **description:** `std::slice::from_raw_parts(public_key_ptr, public_key_len)` is called without a `// SAFETY:` comment. The null check on line 300 validates the pointer is non-null and length is non-zero, but there is no upper bound check on `public_key_len`. A malicious or buggy caller could pass `usize::MAX` as the length, causing `from_raw_parts` to create a slice spanning invalid memory.
- **training_ref:** rust-patterns-book ch12 "The three rules of sound unsafe code" -- "Document invariants -- every SAFETY comment explains why the operation is valid"
- **suggested_fix:** Add a maximum length check (e.g., `if public_key_len > 256 { return Err(...) }`) and add a `// SAFETY:` comment: `// SAFETY: public_key_ptr is non-null (checked above), and public_key_len is bounded by the max check. The caller (Go FFI) guarantees the pointer is valid for public_key_len bytes.`

### Finding 7
- **severity:** medium
- **category:** improvement
- **crate:** ffi
- **file:** crates/ffi/src/node.rs
- **line:** 136-142
- **pattern:** missing-safety-comment-from-raw-parts
- **description:** `std::slice::from_raw_parts(options.signing_private_key, options.signing_private_key_len)` has a null check (line 129) and a max length check (`MAX_PRIVATE_KEY_LEN` = 128, line 130-134), which is good. However, there is no `// SAFETY:` comment documenting why the operation is sound.
- **training_ref:** rust-patterns-book ch12 "The three rules of sound unsafe code" -- "Document invariants"
- **suggested_fix:** Add `// SAFETY: signing_private_key is non-null (checked on line 129), and signing_private_key_len <= MAX_PRIVATE_KEY_LEN (checked on line 130). The caller guarantees the pointer is valid for this many bytes.`

### Finding 8
- **severity:** medium
- **category:** improvement
- **crate:** ffi
- **file:** crates/ffi/src/node.rs
- **line:** 181-186
- **pattern:** missing-safety-comment-from-raw-parts
- **description:** Same pattern as Finding 7 for `sourcehub_signer_key`. The null/length checks exist (lines 169-178) but the `from_raw_parts` call lacks a `// SAFETY:` comment.
- **training_ref:** rust-patterns-book ch12 "The three rules of sound unsafe code"
- **suggested_fix:** Add `// SAFETY:` comment documenting the precondition checks.

### Finding 9
- **severity:** medium
- **category:** improvement
- **crate:** ffi
- **file:** crates/ffi/src/se_key.rs
- **line:** 38
- **pattern:** missing-safety-comment-from-raw-parts
- **description:** `std::slice::from_raw_parts(key_ptr, key_len)` has excellent validation (null check line 27, exact length check line 31) but no `// SAFETY:` comment.
- **training_ref:** rust-patterns-book ch12 "The three rules of sound unsafe code"
- **suggested_fix:** Add `// SAFETY: key_ptr is non-null (checked above) and key_len == 32 (checked above). The caller guarantees the pointer is valid for 32 bytes.`

### Finding 10
- **severity:** medium
- **category:** improvement
- **crate:** query
- **file:** crates/query/src/runner/fetcher.rs
- **line:** 72-73
- **pattern:** unsafe-send-sync-impl
- **description:** `unsafe impl Send for FetcherWrapper {}` and `unsafe impl Sync for FetcherWrapper {}` are implemented with a safety comment (lines 68-71) that argues correctness based on `DocFetcher: Send + Sync`. However, the wrapper holds raw pointers (`*const ()`), which are neither `Send` nor `Sync`. The safety argument is sound IF the lifetime invariant holds (the original reference outlives the wrapper), but this invariant is not enforced by the type system. Any refactoring that changes the call site could silently break the invariant.
- **training_ref:** rust-patterns-book ch12 "Writing Sound Abstractions" -- "Encapsulate -- the unsafe is inside a safe API; users can't trigger UB"
- **suggested_fix:** At minimum, add a module-level `#[deny(unsafe_op_in_unsafe_fn)]` and restrict `FetcherWrapper::new()` visibility. Better: restructure to use `Arc<dyn DocFetcher>` and eliminate the wrapper entirely.

### Finding 11
- **severity:** low
- **category:** improvement
- **crate:** ffi
- **file:** crates/ffi/src/query/mod.rs
- **line:** 61
- **pattern:** missing-safety-comment
- **description:** `unsafe { c_str_to_string(identity_did) }` in `check_and_set_dac_bypass` lacks a `// SAFETY:` comment. The function is `pub(crate)` and receives a raw pointer from FFI context, but doesn't document the safety invariant.
- **training_ref:** rust-patterns-book ch12 "The three rules of sound unsafe code"
- **suggested_fix:** Add `// SAFETY: identity_did is either null or a valid C string from the FFI caller.`

### Finding 12
- **severity:** low
- **category:** improvement
- **crate:** ffi
- **file:** crates/ffi/src/nac_check.rs
- **line:** 41
- **pattern:** missing-safety-comment
- **description:** `unsafe { c_str_to_string(identity_did) }` in `check_nac_permission` lacks a `// SAFETY:` comment. The function's parameter is a raw pointer but the function itself is safe (not `unsafe fn`), so callers don't get a compiler warning about the contract.
- **training_ref:** rust-patterns-book ch12 "The three rules of sound unsafe code"
- **suggested_fix:** Either make `check_nac_permission` an `unsafe fn` (since it requires a valid C string pointer), or add a `// SAFETY:` comment and document the requirement in the function's doc comment.

### Finding 13
- **severity:** low
- **category:** improvement
- **crate:** storage
- **file:** crates/storage/src/backends/rocksdb/transaction.rs
- **line:** 28-29
- **pattern:** missing-safety-comment
- **description:** `unsafe impl Send for OwnedSnapshot {}` and `unsafe impl Sync for OwnedSnapshot {}` have a safety comment (lines 26-27) but it says "the underlying SnapshotWithThreadMode is Send+Sync". This is misleading because if `SnapshotWithThreadMode` were already `Send + Sync`, the `unsafe impl` would not be needed. The real reason the impl is needed is that the type contains a self-referential `'static` lifetime that the compiler cannot verify.
- **training_ref:** rust-patterns-book ch12 "Writing Sound Abstractions"
- **suggested_fix:** Update the safety comment to: `// SAFETY: OwnedSnapshot is safe to Send/Sync because: (1) the Arc<DB> ensures the DB outlives the snapshot, (2) rocksdb::SnapshotWithThreadMode is internally thread-safe (uses a C pointer to a snapshot handle), (3) no &mut access to the snapshot is possible through &OwnedSnapshot.`

### Finding 14
- **severity:** low
- **category:** improvement
- **crate:** p2p
- **file:** crates/p2p/src/sync/dag_sync/config.rs
- **line:** 98-99
- **pattern:** unnecessary-safety-comment
- **description:** The comment `// SAFETY: 16 is non-zero` is on a call to `NonZeroUsize::new(16).unwrap()`. This is not actually unsafe code -- `NonZeroUsize::new` returns `Option`, and `.unwrap()` is safe Rust that will never panic because 16 is provably non-zero. The `SAFETY` comment is misleading because it implies there's an `unsafe` block.
- **training_ref:** rust-patterns-book ch12 "The three rules of sound unsafe code" -- SAFETY comments should only appear on `unsafe` blocks
- **suggested_fix:** Change comment to a regular comment: `// 16 is non-zero, so unwrap is safe.` or remove it entirely since the intent is obvious.

## Verification Candidates

### Miri Candidates
- `query::runner::fetcher.rs:47` -- The `transmute` of fat pointer layout should be tested with Miri under both Stacked Borrows (default) and Tree Borrows models to detect any aliasing violations when the wrapper is used across async boundaries.
- `storage::backends::rocksdb::transaction.rs:35` -- The lifetime-extended snapshot transmute cannot be directly tested by Miri (RocksDB is a C library, FFI is opaque to Miri), but unit tests that exercise `OwnedSnapshot` creation and use patterns could be run under Miri if the rocksdb dependency is mocked.

### Valgrind Candidates
- `ffi::acp::identity.rs:28` -- The `FfiRemoteSigner::sign_sync` method calls a C function pointer callback. This entire callback path should be tested with Valgrind to verify the C-side memory handling is correct (buffer sizes, write bounds, etc.).
- `ffi::node.rs:136` and `ffi::node.rs:181` -- The `from_raw_parts` calls that read C-provided byte slices should be tested with Valgrind under the Go FFI harness to verify memory is valid.
- All `extern "C"` functions in the `ffi` crate -- Run the Go FFI test suite under Valgrind memcheck to detect any memory leaks from `CString::into_raw()` that are never freed by `defra_free_string()`.

### loom Candidates
- No loom candidates identified. The codebase uses standard synchronization primitives (`RwLock`, `Mutex`, `AtomicUsize`, `OnceLock`) throughout and does not implement any custom lock-free data structures. The `FetcherWrapper` in `query` uses raw pointers but is not a concurrent data structure -- it's a lifetime-erasing wrapper used within a single query execution.

## Overall Assessment

The codebase has an excellent unsafe hygiene posture. Only 3 of 25 crates contain any unsafe code at all. The two non-FFI unsafe sites (FetcherWrapper transmute and RocksDB snapshot lifetime extension) are the highest-risk items and deserve the most attention. The FFI crate has a well-designed `ffi_entry!` macro with `catch_unwind` that is consistently applied to nearly all `extern "C"` functions, with a small number of exceptions noted above.

The most impactful improvements would be:
1. Eliminate the `FetcherWrapper` transmute by restructuring to use `Arc<dyn DocFetcher>` (Finding 1)
2. Add `ffi_entry!` to the 5 unprotected `extern "C"` functions (Findings 3-5)
3. Add `// SAFETY:` comments to all `from_raw_parts` calls (Findings 6-9)
