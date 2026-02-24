# Session 1 Summary: FFI Boundary Deep-Dive

## Scope

Comprehensive audit of the FFI boundary between Rust and Go (`crates/ffi/src/`). Covered all 84 `pub unsafe extern "C"` entry points, the handle registry, string ownership model, raw pointer handling, concurrency, panic safety, C header correctness, and runtime architecture.

## Files Audited

| File | Lines | Focus |
|------|-------|-------|
| `crates/ffi/src/types.rs` | 341 | FfiResult, sanitize_to_cstring, defra_free_string, c_str_to_string |
| `crates/ffi/src/node.rs` | 449 | new_node, node_close, NodeInitOptions pointer handling |
| `crates/ffi/src/helpers.rs` | 36 | require_c_str, get_rt, get_node_database, get_node_runner |
| `crates/ffi/src/state/registry.rs` | 309 | NodeRegistry, SubscriptionRegistry, handle allocation |
| `crates/ffi/src/state/mod.rs` | 139 | NodeState, FfiStore, type aliases |
| `crates/ffi/src/txn/lifecycle.rs` | 138 | begin_txn, commit_txn, rollback_txn |
| `crates/ffi/src/query/exec.rs` | 240 | exec_request — the most complex FFI function |
| `crates/ffi/src/batch.rs` | 105 | batch_start, batch_sign |
| `crates/ffi/src/lens.rs` | 287 | lens_add, lens_list, WASM path handling |
| `crates/ffi/src/block.rs` | 93 | block_verify_signature |
| `crates/ffi/src/se_key.rs` | 46 | set_se_encryption_key — raw byte pointer |
| `crates/ffi/src/subscription/mod.rs` | 137 | Result types for subscriptions |
| `crates/ffi/src/subscription/create.rs` | 158 | create_subscription, event handling |
| `crates/ffi/src/collection/write.rs` | 369 | CRUD operations pattern |
| `crates/ffi/src/backup/import.rs` | 120 | basic_import — filesystem access |
| `crates/ffi/src/nac_check.rs` | 93 | NAC permission checking pattern |
| `crates/ffi/src/p2p/node.rs` | 545 | new_node_with_p2p — largest FFI function |
| `crates/ffi/src/runtime.rs` | 94 | Global tokio runtime |
| `crates/ffi/src/lib.rs` | 263 | Macro definitions, re-exports |
| `crates/ffi/cbindgen.toml` | 38 | Header generation config |
| `crates/ffi/Cargo.toml` | 71 | Dependencies |
| `defra.h` | 1657 | Generated C header (full spot-check) |

## Findings Summary

| # | Finding | Severity | Status |
|---|---------|----------|--------|
| 00 | **No `catch_unwind` — panics are UB** | CRITICAL | Open |
| 01 | `from_raw_parts` with no length cap | MEDIUM | Open |
| 02 | Handle counter wraps to 0 on overflow | LOW | Open |
| 03 | `defra_free_string` no double-free guard | LOW | Open (by design) |
| 04 | Race between `node_close` and operations | MEDIUM | Open |
| 05 | `new_node` not marked `unsafe` | LOW | Open |
| 06 | Null pointer check consistency | GREEN | Verified |
| 07 | Handle registry design (no ABA) | GREEN | Verified |
| 08 | CString ownership & sanitization | GREEN | Verified |
| 09 | C header type mapping | GREEN | Verified |
| 10 | Tokio runtime architecture | GREEN | Verified |

## Severity Distribution

- **CRITICAL**: 1 (panic safety)
- **MEDIUM**: 2 (raw slice length, close race)
- **LOW**: 3 (handle overflow, double-free, unsafe annotation)
- **GREEN**: 5 (verified correct)

## Key Architecture Observations

### What's done well

1. **Consistent null-check pattern**: `require_c_str()` / `c_str_to_string()` used uniformly across all 84 entry points
2. **Null byte sanitization**: `sanitize_to_cstring()` prevents panics from embedded nulls in output strings
3. **Handle registry**: Monotonic IDs, HashMap + RwLock, closure-based API preventing dangling references
4. **Arc-based state sharing**: Cloning Arc inside the lock closure is sound — state survives lock release
5. **Defensive defra_free_string**: Null-safe, correct CString reconstruction
6. **C header generation**: cbindgen produces correct type mappings

### What needs attention

1. **Panic safety is the #1 priority**: A single `unwrap()` panic in any call path is UB. Adding `catch_unwind` to all 84 entry points should be the next FFI hardening task.
2. **Raw pointer length validation**: All `from_raw_parts` calls should cap the length to prevent buffer over-reads from buggy callers.
3. **Concurrent close**: The race between `node_close` and operations is benign *only if* the database gracefully handles use-after-close. This should be verified or mitigated.

## Checklist Coverage

| # | Check | Result |
|---|-------|--------|
| 1 | Null pointer before dereference | GREEN — consistent pattern |
| 2 | CString ownership transfer | GREEN — correct model |
| 3 | Handle registry safety | GREEN — sound design |
| 4 | signing_private_key raw slice | MEDIUM — no length cap |
| 5 | Concurrent access safety | MEDIUM — close race |
| 6 | Error path cleanup | GREEN — `?`/`try_ffi!` propagation |
| 7 | Integer overflow in handles | LOW — wrapping to 0 |
| 8 | C header consistency | GREEN — correct mappings |
| 9 | Thread safety annotations | GREEN — all state behind Arc+RwLock |
| 10 | Panic safety | **CRITICAL — no catch_unwind** |

## Recommendations for Session 2+

1. Audit `unsafe` blocks in non-FFI crates (crypto, storage, p2p) — any panic in these propagates through FFI
2. Audit `unwrap()` / `expect()` usage in all crates reachable from FFI call paths
3. Verify database behavior after `close()` — does it return errors or panic?
4. Check for `block_on` inside async contexts (would panic)
5. Audit WASM sandbox in lens crate (filesystem access from FFI)
