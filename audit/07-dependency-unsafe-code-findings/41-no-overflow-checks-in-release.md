# No Integer Overflow Checks in Release Builds

**Severity:** Medium
**Category:** Compiler hardening — Integer safety
**Status:** Yellow — Rust default but relevant for this project

## Summary

The release profile does not enable `overflow-checks = true`. In release mode, Rust integer arithmetic wraps silently on overflow (two's complement). This is defined behavior (not UB), but wrapping can cause logic bugs in CRDT priority calculations, handle counters, and batch operations.

## Affected Files

- `Cargo.toml:121-126` — `[profile.release]` section
- `crates/ffi/src/state/registry.rs` — handle counter (AtomicUsize)
- `crates/crdt/` — priority calculations in LWW and Counter CRDTs
- `crates/db/src/block_builder/` — priority fields (u64)

## Details

### Current Release Profile

```toml
[profile.release]
opt-level = 3
lto = true
codegen-units = 1
strip = true
panic = "abort"
```

Missing: `overflow-checks = true`

### Where Overflow Matters

1. **Handle counter in FFI registry** (`crates/ffi/src/state/registry.rs`): Uses `AtomicUsize` incremented per `new_node()` call. After `usize::MAX` allocations, wraps to 0, potentially reusing a handle that's still in the registry. Finding 02 in this series already flagged this.

2. **CRDT priority fields**: Priority values (u64) are incremented during document updates. While u64 overflow at 2^64 is astronomically unlikely, the correctness of CRDT merge depends on priority ordering.

3. **Batch CID counts**: `dropped_count` (u64) in `PollSubscriptionResult` could theoretically wrap.

### What Overflow Checks Do

With `overflow-checks = true`, integer arithmetic operations (`+`, `-`, `*`) panic on overflow in release mode (same as debug mode). This converts silent data corruption into a crash — fail-fast behavior.

### Performance Impact

Overflow checks add a branch after every arithmetic operation. The overhead is typically 2-5% for compute-heavy code. For a database engine that is primarily I/O-bound, the impact would be negligible.

### Positive Aspects of Current Profile

The existing profile has excellent security hardening:
- `lto = true` — Link-Time Optimization eliminates cross-module dead code
- `codegen-units = 1` — Better optimization, no inter-unit linking issues
- `strip = true` — Removes debug symbols that leak internal structure
- `panic = "abort"` — No unwinding across FFI boundaries (critical for safety)

## Remediation

Add overflow checks to the release profile:

```toml
[profile.release]
opt-level = 3
lto = true
codegen-units = 1
strip = true
panic = "abort"
overflow-checks = true
```

## Exploitability

Not directly exploitable by an external attacker. Integer overflow would cause incorrect internal state (e.g., wrong CRDT merge order) rather than memory safety violations. The `panic = "abort"` setting means overflow checks would terminate the process rather than allowing error recovery, which is the correct behavior for an integrity-critical system.
