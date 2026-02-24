# Finding: View Plans Don't Apply View-Collection ACP Policy

**Stream**: 02 - Access Control Policy
**Severity**: LOW
**Category**: Incomplete Enforcement
**Status**: CONFIRMED
**Session**: S1 - DAC Implementation Review

## Summary

When a view collection has its own ACP policy, the `build_view_plan()` function does not call `maybe_wrap_with_acp_filter()` for the view collection. The source collection's ACP is correctly enforced (via recursive plan building), but any policy attached to the view collection itself is silently ignored.

## Affected Files

| File | Line | Issue |
|------|------|-------|
| `crates/query/src/planner/view_builder.rs` | 55-265 | `build_view_plan()` never calls `maybe_wrap_with_acp_filter()` |
| `crates/query/src/planner/builder/mod.rs` | 186-188 | Early return to `build_view_plan()` skips line 519 ACP insertion |

## Details

### The Missing Call

In `plan_with_index_info()` (builder/mod.rs), the normal flow inserts ACP at line 519:

```rust
plan = self.maybe_wrap_with_acp_filter(plan, &collection);
```

But for views, the function early-returns at line 186-188:

```rust
if let Some(ref query_source) = collection.query {
    return self.build_view_plan(select, &collection, query_source);
}
```

And `build_view_plan()` never calls `maybe_wrap_with_acp_filter()` for the view's own `collection`.

### Source Collection ACP Is Applied

The source collection plan is built via `self.plan_with_index_info(&source_select)` at line 183 of `view_builder.rs`. This recursive call DOES apply ACP on the source collection correctly. So the underlying data is protected.

### Severity Rationale

LOW because:
1. Views are derived from source collections — the source's ACP is enforced
2. Views with their own separate ACP policy is an unusual/unlikely configuration
3. The source data protection provides defense in depth
4. This may be by design — views inherit their source's access control

## Remediation

### Option A: Add ACP wrapping to view plan (if views should have own policies)

In `build_view_plan()`, after building the view plan, wrap it with:

```rust
let plan = self.maybe_wrap_with_acp_filter(plan, collection);
```

### Option B: Document that views inherit source ACP only (if by design)

If views are not intended to have their own ACP policies, enforce this at the policy-attachment layer: reject attempts to attach policies to view collections.

## Test Gap

No test verifies ACP behavior for views with their own policy.
