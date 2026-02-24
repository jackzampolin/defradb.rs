# Finding 60: Identity Propagation Through Query Pipeline Correct (Green)

**Severity**: GREEN
**Category**: Identity Integrity / Cross-Component
**Status**: Verified sound

## Summary

The identity (as a `Did` or `Option<Did>`) is propagated correctly from HTTP extraction through the query pipeline to ACP checks. The flow is: `ExtractIdentity` → handler → `caller_identity: Option<Did>` → query runner → mutation executor → ACP permission check. The identity is passed by value (cloned `Did`), preventing mutation or substitution.

## Affected Files

- `crates/http/src/identity_extractor.rs` — Extracts `Did` from JWT
- `crates/http/src/handlers/graphql/query.rs` — Passes `caller_identity` to query runner
- `crates/query/src/runner/query/select.rs:20` — Receives `caller_identity: Option<Did>`
- `crates/query/src/runner/mutation.rs:56-186` — Passes `caller_identity` to ACP checks
- `crates/query/src/executor.rs` — Orchestrates execution with identity

## Details

### Propagation chain

```
HTTP Request
  → ExtractIdentity (Axum extractor) → Option<Did>
    → Handler function receives Did
      → query_runner.execute_query(query, caller_identity)
        → execute_select_internal(select, fetcher, caller_identity.clone())
          → ACP permission_filter_node(caller_identity)
```

### Key properties

1. **No global/thread-local state**: Identity is passed as function parameters, not stored in thread-local or global state. Each request's identity is isolated.

2. **Clone semantics**: `Did` is cloned at each propagation step. There is no shared mutable reference that could be modified by another request.

3. **None = anonymous**: An anonymous request results in `caller_identity: None`, which the ACP layer handles by checking for wildcard permissions or denying access.

4. **No middleware stripping**: There is no middleware between the identity extractor and the handlers that could strip or modify the identity.

5. **Consistent pattern**: Both query and mutation paths follow the same pattern of receiving `caller_identity: Option<Did>`.

## Remediation

None required. The identity propagation is correct by construction.
