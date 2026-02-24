# Finding: GraphQL Endpoint Bypasses NAC Permission Checks

**Stream**: 02 - Access Control Policy
**Severity**: HIGH
**Category**: Access Control Bypass
**Status**: CONFIRMED
**Session**: S2 - NAC and Zanzibar Evaluation

## Summary

The GraphQL handlers (`graphql`, `graphql_get`, `graphql_transactional`) do not call `require_permission()` to enforce NAC. When NAC is enabled, all REST endpoints gate operations behind `NodePermission` checks (e.g., `DocumentRead`, `DocumentUpdate`, `DocumentDelete`), but GraphQL bypasses this entirely. An attacker denied access via REST can perform the same operations through `/api/v0/graphql`.

## Affected Files

| File | Line | Issue |
|------|------|-------|
| `crates/http/src/handlers/graphql/query.rs` | 96-111 | `graphql()` — no `require_permission()` call |
| `crates/http/src/handlers/graphql/query.rs` | 128-162 | `graphql_get()` — no `require_permission()` call |
| `crates/http/src/handlers/graphql/query.rs` | 199-262 | `graphql_transactional()` — no `require_permission()` call |

## Details

### The Bypass

Every REST handler enforces NAC:

```rust
// crates/http/src/handlers/documents.rs:36
require_permission(&state, &identity, NodePermission::DocumentRead).await?;
```

But the GraphQL handlers skip this entirely:

```rust
// crates/http/src/handlers/graphql/query.rs:96-111
pub async fn graphql(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(mut request): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, HttpError> {
    check_encrypted_fields(&state, &request.query)?;
    request.identity = identity.did().cloned();
    let response = execute_with_context(&state, &identity, request).await;
    // ... no require_permission() anywhere
    Ok(Json(response))
}
```

The handler's doc comments describe the intended NAC permission model:

```
/// - Query operations require `DocumentRead` permission
/// - Delete mutations require `DocumentDelete` permission
/// - Other mutations require `DocumentUpdate` permission
```

But this is aspirational — the code never implements these checks.

### What `execute_with_context()` Does and Doesn't Do

`execute_with_context()` (in `query_context.rs`) resolves:
- `signing_config` — for document signing
- `dac_bypass` — whether the identity has `DacBypass` permission (for document-level ACP)

It does NOT check NAC permissions for the operation type. DAC bypass is about skipping *document-level* access control, not *node-level* operation gating.

### Attack Scenario

1. NAC is enabled with an owner. Non-admin identity `did:key:attacker...` has no permissions.
2. REST `GET /api/v0/collections/Users/doc123` → 401 Unauthorized (NAC blocks `DocumentRead`)
3. GraphQL `POST /api/v0/graphql` with `{ query: "{ Users { name email } }" }` → **200 OK with data**
4. GraphQL mutations also bypass: `mutation { update_Users(docID: "...", input: {...}) { ... } }` → succeeds

### Contrast with REST

All 80+ `require_permission()` calls across REST handlers demonstrate the intended NAC enforcement pattern. The GraphQL handler is the sole exception, creating a complete bypass.

### Scope of Bypass

Through GraphQL, an unauthorized identity can:
- Read all documents (`DocumentRead`)
- Update any document (`DocumentUpdate`)
- Delete any document (`DocumentDelete`)
- Execute schema introspection (`CollectionGet`)

This effectively negates the entire NAC subsystem since GraphQL is the primary query interface.

### Severity Rationale

HIGH because:
1. Complete bypass of all NAC permission checks via the primary query interface
2. Affects all 48 node permissions routed through GraphQL operations
3. NAC admin configuration gives false sense of security — restrictions only apply to REST
4. No indication to administrators that GraphQL is unprotected

## Remediation

Parse the GraphQL operation type before execution and call `require_permission()` with the appropriate `NodePermission`:
- Query → `DocumentRead`
- Mutation (update) → `DocumentUpdate`
- Mutation (delete) → `DocumentDelete`
- Subscription → `DocumentRead`

The `parse_request()` function from the `query` crate already determines operation type — use it before executing.

## Test Gap

No integration test verifies NAC enforcement on GraphQL endpoints. Should add a test where:
1. Enable NAC with owner identity
2. Attempt a GraphQL query as a non-admin identity
3. Verify 401 Unauthorized
