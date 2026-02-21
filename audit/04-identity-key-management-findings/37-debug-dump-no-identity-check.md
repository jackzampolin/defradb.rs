# Debug Dump Endpoint Has No Identity or NAC Check

- **Severity**: High
- **Category**: Access Control
- **Status**: Confirmed

## Summary

The `GET /api/v0/debug/dump` endpoint does not extract identity and does not perform any NAC permission check. Any unauthenticated client with network access can dump all database key/value pairs, potentially exposing sensitive document data, ACP policies, and internal state.

## Affected Files

- `crates/http/src/handlers/utility.rs:50-54` (dump handler)
- `crates/http/src/router/routes.rs:251` (route registration)

## Details

```rust
// utility.rs:50-54
pub async fn dump(State(state): State<AppState>) -> Result<Json<Vec<String>>, HttpError> {
    let dump_ops = state.require_dump()?;
    let lines = dump_ops.print_dump().await.map_err(HttpError::Internal)?;
    Ok(Json(lines))
}
```

Compare with `purge` (utility.rs:63-82) which correctly extracts identity and requires `DocumentUpdate` permission:

```rust
pub async fn purge(
    State(state): State<AppState>,
    identity: ExtractIdentity,        // ← extracted
) -> Result<StatusCode, HttpError> {
    require_permission(&state, &identity, NodePermission::DocumentUpdate).await?;
    // ...
}
```

The dump endpoint is always available, not gated behind `dev_mode` like purge.

**This finding was previously identified in audit session 2 (finding 02-16), but the code has not been remediated.**

## Remediation

Add identity extraction and NAC permission check:

```rust
pub async fn dump(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<Json<Vec<String>>, HttpError> {
    require_permission(&state, &identity, NodePermission::DocumentRead).await?;
    let dump_ops = state.require_dump()?;
    let lines = dump_ops.print_dump().await.map_err(HttpError::Internal)?;
    Ok(Json(lines))
}
```

Also consider gating behind `dev_mode` like purge.

## Test Gap

No test exists for dump endpoint authentication. Need:
- Test that dump requires identity when NAC is enabled
- Test that dump is rejected without authentication when NAC is enabled
