# Finding: Debug Dump Endpoint Has No NAC Permission Check

**Stream**: 02 - Access Control Policy
**Severity**: MEDIUM
**Category**: Access Control Bypass
**Status**: CONFIRMED
**Session**: S2 - NAC and Zanzibar Evaluation

## Summary

The `GET /api/v0/debug/dump` endpoint has no NAC permission check and no identity extraction. When NAC is enabled, all other endpoints require appropriate permissions, but the dump endpoint is accessible to any unauthenticated request. It returns all key-value pairs in the database, including ACP store data (relation tuples, policies).

## Affected Files

| File | Line | Issue |
|------|------|-------|
| `crates/http/src/handlers/utility.rs` | 47-54 | `dump()` — no `ExtractIdentity`, no `require_permission()` |
| `crates/http/src/router/routes.rs` | 251 | Route: `GET /api/v0/debug/dump` — no middleware |

## Details

### The Missing Check

Compare the `dump` handler with its neighbor `purge`:

```rust
// crates/http/src/handlers/utility.rs:47-54
pub async fn dump(State(state): State<AppState>) -> Result<Json<Vec<String>>, HttpError> {
    let dump_ops = state.require_dump()?;
    let lines = dump_ops.print_dump().await.map_err(HttpError::Internal)?;
    Ok(Json(lines))
}

// crates/http/src/handlers/utility.rs:63-82
pub async fn purge(
    State(state): State<AppState>,
    identity: ExtractIdentity,              // <-- has identity
) -> Result<StatusCode, HttpError> {
    require_permission(&state, &identity, NodePermission::DocumentUpdate).await?;  // <-- has NAC check
    // ...
}
```

The `dump` handler:
1. Does not accept `ExtractIdentity`
2. Does not call `require_permission()`
3. Does not check for dev mode
4. Returns all database key-value pairs as raw strings

### Relationship to Finding 01

Finding 01 (`01-dump-bypasses-acp.md`) identified that `print_dump()` iterates ALL keys including AcpStore namespace. This finding adds that the HTTP endpoint also lacks NAC enforcement — meaning even when NAC is configured, the dump endpoint remains open.

### What's Exposed via Dump

The dump output includes:
- ACP relation tuples (who has access to what documents)
- ACP policies (including policy structure)
- NAC relationships (admin identities, disabled state)
- Document data and metadata
- Schema definitions

### Attack Scenario

```bash
# NAC is enabled, attacker has no identity
curl http://node:9181/api/v0/debug/dump
# → 200 OK with all database contents

# Extract admin DIDs from NAC tuples
# Extract document data from collection keys
# Map out the complete permission graph
```

### Severity Rationale

MEDIUM because:
1. The endpoint is named "debug" suggesting it's intended for development
2. But it's registered in the production router with no conditional guard
3. NAC enforcement gap — every other endpoint is gated
4. Exposes the complete database including security-sensitive ACP data
5. Amplifies findings 01, 02, 03, 04 by providing a single endpoint to dump everything

## Remediation

At minimum, add NAC enforcement:

```rust
pub async fn dump(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<Json<Vec<String>>, HttpError> {
    require_permission(&state, &identity, NodePermission::DocumentRead).await?;
    // Also consider: require dev mode
    if !state.dev_mode {
        return Err(HttpError::BadRequest("dump only available in dev mode".into()));
    }
    // ...
}
```

Better: remove from production router entirely, or gate behind a `--enable-debug-endpoints` flag.
