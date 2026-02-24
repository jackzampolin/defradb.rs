# Finding: Database Dump Bypasses ACP and NAC Entirely

**Stream**: 02 - Access Control Policy
**Severity**: HIGH (upgraded from MEDIUM — HTTP-exposed, no auth)
**Category**: Access Control Bypass
**Status**: CONFIRMED — HTTP ENDPOINT FULLY UNAUTHENTICATED

## Summary

The `print_dump()` function iterates all database namespaces directly from the storage layer, completely bypassing the query engine and ACP permission filters. This includes the ACP store itself (revealing who has access to what), blockstore, headstore, encryption store, and all document data.

**Deep-dive confirms**: The dump is exposed via `GET /api/v0/debug/dump` with **no authentication, no identity check, and no NAC permission check**. Any network client that can reach the HTTP API can enumerate all database keys and value sizes.

## Affected Files

| File | Function | Issue |
|------|----------|-------|
| `crates/db/src/dump.rs:11-59` | `print_dump()` | Direct storage iteration, no ACP |
| `crates/http/src/handlers/utility.rs:47-54` | `dump()` | **No identity extraction, no NAC check** |
| `crates/http/src/router/routes.rs:251` | route | `GET /api/v0/debug/dump` exposed |
| `crates/cli/src/commands/server_dump.rs:14-77` | CLI | Local-only, lower risk |
| `crates/db/src/backup/export.rs:21-250` | `export_database()` | Uses `runner.execute()` — ACP applied (GOOD) |

## Details

### The HTTP Handler (No Auth)

```rust
// crates/http/src/handlers/utility.rs:47-54
/// GET /api/v0/debug/dump
///
/// Dumps all database key/value pairs for debugging.
pub async fn dump(State(state): State<AppState>) -> Result<Json<Vec<String>>, HttpError> {
    let dump_ops = state.require_dump()?;
    let lines = dump_ops.print_dump().await.map_err(HttpError::Internal)?;
    Ok(Json(lines))
}
```

Compare with adjacent handlers that DO check permissions:

```rust
// purge() - has auth
pub async fn purge(
    State(state): State<AppState>,
    identity: ExtractIdentity,          // ← extracts caller identity
) -> Result<StatusCode, HttpError> {
    require_permission(&state, &identity, NodePermission::DocumentUpdate).await?;  // ← NAC check
    // ...
}

// get_node_identity() - has auth
pub async fn get_node_identity(
    State(state): State<AppState>,
    identity: ExtractIdentity,          // ← extracts caller identity
) -> Result<Json<NodeIdentityResponse>, HttpError> {
    require_permission(&state, &identity, NodePermission::P2pPeerConnect).await?;  // ← NAC check
    // ...
}
```

The `dump()` handler has neither `ExtractIdentity` nor `require_permission()`.

### The Storage Bypass

```rust
// crates/db/src/dump.rs:20-28
let namespaces = [
    Namespace::Datastore,    // All document data
    Namespace::Blockstore,   // All IPLD blocks
    Namespace::Headstore,    // DAG heads
    Namespace::Systemstore,  // System metadata
    Namespace::Peerstore,    // Peer information
    Namespace::Encstore,     // Encryption data
    Namespace::Acpstore,     // ACP POLICIES AND RELATIONS
];
```

### What's Exposed

The dump output reveals:
- **All document keys** (containing collection IDs and document IDs) — confirms document existence
- **Value sizes** for all documents — leaks document size information
- **ACP store contents** — policy definitions and relation tuples (who has access to what)
- **Encryption store contents** — encrypted field metadata and key references
- **Peer store contents** — peer identity mappings
- **System store contents** — collection schemas and configuration

While raw values are not returned (only key names and sizes), the key names themselves contain structured information that reveals the full database topology.

### Severity Upgrade Justification

The original finding rated this MEDIUM with a note to "check if NAC gates this." Deep-dive confirms:

1. **HTTP-exposed**: `GET /api/v0/debug/dump` is registered on the same router as all other API endpoints
2. **No auth whatsoever**: No `ExtractIdentity`, no `require_permission()`, no dev-mode check
3. **Always available**: The endpoint is registered unconditionally (unlike purge which requires dev mode)
4. **Cross-reference with Finding 16**: Finding 16 already documented the missing NAC check — this finding adds the full ACP bypass analysis and HTTP exposure evidence

### Contrast with Export

`export_database()` correctly uses the query executor:
```rust
let request = query::QueryRequest::new(query);
let response = runner.execute(request).await;
```

This goes through the normal query path which applies `PermissionFilterNode`. Export is properly gated.

### CLI Path (Lower Risk)

`server_dump.rs` opens the database directly from disk and calls `print_dump()`. This requires local filesystem access to the data directory, so it's equivalent to reading the files directly. The CLI path is appropriately lower risk.

## Remediation

### Option A: Gate behind NAC admin permission + dev mode (recommended)

```rust
pub async fn dump(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<Json<Vec<String>>, HttpError> {
    require_permission(&state, &identity, NodePermission::DocumentUpdate).await?;

    if !state.dev_mode {
        return Err(HttpError::BadRequest(
            "dump is only available in development mode".into(),
        ));
    }

    let dump_ops = state.require_dump()?;
    let lines = dump_ops.print_dump().await.map_err(HttpError::Internal)?;
    Ok(Json(lines))
}
```

### Option B: Remove HTTP endpoint entirely

Dump is a debugging tool. Restrict it to CLI-only access via `defradb server-dump`, which already requires local filesystem access.

## Test Coverage

No integration test verifies that dump respects ACP or NAC boundaries. Needed tests:
1. Call `GET /api/v0/debug/dump` without authentication → should be rejected
2. Call `GET /api/v0/debug/dump` with NAC enabled but no admin permission → should be rejected
3. Call `GET /api/v0/debug/dump` in non-dev mode → should be rejected
