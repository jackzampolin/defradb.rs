# Finding: No Explicit HTTP Request Body Size Limit

**Stream**: 05 - Input Validation
**Severity**: HIGH
**Category**: Denial of Service
**Status**: CONFIRMED (deepened in Session 1)

## Summary

The HTTP server does not configure an explicit request body size limit. Axum 0.7's `Json` extractor defaults to 2MB, but this is an implicit framework default, not an intentional security boundary. Several endpoints use `String` or `Bytes` extractors that have **no default limit at all**. The only endpoint with an explicit limit is backup import (100MB, checked after full body is read).

## Affected Files

| File | Function | Issue |
|------|----------|-------|
| `crates/http/src/server.rs:343-404` | `Server::router()` | No `DefaultBodyLimit` layer in middleware stack |
| `crates/http/src/server.rs:403` | Layer stack | Only `TraceLayer` + `CorsLayer` applied |
| `crates/http/src/handlers/schema.rs:24-39` | `add_schema()` | Uses `body: String` — **no size limit** |
| `crates/http/src/handlers/backup.rs:159-163` | `import()` | Uses `body: Bytes` — checks size AFTER reading entire body |
| `crates/http/src/handlers/collections.rs:175-179` | `set_active()` | Uses `body: axum::body::Bytes` — no size limit |
| `crates/http/src/handlers/graphql/query.rs:96-111` | `graphql()` | Uses `Json<QueryRequest>` — Axum 2MB default |
| `crates/http/src/handlers/graphql/query.rs:199-262` | `graphql_transactional()` | Uses `Json<TransactionalQueryRequest>` — Axum 2MB default |
| `crates/http/src/router/routes.rs:37-258` | Route definitions | No per-route body limits configured |

## Details

### Router Middleware Stack

```rust
// server.rs:403
Ok(router.layer(TraceLayer::new_for_http()).layer(cors))
```

No `DefaultBodyLimit` middleware is applied. Grep for `DefaultBodyLimit`, `body_limit`, `content_length`, `max_body` across the entire `crates/http` directory returns **zero results**.

### Endpoint-by-Endpoint Analysis

| Endpoint | Method | Extractor | Effective Limit | Notes |
|----------|--------|-----------|-----------------|-------|
| `/api/v0/graphql` | POST | `Json<TransactionalQueryRequest>` | 2MB (Axum default) | Implicit, not explicit |
| `/api/v0/graphql` | GET | `Query<GraphqlQueryParams>` | URL length (~8KB typical) | Browser/server URL limits |
| `/api/v0/schema` | POST | `body: String` | **NONE** | SDL text, fully unbounded |
| `/api/v0/backup/import` | POST | `body: Bytes` | 100MB (checked late) | Reads entire body first, then checks `body.len()` |
| `/api/v0/backup/export` | POST | `Json<ExportRequest>` | 2MB (Axum default) | |
| `/api/v0/collections/:name` | POST | `Json<JsonValue>` | 2MB (Axum default) | Document creation |
| `/api/v0/collections/:name/:docID` | PATCH | `Json<JsonValue>` | 2MB (Axum default) | Document update |
| `/api/v0/collections` | PATCH | `Json<PatchCollectionRequest>` | 2MB (Axum default) | Schema patching |
| `/api/v0/collections/default` | POST | `body: axum::body::Bytes` | **NONE** | Version ID, but extractor is unbounded |
| `/api/v0/acp/policy` | POST | `Json` | 2MB (Axum default) | ACP policy |
| `/api/v0/lens/set` | POST | `Json` | 2MB (Axum default) | Migration config |
| `/api/v0/p2p/*` | Various | `Json` | 2MB (Axum default) | P2P management |

### Critical: Schema Endpoint (Unbounded String Body)

The `add_schema` handler at `crates/http/src/handlers/schema.rs:24-39` accepts a raw `String` body:

```rust
pub async fn add_schema(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    body: String,  // <-- No size limit! Axum's String extractor has NO default limit
) -> Result<Json<Vec<CollectionVersion>>, HttpError> {
```

An attacker can POST a multi-gigabyte SDL string to this endpoint. The `String` extractor in Axum does NOT have the same 2MB default as the `Json` extractor. It will read the entire body into memory regardless of size.

### Backup Import: Late Check

The backup import handler reads the full body into memory, then checks size:

```rust
pub async fn import(
    // ...
    body: Bytes,  // <-- Full body read into memory first
) -> Result<StatusCode, HttpError> {
    if body.len() > MAX_IMPORT_SIZE {  // 100MB check AFTER allocation
        return Err(...);
    }
```

A 100MB allocation still happens before the limit is enforced. With Axum's `Bytes` extractor having no default limit, an attacker could send a body larger than 100MB, forcing allocation of that amount before the check rejects it.

### SSE Subscription: No Connection Limit

The SSE subscription endpoint (`graphql_transactional` with `Accept: text/event-stream`) keeps a connection open indefinitely, re-executing queries on every database update event. There is no:
- Connection timeout
- Maximum subscription duration
- Maximum number of concurrent SSE connections
- Per-event output size limit

### WebSocket: Not Implemented (Safe)

The WebSocket handler returns 501 Not Implemented, so WebSocket-specific limits are not currently needed:

```rust
pub async fn graphql_ws_handler() -> impl IntoResponse {
    (StatusCode::NOT_IMPLEMENTED, "GraphQL subscriptions over WebSocket are not yet implemented")
}
```

### Chunked Transfer Encoding

Axum/Hyper handle `Transfer-Encoding: chunked` transparently. The body extractors read until the stream completes (or connection drops). Without a `DefaultBodyLimit`, chunked requests to `String`/`Bytes` endpoints could accumulate unbounded memory. However, this is bounded by TCP connection timeouts at the OS level (typically several minutes).

## Impact

1. **Schema endpoint OOM**: Single POST to `/api/v0/schema` with multi-GB body causes OOM kill
2. **Backup import allocation**: 100MB+ allocated before size check rejects
3. **Slowloris via SSE**: Many concurrent SSE subscriptions exhaust server file descriptors and memory
4. **2MB GraphQL**: While limited by Axum default, 2MB is still enough for a devastating depth/width bomb

## Remediation

### Immediate: Add DefaultBodyLimit to Router

```rust
use axum::extract::DefaultBodyLimit;

Ok(router
    .layer(DefaultBodyLimit::max(256 * 1024)) // 256KB global default
    .layer(TraceLayer::new_for_http())
    .layer(cors))
```

### Per-Route Overrides

```rust
// Backup import needs more
.route("/backup/import", post(import).layer(DefaultBodyLimit::max(100 * 1024 * 1024)))
// Schema might need 1MB for large schemas
.route("/schema", post(add_schema).layer(DefaultBodyLimit::max(1_048_576)))
```

### Recommended Limits

| Endpoint Category | Suggested Limit |
|-------------------|-----------------|
| GraphQL queries | 256 KB |
| Schema SDL | 1 MB |
| Document CRUD | 256 KB |
| Backup import | 100 MB (move check to middleware, not late body check) |
| ACP policies | 64 KB |
| Default (everything else) | 256 KB |

### SSE Subscriptions

Add connection limits:
- Maximum concurrent SSE connections per IP
- Maximum subscription duration (e.g., 1 hour)
- Idle timeout (no events for N minutes)

## Test Gap

No tests for:
- Body size rejection on any endpoint
- Schema endpoint with large SDL input
- Backup import with body > 100MB
- Concurrent SSE connection limits
- Chunked transfer encoding behavior on String/Bytes endpoints
