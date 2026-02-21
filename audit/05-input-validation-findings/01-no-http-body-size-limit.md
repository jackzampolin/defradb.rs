# Finding: No Explicit HTTP Request Body Size Limit

**Stream**: 05 - Input Validation
**Severity**: HIGH
**Category**: Denial of Service
**Status**: CONFIRMED

## Summary

The HTTP server does not configure an explicit request body size limit. While Axum has a default 2MB limit on its `Json` extractor, this is not explicitly set, is framework-version-dependent, and 2MB is very large for a database API.

## Affected Files

| File | Function | Issue |
|------|----------|-------|
| `crates/http/src/server.rs:343-404` | `Server::router()` | No `DefaultBodyLimit` layer |
| `crates/http/src/server.rs:403` | Layer stack | Only `TraceLayer` and `CorsLayer` applied |

## Details

### Current Router Configuration

```rust
pub fn router(&self) -> Result<Router> {
    let cors = self.build_cors_layer()?;
    // ... build state ...
    let router = create_router_with_state(state);
    Ok(router.layer(TraceLayer::new_for_http()).layer(cors))
    //        ^^^ No DefaultBodyLimit layer
}
```

The router is built with only tracing and CORS middleware. No body size limit is configured.

### Axum Default Behavior

Axum's `Json` extractor has a default limit of 2MB. However:
1. This is not documented as a stability guarantee
2. 2MB is extremely large for a GraphQL query or schema SDL
3. Other extractors (`String`, `Bytes`) have no default limit
4. The `POST /api/v0/schema` endpoint accepts raw text (SDL) with no size limit
5. The `POST /api/v0/backup/import` endpoint has its own 100MB limit but uses Axum's `Json` extractor

### Endpoints at Risk

| Endpoint | Extractor | Risk |
|----------|-----------|------|
| `POST /api/v0/graphql` | `Json<QueryRequest>` | 2MB default (still large) |
| `POST /api/v0/schema` | `String` or `Json` | Potentially unlimited |
| `POST /api/v0/lens/set` | `Json` | 2MB default |
| `POST /api/v0/backup/import` | `Json` | 100MB custom limit |
| `POST /api/v0/collections/{name}` | `Json` | 2MB default |
| `PATCH /api/v0/collections/{name}/{id}` | `Json` | 2MB default |
| `POST /api/v0/acp/policy` | `Json` | 2MB default |

## Impact

An attacker can send large request bodies to consume server memory and bandwidth. Combined with the GraphQL depth/complexity issue (finding 00), this amplifies the DoS potential.

The schema endpoint is particularly concerning - a malicious SDL with thousands of type definitions could consume significant memory during parsing.

## Remediation

Add explicit body size limit to the router:

```rust
use axum::extract::DefaultBodyLimit;

Ok(router
    .layer(DefaultBodyLimit::max(256 * 1024)) // 256KB global default
    .layer(TraceLayer::new_for_http())
    .layer(cors))
```

For endpoints that legitimately need larger bodies (backup import), use per-route overrides:

```rust
.route("/backup/import", post(import).layer(DefaultBodyLimit::max(100 * 1024 * 1024)))
```

### Recommended Limits

| Endpoint Category | Suggested Limit |
|-------------------|-----------------|
| GraphQL queries | 256 KB |
| Schema SDL | 1 MB |
| Document CRUD | 256 KB |
| Backup import | 100 MB (already partially enforced) |
| ACP policies | 64 KB |
| Default | 256 KB |
