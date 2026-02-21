# WebSocket Endpoint Registered Without Authentication

- **Severity**: Low
- **Category**: Access Control
- **Status**: Confirmed — Not Implemented

## Summary

The `/api/v0/graphql/ws` endpoint is registered in the router but returns `501 Not Implemented`. It uses `axum::routing::any()` which matches all HTTP methods. The handler does NOT extract identity — it returns a static response with no auth check.

## Affected Files

- `crates/http/src/router/routes.rs:214-217` (route registration)
- `crates/http/src/handlers/graphql/query.rs:348-353` (handler)

## Details

```rust
// routes.rs:214-217
.route(
    "/graphql/ws",
    axum::routing::any(handlers::graphql_ws_handler),
)

// query.rs:348-353
pub async fn graphql_ws_handler() -> impl IntoResponse {
    (
        StatusCode::NOT_IMPLEMENTED,
        "GraphQL subscriptions over WebSocket are not yet implemented",
    )
}
```

**Current risk**: None — the endpoint immediately returns 501. No data is processed.

**Future risk**: When WebSocket subscriptions are implemented, the handler must:
1. Extract identity from the initial HTTP upgrade request
2. Validate the JWT before upgrading the connection
3. Apply NAC permission checks
4. Ensure the identity is carried through the WebSocket lifetime

Since WebSocket connections cannot change `Authorization` headers after upgrade, the identity established at connection time must be enforced for all subsequent messages.

## Remediation

When implementing WebSocket subscriptions, add identity extraction to the upgrade handler. For now, the 501 response is safe.

Consider removing the route entirely until implementation to reduce attack surface:
```rust
// Only register when WebSocket support is ready
// .route("/graphql/ws", axum::routing::any(handlers::graphql_ws_handler))
```

## Test Gap

No test for WebSocket endpoint. Minimal risk since it returns 501.
