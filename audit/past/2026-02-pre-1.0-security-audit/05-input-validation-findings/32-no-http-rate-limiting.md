# 32: No HTTP Rate Limiting, Request Timeout, or Connection Limits

| Field    | Value |
|----------|-------|
| Severity | MEDIUM |
| Category | Denial of Service |
| Status   | Confirmed |

## Summary

The HTTP server in `crates/http/src/server.rs` has no rate limiting middleware, no per-request timeout, and no concurrent connection limit. An attacker can exhaust node resources by sending high-volume requests to expensive endpoints (GraphQL queries, schema mutations, backup exports) or by opening thousands of idle TCP connections to exhaust file descriptors.

## Affected Files

- `crates/http/src/server.rs` — Server construction and `axum::serve()` call
- `crates/http/src/router/routes.rs` — All routes registered without middleware
- `crates/http/src/nac_guard.rs` — Permission checks but no rate checks

## Details

### No Rate Limiting

The HTTP router is constructed with only two middleware layers:

```rust
Ok(router.layer(TraceLayer::new_for_http()).layer(cors))
```

No `tower::limit::RateLimitLayer`, `tower::limit::ConcurrencyLimitLayer`, or equivalent middleware is applied. Grep for `rate.*limit|throttle|RateLimit|tower.*limit` found zero matches in the HTTP crate.

### No Request Timeout

There is no `tower::timeout::TimeoutLayer` or `axum::middleware::from_fn()` timeout wrapper. The `axum::serve()` call uses default Hyper settings:

```rust
axum::serve(listener, router).await.map_err(|e| ...)?;
```

Hyper/Axum do not set a default request processing timeout. Only TCP-level keepalive/idle timeouts apply. A slow query (deeply nested GraphQL, full collection scan) holds its connection and tokio task indefinitely.

### No Connection Limits

`axum::serve()` accepts connections without limit. There is no `tower::limit::ConcurrencyLimitLayer` on the listener. An attacker can open thousands of TCP connections to exhaust file descriptors, preventing legitimate clients from connecting.

### NAC Guard: Permission, Not Rate

The NAC guard (`nac_guard.rs`) checks identity permissions (authorization) but performs no rate-based enforcement:

```rust
pub async fn require_permission(
    state: &AppState,
    identity: &ExtractIdentity,
    permission: NodePermission,
) -> Result<(), HttpError> {
    // ... checks permission, not rate ...
}
```

### Most Expensive Endpoints (Attack Surface)

An attacker can spam these endpoints to exhaust resources:

| Endpoint | Method | Resource Cost | Impact |
|----------|--------|---------------|--------|
| `/api/v0/graphql` | POST | CPU: parsing, planning, execution | CPU exhaustion from complex queries |
| `/api/v0/schema` | POST | Write: storage, index creation | Write amplification, I/O saturation |
| `/api/v0/backup/export` | POST | Memory: full DB serialization | OOM from concurrent exports |
| `/api/v0/debug/dump` | GET | Memory: full DB serialization | OOM from concurrent dumps |
| `/api/v0/p2p/peers` | POST | Network: outbound connection | SSRF, connection flooding |
| `/api/v0/collections/:name` | POST | Write: document creation | Storage exhaustion |
| `/api/v0/lens/set` | POST | CPU: WASM compilation | CPU exhaustion from large modules |
| `/api/v0/purge` | POST | Write: delete all data | Data destruction (requires dev mode) |

### GraphQL-Specific Amplification

Combined with the existing findings (00: no depth/complexity limits, 02: unbounded filter recursion, 04: fragment width amplification), the absence of rate limiting means an attacker can send a high volume of already-expensive queries. A single complex query can be CPU-intensive; at high volume, this overwhelms the node.

### SSE Subscription Exhaustion

The `/api/v0/graphql/ws` WebSocket endpoint (finding 06: SSE subscription no limits) combined with no connection limits means an attacker can open thousands of subscription connections, each holding a long-lived connection and consuming memory for event buffering.

## Remediation

1. **Request timeout**: Add `tower::timeout::TimeoutLayer` with a reasonable default (e.g., 60 seconds):
   ```rust
   use tower::timeout::TimeoutLayer;
   router.layer(TimeoutLayer::new(Duration::from_secs(60)))
   ```

2. **Connection limits**: Add `tower::limit::ConcurrencyLimitLayer`:
   ```rust
   router.layer(ConcurrencyLimitLayer::new(1000)) // Max 1000 concurrent requests
   ```

3. **Rate limiting**: Consider `tower::limit::RateLimitLayer` or `tower_governor` for per-IP rate limiting. This is more complex but important for public-facing deployments.

4. **Endpoint-specific limits**: Apply stricter limits to expensive endpoints (backup/export, dump, schema mutations) with lower concurrency caps.

5. **HTTP/2 stream limits**: If HTTP/2 is enabled, configure `hyper::server::conn::Http::max_concurrent_streams()`.

## Test Gap

No tests verify rate limiting behavior (expected: none exists to test). No test verifies that the server rejects connections beyond a configured limit. No test sends a slow request and verifies it is timed out.
