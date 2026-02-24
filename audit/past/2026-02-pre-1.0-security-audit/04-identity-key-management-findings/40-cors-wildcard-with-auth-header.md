# CORS Allows Wildcard Origin With Authorization Header

- **Severity**: Medium
- **Category**: Cross-Origin Security
- **Status**: Confirmed — Go Compatibility

## Summary

When `allowed_origins` contains `"*"`, the CORS layer is configured with `tower_http::cors::Any` while also allowing `Authorization` in the `allow_headers` list. Per the Fetch specification, browsers will NOT send credentials with `Access-Control-Allow-Origin: *`, so the `Authorization` header will be blocked by the browser in cross-origin requests. However, `tower_http::cors::Any` does NOT set `Access-Control-Allow-Credentials: true`, so browsers enforce this correctly.

## Affected Files

- `crates/http/src/server.rs:416-437` (build_cors_layer)

## Details

```rust
// server.rs:417-437
let cors = CorsLayer::new()
    .allow_methods([Method::GET, Method::HEAD, Method::POST, Method::PATCH, Method::DELETE])
    .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION])
    .max_age(Duration::from_secs(300));

if allow_any {
    Ok(cors.allow_origin(tower_http::cors::Any))
} else {
    let valid_origins = self.validate_cors_origins()?;
    Ok(cors.allow_origin(valid_origins))
}
```

**Analysis**:
1. `tower_http::cors::Any` sets `Access-Control-Allow-Origin: *`
2. With `*` origin, browsers disallow credentials (cookies, Authorization header) per Fetch spec
3. This means cross-origin requests with `Authorization: Bearer ...` will fail in browsers even when CORS is configured with `*`
4. Non-browser clients (curl, SDKs) ignore CORS entirely

**This is safe because**: `Any` without `allow_credentials(true)` means browsers block credential-bearing cross-origin requests. The `Authorization` header in `allow_headers` only takes effect for non-credentialed preflight requests.

**For specific origins** (non-wildcard): The code correctly validates and lowercases origins but does NOT set `allow_credentials(true)`, so even specific-origin CORS won't allow `Authorization` headers from browsers.

## Remediation

If cross-origin browser clients need to send `Authorization` headers, specific origins with `allow_credentials(true)` are required:

```rust
if !allow_any {
    cors = cors.allow_credentials(true);
}
```

However, this should only be done when explicitly needed. The current behavior is safe-by-default.

**Accept as-is for Go compatibility** — Go DefraDB has the same CORS configuration.

## Test Gap

No test verifies CORS preflight behavior with Authorization header. Consider adding integration tests for:
- Cross-origin preflight with `Authorization` header in specific-origin mode
- Cross-origin preflight with wildcard origin
