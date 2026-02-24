# Content-Type Not Enforced on Schema Endpoint

**Severity**: LOW
**Category**: Input Validation — HTTP Protocol
**Status**: Confirmed

## Summary

The schema endpoint (`POST /api/v0/schema`) accepts a raw `String` body without enforcing `Content-Type`. Clients can submit SDL with any Content-Type header (or none). Similarly, the lens endpoint (`POST /api/v0/lens/set`) accepts arbitrary Content-Type. While this doesn't create a direct vulnerability, it violates defense-in-depth and could lead to Content-Type confusion in proxies or WAFs.

## Affected Files

- `crates/http/src/handlers/schema.rs:24-27` — `add_schema()` accepts `body: String`
- `crates/http/src/handlers/lens.rs:48` — lens endpoint accepts `body: String`

## Details

### Schema Endpoint

```rust
// schema.rs:24-27
pub async fn add_schema(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    body: String,  // No Content-Type enforcement
) -> Result<Json<Vec<CollectionVersion>>, HttpError>
```

Axum's `String` extractor reads the request body as UTF-8 text regardless of Content-Type. An attacker could send:

- `Content-Type: text/html` — accepted
- `Content-Type: application/xml` — accepted
- `Content-Type: multipart/form-data` — accepted (body parsed as raw text)
- No Content-Type header — accepted

### Security Assessment

**Risk is LOW** because:
1. The body is always parsed as SDL text regardless of Content-Type
2. No downstream processing changes based on Content-Type
3. Response Content-Type is always `application/json` (via `Json()` extractor)
4. This matches Go DefraDB behavior

**Risk would increase if**:
- A WAF or reverse proxy makes routing decisions based on Content-Type
- The body were parsed differently based on Content-Type

## Remediation

Add Content-Type validation:

```rust
use axum::http::header;

pub async fn add_schema(
    headers: HeaderMap,
    body: String,
) -> Result<Json<Vec<CollectionVersion>>, HttpError> {
    if let Some(ct) = headers.get(header::CONTENT_TYPE) {
        let ct_str = ct.to_str().unwrap_or("");
        if !ct_str.starts_with("text/plain") && !ct_str.starts_with("application/graphql") {
            return Err(HttpError::BadRequest("Expected text/plain or application/graphql".into()));
        }
    }
    // ...
}
```

## Test Gap

No test verifies Content-Type enforcement on the schema endpoint.
