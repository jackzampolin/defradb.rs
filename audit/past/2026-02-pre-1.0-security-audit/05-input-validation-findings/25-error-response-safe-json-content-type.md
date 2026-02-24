# Error Responses Safe — JSON Content-Type Prevents XSS

**Severity**: INFO (GREEN)
**Category**: Input Validation — Response Safety
**Status**: Confirmed Safe

## Summary

All HTTP error responses use `Content-Type: application/json` via Axum's `Json()` extractor. Even though user input is reflected in some error messages (see finding #20), XSS is not possible because browsers do not execute JavaScript in JSON responses. CRLF header injection is also prevented by Rust's string handling and Axum's `HeaderValue` validation.

## Affected Files

- `crates/http/src/error.rs:54-69` — `IntoResponse` implementation for `HttpError`

## Details

### Response Format

```rust
// error.rs:48-69
#[derive(Debug, Clone, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let (status, message) = match &self { ... };
        (status, Json(ErrorResponse { error: message })).into_response()
    }
}
```

Every error response is serialized as `{"error": "..."}` with `Content-Type: application/json`. This is the safe pattern.

### CRLF Injection Protection

Rust's `format!()` macro treats `\r\n` as literal characters (backslash-r, backslash-n), not as carriage return and line feed. Even if an attacker includes `\r\nSet-Cookie: admin=true` in input, it appears as literal text in the JSON string, not as HTTP headers.

Axum's `HeaderValue::from_str()` also rejects values containing control characters (bytes 0x00-0x1F except horizontal tab).

### Positive Findings

- All 9 `HttpError` variants return JSON
- No endpoint returns `text/html` error responses
- The backup export handler explicitly sets `Content-Type: application/json` on success responses
- The GraphQL error format (`QueryResponse` with `errors` array) is also JSON

## Test Gap

Consider adding a test that verifies error responses have `Content-Type: application/json`:

```rust
#[test]
fn test_error_content_type() {
    let error = HttpError::BadRequest("test".into());
    let response = error.into_response();
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/json"
    );
}
```
