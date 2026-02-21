# Error Messages Echo User Input Unsanitized

**Severity**: LOW
**Category**: Information Disclosure — Error Handling
**Status**: Confirmed

## Summary

Multiple HTTP error paths echo user-supplied input (filepaths, collection names, field names, multiaddrs, serde error details) directly into JSON error responses. While the `Content-Type: application/json` header mitigates XSS risk, the reflected input could be used for log poisoning and provides attackers with implementation feedback.

## Affected Files

- `crates/http/src/handlers/backup.rs:188-192` — filepath echoed in error
- `crates/http/src/error.rs:76-86` — collection names, document IDs echoed
- `crates/http/src/validation.rs:56-59` — multiaddr echoed in error
- `crates/http/src/handlers/graphql/query.rs:141-143` — serde error details echoed
- `crates/query/src/query_parse/parser.rs:97-100` — directive arguments echoed

## Details

### Filepath Reflection (Most Concerning)

```rust
// backup.rs:188-192
return Err(HttpError::BadRequest(format!(
    "file-based import is not supported in HTTP mode. \
     Go DefraDB requested filepath '{}'. \
     Please send the backup data directly in the request body instead.",
    filepath  // USER INPUT — no sanitization
)));
```

An attacker submits `{"filepath": "../../etc/passwd <script>alert(1)</script>"}` and sees it reflected verbatim in the response.

### Collection Name Reflection

```rust
// error.rs:76
RestError::CollectionNotFound(name) => {
    HttpError::NotFound(format!("Collection '{}' not found", name))
}
```

Collection names are validated by `validate_identifier()` which only allows `[A-Za-z_][A-Za-z0-9_]*`, so injection via collection names in the REST API is blocked. However, the GraphQL path can echo arbitrary field names.

### Multiaddr Reflection

```rust
// validation.rs:56-59
HttpError::BadRequest(format!(
    "invalid multiaddr '{}': must start with '/' ...", address
))
```

### Serde Error Detail Leakage

```rust
// graphql/query.rs:141-143
return Ok(Json(QueryResponse::error(format!(
    "invalid JSON in 'variables' parameter: {}", e
))));
```

Serde errors can reveal expected types, field names, and parsing positions — useful for fingerprinting.

### Why XSS Is Mitigated

All error responses are returned via Axum's `Json()` extractor which sets `Content-Type: application/json`. Browsers do not execute JavaScript in JSON responses. The `ErrorResponse` struct serializes as `{"error": "..."}`, which is safe.

```rust
// error.rs:68
(status, Json(ErrorResponse { error: message })).into_response()
```

### Why CRLF Injection Is Mitigated

Rust's `format!()` macro produces a String — it does not interpret escape sequences like `\r\n` as literal control characters. Axum's `HeaderValue` type also rejects control characters.

## Remediation

1. **Remove filepath from backup error** — replace with generic message:
   ```rust
   return Err(HttpError::BadRequest(
       "file-based import is not supported in HTTP mode".to_string()
   ));
   ```

2. **Generify serde errors** — don't expose parsing details:
   ```rust
   return Ok(Json(QueryResponse::error(
       "invalid JSON in 'variables' parameter".to_string()
   )));
   ```

3. **Truncate reflected values** — if echoing is necessary, truncate to 100 chars max

## Test Gap

No test verifies that error messages don't leak implementation details. Consider adding:
- Test that serde errors are generified (don't contain "expected", "found", "at line")
- Test that filepaths are not reflected in error responses
