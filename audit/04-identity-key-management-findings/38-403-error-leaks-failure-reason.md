# 403 Error Response Leaks Authentication Failure Reason

- **Severity**: Low
- **Category**: Information Disclosure
- **Status**: Confirmed

## Summary

The identity extractor returns detailed error messages in 403 responses that distinguish between different token failure modes. This could help an attacker iterate on token forgery by providing specific feedback about what went wrong.

## Affected Files

- `crates/http/src/identity_extractor.rs:104-123` (IntoResponse for IdentityExtractionError)

## Details

Three distinct error message patterns reveal the failure mode:

```rust
// Line 109 — token parsing failed
format!("Invalid token: {}", msg)
// e.g. "Invalid token: invalid JWT structure"

// Line 113-114 — token valid but verification failed
format!("Token verification failed: {}", msg)
// e.g. "Token verification failed: audience mismatch: expected localhost:9181"

// Line 118 — host header issue
format!("Host header error: {}", msg)
```

An attacker can distinguish:
1. **Token is malformed** → "Invalid token: ..."
2. **Token is well-formed but expired/wrong audience** → "Token verification failed: audience mismatch: expected `localhost:9181`"
3. **Token is valid but Host header is missing** → "Host header error: ..."

The audience mismatch message is particularly concerning as it reveals the expected audience value (the Host the server sees), which could help in crafting a token for a specific target.

## Remediation

Use a generic error message for all auth failures while preserving details in logs:

```rust
fn into_response(self) -> Response {
    // Log detailed error for debugging
    tracing::debug!(error = ?self, "Identity extraction failed");

    // Return generic 403 to client
    let message = "authentication failed".to_string();
    (StatusCode::FORBIDDEN, Json(ErrorResponse { error: message })).into_response()
}
```

**Note**: Check Go DefraDB behavior first — if Go returns similar detailed messages, this may be intentional for compatibility.

## Test Gap

No test verifies the exact error response body content. Tests only check for `IdentityExtractionError` variant matching.
