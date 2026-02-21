# Multiple Authorization Headers: First Value Used

- **Severity**: Low
- **Category**: HTTP Authentication
- **Status**: Confirmed — Framework Behavior

## Summary

When multiple `Authorization` headers are present in a request, `parts.headers.get(AUTHORIZATION)` returns only the first value. This is the standard `http` crate behavior for `HeaderMap::get()`. Subsequent `Authorization` headers are silently ignored.

## Affected Files

- `crates/http/src/identity_extractor.rs:200` (`parts.headers.get(AUTHORIZATION)`)

## Details

```rust
// identity_extractor.rs:199-208
let auth_result: Result<Option<String>, IdentityExtractionError> =
    match parts.headers.get(AUTHORIZATION) {  // ← gets first value only
        Some(value) => match value.to_str() {
            Ok(s) => Ok(Some(s.to_string())),
            // ...
        },
        None => Ok(None),
    };
```

**Scenario**: A request with:
```
Authorization: Bearer valid-token-for-alice
Authorization: Bearer valid-token-for-bob
```

Only Alice's token is used. Bob's is silently ignored.

**Practical impact**: Very low. Multiple Authorization headers are rare and usually indicate misconfigured proxies. RFC 7230 says the `Authorization` header is a singleton (not comma-delimited), so multiple values shouldn't occur in well-formed requests.

If strict enforcement is desired, `parts.headers.get_all(AUTHORIZATION)` could check for multiple values and reject.

## Remediation

**Accept as-is.** This matches standard HTTP library behavior and Go DefraDB's behavior (Go's `req.Header.Get()` also returns only the first value).

Optionally, detect and warn on multiple Authorization headers:
```rust
if parts.headers.get_all(AUTHORIZATION).iter().count() > 1 {
    tracing::warn!("Multiple Authorization headers detected; using first");
}
```

## Test Gap

No test for duplicate Authorization headers. Low priority.
