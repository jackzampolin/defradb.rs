# Identity Extraction Occurs Before Body Read

- **Severity**: Info
- **Category**: Architecture
- **Status**: Green

## Summary

Axum's `FromRequestParts` extractor runs before `FromRequest` body extractors. The identity extraction correctly processes only request headers (Authorization, Host) without touching the body. This means authentication is validated BEFORE the request body is read and parsed, preventing DoS via large unauthenticated request bodies.

## Affected Files

- `crates/http/src/identity_extractor.rs:183-249` (FromRequestParts impl)

## Details

The Axum extraction order guarantee:

```rust
pub async fn graphql(
    State(state): State<AppState>,
    identity: ExtractIdentity,           // ← FromRequestParts: runs first
    Json(mut request): Json<QueryRequest>, // ← FromRequest (body): runs second
) -> Result<Json<QueryResponse>, HttpError> {
```

`ExtractIdentity` implements `FromRequestParts<S>` (not `FromRequest<S>`), so it only accesses `Parts` (method, URI, headers, extensions). The body is not consumed.

If identity extraction fails (invalid token → 403), Axum short-circuits and never reads the request body. This prevents:
1. Large body DoS against unauthenticated endpoints
2. Body parsing cost for invalid requests
3. Potential body-related vulnerabilities from unauthenticated clients

**Exception**: The `dump` endpoint has no identity extractor, so its body (none needed) doesn't benefit from this protection. The `import` endpoint reads `Bytes` after identity check — correct.

## Remediation

No action needed. The architecture is correct.

## Test Gap

None needed — this is an Axum framework guarantee.
