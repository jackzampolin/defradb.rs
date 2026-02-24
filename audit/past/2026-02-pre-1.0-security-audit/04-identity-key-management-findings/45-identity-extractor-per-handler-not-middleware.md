# Identity Extraction is Per-Handler, Not Global Middleware

- **Severity**: Medium
- **Category**: Architecture
- **Status**: Confirmed — Design Trade-off

## Summary

Identity extraction uses Axum's `FromRequestParts` extractor pattern, meaning each handler must explicitly include `ExtractIdentity` in its function signature. There is no global middleware that enforces authentication. A developer adding a new endpoint can simply omit the extractor and the endpoint will accept all requests without any identity validation.

## Affected Files

- `crates/http/src/identity_extractor.rs` (extractor implementation)
- `crates/http/src/router/routes.rs` (all route registrations)
- `crates/http/src/handlers/utility.rs:50-54` (dump — missing extractor)

## Details

The Axum extractor pattern:

```rust
// Handler WITH identity extraction (correct)
pub async fn graphql(
    State(state): State<AppState>,
    identity: ExtractIdentity,      // ← must be explicitly added
    Json(request): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, HttpError> { ... }

// Handler WITHOUT identity extraction (potentially unsafe)
pub async fn dump(
    State(state): State<AppState>,  // ← no identity extractor
) -> Result<Json<Vec<String>>, HttpError> { ... }
```

**Endpoints confirmed to have identity extraction**:
- All GraphQL handlers (query, mutation, transactional, SSE)
- All collection handlers (list, get, create, update, delete)
- All document handlers (get, create, update, delete)
- Backup export/import
- P2P handlers (via NAC guard)
- Index, lens, NAC, ACP handlers
- Purge, node identity

**Endpoints confirmed MISSING identity extraction**:
- `GET /api/v0/debug/dump` — no identity, no NAC check (see finding 37)
- `GET /health-check` — intentionally unauthenticated
- `GET /api/v0/version` — intentionally unauthenticated
- `GET /api/v0/schema` — needs verification
- `ANY /api/v0/graphql/ws` — returns 501, safe

## Remediation

Consider adding a "deny by default" middleware layer that rejects requests without identity unless the route is explicitly marked as public:

```rust
// Option 1: Global middleware with allowlist
let public_routes = ["/health-check", "/api/v0/version"];
router.layer(RequireIdentityLayer::new(public_routes))

// Option 2: Compile-time enforcement via type system
// Wrap handlers in a type that requires ExtractIdentity
```

Alternatively, add an audit test that scans all registered routes and verifies each has an identity extractor.

## Test Gap

No automated test verifies that all endpoints include identity extraction. This should be added as a compile-time or integration test.
