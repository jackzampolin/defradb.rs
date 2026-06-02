//! Request-scoped acting-identity middleware.
//!
//! REST handlers call the DB directly without setting an ambient identity,
//! so DB-layer NAC checks would resolve to the wildcard and wrongly deny
//! authorized admins. This middleware binds the caller's DID to the request
//! task (via a `task_local` that survives `.await`) so those checks can
//! resolve who is acting. It does not enforce anything: extraction errors
//! become `None`, and handlers keep their own permission checks.

use axum::{extract::Request, middleware::Next, response::Response};

use crate::identity_extractor::parse_identity_from_headers;

/// Bind the caller's DID to the request task for DB-layer NAC checks.
///
/// Malformed or missing identity resolves to `None`; authorization remains
/// the responsibility of the per-route auth middleware and handlers.
pub async fn scope_identity(req: Request, next: Next) -> Response {
    let did = parse_identity_from_headers(req.headers())
        .ok()
        .flatten()
        .map(|d| d.to_string());
    defra_core::current_identity::with_scoped_identity(did, next.run(req)).await
}
