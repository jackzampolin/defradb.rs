//! Global auth middleware for consistent access control enforcement.
//!
//! Intercepts every matched request, parses the identity from the
//! Authorization header, and enforces the route's permission requirement
//! before the handler runs. This is Phase 1 — existing per-handler
//! `require_permission()` calls remain as a redundant safety net.

use axum::{
    extract::{MatchedPath, Request, State},
    middleware::Next,
    response::{IntoResponse, Response},
};

use crate::identity_extractor::{parse_identity_from_headers, ExtractIdentity, MiddlewareIdentity};
use crate::nac_guard::require_permission;
use crate::route_permissions::{route_permission, RoutePermission};
use crate::router::AppState;

/// Auth middleware that enforces route-level permissions.
///
/// Applied via `Router::route_layer()` so it runs after routing
/// (MatchedPath is available) but before the handler.
pub async fn auth_middleware(
    State(state): State<AppState>,
    matched_path: Option<MatchedPath>,
    mut request: Request,
    next: Next,
) -> Response {
    let method = request.method().clone();
    let path = matched_path
        .as_ref()
        .map(|mp| mp.as_str())
        .unwrap_or_else(|| {
            tracing::warn!("MatchedPath unavailable in auth middleware — allowing request");
            ""
        });

    let permission = route_permission(path, &method);

    match permission {
        RoutePermission::Exempt => {
            return next.run(request).await;
        }
        RoutePermission::IdentityOnly => match parse_identity_from_headers(request.headers()) {
            Ok(did) => {
                request.extensions_mut().insert(MiddlewareIdentity(did));
            }
            Err(err) => return err.into_response(),
        },
        RoutePermission::Required(perm) => match parse_identity_from_headers(request.headers()) {
            Ok(did) => {
                let identity = ExtractIdentity::from_did(did.clone());
                if let Err(err) = require_permission(&state, &identity, perm).await {
                    return err.into_response();
                }
                request.extensions_mut().insert(MiddlewareIdentity(did));
            }
            Err(err) => return err.into_response(),
        },
        RoutePermission::Dynamic => match parse_identity_from_headers(request.headers()) {
            Ok(did) => {
                request.extensions_mut().insert(MiddlewareIdentity(did));
            }
            Err(err) => return err.into_response(),
        },
    }

    next.run(request).await
}
