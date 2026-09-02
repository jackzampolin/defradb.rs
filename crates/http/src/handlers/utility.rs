//! Utility endpoint handlers.
//!
//! These handlers provide HTTP access to utility operations:
//! - Database purge
//! - Node identity retrieval
//!
//! These endpoints match Go DefraDB's utility endpoints for compatibility.

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::Serialize;
use serde_json::{Map, Value};

use crate::error::{http_error_from_backend_message, HttpError};
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AppState, NodePermission};

/// Response for node identity endpoint (Go-compatible).
#[derive(Debug, Clone, Serialize)]
pub struct NodeIdentityResponse {
    /// The node's peer ID (if P2P is enabled).
    #[serde(rename = "PeerID", skip_serializing_if = "Option::is_none")]
    pub peer_id: Option<String>,
    /// DID used by the node to sign database mutations, when configured.
    #[serde(rename = "DID", skip_serializing_if = "Option::is_none")]
    pub did: Option<String>,
}

/// GET /api/v0/node/options
pub async fn get_node_options(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<Json<Map<String, Value>>, HttpError> {
    require_permission(&state, &identity, NodePermission::P2pPeerInfo).await?;
    let options = state
        .node_options
        .as_ref()
        .ok_or_else(|| HttpError::NotFound("node options not available".into()))?;
    Ok(Json((**options).clone()))
}

/// GET /api/v0/node/identity
///
/// Returns the node's identity information including peer ID.
///
/// Requires `P2pPeerConnect` permission when NAC is enabled.
pub async fn get_node_identity(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<Json<NodeIdentityResponse>, HttpError> {
    require_permission(&state, &identity, NodePermission::P2pPeerConnect).await?;

    let peer_id = if let Some(ref p2p) = state.p2p {
        p2p.local_peer_id().await.ok()
    } else {
        None
    };

    Ok(Json(NodeIdentityResponse {
        peer_id,
        did: state.node_identity_did.clone(),
    }))
}

/// GET /api/v0/debug/dump
///
/// Dumps all database key/value pairs for debugging.
///
/// Only available in development mode. Requires `DocumentRead` permission when NAC is enabled.
pub async fn dump(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<Json<Vec<String>>, HttpError> {
    require_permission(&state, &identity, NodePermission::DocumentRead).await?;

    if !state.dev_mode {
        return Err(HttpError::Forbidden(
            "dump is only available in development mode".into(),
        ));
    }

    let dump_ops = state.require_dump()?;
    let lines = dump_ops
        .print_dump()
        .await
        .map_err(http_error_from_backend_message)?;
    Ok(Json(lines))
}

/// POST /api/v0/purge
///
/// Purges all data from the database.
///
/// WARNING: This is a destructive operation that cannot be undone.
///
/// Requires `DocumentUpdate` permission when NAC is enabled.
pub async fn purge(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<StatusCode, HttpError> {
    require_permission(&state, &identity, NodePermission::DocumentUpdate).await?;

    if !state.dev_mode {
        return Err(HttpError::Forbidden(
            "cannot purge database when development mode is disabled".into(),
        ));
    }

    state
        .require_collection_mgmt()?
        .purge()
        .await
        .map_err(http_error_from_backend_message)?;

    Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{to_bytes, Body};
    use axum::http::Request;
    use serde_json::json;
    use tower::ServiceExt;

    use crate::mock::MockQueryExecutor;
    use crate::Server;

    #[test]
    fn test_node_identity_response_serialize() {
        let response = NodeIdentityResponse {
            peer_id: Some("12D3KooWtest".to_string()),
            did: Some("did:key:zNodeSigner".to_string()),
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"PeerID\""));
        assert!(json.contains("12D3KooWtest"));
        assert!(json.contains("\"DID\""));
        assert!(json.contains("did:key:zNodeSigner"));
    }

    #[test]
    fn test_node_identity_response_empty() {
        let response = NodeIdentityResponse {
            peer_id: None,
            did: None,
        };
        let json = serde_json::to_string(&response).unwrap();
        // PeerID should be omitted when None
        assert!(!json.contains("PeerID"));
        assert!(!json.contains("DID"));
    }

    #[tokio::test]
    async fn node_options_returns_configured_sanitized_object() {
        let options = serde_json::from_value(json!({
            "DisableP2P": true,
            "Store": {"Path": "<redacted>"},
        }))
        .unwrap();
        let app = Server::new(MockQueryExecutor::new())
            .with_node_options(options)
            .router()
            .unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v0/node/options")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(body["DisableP2P"], true);
        assert_eq!(body["Store"]["Path"], "<redacted>");
    }

    #[tokio::test]
    async fn node_options_returns_not_found_when_host_does_not_supply_them() {
        let response = Server::new(MockQueryExecutor::new())
            .router()
            .unwrap()
            .oneshot(
                Request::builder()
                    .uri("/api/v0/node/options")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(body["error"], "node options not available");
    }
}
