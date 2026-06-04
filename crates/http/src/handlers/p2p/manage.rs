//! P2P management-relay handlers.
//!
//! These endpoints let an HTTP caller manage a P2P-only peer (node B) via this
//! node (node A). Node A relays a signed management request to B, carrying the
//! caller-minted actor token (a JWT with `aud` = B's peer-id).

use axum::{extract::State, Json};
use serde::Deserialize;

use crate::error::HttpError;
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{
    AppState, NodePermission, RemoteManageOp, RemoteManageQueryOp, RemoteManageQueryResult,
    MANAGE_UNAUTHORIZED,
};

/// Map a [`ManageRequester`](crate::router::ManageRequester) error string to an
/// HTTP error. A remote NAC denial surfaces as `"unauthorized"` → 403 Forbidden;
/// any other failure is a relayed remote-op failure → 400 Bad Request.
fn map_manage_err(message: String) -> HttpError {
    if message == MANAGE_UNAUTHORIZED {
        HttpError::Forbidden(MANAGE_UNAUTHORIZED.into())
    } else {
        HttpError::BadRequest(message)
    }
}

/// Request body for a relayed mutating management operation.
#[derive(Debug, Clone, Deserialize)]
pub struct ManageRequestBody {
    /// Target peer address that node A dials.
    #[serde(rename = "Target")]
    pub target: String,
    /// Caller-minted JWT (`aud` = target peer-id).
    #[serde(rename = "AuthToken")]
    pub auth_token: String,
    /// The management operation to relay.
    #[serde(rename = "Op")]
    pub op: RemoteManageOp,
}

/// Request body for a relayed read-only management query.
#[derive(Debug, Clone, Deserialize)]
pub struct ManageQueryRequestBody {
    /// Target peer address that node A dials.
    #[serde(rename = "Target")]
    pub target: String,
    /// Caller-minted JWT (`aud` = target peer-id).
    #[serde(rename = "AuthToken")]
    pub auth_token: String,
    /// The read-only management query to relay.
    #[serde(rename = "Op")]
    pub op: RemoteManageQueryOp,
}

/// Relay a mutating management request to a P2P-only peer.
///
/// POST /api/v0/p2p/manage
///
/// Body: `{"Target": "<addr>", "AuthToken": "<jwt>", "Op": {"Kind": "...", ...}}`
///
/// Requires `P2pPeerConnect` permission when NAC is enabled (this node acts as a
/// relay: it connects to and commands the target peer).
pub async fn manage(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(body): Json<ManageRequestBody>,
) -> Result<Json<()>, HttpError> {
    require_permission(&state, &identity, NodePermission::P2pPeerConnect).await?;

    let manage = state.require_manage()?;

    manage
        .manage(&body.target, body.auth_token.into_bytes(), body.op)
        .await
        .map_err(map_manage_err)?;

    Ok(Json(()))
}

/// Relay a read-only management query to a P2P-only peer.
///
/// POST /api/v0/p2p/manage/query
///
/// Body: `{"Target": "<addr>", "AuthToken": "<jwt>", "Op": {"Kind": "..."}}`
///
/// Requires `P2pPeerConnect` permission when NAC is enabled.
pub async fn manage_query(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(body): Json<ManageQueryRequestBody>,
) -> Result<Json<RemoteManageQueryResult>, HttpError> {
    require_permission(&state, &identity, NodePermission::P2pPeerConnect).await?;

    let manage = state.require_manage()?;

    let result = manage
        .manage_query(&body.target, body.auth_token.into_bytes(), body.op)
        .await
        .map_err(map_manage_err)?;

    Ok(Json(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    #[test]
    fn map_manage_err_unauthorized_is_forbidden() {
        let err = map_manage_err("unauthorized".into());
        assert_eq!(err.into_response().status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn map_manage_err_other_is_bad_request() {
        let err = map_manage_err("dial failed".into());
        assert_eq!(err.into_response().status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn manage_request_body_deserialize() {
        let json = r#"{
            "Target": "/ip4/127.0.0.1/tcp/9000/p2p/12D3KooWtest",
            "AuthToken": "header.payload.sig",
            "Op": {"Kind": "CollectionAdd", "collection_ids": ["Users"]}
        }"#;
        let body: ManageRequestBody = serde_json::from_str(json).unwrap();
        assert_eq!(body.auth_token, "header.payload.sig");
        assert!(matches!(body.op, RemoteManageOp::CollectionAdd { .. }));
    }

    #[test]
    fn manage_query_request_body_deserialize() {
        let json = r#"{
            "Target": "/ip4/127.0.0.1/tcp/9000/p2p/12D3KooWtest",
            "AuthToken": "header.payload.sig",
            "Op": {"Kind": "ReplicatorList"}
        }"#;
        let body: ManageQueryRequestBody = serde_json::from_str(json).unwrap();
        assert!(matches!(body.op, RemoteManageQueryOp::ReplicatorList));
    }
}
