//! P2P peer management handlers.

use axum::{extract::State, Json};
use serde::{Deserialize, Serialize};

use crate::error::HttpError;
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AppState, NodePermission};
use crate::validation::validate_multiaddr;

/// Response for P2P node info (Go-compatible format).
/// Returns array of full multiaddrs with peer ID embedded.
/// Example: ["/ip4/127.0.0.1/tcp/9181/p2p/12D3KooWxyz..."]
pub type P2pInfoResponse = Vec<String>;

/// Response for listing peers.
#[derive(Debug, Clone, Serialize)]
pub struct PeerInfo {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
}

/// Request to connect to a peer.
#[derive(Debug, Clone, Deserialize)]
pub struct ConnectPeerRequest {
    pub address: String,
}

/// Get P2P node information (Go-compatible format).
///
/// GET /api/v0/p2p/info
///
/// Returns array of full multiaddrs with peer ID embedded.
/// Example: ["/ip4/127.0.0.1/tcp/9181/p2p/12D3KooWxyz..."]
///
/// Requires `P2pPeerConnect` permission when NAC is enabled.
pub async fn get_info(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<Json<P2pInfoResponse>, HttpError> {
    require_permission(&state, &identity, NodePermission::P2pPeerConnect).await?;

    let p2p = state.require_p2p()?;

    let peer_id = p2p.local_peer_id().await.map_err(HttpError::Internal)?;

    let addresses = p2p.listen_addresses().await.map_err(HttpError::Internal)?;

    // Build full multiaddrs with peer ID embedded (Go-compatible format)
    let full_addrs: Vec<String> = addresses
        .into_iter()
        .map(|addr| format!("{}/p2p/{}", addr, peer_id))
        .collect();

    Ok(Json(full_addrs))
}

/// List connected peers.
///
/// GET /api/v0/p2p/peers
///
/// Requires `P2pPeerConnect` permission when NAC is enabled.
pub async fn list_peers(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<Json<Vec<PeerInfo>>, HttpError> {
    require_permission(&state, &identity, NodePermission::P2pPeerConnect).await?;

    let p2p = state.require_p2p()?;

    let peers = p2p.connected_peers().await.map_err(HttpError::Internal)?;

    let peer_infos: Vec<PeerInfo> = peers
        .into_iter()
        .map(|id| PeerInfo { id, address: None })
        .collect();

    Ok(Json(peer_infos))
}

/// List active peers (Go-compatible).
///
/// GET /api/v0/p2p/active-peers
///
/// Returns array of connected peer IDs as strings.
///
/// Requires `P2pPeerConnect` permission when NAC is enabled.
pub async fn active_peers(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<Json<Vec<String>>, HttpError> {
    require_permission(&state, &identity, NodePermission::P2pPeerConnect).await?;

    let p2p = state.require_p2p()?;

    let peers = p2p.connected_peers().await.map_err(HttpError::Internal)?;

    Ok(Json(peers))
}

/// Connect to a peer (legacy format).
///
/// POST /api/v0/p2p/peers
///
/// Body: {"address": "/ip4/..."}
///
/// Requires `P2pPeerConnect` permission when NAC is enabled.
pub async fn connect_peer(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(request): Json<ConnectPeerRequest>,
) -> Result<Json<()>, HttpError> {
    require_permission(&state, &identity, NodePermission::P2pPeerConnect).await?;

    let p2p = state.require_p2p()?;

    // Validate the multiaddr format
    validate_multiaddr(&request.address)?;

    p2p.connect_peer(&request.address)
        .await
        .map_err(HttpError::BadRequest)?;

    Ok(Json(()))
}

/// Connect to peers (Go-compatible format).
///
/// POST /api/v0/p2p/connect
///
/// Body: ["/ip4/.../p2p/...", ...]
///
/// Requires `P2pPeerConnect` permission when NAC is enabled.
pub async fn connect(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(addresses): Json<Vec<String>>,
) -> Result<(), HttpError> {
    require_permission(&state, &identity, NodePermission::P2pPeerConnect).await?;

    let p2p = state.require_p2p()?;

    for addr in &addresses {
        validate_multiaddr(addr)?;
        p2p.connect_peer(addr)
            .await
            .map_err(HttpError::BadRequest)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connect_peer_request_deserialize() {
        let json = r#"{"address": "/ip4/127.0.0.1/tcp/9000/p2p/12D3KooWtest"}"#;
        let request: ConnectPeerRequest = serde_json::from_str(json).unwrap();
        assert!(request.address.contains("12D3KooW"));
    }

    #[test]
    fn test_p2p_info_response_serialize() {
        // Response is now array of full multiaddrs (Go-compatible format)
        let response: P2pInfoResponse =
            vec!["/ip4/127.0.0.1/tcp/9000/p2p/12D3KooWtest".to_string()];
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("12D3KooWtest"));
        assert!(json.contains("/ip4/127.0.0.1/tcp/9000"));
        // Verify it's an array, not an object
        assert!(json.starts_with('['));
    }
}
