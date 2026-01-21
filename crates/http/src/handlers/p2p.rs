//! P2P endpoint handlers.
//!
//! These handlers provide HTTP access to P2P networking functionality:
//! - Node info (peer ID, addresses)
//! - Peer management (list, connect)
//! - Replicator management (list, add, remove)
//! - P2P collection management (list, add, remove)

use axum::{
    extract::{Query, State},
    Json,
};
use serde::{Deserialize, Serialize};

use crate::error::HttpError;
use crate::router::AppState;
use crate::validation::{validate_collection_name, validate_multiaddr};

/// Response for P2P node info.
#[derive(Debug, Clone, Serialize)]
pub struct P2pInfoResponse {
    pub id: String,
    pub addresses: Vec<String>,
}

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

/// Response for replicator info.
#[derive(Debug, Clone, Serialize)]
pub struct ReplicatorInfoResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub collections: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
}

/// Request to add/remove a replicator.
#[derive(Debug, Clone, Deserialize)]
pub struct ReplicatorRequest {
    pub collections: Vec<String>,
    #[serde(default)]
    pub address: Option<String>,
}

/// Query parameters for replicator removal.
#[derive(Debug, Clone, Deserialize)]
pub struct ReplicatorDeleteQuery {
    #[serde(default)]
    pub collections: Vec<String>,
    #[serde(default)]
    pub address: Option<String>,
}

/// Request to add/remove P2P collections.
#[derive(Debug, Clone, Deserialize)]
pub struct CollectionsRequest {
    pub collections: Vec<String>,
}

/// Query parameters for collection removal.
#[derive(Debug, Clone, Deserialize)]
pub struct CollectionsDeleteQuery {
    #[serde(default)]
    pub collections: Vec<String>,
}

/// Get P2P node information.
///
/// GET /api/v0/p2p/info
pub async fn get_info(State(state): State<AppState>) -> Result<Json<P2pInfoResponse>, HttpError> {
    let p2p = state
        .p2p
        .as_ref()
        .ok_or_else(|| HttpError::Internal("P2P not configured".into()))?;

    let peer_id = p2p
        .local_peer_id()
        .await
        .map_err(HttpError::Internal)?;

    let addresses = p2p
        .listen_addresses()
        .await
        .map_err(HttpError::Internal)?;

    Ok(Json(P2pInfoResponse {
        id: peer_id,
        addresses,
    }))
}

/// List connected peers.
///
/// GET /api/v0/p2p/peers
pub async fn list_peers(State(state): State<AppState>) -> Result<Json<Vec<PeerInfo>>, HttpError> {
    let p2p = state
        .p2p
        .as_ref()
        .ok_or_else(|| HttpError::Internal("P2P not configured".into()))?;

    let peers = p2p
        .connected_peers()
        .await
        .map_err(HttpError::Internal)?;

    let peer_infos: Vec<PeerInfo> = peers
        .into_iter()
        .map(|id| PeerInfo { id, address: None })
        .collect();

    Ok(Json(peer_infos))
}

/// Connect to a peer.
///
/// POST /api/v0/p2p/peers
pub async fn connect_peer(
    State(state): State<AppState>,
    Json(request): Json<ConnectPeerRequest>,
) -> Result<Json<()>, HttpError> {
    let p2p = state
        .p2p
        .as_ref()
        .ok_or_else(|| HttpError::Internal("P2P not configured".into()))?;

    // Validate the multiaddr format
    validate_multiaddr(&request.address)?;

    p2p.connect_peer(&request.address)
        .await
        .map_err(HttpError::BadRequest)?;

    Ok(Json(()))
}

/// List replicators.
///
/// GET /api/v0/p2p/replicator
pub async fn list_replicators(
    State(state): State<AppState>,
) -> Result<Json<Vec<ReplicatorInfoResponse>>, HttpError> {
    let p2p = state
        .p2p
        .as_ref()
        .ok_or_else(|| HttpError::Internal("P2P not configured".into()))?;

    let replicators = p2p
        .get_replicators()
        .await
        .map_err(HttpError::Internal)?;

    let response: Vec<ReplicatorInfoResponse> = replicators
        .into_iter()
        .map(|r| ReplicatorInfoResponse {
            id: r.id,
            collections: r.collections,
            address: r.address,
        })
        .collect();

    Ok(Json(response))
}

/// Add a replicator.
///
/// POST /api/v0/p2p/replicator
pub async fn add_replicator(
    State(state): State<AppState>,
    Json(request): Json<ReplicatorRequest>,
) -> Result<Json<()>, HttpError> {
    let p2p = state
        .p2p
        .as_ref()
        .ok_or_else(|| HttpError::Internal("P2P not configured".into()))?;

    if request.collections.is_empty() {
        return Err(HttpError::BadRequest(
            "at least one collection is required".into(),
        ));
    }

    // Validate collection names
    for col in &request.collections {
        validate_collection_name(col)?;
    }

    // Validate address if provided
    if let Some(ref addr) = request.address {
        validate_multiaddr(addr)?;
    }

    p2p.add_replicator(request.collections, request.address.as_deref())
        .await
        .map_err(HttpError::BadRequest)?;

    Ok(Json(()))
}

/// Remove a replicator.
///
/// DELETE /api/v0/p2p/replicator
pub async fn remove_replicator(
    State(state): State<AppState>,
    Query(query): Query<ReplicatorDeleteQuery>,
) -> Result<Json<()>, HttpError> {
    let p2p = state
        .p2p
        .as_ref()
        .ok_or_else(|| HttpError::Internal("P2P not configured".into()))?;

    if query.collections.is_empty() {
        return Err(HttpError::BadRequest(
            "at least one collection is required".into(),
        ));
    }

    // Validate collection names
    for col in &query.collections {
        validate_collection_name(col)?;
    }

    // Validate address if provided
    if let Some(ref addr) = query.address {
        validate_multiaddr(addr)?;
    }

    p2p.remove_replicator(query.collections, query.address.as_deref())
        .await
        .map_err(HttpError::BadRequest)?;

    Ok(Json(()))
}

/// List P2P collections.
///
/// GET /api/v0/p2p/collections
pub async fn list_collections(
    State(state): State<AppState>,
) -> Result<Json<Vec<String>>, HttpError> {
    let p2p = state
        .p2p
        .as_ref()
        .ok_or_else(|| HttpError::Internal("P2P not configured".into()))?;

    let collections = p2p
        .get_collections()
        .await
        .map_err(HttpError::Internal)?;

    Ok(Json(collections))
}

/// Add collections to P2P.
///
/// POST /api/v0/p2p/collections
pub async fn add_collections(
    State(state): State<AppState>,
    Json(request): Json<CollectionsRequest>,
) -> Result<Json<()>, HttpError> {
    let p2p = state
        .p2p
        .as_ref()
        .ok_or_else(|| HttpError::Internal("P2P not configured".into()))?;

    if request.collections.is_empty() {
        return Err(HttpError::BadRequest(
            "at least one collection is required".into(),
        ));
    }

    // Validate collection names
    for col in &request.collections {
        validate_collection_name(col)?;
    }

    p2p.add_collections(request.collections)
        .await
        .map_err(HttpError::BadRequest)?;

    Ok(Json(()))
}

/// Remove collections from P2P.
///
/// DELETE /api/v0/p2p/collections
pub async fn remove_collections(
    State(state): State<AppState>,
    Query(query): Query<CollectionsDeleteQuery>,
) -> Result<Json<()>, HttpError> {
    let p2p = state
        .p2p
        .as_ref()
        .ok_or_else(|| HttpError::Internal("P2P not configured".into()))?;

    if query.collections.is_empty() {
        return Err(HttpError::BadRequest(
            "at least one collection is required".into(),
        ));
    }

    // Validate collection names
    for col in &query.collections {
        validate_collection_name(col)?;
    }

    p2p.remove_collections(query.collections)
        .await
        .map_err(HttpError::BadRequest)?;

    Ok(Json(()))
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
    fn test_replicator_request_deserialize() {
        let json = r#"{"collections": ["Users", "Posts"], "address": "/ip4/127.0.0.1/tcp/9000"}"#;
        let request: ReplicatorRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.collections.len(), 2);
        assert!(request.address.is_some());
    }

    #[test]
    fn test_replicator_request_without_address() {
        let json = r#"{"collections": ["Users"]}"#;
        let request: ReplicatorRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.collections.len(), 1);
        assert!(request.address.is_none());
    }

    #[test]
    fn test_collections_request_deserialize() {
        let json = r#"{"collections": ["Users", "Posts"]}"#;
        let request: CollectionsRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.collections.len(), 2);
    }

    #[test]
    fn test_p2p_info_response_serialize() {
        let response = P2pInfoResponse {
            id: "12D3KooWtest".to_string(),
            addresses: vec!["/ip4/127.0.0.1/tcp/9000".to_string()],
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("12D3KooWtest"));
        assert!(json.contains("/ip4/127.0.0.1/tcp/9000"));
    }
}
