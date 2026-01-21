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

/// Response for replicator info.
#[derive(Debug, Clone, Serialize)]
pub struct ReplicatorInfoResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub collections: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
}

/// Request to add a replicator (Go-compatible format).
#[derive(Debug, Clone, Deserialize)]
pub struct ReplicatorRequest {
    /// List of collection names to replicate.
    #[serde(rename = "Collections")]
    pub collections: Vec<String>,
    /// List of peer multiaddrs to replicate to.
    #[serde(rename = "Addresses", default)]
    pub addresses: Vec<String>,
}

/// Query parameters for replicator removal.
#[derive(Debug, Clone, Deserialize)]
pub struct ReplicatorDeleteQuery {
    #[serde(default)]
    pub collections: Vec<String>,
    #[serde(default)]
    pub address: Option<String>,
}

/// Request to add P2P collections.
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

/// Get P2P node information (Go-compatible format).
///
/// GET /api/v0/p2p/info
///
/// Returns array of full multiaddrs with peer ID embedded.
/// Example: ["/ip4/127.0.0.1/tcp/9181/p2p/12D3KooWxyz..."]
pub async fn get_info(State(state): State<AppState>) -> Result<Json<P2pInfoResponse>, HttpError> {
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
pub async fn list_peers(State(state): State<AppState>) -> Result<Json<Vec<PeerInfo>>, HttpError> {
    let p2p = state.require_p2p()?;

    let peers = p2p.connected_peers().await.map_err(HttpError::Internal)?;

    let peer_infos: Vec<PeerInfo> = peers
        .into_iter()
        .map(|id| PeerInfo { id, address: None })
        .collect();

    Ok(Json(peer_infos))
}

/// Connect to a peer (legacy format).
///
/// POST /api/v0/p2p/peers
///
/// Body: {"address": "/ip4/..."}
pub async fn connect_peer(
    State(state): State<AppState>,
    Json(request): Json<ConnectPeerRequest>,
) -> Result<Json<()>, HttpError> {
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
pub async fn connect(
    State(state): State<AppState>,
    Json(addresses): Json<Vec<String>>,
) -> Result<(), HttpError> {
    let p2p = state.require_p2p()?;

    for addr in &addresses {
        validate_multiaddr(addr)?;
        p2p.connect_peer(addr)
            .await
            .map_err(HttpError::BadRequest)?;
    }

    Ok(())
}

/// List replicators.
///
/// GET /api/v0/p2p/replicator
pub async fn list_replicators(
    State(state): State<AppState>,
) -> Result<Json<Vec<ReplicatorInfoResponse>>, HttpError> {
    let p2p = state.require_p2p()?;

    let replicators = p2p.get_replicators().await.map_err(HttpError::Internal)?;

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

/// Add a replicator (Go-compatible format).
///
/// POST /api/v0/p2p/replicators
///
/// Body: {"Addresses": ["..."], "Collections": ["..."]}
pub async fn add_replicator(
    State(state): State<AppState>,
    Json(request): Json<ReplicatorRequest>,
) -> Result<Json<()>, HttpError> {
    let p2p = state.require_p2p()?;

    if request.collections.is_empty() {
        return Err(HttpError::BadRequest(
            "at least one collection is required".into(),
        ));
    }

    // Validate collection names
    for col in &request.collections {
        validate_collection_name(col)?;
    }

    // Validate and use addresses
    for addr in &request.addresses {
        validate_multiaddr(addr)?;
    }

    // Use first address if provided (Go sends array, trait takes optional single)
    let addr = request.addresses.first().map(|s| s.as_str());

    p2p.add_replicator(request.collections, addr)
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
    let p2p = state.require_p2p()?;

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
    let p2p = state.require_p2p()?;

    let collections = p2p.get_collections().await.map_err(HttpError::Internal)?;

    Ok(Json(collections))
}

/// Add collections to P2P.
///
/// POST /api/v0/p2p/collections
pub async fn add_collections(
    State(state): State<AppState>,
    Json(request): Json<CollectionsRequest>,
) -> Result<Json<()>, HttpError> {
    let p2p = state.require_p2p()?;

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
    let p2p = state.require_p2p()?;

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
        // Go-compatible format with PascalCase field names
        let json =
            r#"{"Collections": ["Users", "Posts"], "Addresses": ["/ip4/127.0.0.1/tcp/9000"]}"#;
        let request: ReplicatorRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.collections.len(), 2);
        assert_eq!(request.addresses.len(), 1);
    }

    #[test]
    fn test_replicator_request_without_address() {
        let json = r#"{"Collections": ["Users"]}"#;
        let request: ReplicatorRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.collections.len(), 1);
        assert!(request.addresses.is_empty());
    }

    #[test]
    fn test_collections_request_deserialize() {
        let json = r#"{"collections": ["Users", "Posts"]}"#;
        let request: CollectionsRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.collections.len(), 2);
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
