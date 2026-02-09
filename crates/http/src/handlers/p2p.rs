//! P2P endpoint handlers.
//!
//! These handlers provide HTTP access to P2P networking functionality:
//! - Node info (peer ID, addresses)
//! - Peer management (list, connect)
//! - Replicator management (list, add, remove)
//! - P2P collection management (list, add, remove)
//!
//! All endpoints enforce NAC permissions when NAC is enabled.

use axum::{extract::State, Json};
use serde::{Deserialize, Serialize};

use crate::error::HttpError;
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AppState, NodePermission, P2pDocumentRequest};
use crate::validation::{validate_collection_name, validate_doc_id, validate_multiaddr};

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

/// Response for replicator info (Go-compatible format with PascalCase).
/// Matches Go's `client.Replicator` struct.
#[derive(Debug, Clone, Serialize)]
pub struct ReplicatorInfoResponse {
    /// Replicator ID (Go uses PascalCase).
    #[serde(rename = "ID", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// List of peer addresses (Go uses Addresses plural).
    #[serde(rename = "Addresses")]
    pub addresses: Vec<String>,
    /// Collection IDs being replicated (Go uses CollectionIDs).
    #[serde(rename = "CollectionIDs")]
    pub collection_ids: Vec<String>,
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

/// Request body for replicator removal (Go-compatible).
/// Go DefraDB uses body JSON with ID and Collections fields.
#[derive(Debug, Clone, Deserialize)]
pub struct ReplicatorDeleteRequest {
    /// Replicator ID (optional in Go).
    #[serde(rename = "ID", default)]
    pub id: Option<String>,
    /// Collections to remove from replicator.
    #[serde(rename = "Collections", default)]
    pub collections: Vec<String>,
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

/// List replicators (Go-compatible format).
///
/// GET /api/v0/p2p/replicators
///
/// Returns array of replicators with Go-compatible PascalCase field names:
/// `[{"ID": "...", "Addresses": [...], "CollectionIDs": [...]}]`
///
/// Requires `P2pReplicatorList` permission when NAC is enabled.
pub async fn list_replicators(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<Json<Vec<ReplicatorInfoResponse>>, HttpError> {
    require_permission(&state, &identity, NodePermission::P2pReplicatorList).await?;

    let p2p = state.require_p2p()?;

    let replicators = p2p.get_replicators().await.map_err(HttpError::Internal)?;

    let response: Vec<ReplicatorInfoResponse> = replicators
        .into_iter()
        .map(|r| ReplicatorInfoResponse {
            id: r.id,
            // Convert single address to array (Go uses Addresses plural)
            addresses: r.address.into_iter().collect(),
            // Use collections as collection IDs
            collection_ids: r.collections,
        })
        .collect();

    Ok(Json(response))
}

/// Add a replicator (Go-compatible format).
///
/// POST /api/v0/p2p/replicators
///
/// Body: {"Addresses": ["..."], "Collections": ["..."]}
///
/// Requires `P2pReplicatorCreate` permission when NAC is enabled.
pub async fn add_replicator(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(request): Json<ReplicatorRequest>,
) -> Result<Json<()>, HttpError> {
    require_permission(&state, &identity, NodePermission::P2pReplicatorCreate).await?;

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

/// Remove a replicator (Go-compatible).
///
/// DELETE /api/v0/p2p/replicators
///
/// Go DefraDB uses body JSON with ID and Collections fields:
/// `{"ID": "replicator-id", "Collections": ["col1", "col2"]}`
///
/// Requires `P2pReplicatorDelete` permission when NAC is enabled.
pub async fn remove_replicator(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(request): Json<ReplicatorDeleteRequest>,
) -> Result<(), HttpError> {
    require_permission(&state, &identity, NodePermission::P2pReplicatorDelete).await?;

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

    // Use ID as address if provided (Go uses ID field for peer identification)
    let addr = request.id.as_deref();

    p2p.remove_replicator(request.collections, addr)
        .await
        .map_err(HttpError::BadRequest)?;

    // Go returns 200 OK with empty body
    Ok(())
}

/// List P2P collections.
///
/// GET /api/v0/p2p/collections
///
/// Requires `P2pCollectionList` permission when NAC is enabled.
pub async fn list_collections(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<Json<Vec<String>>, HttpError> {
    require_permission(&state, &identity, NodePermission::P2pCollectionList).await?;

    let p2p = state.require_p2p()?;

    let collections = p2p.get_collections().await.map_err(HttpError::Internal)?;

    Ok(Json(collections))
}

/// Add collections to P2P (Go-compatible).
///
/// POST /api/v0/p2p/collections
///
/// Go DefraDB accepts raw array: `["collection1", "collection2"]`
///
/// Requires `P2pCollectionCreate` permission when NAC is enabled.
pub async fn add_collections(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(collections): Json<Vec<String>>,
) -> Result<(), HttpError> {
    require_permission(&state, &identity, NodePermission::P2pCollectionCreate).await?;

    let p2p = state.require_p2p()?;

    if collections.is_empty() {
        return Err(HttpError::BadRequest(
            "at least one collection is required".into(),
        ));
    }

    // Validate collection names
    for col in &collections {
        validate_collection_name(col)?;
    }

    p2p.add_collections(collections)
        .await
        .map_err(HttpError::BadRequest)?;

    // Go returns 200 OK with empty body
    Ok(())
}

/// Remove collections from P2P (Go-compatible).
///
/// DELETE /api/v0/p2p/collections
///
/// Go DefraDB accepts body JSON: `["collection1", "collection2"]`
///
/// Requires `P2pCollectionDelete` permission when NAC is enabled.
pub async fn remove_collections(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(collections): Json<Vec<String>>,
) -> Result<(), HttpError> {
    require_permission(&state, &identity, NodePermission::P2pCollectionDelete).await?;

    let p2p = state.require_p2p()?;

    if collections.is_empty() {
        return Err(HttpError::BadRequest(
            "at least one collection is required".into(),
        ));
    }

    // Validate collection names
    for col in &collections {
        validate_collection_name(col)?;
    }

    p2p.remove_collections(collections)
        .await
        .map_err(HttpError::BadRequest)?;

    // Go returns 200 OK with empty body
    Ok(())
}

/// List P2P documents (Go-compatible).
///
/// GET /api/v0/p2p/documents
///
/// Go DefraDB returns flat array of document IDs: `["doc-id-1", "doc-id-2"]`
///
/// Requires `P2pDocumentList` permission when NAC is enabled.
pub async fn list_documents(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<Json<Vec<String>>, HttpError> {
    require_permission(&state, &identity, NodePermission::P2pDocumentList).await?;

    let p2p = state.require_p2p()?;

    let docs = p2p.get_documents().await.map_err(HttpError::Internal)?;

    // Convert to flat array of document IDs (Go-compatible format)
    let doc_ids: Vec<String> = docs.into_iter().map(|d| d.doc_id).collect();

    Ok(Json(doc_ids))
}

/// Add documents to P2P replication (Go-compatible).
///
/// POST /api/v0/p2p/documents
///
/// Go DefraDB accepts flat array of document IDs: `["doc-id-1", "doc-id-2"]`
///
/// Requires `P2pDocumentCreate` permission when NAC is enabled.
pub async fn add_documents(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(doc_ids): Json<Vec<String>>,
) -> Result<(), HttpError> {
    require_permission(&state, &identity, NodePermission::P2pDocumentCreate).await?;

    let p2p = state.require_p2p()?;

    if doc_ids.is_empty() {
        return Err(HttpError::BadRequest(
            "at least one document is required".into(),
        ));
    }

    // Validate doc IDs
    for doc_id in &doc_ids {
        validate_doc_id(doc_id)?;
    }

    // Convert flat doc IDs to P2pDocumentRequest format
    // Note: Go DefraDB infers collection from doc ID prefix
    let docs: Vec<P2pDocumentRequest> = doc_ids
        .into_iter()
        .map(|doc_id| P2pDocumentRequest {
            collection: String::new(), // Collection inferred from doc ID
            doc_id,
        })
        .collect();

    p2p.add_documents(docs)
        .await
        .map_err(HttpError::BadRequest)?;

    // Go returns 200 OK with empty body
    Ok(())
}

/// Remove documents from P2P replication (Go-compatible).
///
/// DELETE /api/v0/p2p/documents
///
/// Go DefraDB accepts body JSON with flat array of document IDs: `["doc-id-1", "doc-id-2"]`
///
/// Requires `P2pDocumentDelete` permission when NAC is enabled.
pub async fn remove_documents(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(doc_ids): Json<Vec<String>>,
) -> Result<(), HttpError> {
    require_permission(&state, &identity, NodePermission::P2pDocumentDelete).await?;

    let p2p = state.require_p2p()?;

    if doc_ids.is_empty() {
        return Err(HttpError::BadRequest(
            "at least one document is required".into(),
        ));
    }

    // Validate doc IDs
    for doc_id in &doc_ids {
        validate_doc_id(doc_id)?;
    }

    // Convert flat doc IDs to P2pDocumentRequest format
    let docs: Vec<P2pDocumentRequest> = doc_ids
        .into_iter()
        .map(|doc_id| P2pDocumentRequest {
            collection: String::new(),
            doc_id,
        })
        .collect();

    p2p.remove_documents(docs)
        .await
        .map_err(HttpError::BadRequest)?;

    // Go returns 200 OK with empty body
    Ok(())
}

/// Sync collections with peers (trigger immediate sync).
///
/// POST /api/v0/p2p/collections/sync
///
/// Requires `P2pCollectionList` permission when NAC is enabled (per Go behavior).
pub async fn sync_collections(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<Json<()>, HttpError> {
    require_permission(&state, &identity, NodePermission::P2pCollectionList).await?;

    let p2p = state.require_p2p()?;

    p2p.sync_collections().await.map_err(HttpError::Internal)?;

    Ok(Json(()))
}

/// Sync documents with peers (trigger immediate sync).
///
/// POST /api/v0/p2p/documents/sync
///
/// Requires `P2pDocumentCreate` permission when NAC is enabled (per Go behavior).
pub async fn sync_documents(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<Json<()>, HttpError> {
    require_permission(&state, &identity, NodePermission::P2pDocumentCreate).await?;

    let p2p = state.require_p2p()?;

    p2p.sync_documents().await.map_err(HttpError::Internal)?;

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
    fn test_replicator_delete_request_deserialize() {
        // Go-compatible format with PascalCase field names
        let json = r#"{"ID": "replicator-123", "Collections": ["Users", "Posts"]}"#;
        let request: ReplicatorDeleteRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.id, Some("replicator-123".to_string()));
        assert_eq!(request.collections.len(), 2);
    }

    #[test]
    fn test_collections_array_deserialize() {
        // Go-compatible format: raw array
        let json = r#"["Users", "Posts"]"#;
        let collections: Vec<String> = serde_json::from_str(json).unwrap();
        assert_eq!(collections.len(), 2);
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

    #[test]
    fn test_replicator_info_response_serialize() {
        // Go-compatible format with PascalCase field names
        let response = ReplicatorInfoResponse {
            id: Some("replicator-123".to_string()),
            addresses: vec!["/ip4/127.0.0.1/tcp/9000".to_string()],
            collection_ids: vec!["Users".to_string(), "Posts".to_string()],
        };
        let json = serde_json::to_string(&response).unwrap();
        // Verify PascalCase field names
        assert!(json.contains("\"ID\""));
        assert!(json.contains("\"Addresses\""));
        assert!(json.contains("\"CollectionIDs\""));
    }
}
