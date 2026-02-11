//! P2P replicator management handlers.

use axum::{extract::State, Json};
use serde::{Deserialize, Serialize};

use crate::error::HttpError;
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AppState, NodePermission};
use crate::validation::{validate_collection_name, validate_multiaddr};

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

#[cfg(test)]
mod tests {
    use super::*;

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
