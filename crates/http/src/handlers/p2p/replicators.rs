//! P2P replicator management handlers.

use axum::{extract::State, Json};
use serde::{Deserialize, Serialize};

use super::{map_p2p_bad_request, map_p2p_internal};
use crate::error::HttpError;
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AppState, ExplicitReplayCapabilityInput, NodePermission, ReplicationFilters};
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
    /// Active=0, Inactive=1, matching Go's client.ReplicatorStatus.
    #[serde(rename = "Status")]
    pub status: u8,
    /// Last time the status changed, formatted like Go's time.Time JSON.
    #[serde(rename = "LastStatusChange")]
    pub last_status_change: String,
    /// Optional Rust extension for filtered replication.
    #[serde(
        rename = "Filters",
        skip_serializing_if = "ReplicationFilters::is_empty"
    )]
    pub filters: ReplicationFilters,
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
    #[serde(rename = "ExplicitReplayCapabilities", default)]
    pub explicit_replay_capabilities: Vec<ExplicitReplayCapabilityInput>,
    /// Optional per-collection filtered replication predicates.
    #[serde(rename = "Filters", default)]
    pub filters: ReplicationFilters,
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
    /// Delete the entire replicator and any persisted retry state.
    #[serde(rename = "Forget", default)]
    pub forget: bool,
}

/// List replicators (Go-compatible format).
///
/// GET /api/v0/p2p/replicators
///
/// Returns array of replicators with Go-compatible PascalCase field names:
/// `[{"ID": "...", "Addresses": [...], "CollectionIDs": [...], "Status": 0, "LastStatusChange": "..."}]`
///
/// Requires `P2pReplicatorList` permission when NAC is enabled.
pub async fn list_replicators(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<Json<Vec<ReplicatorInfoResponse>>, HttpError> {
    require_permission(&state, &identity, NodePermission::P2pReplicatorList).await?;

    let p2p = state.require_p2p()?;

    let replicators = p2p.get_replicators().await.map_err(map_p2p_internal)?;

    let response: Vec<ReplicatorInfoResponse> = replicators
        .into_iter()
        .map(|r| ReplicatorInfoResponse {
            id: r.id,
            // Convert single address to array (Go uses Addresses plural)
            addresses: r.address.into_iter().collect(),
            // Use collections as collection IDs
            collection_ids: r.collections,
            status: r.status.unwrap_or(0),
            last_status_change: r
                .last_status_change
                .unwrap_or_else(|| "0001-01-01T00:00:00Z".to_string()),
            filters: r.filters,
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
/// Requires `P2pReplicatorAdd` permission when NAC is enabled.
pub async fn add_replicator(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(request): Json<ReplicatorRequest>,
) -> Result<Json<()>, HttpError> {
    require_permission(&state, &identity, NodePermission::P2pReplicatorAdd).await?;

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
    let expected_authorizer_did = identity.did().map(|did| did.to_string());

    p2p.add_replicator(
        request.collections,
        addr,
        request.filters,
        request.explicit_replay_capabilities,
        expected_authorizer_did.as_deref(),
    )
    .await
    .map_err(map_p2p_bad_request)?;

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

    if request.forget {
        if !request.collections.is_empty() {
            return Err(HttpError::BadRequest(
                "Forget cannot be combined with Collections".into(),
            ));
        }
        let peer_id = request
            .id
            .as_deref()
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(|| HttpError::BadRequest("ID is required when Forget is true".into()))?;
        p2p.remove_replicator(Vec::new(), Some(peer_id))
            .await
            .map_err(map_p2p_bad_request)?;
        return Ok(());
    }

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
        .map_err(map_p2p_bad_request)?;

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
        assert!(!request.forget);
    }

    #[test]
    fn test_replicator_forget_request_deserialize() {
        let request: ReplicatorDeleteRequest =
            serde_json::from_str(r#"{"ID":"replicator-123","Forget":true}"#).unwrap();

        assert_eq!(request.id.as_deref(), Some("replicator-123"));
        assert!(request.collections.is_empty());
        assert!(request.forget);
    }

    #[test]
    fn test_replicator_info_response_serialize() {
        // Go-compatible format with PascalCase field names
        let response = ReplicatorInfoResponse {
            id: Some("replicator-123".to_string()),
            addresses: vec!["/ip4/127.0.0.1/tcp/9000".to_string()],
            collection_ids: vec!["Users".to_string(), "Posts".to_string()],
            status: 0,
            last_status_change: "0001-01-01T00:00:00Z".to_string(),
            filters: Default::default(),
        };
        let json = serde_json::to_string(&response).unwrap();
        // Verify PascalCase field names
        assert!(json.contains("\"ID\""));
        assert!(json.contains("\"Addresses\""));
        assert!(json.contains("\"CollectionIDs\""));
        assert!(json.contains("\"Status\""));
        assert!(json.contains("\"LastStatusChange\""));
    }
}
