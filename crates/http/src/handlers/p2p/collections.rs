//! P2P collection management handlers.

use axum::{extract::State, Json};

use super::{map_p2p_bad_request, map_p2p_internal};
use crate::error::HttpError;
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AppState, NodePermission, SyncBranchableRequest, SyncVersionsRequest};
use crate::validation::validate_collection_name;

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

    let collections = p2p.get_collections().await.map_err(map_p2p_internal)?;

    Ok(Json(collections))
}

/// Add collections to P2P (Go-compatible).
///
/// POST /api/v0/p2p/collections
///
/// Go DefraDB accepts raw array: `["collection1", "collection2"]`
///
/// Requires `P2pCollectionAdd` permission when NAC is enabled.
pub async fn add_collections(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(collections): Json<Vec<String>>,
) -> Result<(), HttpError> {
    require_permission(&state, &identity, NodePermission::P2pCollectionAdd).await?;

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
        .map_err(map_p2p_bad_request)?;

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
        .map_err(map_p2p_bad_request)?;

    // Go returns 200 OK with empty body
    Ok(())
}

/// Sync a branchable collection from connected peers.
///
/// POST /api/v0/p2p/collections/sync-branchable
///
/// Accepts JSON body: `{"collectionID": "..."}`
///
/// Requires `P2pSyncBranchableCollection` permission when NAC is enabled.
pub async fn sync_branchable(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(body): Json<SyncBranchableRequest>,
) -> Result<Json<()>, HttpError> {
    require_permission(
        &state,
        &identity,
        NodePermission::P2pSyncBranchableCollection,
    )
    .await?;

    let p2p = state.require_p2p()?;

    p2p.sync_branchable_collection(&body.collection_id)
        .await
        .map_err(map_p2p_internal)?;

    Ok(Json(()))
}

/// Sync collection versions (schema definitions) from connected peers via Bitswap.
///
/// POST /api/v0/p2p/collections/sync-versions
///
/// Accepts JSON body: `{"versionIDs": ["bafyrei...", ...]}`
///
/// Requires `P2pSyncCollectionVersions` permission when NAC is enabled.
pub async fn sync_versions(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(body): Json<SyncVersionsRequest>,
) -> Result<Json<()>, HttpError> {
    require_permission(&state, &identity, NodePermission::P2pSyncCollectionVersions).await?;

    let p2p = state.require_p2p()?;

    p2p.sync_collection_versions(body.version_ids)
        .await
        .map_err(map_p2p_internal)?;

    Ok(Json(()))
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_collections_array_deserialize() {
        // Go-compatible format: raw array
        let json = r#"["Users", "Posts"]"#;
        let collections: Vec<String> = serde_json::from_str(json).unwrap();
        assert_eq!(collections.len(), 2);
    }
}
