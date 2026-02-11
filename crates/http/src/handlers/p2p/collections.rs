//! P2P collection management handlers.

use axum::{extract::State, Json};

use crate::error::HttpError;
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AppState, NodePermission};
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
