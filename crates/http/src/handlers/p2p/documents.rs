//! P2P document replication handlers.

use axum::{extract::State, Json};

use crate::error::HttpError;
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AppState, NodePermission, P2pDocumentRequest, SyncDocumentsRequest};
use crate::validation::validate_doc_id;

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
/// Requires `P2pDocumentAdd` permission when NAC is enabled.
pub async fn add_documents(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(doc_ids): Json<Vec<String>>,
) -> Result<(), HttpError> {
    require_permission(&state, &identity, NodePermission::P2pDocumentAdd).await?;

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

/// Sync specific documents from connected peers.
///
/// POST /api/v0/p2p/documents/sync
///
/// Accepts JSON body: `{"collectionName": "...", "docIDs": ["..."]}`
///
/// Requires `P2pSyncDocuments` permission when NAC is enabled.
pub async fn sync_documents(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(body): Json<SyncDocumentsRequest>,
) -> Result<Json<()>, HttpError> {
    require_permission(&state, &identity, NodePermission::P2pSyncDocuments).await?;

    let p2p = state.require_p2p()?;

    p2p.sync_documents(&body.collection_name, body.doc_ids)
        .await
        .map_err(HttpError::Internal)?;

    Ok(Json(()))
}
