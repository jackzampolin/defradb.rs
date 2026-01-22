//! Document REST endpoint handlers.
//!
//! These handlers extract identity from the Authorization header and pass it
//! to the REST operations layer for ACP (Access Control Policy) enforcement.
//!
//! All endpoints enforce NAC permissions when NAC is enabled.

use axum::{
    extract::{Path, State},
    Json,
};
use serde::Serialize;
use serde_json::Value as JsonValue;

use crate::error::HttpError;
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AppState, NodePermission};

/// Response for delete operations.
#[derive(Debug, Clone, Serialize)]
pub struct DeleteResponse {
    pub deleted: bool,
}

/// Get a single document by ID.
///
/// GET /api/v0/collections/{name}/{docID}
///
/// Identity is extracted from the Authorization header and used for ACP checks.
/// Protected documents require read permission.
///
/// Requires `DocumentRead` permission when NAC is enabled.
pub async fn get_document(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path((collection, doc_id)): Path<(String, String)>,
) -> Result<Json<JsonValue>, HttpError> {
    require_permission(&state, &identity, NodePermission::DocumentRead).await?;

    let rest = state
        .rest
        .as_ref()
        .ok_or_else(|| HttpError::Internal("REST operations not configured".into()))?;

    match rest
        .get_document(&collection, &doc_id, identity.did())
        .await
    {
        Ok(Some(doc)) => Ok(Json(doc)),
        Ok(None) => Err(HttpError::NotFound(format!(
            "Document '{}' not found in collection '{}'",
            doc_id, collection
        ))),
        Err(e) => {
            tracing::warn!(
                collection = %collection,
                doc_id = %doc_id,
                error = %e,
                "Failed to get document"
            );
            Err(e.into())
        }
    }
}

/// Create document(s) in a collection.
///
/// POST /api/v0/collections/{name}
///
/// Accepts either a single document object or an array of documents.
/// Identity is extracted from the Authorization header and used for ACP:
/// - If the collection has a policy and identity is provided, the document
///   is registered with ACP and the identity becomes the owner.
///
/// Requires `DocumentUpdate` permission when NAC is enabled.
pub async fn create_document(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path(collection): Path<String>,
    Json(body): Json<JsonValue>,
) -> Result<Json<JsonValue>, HttpError> {
    require_permission(&state, &identity, NodePermission::DocumentUpdate).await?;

    let rest = state
        .rest
        .as_ref()
        .ok_or_else(|| HttpError::Internal("REST operations not configured".into()))?;

    let result = if body.is_array() {
        let docs: Vec<JsonValue> = body
            .as_array()
            .ok_or_else(|| HttpError::BadRequest("Expected array of documents".into()))?
            .clone();
        rest.create_documents(&collection, docs, identity.did())
            .await
    } else {
        rest.create_document(&collection, body, identity.did())
            .await
            .map(|doc| vec![doc])
    };

    match result {
        Ok(docs) => {
            tracing::info!(
                collection = %collection,
                count = docs.len(),
                "Documents created"
            );
            // Return single doc if single input, array if array input
            let response = if docs.len() == 1 {
                docs.into_iter()
                    .next()
                    .expect("docs.len() == 1 but iterator was empty")
            } else {
                JsonValue::Array(docs)
            };
            Ok(Json(response))
        }
        Err(e) => {
            tracing::warn!(collection = %collection, error = %e, "Failed to create document");
            Err(e.into())
        }
    }
}

/// Update a single document.
///
/// PATCH /api/v0/collections/{name}/{docID}
///
/// Identity is extracted from the Authorization header and used to check
/// update permission on protected documents.
///
/// Requires `DocumentUpdate` permission when NAC is enabled.
pub async fn update_document(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path((collection, doc_id)): Path<(String, String)>,
    Json(patch): Json<JsonValue>,
) -> Result<Json<JsonValue>, HttpError> {
    require_permission(&state, &identity, NodePermission::DocumentUpdate).await?;

    let rest = state
        .rest
        .as_ref()
        .ok_or_else(|| HttpError::Internal("REST operations not configured".into()))?;

    match rest
        .update_document(&collection, &doc_id, patch, identity.did())
        .await
    {
        Ok(doc) => {
            tracing::info!(
                collection = %collection,
                doc_id = %doc_id,
                "Document updated"
            );
            Ok(Json(doc))
        }
        Err(e) => {
            tracing::warn!(
                collection = %collection,
                doc_id = %doc_id,
                error = %e,
                "Failed to update document"
            );
            Err(e.into())
        }
    }
}

/// Delete a single document.
///
/// DELETE /api/v0/collections/{name}/{docID}
///
/// Identity is extracted from the Authorization header and used to check
/// delete permission on protected documents.
///
/// Requires `DocumentDelete` permission when NAC is enabled.
pub async fn delete_document(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path((collection, doc_id)): Path<(String, String)>,
) -> Result<Json<DeleteResponse>, HttpError> {
    require_permission(&state, &identity, NodePermission::DocumentDelete).await?;

    let rest = state
        .rest
        .as_ref()
        .ok_or_else(|| HttpError::Internal("REST operations not configured".into()))?;

    match rest
        .delete_document(&collection, &doc_id, identity.did())
        .await
    {
        Ok(deleted) => {
            if deleted {
                tracing::info!(
                    collection = %collection,
                    doc_id = %doc_id,
                    "Document deleted"
                );
            }
            Ok(Json(DeleteResponse { deleted }))
        }
        Err(e) => {
            tracing::warn!(
                collection = %collection,
                doc_id = %doc_id,
                error = %e,
                "Failed to delete document"
            );
            Err(e.into())
        }
    }
}
