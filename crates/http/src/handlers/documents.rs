//! Document REST endpoint handlers.
//!
//! These handlers extract identity from the Authorization header and pass it
//! to the REST operations layer for ACP (Access Control Policy) enforcement.
//!
//! All endpoints enforce NAC permissions when NAC is enabled.
//!
//! Note: Create, update, and delete operations return empty bodies to match
//! Go DefraDB behavior. Go returns only HTTP 200 status with no body.

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::error::HttpError;
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AppState, NodePermission};

/// Get a single document by ID.
///
/// GET /api/v0/collections/{name}/document/{docID}
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
            "document not found or not authorized: {}",
            doc_id
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
///
/// Returns HTTP 200 with empty body to match Go DefraDB behavior.
pub async fn create_document(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path(collection): Path<String>,
    Json(body): Json<JsonValue>,
) -> Result<StatusCode, HttpError> {
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
            // Return empty body to match Go DefraDB behavior
            Ok(StatusCode::OK)
        }
        Err(e) => {
            tracing::warn!(collection = %collection, error = %e, "Failed to create document");
            Err(e.into())
        }
    }
}

/// Update a single document.
///
/// PATCH /api/v0/collections/{name}/document/{docID}
///
/// Identity is extracted from the Authorization header and used to check
/// update permission on protected documents.
///
/// Requires `DocumentUpdate` permission when NAC is enabled.
///
/// Returns HTTP 200 with empty body to match Go DefraDB behavior.
pub async fn update_document(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path((collection, doc_id)): Path<(String, String)>,
    Json(patch): Json<JsonValue>,
) -> Result<StatusCode, HttpError> {
    require_permission(&state, &identity, NodePermission::DocumentUpdate).await?;

    let rest = state
        .rest
        .as_ref()
        .ok_or_else(|| HttpError::Internal("REST operations not configured".into()))?;

    match rest
        .update_document(&collection, &doc_id, patch, identity.did())
        .await
    {
        Ok(_doc) => {
            tracing::info!(
                collection = %collection,
                doc_id = %doc_id,
                "Document updated"
            );
            // Return empty body to match Go DefraDB behavior
            Ok(StatusCode::OK)
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
/// DELETE /api/v0/collections/{name}/document/{docID}
///
/// Identity is extracted from the Authorization header and used to check
/// delete permission on protected documents.
///
/// Requires `DocumentDelete` permission when NAC is enabled.
///
/// Returns HTTP 200 with empty body to match Go DefraDB behavior.
pub async fn delete_document(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path((collection, doc_id)): Path<(String, String)>,
) -> Result<StatusCode, HttpError> {
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
            // Return empty body to match Go DefraDB behavior
            Ok(StatusCode::OK)
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

/// Go's `DeleteCollectionRequest` (`http/handler_collection.go:32`).
#[derive(Debug, Deserialize)]
pub struct DeleteDocumentsRequest {
    pub filter: Option<JsonValue>,
}

/// Go's `client.DeleteResult` / `client.UpdateResult` (`client/collection.go`).
///
/// Go's fields carry no json tags, so they marshal capitalised. A client
/// reading `Count` off a lowercase `count` sees nothing.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct DocumentsResult {
    #[serde(rename = "Count")]
    pub count: usize,
    #[serde(rename = "DocIDs")]
    pub doc_ids: Vec<String>,
}

impl From<Vec<String>> for DocumentsResult {
    fn from(doc_ids: Vec<String>) -> Self {
        Self {
            count: doc_ids.len(),
            doc_ids,
        }
    }
}

/// Read the `filter` a filtered mutation must have.
///
/// A missing or null filter is refused rather than treated as match-all. Go's
/// behaviour for that case could not be confirmed against its source here, and
/// the failure mode of guessing wrong is deleting or rewriting every document
/// in the collection, which is exactly the silent-destruction bug this route
/// is being fixed for. Refusing is recoverable; guessing is not.
pub(crate) fn required_filter(body: &Bytes, field: &str) -> Result<JsonValue, HttpError> {
    let parsed: JsonValue = serde_json::from_slice(body)
        .map_err(|e| HttpError::BadRequest(format!("invalid request body: {e}")))?;

    match parsed.get(field) {
        Some(JsonValue::Null) | None => Err(HttpError::BadRequest(format!(
            "'{field}' is required; send a filter object to select the documents to act on"
        ))),
        Some(filter) => Ok(filter.clone()),
    }
}

/// Delete every document matching a filter.
///
/// DELETE /api/v0/collections/{name}
///
/// This is Go's `DeleteDocumentsWithFilter` (`http/handler_collection.go:511`),
/// which its own client calls by `DELETE`ing this path with `{"filter": ...}`
/// (`http/client_document.go:299-321`). Rust used to drop the collection and
/// every one of its versions here, and answer success, so a Go-compatible
/// client asking to delete a few documents destroyed the collection instead.
///
/// Dropping a collection lives on `DELETE /api/v0/collections?name=...`.
///
/// Requires `DocumentDelete` permission when NAC is enabled.
pub async fn delete_documents_with_filter(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path(collection): Path<String>,
    body: Bytes,
) -> Result<Json<DocumentsResult>, HttpError> {
    require_permission(&state, &identity, NodePermission::DocumentDelete).await?;

    let filter = required_filter(&body, "filter")?;

    let rest = state
        .rest
        .as_ref()
        .ok_or_else(|| HttpError::Internal("REST operations not configured".into()))?;

    match rest
        .delete_documents_with_filter(&collection, &filter, identity.did())
        .await
    {
        Ok(doc_ids) => Ok(Json(doc_ids.into())),
        Err(e) => {
            tracing::warn!(
                collection = %collection,
                error = %e,
                "Failed to delete documents with filter"
            );
            Err(e.into())
        }
    }
}
