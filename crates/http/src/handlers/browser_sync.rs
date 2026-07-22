use axum::extract::State;
use axum::Json;

use defra_core::browser_sync::{
    BrowserSyncRequest, BrowserSyncResponse, MAX_SYNC_DOCUMENTS_PER_REQUEST, MAX_SYNC_PULL_DOC_IDS,
};

use crate::error::HttpError;
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::query_context::resolve_dac_bypass;
use crate::router::{AppState, BrowserSyncError, NodePermission};

pub async fn sync(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(request): Json<BrowserSyncRequest>,
) -> Result<Json<BrowserSyncResponse>, HttpError> {
    if request.documents.len() > MAX_SYNC_DOCUMENTS_PER_REQUEST {
        return Err(HttpError::BadRequest(format!(
            "sync request exceeds {MAX_SYNC_DOCUMENTS_PER_REQUEST} documents"
        )));
    }
    if request
        .pull
        .as_ref()
        .is_some_and(|pull| pull.doc_ids.len() > MAX_SYNC_PULL_DOC_IDS)
    {
        return Err(HttpError::BadRequest(format!(
            "sync pull exceeds {MAX_SYNC_PULL_DOC_IDS} document IDs"
        )));
    }

    if request.pull.is_some() {
        require_permission(&state, &identity, NodePermission::DocumentRead).await?;
    }
    if !request.documents.is_empty() {
        require_permission(&state, &identity, NodePermission::DocumentUpdate).await?;
    }

    let bypass_dac = resolve_dac_bypass(&state, &identity).await;
    let response = state
        .require_browser_sync()?
        .sync(request, identity.did().map(|did| did.as_str()), bypass_dac)
        .await
        .map_err(|error| match error {
            BrowserSyncError::InvalidInput(message) => HttpError::UnprocessableEntity(message),
            BrowserSyncError::Forbidden(message) => HttpError::Forbidden(message),
            BrowserSyncError::Internal(message) => HttpError::Internal(message),
        })?;
    Ok(Json(response))
}

#[cfg(test)]
#[path = "browser_sync_tests.rs"]
mod tests;
