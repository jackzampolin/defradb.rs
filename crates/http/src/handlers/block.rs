use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::Json;
use base64::Engine as _;
use serde::{Deserialize, Serialize};

use crate::error::{http_error_from_backend_message, HttpError};
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AppState, NodePermission};

/// Query parameters for block verify-signature (Go-compatible).
#[derive(Debug, Deserialize)]
pub struct VerifySignatureParams {
    pub cid: String,
    #[serde(rename = "public-key")]
    pub public_key: String,
    #[serde(rename = "type")]
    pub key_type: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SignedBlockParams {
    pub cid: String,
}

#[derive(Debug, Serialize)]
pub struct SignedBlockResponse {
    pub cid: String,
    pub block: String,
    pub signature: String,
}

/// Return canonical signed-block material after document ACP authorization.
/// The client must still verify both CIDs and the signature locally.
pub async fn signed_block(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Query(params): Query<SignedBlockParams>,
) -> Result<Json<SignedBlockResponse>, HttpError> {
    require_permission(&state, &identity, NodePermission::SignatureVerify).await?;
    let block = state.require_block()?;
    let caller_did = identity.did().map(|did| did.to_string());
    let (block_bytes, signature_bytes) = block
        .signed_block_bytes(&params.cid, caller_did.as_deref())
        .await
        .map_err(http_error_from_backend_message)?;
    let encoder = base64::engine::general_purpose::STANDARD;
    Ok(Json(SignedBlockResponse {
        cid: params.cid,
        block: encoder.encode(block_bytes),
        signature: encoder.encode(signature_bytes),
    }))
}

/// Verify the signature of a block.
///
/// GET /api/v0/block/verify-signature?cid=<cid>&public-key=<key>&type=<type>
///
/// Requires `SignatureVerify` permission when NAC is enabled.
pub async fn verify_signature(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Query(params): Query<VerifySignatureParams>,
) -> Result<StatusCode, HttpError> {
    require_permission(&state, &identity, NodePermission::SignatureVerify).await?;

    let block = state.require_block()?;
    let caller_did = identity.did().map(|d| d.to_string());
    block
        .verify_signature(
            &params.cid,
            &params.public_key,
            params.key_type.as_deref(),
            caller_did.as_deref(),
        )
        .await
        .map_err(http_error_from_backend_message)?;
    Ok(StatusCode::OK)
}

#[cfg(test)]
#[path = "block_tests.rs"]
mod tests;
