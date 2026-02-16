//! Transaction-scoped operation handlers.
//!
//! These handlers provide HTTP access to operations that execute within
//! an existing transaction (e.g., setting migrations, reading collections
//! including uncommitted writes).
//!
//! All endpoints enforce NAC permissions when NAC is enabled.

use axum::extract::{Path, State};
use axum::Json;
use serde::Deserialize;

use crate::error::HttpError;
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AppState, NodePermission};

/// Path parameter for transaction-scoped routes.
#[derive(Debug, Deserialize)]
pub struct TxnPathParam {
    pub id: String,
}

/// Request body for setting a migration in a transaction.
#[derive(Debug, Deserialize)]
pub struct SetMigrationInTxnRequest {
    #[serde(rename = "Config", alias = "config")]
    pub config: String,
}

/// Set a lens migration within an existing transaction.
///
/// POST /api/v0/tx/{id}/lens
///
/// The transaction must have been started via `POST /api/v0/tx`.
/// The migration is registered within the transaction and only becomes
/// visible after the transaction is committed.
///
/// Requires `CollectionPatch` permission when NAC is enabled.
pub async fn set_migration_in_txn(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path(params): Path<TxnPathParam>,
    Json(body): Json<SetMigrationInTxnRequest>,
) -> Result<Json<serde_json::Value>, HttpError> {
    require_permission(&state, &identity, NodePermission::CollectionPatch).await?;

    let txn_ops = state.require_txn_ops()?;

    let txn_id = format!("txn-{}", params.id);

    let transform_id = txn_ops
        .set_migration_in_txn(&txn_id, &body.config)
        .await
        .map_err(HttpError::BadRequest)?;

    Ok(Json(serde_json::json!({ "lensId": transform_id })))
}

/// Get all collection versions visible within a transaction.
///
/// GET /api/v0/tx/{id}/collections
///
/// Returns collection versions including any uncommitted writes made
/// within the specified transaction.
///
/// Requires `CollectionGet` permission when NAC is enabled.
pub async fn get_collections_in_txn(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path(params): Path<TxnPathParam>,
) -> Result<Json<Vec<schema::CollectionVersion>>, HttpError> {
    require_permission(&state, &identity, NodePermission::CollectionGet).await?;

    let txn_ops = state.require_txn_ops()?;

    let txn_id = format!("txn-{}", params.id);

    let collections = txn_ops
        .get_collections_in_txn(&txn_id)
        .await
        .map_err(HttpError::BadRequest)?;

    Ok(Json(collections))
}
