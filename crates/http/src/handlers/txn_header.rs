//! Shared transaction header handling for HTTP endpoints.

use axum::http::HeaderMap;

use crate::error::HttpError;
use crate::handlers::graphql::TX_HEADER_NAME;

/// Extract the Go-compatible transaction header.
///
/// New HTTP handlers should prefer this header form for client parity. The
/// path-scoped `/tx/{id}/...` endpoints remain available where already exposed.
pub(crate) fn txn_id_from_headers(headers: &HeaderMap) -> Result<Option<&str>, HttpError> {
    let Some(txn_id) = headers.get(TX_HEADER_NAME) else {
        return Ok(None);
    };

    let txn_id = txn_id
        .to_str()
        .map_err(|_| HttpError::BadRequest("invalid transaction id header".to_string()))?;

    if txn_id.is_empty() {
        tracing::debug!("ignoring empty x-defradb-tx header");
        return Ok(None);
    }

    Ok(Some(txn_id))
}
