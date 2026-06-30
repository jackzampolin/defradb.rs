//! P2P FFI functions for DefraDB.
//!
//! This module provides FFI functions for P2P networking operations:
//! - Peer info and connection
//! - Replicator management
//! - P2P collection management

use std::fmt;

mod collections;
mod documents;
mod node;
mod peer;
mod push;
mod replicator;
mod sync;
mod version_sync;

pub use collections::{p2p_add_collections, p2p_delete_collections, p2p_list_collections};
pub use documents::{p2p_add_documents, p2p_delete_documents, p2p_list_documents};
pub use node::new_node_with_p2p;
pub use peer::{
    p2p_active_peers, p2p_connect, p2p_disconnect, p2p_notify_network_change, p2p_peer_info,
};
pub use push::p2p_retry_replicators;
pub use replicator::{
    p2p_add_replicator, p2p_add_replicator_with_filter, p2p_delete_replicator, p2p_list_replicators,
};
pub use sync::{p2p_sync_branchable_collection, p2p_sync_documents};
pub use version_sync::p2p_sync_collection_versions;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FfiP2PErrorCode {
    InvalidInput,
    NotFound,
    Unsupported,
    Transport,
    Internal,
}

/// Internal P2P error classification for the FFI layer.
///
/// The public C ABI still only exposes `status + error string`, but keeping
/// a structured error inside Rust lets us standardize the boundary now and add
/// error codes later without re-auditing every entrypoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FfiP2PError {
    pub(crate) code: FfiP2PErrorCode,
    pub(crate) message: String,
}

pub(crate) type FfiP2PResult<T> = Result<T, FfiP2PError>;

impl FfiP2PError {
    pub(crate) fn invalid_input(message: impl Into<String>) -> Self {
        Self {
            code: FfiP2PErrorCode::InvalidInput,
            message: message.into(),
        }
    }

    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self {
            code: FfiP2PErrorCode::NotFound,
            message: message.into(),
        }
    }

    pub(crate) fn unsupported(message: impl Into<String>) -> Self {
        Self {
            code: FfiP2PErrorCode::Unsupported,
            message: message.into(),
        }
    }

    pub(crate) fn transport(message: impl Into<String>) -> Self {
        Self {
            code: FfiP2PErrorCode::Transport,
            message: message.into(),
        }
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self {
            code: FfiP2PErrorCode::Internal,
            message: message.into(),
        }
    }

    pub(crate) fn no_p2p_system() -> Self {
        Self::unsupported("no p2p system configured")
    }

    pub(crate) fn invalid_node_handle() -> Self {
        Self::invalid_input(crate::ERR_INVALID_NODE_HANDLE)
    }
}

impl fmt::Display for FfiP2PError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl From<defra_http::router::P2PError> for FfiP2PError {
    fn from(error: defra_http::router::P2PError) -> Self {
        match error {
            defra_http::router::P2PError::InvalidInput(message) => Self::invalid_input(message),
            defra_http::router::P2PError::NotFound(message) => Self::not_found(message),
            defra_http::router::P2PError::Unsupported(message) => Self::unsupported(message),
            defra_http::router::P2PError::Transport(message) => Self::transport(message),
            defra_http::router::P2PError::Internal(message) => Self::internal(message),
            _ => Self::internal(error.to_string()),
        }
    }
}

impl From<String> for FfiP2PError {
    fn from(message: String) -> Self {
        Self::internal(message)
    }
}

impl From<&str> for FfiP2PError {
    fn from(message: &str) -> Self {
        Self::internal(message.to_string())
    }
}

pub(crate) fn into_ffi_result(result: FfiP2PResult<String>) -> crate::types::FfiResult {
    match result {
        Ok(json) => crate::types::FfiResult::success(json),
        Err(error) => crate::types::FfiResult::error(error.message),
    }
}

pub(crate) fn into_ffi_ok(result: FfiP2PResult<()>) -> crate::types::FfiResult {
    match result {
        Ok(()) => crate::types::FfiResult::ok(),
        Err(error) => crate::types::FfiResult::error(error.message),
    }
}

/// Parse a JSON array of collection names.
///
/// Expects format like: `["collection1", "collection2"]`
/// Also handles JSON `null` (treated as empty array).
pub(crate) fn parse_collections_json(json_str: &str) -> FfiP2PResult<Vec<String>> {
    let opt: Option<Vec<String>> = serde_json::from_str(json_str)
        .map_err(|e| FfiP2PError::invalid_input(format!("invalid collections JSON: {}", e)))?;
    Ok(opt.unwrap_or_default())
}

/// Parse a JSON map of per-collection replication filters keyed by collection name.
///
/// Accepts `null`, `{}`, or an empty string as an empty (unfiltered) map. The
/// accepted shape mirrors the HTTP `"Filters"` field (`ReplicationFilters`).
pub(crate) fn parse_filters_json(
    json_str: &str,
) -> FfiP2PResult<defra_http::router::ReplicationFilters> {
    if json_str.is_empty() {
        return Ok(defra_http::router::ReplicationFilters::new());
    }

    let opt: Option<defra_http::router::ReplicationFilters> = serde_json::from_str(json_str)
        .map_err(|e| FfiP2PError::invalid_input(format!("invalid filters JSON: {}", e)))?;
    Ok(opt.unwrap_or_default())
}

pub(crate) fn parse_doc_ids_json(json_str: &str) -> FfiP2PResult<Vec<String>> {
    let opt: Option<Vec<String>> = serde_json::from_str(json_str)
        .map_err(|e| FfiP2PError::invalid_input(format!("invalid doc_ids JSON: {}", e)))?;
    Ok(opt.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::{parse_collections_json, FfiP2PError, FfiP2PErrorCode};

    #[test]
    fn maps_http_error_variants() {
        let error = FfiP2PError::from(defra_http::router::P2PError::InvalidInput(
            "bad addr".to_string(),
        ));
        assert_eq!(error.code, FfiP2PErrorCode::InvalidInput);
        assert_eq!(error.message, "bad addr");

        let error = FfiP2PError::from(defra_http::router::P2PError::Transport(
            "dial failed".to_string(),
        ));
        assert_eq!(error.code, FfiP2PErrorCode::Transport);
        assert_eq!(error.message, "dial failed");
    }

    #[test]
    fn parses_invalid_collections_as_invalid_input() {
        let error = parse_collections_json("{").unwrap_err();
        assert_eq!(error.code, FfiP2PErrorCode::InvalidInput);
        assert!(error.message.contains("invalid collections JSON"));
    }

    #[test]
    fn parse_filters_valid_map_with_http_conditions() {
        use super::parse_filters_json;

        let json = r#"{
            "Users": {"Conditions": {"agent_did": {"_eq": "did:key:z6"}}},
            "Posts": {"Conditions": {"active": {"_eq": true}}}
        }"#;
        let filters = parse_filters_json(json).unwrap();
        assert_eq!(filters.len(), 2);
        assert!(filters.contains_key("Users"));
        assert!(filters.contains_key("Posts"));
        assert!(filters["Users"].conditions.is_some());
    }

    #[test]
    fn parse_filters_legacy_shape() {
        use super::parse_filters_json;

        let json = r#"{"Users": {"Field": "agent_did", "Value": "did:key:z6"}}"#;
        let filters = parse_filters_json(json).unwrap();
        assert_eq!(filters.len(), 1);
        assert!(filters.contains_key("Users"));
    }

    #[test]
    fn parse_filters_accepts_predicate_alias() {
        use super::parse_filters_json;

        let json = r#"{"Users": {"predicate": {"agent_did": {"_eq": "did:key:z6"}}}}"#;
        let filters = parse_filters_json(json).unwrap();
        assert_eq!(filters.len(), 1);
        assert!(filters["Users"].conditions.is_some());
    }

    #[test]
    fn parse_filters_null_or_empty_yields_empty_map() {
        use super::parse_filters_json;

        assert!(parse_filters_json("null").unwrap().is_empty());
        assert!(parse_filters_json("{}").unwrap().is_empty());
        assert!(parse_filters_json("").unwrap().is_empty());
    }

    #[test]
    fn parse_filters_invalid_json_is_invalid_input() {
        use super::parse_filters_json;

        let err = parse_filters_json("{not json}").unwrap_err();
        assert_eq!(err.code, FfiP2PErrorCode::InvalidInput);
        assert!(err.message.contains("invalid filters JSON"));
    }
}
