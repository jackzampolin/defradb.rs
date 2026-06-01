//! Searchable-encryption remote query transport.
//!
//! Injection seam mirroring the `DocumentACP` pattern: the query crate defines
//! a thin trait and the P2P-aware implementation lives in `db-merge` (which can
//! see `p2p`, `crypto`, and `se`). This keeps `query` free of crypto/SE/P2P
//! dependencies while still allowing `execute_encrypted_select` to fan a search
//! out to replicators.
//!
//! Tag generation (HMAC over the SE key + identity + value) happens entirely on
//! the implementation side, so the trait passes raw `(field, json value)` pairs.

use async_trait::async_trait;
use serde_json::Value as JsonValue;

/// Resolves `encrypted_<Collection>` equality queries by fanning search tags
/// out to replicators and returning the matching document IDs.
///
/// Matches Go's `Coordinator.QueryDocIDsByValues`: the querying node is the
/// document owner, which queries its replicators; it never serves from local
/// state. Zero replicators yields an empty result.
#[async_trait]
pub trait SeQueryTransport: Send + Sync {
    /// Query replicators for document IDs whose encrypted index tags match the
    /// given `_eq` conditions on `collection_id` (the collection version ID).
    ///
    /// `eq_conditions` is a list of `(field_name, equality_value)` pairs. The
    /// implementation generates a search tag per condition and intersects the
    /// results. Returns an error string on transport failure.
    async fn query_doc_ids(
        &self,
        collection_id: &str,
        eq_conditions: Vec<(String, JsonValue)>,
    ) -> std::result::Result<Vec<String>, String>;
}
