//! REST operations trait definition.

use async_trait::async_trait;
use identity::Did;
use serde_json::Value as JsonValue;
use storage::corekv::MaybeSendSync;

use super::error::RestResult;

/// REST operations trait for collection and document CRUD.
///
/// This trait provides REST-specific operations separate from GraphQL execution.
/// Each operation runs with auto-commit semantics (one transaction per operation).
///
/// # Identity and ACP
///
/// All document operations accept an optional `identity` parameter for access control.
/// When provided, the identity is used for ACP (Access Control Policy) permission checks:
/// - Read operations check read permission on protected documents
/// - Create operations register the document with the identity as owner
/// - Update/Delete operations check the corresponding permissions
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait RestOperations: MaybeSendSync {
    /// List all collection names.
    async fn list_collections(&self) -> RestResult<Vec<String>>;

    /// Get all document IDs in a collection.
    async fn get_collection_doc_ids(
        &self,
        collection: &str,
        identity: Option<&Did>,
    ) -> RestResult<Vec<String>>;

    /// Get a single document by ID.
    async fn get_document(
        &self,
        collection: &str,
        doc_id: &str,
        identity: Option<&Did>,
    ) -> RestResult<Option<JsonValue>>;

    /// Create a single document.
    async fn create_document(
        &self,
        collection: &str,
        data: JsonValue,
        identity: Option<&Did>,
    ) -> RestResult<JsonValue>;

    /// Create multiple documents.
    async fn create_documents(
        &self,
        collection: &str,
        data: Vec<JsonValue>,
        identity: Option<&Did>,
    ) -> RestResult<Vec<JsonValue>>;

    /// Update a single document.
    async fn update_document(
        &self,
        collection: &str,
        doc_id: &str,
        patch: JsonValue,
        identity: Option<&Did>,
    ) -> RestResult<JsonValue>;

    /// Delete a single document.
    async fn delete_document(
        &self,
        collection: &str,
        doc_id: &str,
        identity: Option<&Did>,
    ) -> RestResult<bool>;
}
