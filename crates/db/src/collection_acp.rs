//! Collection-level ACP helpers.
//!
//! These helpers provide document-level access control integration for
//! collection mutations (create/update/delete).

use std::sync::Arc;

use acp::{DocumentACP, DocumentPermission, Identity};
use identity::Did;
use schema::CollectionVersion;

/// Check if identity has permission for a document operation.
///
/// Returns true if:
/// 1. Collection has no policy (ACP not enforced)
/// 2. Document is unregistered (public)
/// 3. Identity has the required permission
pub async fn check_doc_permission(
    acp: &dyn DocumentACP,
    identity: &Identity,
    permission: DocumentPermission,
    collection: &CollectionVersion,
    doc_id: &str,
) -> acp::Result<bool> {
    // If collection has no policy, ACP is not enforced
    let policy = match &collection.policy {
        Some(p) => p,
        None => return Ok(true),
    };

    acp.check_doc_access(
        identity,
        permission,
        &policy.id,
        &policy.resource_name,
        doc_id,
    )
    .await
}

/// Register a document with ACP after creation.
///
/// Only registers if:
/// 1. Collection has a policy
/// 2. Identity is provided
///
/// If collection has no policy or no identity is provided, the document
/// remains unregistered (public).
pub async fn register_doc_if_needed(
    acp: &dyn DocumentACP,
    identity: Option<&Did>,
    collection: &CollectionVersion,
    doc_id: &str,
) -> acp::Result<()> {
    // Only register if collection has policy AND identity is provided
    let (policy, did) = match (&collection.policy, identity) {
        (Some(p), Some(id)) => (p, id),
        _ => return Ok(()), // No policy or no identity = public document
    };

    acp.register_doc_object(did, &policy.id, &policy.resource_name, doc_id)
        .await
}

/// Clean up ACP relations when deleting a document.
///
/// This should be called when deleting a document to remove all
/// associated relation tuples from the ACP store (owner, reader, updater, deleter).
pub async fn unregister_doc_if_needed(
    acp: &dyn DocumentACP,
    collection: &CollectionVersion,
    doc_id: &str,
) -> acp::Result<()> {
    // Only need to clean up if collection has policy
    let policy = match &collection.policy {
        Some(p) => p,
        None => return Ok(()), // No policy = no ACP tuples to clean up
    };

    // Check if document is registered
    if !acp
        .is_doc_registered(&policy.id, &policy.resource_name, doc_id)
        .await?
    {
        return Ok(()); // Not registered, nothing to clean up
    }

    // Delete all ACP tuples for this document
    acp.unregister_doc_object(&policy.id, &policy.resource_name, doc_id)
        .await
}

/// ACP context for mutation operations.
///
/// This wraps the DocumentACP and identity for convenient access
/// during collection mutations.
#[derive(Clone)]
pub struct AcpContext {
    /// Document ACP for permission checks
    pub acp: Arc<dyn DocumentACP>,
    /// Identity making the request
    pub identity: Identity,
}

impl AcpContext {
    /// Create a new ACP context.
    pub fn new(acp: Arc<dyn DocumentACP>, identity: Identity) -> Self {
        Self { acp, identity }
    }

    /// Create from an optional DID for backward compatibility.
    pub fn from_optional_did(acp: Arc<dyn DocumentACP>, did: Option<Did>) -> Self {
        Self {
            acp,
            identity: Identity::from(did),
        }
    }

    /// Check if identity has permission for a document operation.
    pub async fn check_permission(
        &self,
        permission: DocumentPermission,
        collection: &CollectionVersion,
        doc_id: &str,
    ) -> acp::Result<bool> {
        check_doc_permission(
            self.acp.as_ref(),
            &self.identity,
            permission,
            collection,
            doc_id,
        )
        .await
    }

    /// Register a document after creation.
    pub async fn register_doc(
        &self,
        collection: &CollectionVersion,
        doc_id: &str,
    ) -> acp::Result<()> {
        register_doc_if_needed(self.acp.as_ref(), self.identity.did(), collection, doc_id).await
    }
}

// Tests extracted to crates/db/tests/collection_acp_tests.rs
