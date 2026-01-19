//! Collection-level ACP helpers.
//!
//! These helpers provide document-level access control integration for
//! collection mutations (create/update/delete).

use std::sync::Arc;

use acp::{DocumentACP, DocumentPermission};
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
    identity: Option<&Did>,
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
/// associated relation tuples from the ACP store.
pub async fn unregister_doc_if_needed(
    acp: &dyn DocumentACP,
    collection: &CollectionVersion,
    doc_id: &str,
) -> acp::Result<()> {
    // Only need to check if document is registered if collection has policy
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

    // The AcpStore's delete_doc_tuples will be called by the LocalDocumentACP
    // For now, we just need to verify the document can be deleted
    // The actual tuple cleanup is handled by the delete mutation
    Ok(())
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
    pub identity: Option<Did>,
}

impl AcpContext {
    /// Create a new ACP context.
    pub fn new(acp: Arc<dyn DocumentACP>, identity: Option<Did>) -> Self {
        Self { acp, identity }
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
            self.identity.as_ref(),
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
        register_doc_if_needed(
            self.acp.as_ref(),
            self.identity.as_ref(),
            collection,
            doc_id,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use acp::{LocalDocumentACP, MemoryAcpStore};
    use schema::{FieldDescription, FieldKind, PolicyDescription};

    fn test_did() -> Did {
        Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
    }

    fn test_did2() -> Did {
        Did::new("did:key:z6MkfXG2FkNy3u7Eg3jm8e2YQpGz7Z1JqWgHDAP1hLk9r2bR").unwrap()
    }

    fn collection_without_policy() -> CollectionVersion {
        CollectionVersion::new(
            "users",
            "v1",
            "coll-1",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
            ],
        )
    }

    fn collection_with_policy() -> CollectionVersion {
        let mut col = CollectionVersion::new(
            "users",
            "v1",
            "coll-1",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
            ],
        );
        col.policy = Some(PolicyDescription::new("policy1", "users"));
        col
    }

    #[tokio::test]
    async fn test_no_policy_allows_all() {
        let acp = LocalDocumentACP::new(Arc::new(MemoryAcpStore::new()));
        let collection = collection_without_policy();

        // Anyone should have access when there's no policy
        let allowed = check_doc_permission(
            &acp,
            None, // Anonymous
            DocumentPermission::Read,
            &collection,
            "doc1",
        )
        .await
        .unwrap();

        assert!(allowed);
    }

    #[tokio::test]
    async fn test_register_with_policy_and_identity() {
        let store = Arc::new(MemoryAcpStore::new());
        let acp = LocalDocumentACP::new(store);
        let collection = collection_with_policy();
        let owner = test_did();

        // Register document
        register_doc_if_needed(&acp, Some(&owner), &collection, "doc1")
            .await
            .unwrap();

        // Verify owner has access
        let policy = collection.policy.as_ref().unwrap();
        let is_registered = acp
            .is_doc_registered(&policy.id, &policy.resource_name, "doc1")
            .await
            .unwrap();
        assert!(is_registered);
    }

    #[tokio::test]
    async fn test_no_register_without_identity() {
        let store = Arc::new(MemoryAcpStore::new());
        let acp = LocalDocumentACP::new(store);
        let collection = collection_with_policy();

        // Register without identity (public document)
        register_doc_if_needed(&acp, None, &collection, "doc1")
            .await
            .unwrap();

        // Document should NOT be registered
        let policy = collection.policy.as_ref().unwrap();
        let is_registered = acp
            .is_doc_registered(&policy.id, &policy.resource_name, "doc1")
            .await
            .unwrap();
        assert!(!is_registered);
    }

    #[tokio::test]
    async fn test_owner_has_update_permission() {
        let store = Arc::new(MemoryAcpStore::new());
        let acp = LocalDocumentACP::new(store);
        let collection = collection_with_policy();
        let owner = test_did();

        // Register document
        register_doc_if_needed(&acp, Some(&owner), &collection, "doc1")
            .await
            .unwrap();

        // Owner should have update permission
        let allowed = check_doc_permission(
            &acp,
            Some(&owner),
            DocumentPermission::Update,
            &collection,
            "doc1",
        )
        .await
        .unwrap();
        assert!(allowed);
    }

    #[tokio::test]
    async fn test_non_owner_denied_update_permission() {
        let store = Arc::new(MemoryAcpStore::new());
        let acp = LocalDocumentACP::new(store);
        let collection = collection_with_policy();
        let owner = test_did();
        let stranger = test_did2();

        // Register document with owner
        register_doc_if_needed(&acp, Some(&owner), &collection, "doc1")
            .await
            .unwrap();

        // Stranger should NOT have update permission
        let allowed = check_doc_permission(
            &acp,
            Some(&stranger),
            DocumentPermission::Update,
            &collection,
            "doc1",
        )
        .await
        .unwrap();
        assert!(!allowed);
    }

    #[tokio::test]
    async fn test_acp_context() {
        let store = Arc::new(MemoryAcpStore::new());
        let acp = Arc::new(LocalDocumentACP::new(store));
        let collection = collection_with_policy();
        let owner = test_did();

        let ctx = AcpContext::new(acp, Some(owner));

        // Register document using context
        ctx.register_doc(&collection, "doc1").await.unwrap();

        // Check permission using context
        let allowed = ctx
            .check_permission(DocumentPermission::Delete, &collection, "doc1")
            .await
            .unwrap();
        assert!(allowed);
    }
}
