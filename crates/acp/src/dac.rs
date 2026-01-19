//! Document ACP trait (matches Go acp/dac/dac.go)
//!
//! This trait defines the interface for document-level access control.

use async_trait::async_trait;
use identity::Did;

use crate::error::Result;
use crate::permission::DocumentPermission;

/// Document ACP interface (matches Go acp/dac/dac.go)
///
/// This trait provides document-level access control operations:
/// - Registration: Documents are registered with an owner when created with identity
/// - Access checks: Verify if an identity has permission to perform an operation
/// - Relationship management: Add/remove actor relationships for sharing
#[async_trait]
pub trait DocumentACP: Send + Sync {
    /// Register a document with creator as owner.
    ///
    /// This is called when a document is created with an identity.
    /// The identity becomes the owner of the document.
    ///
    /// # Arguments
    /// * `identity` - The DID of the document creator (becomes owner)
    /// * `policy_id` - The policy ID from the collection
    /// * `resource_name` - The resource name from the policy
    /// * `doc_id` - The document ID being registered
    async fn register_doc_object(
        &self,
        identity: &Did,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<()>;

    /// Check if document is registered with ACP.
    ///
    /// Documents created without identity are unregistered (public).
    /// Documents created with identity are registered.
    async fn is_doc_registered(
        &self,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<bool>;

    /// Check if identity has permission on document.
    ///
    /// # Access Rules
    /// 1. If document is unregistered (public) -> allow all
    /// 2. If identity is None -> deny (anonymous cannot access registered docs)
    /// 3. If identity is owner -> allow all
    /// 4. Check if identity has specific relation granting permission
    ///
    /// # Arguments
    /// * `identity` - The DID of the requester (None for anonymous)
    /// * `permission` - The permission being requested
    /// * `policy_id` - The policy ID from the collection
    /// * `resource_name` - The resource name from the policy
    /// * `doc_id` - The document ID being accessed
    async fn check_doc_access(
        &self,
        identity: Option<&Did>,
        permission: DocumentPermission,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<bool>;

    /// Add actor relationship (sharing).
    ///
    /// Only the document owner can add relationships.
    ///
    /// # Arguments
    /// * `requestor` - The DID making the request (must be owner)
    /// * `target` - The DID to grant the relation to
    /// * `collection_id` - The collection ID (used as resource identifier)
    /// * `doc_id` - The document ID
    /// * `relation` - The relation to grant (e.g., "reader", "updater")
    ///
    /// # Returns
    /// * `Ok(true)` - Relationship was added
    /// * `Ok(false)` - Relationship already exists
    async fn add_actor_relationship(
        &self,
        requestor: &Did,
        target: &Did,
        collection_id: &str,
        doc_id: &str,
        relation: &str,
    ) -> Result<bool>;

    /// Remove actor relationship.
    ///
    /// Only the document owner can remove relationships.
    ///
    /// # Arguments
    /// * `requestor` - The DID making the request (must be owner)
    /// * `target` - The DID to remove the relation from
    /// * `collection_id` - The collection ID
    /// * `doc_id` - The document ID
    /// * `relation` - The relation to remove
    ///
    /// # Returns
    /// * `Ok(true)` - Relationship was removed
    /// * `Ok(false)` - Relationship didn't exist
    async fn delete_actor_relationship(
        &self,
        requestor: &Did,
        target: &Did,
        collection_id: &str,
        doc_id: &str,
        relation: &str,
    ) -> Result<bool>;

    /// Unregister a document, removing all ACP tuples.
    ///
    /// This should be called when a document is deleted to clean up
    /// all associated relation tuples (owner, reader, updater, etc.).
    ///
    /// # Arguments
    /// * `policy_id` - The policy ID from the collection
    /// * `resource_name` - The resource name from the policy
    /// * `doc_id` - The document ID being unregistered
    async fn unregister_doc_object(
        &self,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<()>;
}
