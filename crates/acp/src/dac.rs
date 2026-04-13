//! Document ACP trait (matches Go acp/dac/dac.go)
//!
//! This trait defines the interface for document-level access control.

use async_trait::async_trait;
use identity::Did;
use serde::{Deserialize, Serialize};
use storage::corekv::MaybeSendSync;

use crate::error::Result;
use crate::identity::Identity;
use crate::permission::DocumentPermission;

/// A document-scoped actor relationship snapshot suitable for P2P replication.
///
/// `actor` is either a DID string or `"*"` for wildcard grants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicatedActorRelationship {
    pub relation: String,
    pub actor: String,
}

/// Replicated actor relationships for a single document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicatedDocActorRelationships {
    pub policy_id: String,
    pub resource_name: String,
    pub relationships: Vec<ReplicatedActorRelationship>,
}

/// Document ACP interface (matches Go acp/dac/dac.go)
///
/// This trait provides document-level access control operations:
/// - Registration: Documents are registered with an owner when created with identity
/// - Access checks: Verify if an identity has permission to perform an operation
/// - Relationship management: Add/remove actor relationships for sharing
#[allow(clippy::too_many_arguments)]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait DocumentACP: MaybeSendSync {
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

    /// Get the current owner of a registered document, if available.
    ///
    /// This is used by replication paths that need to preserve the document's
    /// owner identity when replaying previously-created blocks.
    async fn get_doc_owner(
        &self,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<Option<Did>> {
        let _ = (policy_id, resource_name, doc_id);
        Ok(None)
    }

    /// Check if identity has permission on document.
    ///
    /// # Access Rules
    /// 1. If document is unregistered (public) -> allow all
    /// 2. If identity is Anonymous -> deny (anonymous cannot access registered docs)
    /// 3. If identity is owner -> allow all
    /// 4. Check if identity has specific relation granting permission
    ///
    /// # Arguments
    /// * `identity` - The identity of the requester (Anonymous or Authenticated)
    /// * `permission` - The permission being requested
    /// * `policy_id` - The policy ID from the collection
    /// * `resource_name` - The resource name from the policy
    /// * `doc_id` - The document ID being accessed
    async fn check_doc_access(
        &self,
        identity: &Identity,
        permission: DocumentPermission,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<bool>;

    /// Create a persisted access decision for a caller/object/permission check.
    ///
    /// SourceHub-backed ACP can use this to mint a decision that downstream
    /// signers verify via a light client before producing a signature.
    ///
    /// Returns `Ok(Some(decision_id))` when the backend created a reusable
    /// decision, `Ok(None)` when the backend does not support this flow.
    async fn create_access_decision(
        &self,
        identity: &Identity,
        policy_id: &str,
        resource_name: &str,
        object_id: &str,
        permission: &str,
    ) -> Result<Option<String>> {
        let _ = (identity, policy_id, resource_name, object_id, permission);
        Ok(None)
    }

    /// Add actor relationship (sharing).
    ///
    /// The requestor must be the document owner or have a managing relation
    /// that grants them authority to add the requested relation.
    ///
    /// # Arguments
    /// * `requestor` - The DID making the request (must be owner or manager)
    /// * `target` - The DID to grant the relation to
    /// * `policy_id` - The policy ID from the collection schema
    /// * `collection_id` - The collection ID (used as resource identifier)
    /// * `doc_id` - The document ID
    /// * `relation` - The relation to grant (e.g., "reader", "updater")
    /// * `managing_relations` - Relations that can manage `relation` (from policy)
    ///
    /// # Returns
    /// * `Ok(true)` - Relationship was added
    /// * `Ok(false)` - Relationship already exists
    async fn add_actor_relationship(
        &self,
        requestor: &Did,
        target: &Did,
        policy_id: &str,
        collection_id: &str,
        doc_id: &str,
        relation: &str,
        managing_relations: &[String],
    ) -> Result<bool>;

    /// Remove actor relationship.
    ///
    /// The requestor must be the document owner or have a managing relation
    /// that grants them authority to remove the requested relation.
    ///
    /// # Arguments
    /// * `requestor` - The DID making the request (must be owner or manager)
    /// * `target` - The DID to remove the relation from
    /// * `policy_id` - The policy ID from the collection schema
    /// * `collection_id` - The collection ID
    /// * `doc_id` - The document ID
    /// * `relation` - The relation to remove
    /// * `managing_relations` - Relations that can manage `relation` (from policy)
    ///
    /// # Returns
    /// * `Ok(true)` - Relationship was removed
    /// * `Ok(false)` - Relationship didn't exist
    async fn delete_actor_relationship(
        &self,
        requestor: &Did,
        target: &Did,
        policy_id: &str,
        collection_id: &str,
        doc_id: &str,
        relation: &str,
        managing_relations: &[String],
    ) -> Result<bool>;

    /// Export the current non-owner actor relationships for a document.
    ///
    /// This is used by Rust-only local ACP P2P replication to propagate
    /// relationship grants and revocations alongside document republishes.
    async fn export_actor_relationships(
        &self,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<Vec<ReplicatedActorRelationship>> {
        let _ = (policy_id, resource_name, doc_id);
        Ok(Vec::new())
    }

    /// Replace the current non-owner actor relationships for a document.
    ///
    /// Implementations should reconcile the local document-scoped actor
    /// relationships to match the provided snapshot while preserving the
    /// existing owner registration.
    async fn replace_actor_relationships(
        &self,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
        relationships: &[ReplicatedActorRelationship],
    ) -> Result<()> {
        let _ = (policy_id, resource_name, doc_id, relationships);
        Ok(())
    }

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
