//! Zanzibar-based DocumentACP implementation.
//!
//! Implements the DocumentACP trait using the full Zanzibar permission model
//! with computed usersets and set operations.

mod document_acp;

use async_lock::RwLock;
use identity::Did;
use std::sync::Arc;

use super::engine::PermissionEngine;
use super::expression::RelationExpression;
use super::store::ZanzibarStore;
use super::types::{Policy, Relation, Resource};
use crate::error::{Error, Result};
use crate::permission::DocumentPermission;

/// Relation names for document permissions.
pub const OWNER_RELATION: &str = "owner";
pub const READER_RELATION: &str = "reader";
pub const UPDATER_RELATION: &str = "updater";
pub const DELETER_RELATION: &str = "deleter";
pub const ADMIN_RELATION: &str = "admin";

/// DocumentACP implementation using the Zanzibar permission model.
///
/// This implementation uses computed usersets to model permission inheritance:
/// - `owner` implies all permissions
/// - `reader` = direct readers + owner (read permission)
/// - `updater` = direct updaters + owner (update permission)
/// - `deleter` = direct deleters + owner (delete permission)
pub struct ZanzibarDocumentACP<S: ZanzibarStore> {
    store: Arc<S>,
    engine: RwLock<PermissionEngine<S>>,
}

impl<S: ZanzibarStore> ZanzibarDocumentACP<S> {
    /// Create a new ZanzibarDocumentACP with the given store.
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store: store.clone(),
            engine: RwLock::new(PermissionEngine::new(store)),
        }
    }

    /// Create a default document policy for a collection.
    ///
    /// This creates a policy with the standard DPI relations:
    /// - owner: direct relation (base case)
    /// - admin: direct relation that manages [reader, updater, deleter]
    /// - reader: owner + admin + direct readers
    /// - updater: owner + admin + direct updaters
    /// - deleter: owner + admin + direct deleters
    pub fn create_default_policy(policy_id: &str, resource_name: &str) -> Policy {
        Policy::new(policy_id, format!("Policy for {}", resource_name)).with_resource(
            Resource::new(resource_name)
                .with_relation(Relation::direct(OWNER_RELATION))
                // Admin relation with manages capability
                .with_relation(
                    Relation::computed(
                        ADMIN_RELATION,
                        RelationExpression::union(vec![
                            RelationExpression::this(),
                            RelationExpression::computed_userset(OWNER_RELATION),
                        ]),
                    )
                    .with_manages(vec![
                        READER_RELATION,
                        UPDATER_RELATION,
                        DELETER_RELATION,
                    ]),
                )
                .with_relation(Relation::computed(
                    READER_RELATION,
                    RelationExpression::union(vec![
                        RelationExpression::this(),
                        RelationExpression::computed_userset(OWNER_RELATION),
                        RelationExpression::computed_userset(ADMIN_RELATION),
                        // Updater and deleter also imply read
                        RelationExpression::computed_userset(UPDATER_RELATION),
                        RelationExpression::computed_userset(DELETER_RELATION),
                    ]),
                ))
                .with_relation(Relation::computed(
                    UPDATER_RELATION,
                    RelationExpression::union(vec![
                        RelationExpression::this(),
                        RelationExpression::computed_userset(OWNER_RELATION),
                        RelationExpression::computed_userset(ADMIN_RELATION),
                    ]),
                ))
                .with_relation(Relation::computed(
                    DELETER_RELATION,
                    RelationExpression::union(vec![
                        RelationExpression::this(),
                        RelationExpression::computed_userset(OWNER_RELATION),
                        RelationExpression::computed_userset(ADMIN_RELATION),
                    ]),
                )),
        )
    }

    /// Ensure a policy exists for the given policy_id and resource.
    ///
    /// Creates a default policy if one doesn't exist.
    async fn ensure_policy(&self, policy_id: &str, resource_name: &str) -> Result<()> {
        let exists = {
            let engine = self.engine.read().await;
            engine.lookup.has_policy(policy_id)
        };

        if !exists {
            // Check if policy exists in store
            if let Some(policy) = self.store.get_policy(policy_id).await? {
                let mut engine = self.engine.write().await;
                engine.add_policy(&policy);
            } else {
                // Create default policy
                let policy = Self::create_default_policy(policy_id, resource_name);
                self.store.store_policy(&policy).await?;
                let mut engine = self.engine.write().await;
                engine.add_policy(&policy);
            }
        }

        Ok(())
    }

    /// Check if subject is the owner of the document.
    async fn is_owner(
        &self,
        subject: &Did,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<bool> {
        self.store
            .check_permission_direct(policy_id, resource_name, doc_id, OWNER_RELATION, subject)
            .await
    }

    /// Check if subject can manage a given relation (is owner OR has a managing relation).
    ///
    /// DefraDB pattern: actors can manage relationships if they are either:
    /// 1. The owner of the object, OR
    /// 2. Have a relation that has the target relation in its `manages` list
    ///
    /// Returns `Ok(true)` if authorized, or an appropriate error if not.
    async fn check_manage_relation(
        &self,
        subject: &Did,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
        target_relation: &str,
        operation: &str,
    ) -> Result<()> {
        // Check if owner first (fast path)
        if self
            .is_owner(subject, policy_id, resource_name, doc_id)
            .await?
        {
            return Ok(());
        }

        // Get the policy to find managing relations
        let policy = match self.store.get_policy(policy_id).await? {
            Some(p) => p,
            None => {
                return Err(Error::NotOwner {
                    operation: format!("{} actor relationship", operation),
                });
            }
        };

        // Find all relations that can manage the target relation
        let managers = policy.get_managers_for_relation(resource_name, target_relation);
        let has_managers = !managers.is_empty();

        // Check if subject has any of the managing relations
        for manager_relation in managers {
            let has_manager = self
                .store
                .check_permission_direct(
                    policy_id,
                    resource_name,
                    doc_id,
                    manager_relation,
                    subject,
                )
                .await?;

            if has_manager {
                tracing::debug!(
                    target: "acp::audit",
                    event = "manager_authorized",
                    subject = %subject,
                    manager_relation = %manager_relation,
                    target_relation = %target_relation,
                    collection = %resource_name,
                    doc_id = %doc_id,
                    "Subject authorized via manager relation"
                );
                return Ok(());
            }
        }

        if has_managers {
            Err(Error::NotManager {
                operation: format!("{} relationship", operation),
            })
        } else {
            Err(Error::NotOwner {
                operation: format!("{} actor relationship", operation),
            })
        }
    }

    /// Map DocumentPermission to relation name.
    fn permission_to_relation(permission: DocumentPermission) -> &'static str {
        match permission {
            DocumentPermission::Read => READER_RELATION,
            DocumentPermission::Update => UPDATER_RELATION,
            DocumentPermission::Delete => DELETER_RELATION,
        }
    }

    /// Invalidate cached policy, forcing reload on next access.
    ///
    /// Call this after modifying a policy to ensure the cache is updated.
    pub async fn invalidate_policy_cache(&self, policy_id: &str) {
        let mut engine = self.engine.write().await;
        engine.remove_policy(policy_id);
    }

    /// Reload a policy from the store, updating the cache.
    pub async fn reload_policy(&self, policy_id: &str) -> Result<()> {
        let mut engine = self.engine.write().await;
        engine.reload_policy(policy_id).await
    }

    /// Clear all cached policies.
    pub async fn clear_policy_cache(&self) {
        let mut engine = self.engine.write().await;
        engine.clear_cache();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dac::DocumentACP;
    use crate::identity::Identity;
    use crate::zanzibar::store::MemoryZanzibarStore;

    fn test_did() -> Did {
        Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
    }

    fn test_did2() -> Did {
        Did::new("did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH").unwrap()
    }

    #[tokio::test]
    async fn test_register_document() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let acp = ZanzibarDocumentACP::new(store);

        let owner = test_did();

        // Register document
        acp.register_doc_object(&owner, "policy1", "documents", "doc1")
            .await
            .unwrap();

        // Check registration
        let registered = acp
            .is_doc_registered("policy1", "documents", "doc1")
            .await
            .unwrap();
        assert!(registered);

        // Double registration should fail
        let result = acp
            .register_doc_object(&owner, "policy1", "documents", "doc1")
            .await;
        assert!(matches!(result, Err(Error::DocumentAlreadyRegistered(_))));
    }

    #[tokio::test]
    async fn test_owner_has_all_permissions() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let acp = ZanzibarDocumentACP::new(store);

        let owner = test_did();

        acp.register_doc_object(&owner, "policy1", "documents", "doc1")
            .await
            .unwrap();

        let identity = Identity::Authenticated(owner);

        // Owner should have read permission
        let can_read = acp
            .check_doc_access(
                &identity,
                DocumentPermission::Read,
                "policy1",
                "documents",
                "doc1",
            )
            .await
            .unwrap();
        assert!(can_read);

        // Owner should have update permission
        let can_update = acp
            .check_doc_access(
                &identity,
                DocumentPermission::Update,
                "policy1",
                "documents",
                "doc1",
            )
            .await
            .unwrap();
        assert!(can_update);

        // Owner should have delete permission
        let can_delete = acp
            .check_doc_access(
                &identity,
                DocumentPermission::Delete,
                "policy1",
                "documents",
                "doc1",
            )
            .await
            .unwrap();
        assert!(can_delete);
    }

    #[tokio::test]
    async fn test_unregistered_doc_is_public() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let acp = ZanzibarDocumentACP::new(store);

        // Check access to unregistered doc - should be allowed
        let can_read = acp
            .check_doc_access(
                &Identity::Anonymous,
                DocumentPermission::Read,
                "policy1",
                "documents",
                "doc1",
            )
            .await
            .unwrap();
        assert!(can_read);
    }

    #[tokio::test]
    async fn test_anonymous_cannot_access_registered() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let acp = ZanzibarDocumentACP::new(store);

        let owner = test_did();

        acp.register_doc_object(&owner, "policy1", "documents", "doc1")
            .await
            .unwrap();

        // Anonymous should not be able to read
        let can_read = acp
            .check_doc_access(
                &Identity::Anonymous,
                DocumentPermission::Read,
                "policy1",
                "documents",
                "doc1",
            )
            .await
            .unwrap();
        assert!(!can_read);
    }

    #[tokio::test]
    async fn test_add_reader_relationship() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let acp = ZanzibarDocumentACP::new(store);

        let owner = test_did();
        let reader = test_did2();

        acp.register_doc_object(&owner, "collection1", "collection1", "doc1")
            .await
            .unwrap();

        // Add reader relationship
        let added = acp
            .add_actor_relationship(
                &owner,
                &reader,
                "collection1",
                "collection1",
                "doc1",
                "reader",
                &[],
            )
            .await
            .unwrap();
        assert!(added);

        // Reader should now have read access
        let can_read = acp
            .check_doc_access(
                &Identity::Authenticated(reader.clone()),
                DocumentPermission::Read,
                "collection1",
                "collection1",
                "doc1",
            )
            .await
            .unwrap();
        assert!(can_read);

        // Reader should not have update access
        let can_update = acp
            .check_doc_access(
                &Identity::Authenticated(reader),
                DocumentPermission::Update,
                "collection1",
                "collection1",
                "doc1",
            )
            .await
            .unwrap();
        assert!(!can_update);
    }

    #[tokio::test]
    async fn test_updater_implies_read() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let acp = ZanzibarDocumentACP::new(store);

        let owner = test_did();
        let updater = test_did2();

        acp.register_doc_object(&owner, "collection1", "collection1", "doc1")
            .await
            .unwrap();

        // Add updater relationship
        acp.add_actor_relationship(
            &owner,
            &updater,
            "collection1",
            "collection1",
            "doc1",
            "updater",
            &[],
        )
        .await
        .unwrap();

        // Updater should have read access (implied)
        let can_read = acp
            .check_doc_access(
                &Identity::Authenticated(updater.clone()),
                DocumentPermission::Read,
                "collection1",
                "collection1",
                "doc1",
            )
            .await
            .unwrap();
        assert!(can_read);

        // Updater should have update access
        let can_update = acp
            .check_doc_access(
                &Identity::Authenticated(updater),
                DocumentPermission::Update,
                "collection1",
                "collection1",
                "doc1",
            )
            .await
            .unwrap();
        assert!(can_update);
    }

    #[tokio::test]
    async fn test_non_owner_cannot_add_relationship() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let acp = ZanzibarDocumentACP::new(store);

        let owner = test_did();
        let non_owner = test_did2();

        acp.register_doc_object(&owner, "collection1", "collection1", "doc1")
            .await
            .unwrap();

        // Non-owner should not be able to add relationships
        // Returns NotManager because admin manages reader in the default policy
        let result = acp
            .add_actor_relationship(
                &non_owner,
                &owner,
                "collection1",
                "collection1",
                "doc1",
                "reader",
                &[],
            )
            .await;

        assert!(matches!(result, Err(Error::NotManager { .. })));
    }

    #[tokio::test]
    async fn test_delete_relationship() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let acp = ZanzibarDocumentACP::new(store);

        let owner = test_did();
        let reader = test_did2();

        acp.register_doc_object(&owner, "collection1", "collection1", "doc1")
            .await
            .unwrap();

        // Add reader relationship
        acp.add_actor_relationship(
            &owner,
            &reader,
            "collection1",
            "collection1",
            "doc1",
            "reader",
            &[],
        )
        .await
        .unwrap();

        // Delete relationship
        let deleted = acp
            .delete_actor_relationship(
                &owner,
                &reader,
                "collection1",
                "collection1",
                "doc1",
                "reader",
                &[],
            )
            .await
            .unwrap();
        assert!(deleted);

        // Reader should no longer have access
        let can_read = acp
            .check_doc_access(
                &Identity::Authenticated(reader),
                DocumentPermission::Read,
                "collection1",
                "collection1",
                "doc1",
            )
            .await
            .unwrap();
        assert!(!can_read);
    }

    #[tokio::test]
    async fn test_unregister_document() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let acp = ZanzibarDocumentACP::new(store);

        let owner = test_did();

        acp.register_doc_object(&owner, "policy1", "documents", "doc1")
            .await
            .unwrap();

        // Unregister
        acp.unregister_doc_object("policy1", "documents", "doc1")
            .await
            .unwrap();

        // Document should no longer be registered
        let registered = acp
            .is_doc_registered("policy1", "documents", "doc1")
            .await
            .unwrap();
        assert!(!registered);
    }

    #[tokio::test]
    async fn test_cannot_add_owner_relation() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let acp = ZanzibarDocumentACP::new(store);

        let owner = test_did();
        let target = test_did2();

        acp.register_doc_object(&owner, "collection1", "collection1", "doc1")
            .await
            .unwrap();

        let result = acp
            .add_actor_relationship(
                &owner,
                &target,
                "collection1",
                "collection1",
                "doc1",
                "owner",
                &[],
            )
            .await;

        assert!(matches!(result, Err(Error::InvalidRelation(_))));
    }

    #[tokio::test]
    async fn test_invalid_relation() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let acp = ZanzibarDocumentACP::new(store);

        let owner = test_did();
        let target = test_did2();

        acp.register_doc_object(&owner, "collection1", "collection1", "doc1")
            .await
            .unwrap();

        let result = acp
            .add_actor_relationship(
                &owner,
                &target,
                "collection1",
                "collection1",
                "doc1",
                "invalid_relation",
                &[],
            )
            .await;

        assert!(matches!(result, Err(Error::InvalidRelation(_))));
    }

    // ==========================================================================
    // Manager Delegation Pattern Tests
    // ==========================================================================

    fn test_did3() -> Did {
        Did::new("did:key:z6MknSLrJoTcukLrE435hVNQT4JUhbvWLX4kUzqkEStBU8Vi").unwrap()
    }

    #[tokio::test]
    async fn test_admin_can_add_reader_relationship() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let acp = ZanzibarDocumentACP::new(store);

        let owner = test_did();
        let admin = test_did2();
        let reader = test_did3();

        // Register document
        acp.register_doc_object(&owner, "collection1", "collection1", "doc1")
            .await
            .unwrap();

        // Owner adds admin
        acp.add_actor_relationship(
            &owner,
            &admin,
            "collection1",
            "collection1",
            "doc1",
            "admin",
            &[],
        )
        .await
        .unwrap();

        // Admin should be able to add reader (admin manages reader)
        let added = acp
            .add_actor_relationship(
                &admin,
                &reader,
                "collection1",
                "collection1",
                "doc1",
                "reader",
                &[],
            )
            .await
            .unwrap();
        assert!(added);

        // Reader should now have read access
        let can_read = acp
            .check_doc_access(
                &Identity::Authenticated(reader),
                DocumentPermission::Read,
                "collection1",
                "collection1",
                "doc1",
            )
            .await
            .unwrap();
        assert!(can_read);
    }

    #[tokio::test]
    async fn test_admin_can_delete_reader_relationship() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let acp = ZanzibarDocumentACP::new(store);

        let owner = test_did();
        let admin = test_did2();
        let reader = test_did3();

        // Register document
        acp.register_doc_object(&owner, "collection1", "collection1", "doc1")
            .await
            .unwrap();

        // Owner adds admin
        acp.add_actor_relationship(
            &owner,
            &admin,
            "collection1",
            "collection1",
            "doc1",
            "admin",
            &[],
        )
        .await
        .unwrap();

        // Owner adds reader
        acp.add_actor_relationship(
            &owner,
            &reader,
            "collection1",
            "collection1",
            "doc1",
            "reader",
            &[],
        )
        .await
        .unwrap();

        // Admin should be able to delete reader (admin manages reader)
        let deleted = acp
            .delete_actor_relationship(
                &admin,
                &reader,
                "collection1",
                "collection1",
                "doc1",
                "reader",
                &[],
            )
            .await
            .unwrap();
        assert!(deleted);

        // Reader should no longer have access
        let can_read = acp
            .check_doc_access(
                &Identity::Authenticated(reader),
                DocumentPermission::Read,
                "collection1",
                "collection1",
                "doc1",
            )
            .await
            .unwrap();
        assert!(!can_read);
    }

    #[tokio::test]
    async fn test_admin_cannot_add_admin_relationship() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let acp = ZanzibarDocumentACP::new(store);

        let owner = test_did();
        let admin = test_did2();
        let other = test_did3();

        // Register document
        acp.register_doc_object(&owner, "collection1", "collection1", "doc1")
            .await
            .unwrap();

        // Owner adds admin
        acp.add_actor_relationship(
            &owner,
            &admin,
            "collection1",
            "collection1",
            "doc1",
            "admin",
            &[],
        )
        .await
        .unwrap();

        // Admin should NOT be able to add another admin (admin doesn't manage admin)
        let result = acp
            .add_actor_relationship(
                &admin,
                &other,
                "collection1",
                "collection1",
                "doc1",
                "admin",
                &[],
            )
            .await;
        assert!(matches!(result, Err(Error::NotOwner { .. })));
    }

    #[tokio::test]
    async fn test_reader_cannot_add_relationships() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let acp = ZanzibarDocumentACP::new(store);

        let owner = test_did();
        let reader = test_did2();
        let other = test_did3();

        // Register document
        acp.register_doc_object(&owner, "collection1", "collection1", "doc1")
            .await
            .unwrap();

        // Owner adds reader
        acp.add_actor_relationship(
            &owner,
            &reader,
            "collection1",
            "collection1",
            "doc1",
            "reader",
            &[],
        )
        .await
        .unwrap();

        // Reader should NOT be able to add another reader
        // Returns NotManager because admin manages reader, but reader doesn't have admin relation
        let result = acp
            .add_actor_relationship(
                &reader,
                &other,
                "collection1",
                "collection1",
                "doc1",
                "reader",
                &[],
            )
            .await;
        assert!(matches!(result, Err(Error::NotManager { .. })));
    }

    #[tokio::test]
    async fn test_admin_has_read_update_delete_permissions() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let acp = ZanzibarDocumentACP::new(store);

        let owner = test_did();
        let admin = test_did2();

        // Register document
        acp.register_doc_object(&owner, "collection1", "collection1", "doc1")
            .await
            .unwrap();

        // Owner adds admin
        acp.add_actor_relationship(
            &owner,
            &admin,
            "collection1",
            "collection1",
            "doc1",
            "admin",
            &[],
        )
        .await
        .unwrap();

        let admin_identity = Identity::Authenticated(admin);

        // Admin should have read access (reader includes admin)
        let can_read = acp
            .check_doc_access(
                &admin_identity,
                DocumentPermission::Read,
                "collection1",
                "collection1",
                "doc1",
            )
            .await
            .unwrap();
        assert!(can_read);

        // Admin should have update access (updater includes admin)
        let can_update = acp
            .check_doc_access(
                &admin_identity,
                DocumentPermission::Update,
                "collection1",
                "collection1",
                "doc1",
            )
            .await
            .unwrap();
        assert!(can_update);

        // Admin should have delete access (deleter includes admin)
        let can_delete = acp
            .check_doc_access(
                &admin_identity,
                DocumentPermission::Delete,
                "collection1",
                "collection1",
                "doc1",
            )
            .await
            .unwrap();
        assert!(can_delete);
    }

    #[tokio::test]
    async fn test_revoking_admin_removes_management_capability() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let acp = ZanzibarDocumentACP::new(store);

        let owner = test_did();
        let admin = test_did2();
        let reader = test_did3();

        // Register document
        acp.register_doc_object(&owner, "collection1", "collection1", "doc1")
            .await
            .unwrap();

        // Owner adds admin
        acp.add_actor_relationship(
            &owner,
            &admin,
            "collection1",
            "collection1",
            "doc1",
            "admin",
            &[],
        )
        .await
        .unwrap();

        // Owner revokes admin
        acp.delete_actor_relationship(
            &owner,
            &admin,
            "collection1",
            "collection1",
            "doc1",
            "admin",
            &[],
        )
        .await
        .unwrap();

        // Former admin should NOT be able to add reader anymore
        // Returns NotManager because admin manages reader, but former admin no longer has admin relation
        let result = acp
            .add_actor_relationship(
                &admin,
                &reader,
                "collection1",
                "collection1",
                "doc1",
                "reader",
                &[],
            )
            .await;
        assert!(matches!(result, Err(Error::NotManager { .. })));
    }
}
