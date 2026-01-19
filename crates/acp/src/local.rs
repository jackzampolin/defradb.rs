//! Local DocumentACP implementation.
//!
//! This implementation stores relation tuples locally and evaluates
//! permission checks against them. SourceHub integration is deferred.

use async_trait::async_trait;
use identity::Did;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

use crate::dac::DocumentACP;
use crate::error::{Error, Result};
use crate::permission::DocumentPermission;
use crate::relation::{RelationTuple, DELETER_RELATION, OWNER_RELATION, READER_RELATION, UPDATER_RELATION};
use crate::store::AcpStore;

/// Known valid relation names that can be added.
/// Owner relation is excluded because it's immutable (set at registration time).
const VALID_ADDABLE_RELATIONS: &[&str] = &[READER_RELATION, UPDATER_RELATION, DELETER_RELATION];

/// Check if a relation name is valid for adding.
fn is_valid_relation(relation: &str) -> bool {
    VALID_ADDABLE_RELATIONS.contains(&relation)
}

/// Local document ACP implementation using in-memory storage.
///
/// This provides ACP functionality without requiring SourceHub.
/// Relation tuples are stored locally and permission checks are
/// evaluated based on the DPI rules (owner + relation unions).
pub struct LocalDocumentACP {
    store: Arc<dyn AcpStore>,
}

impl LocalDocumentACP {
    /// Create a new LocalDocumentACP with the given store.
    pub fn new(store: Arc<dyn AcpStore>) -> Self {
        Self { store }
    }

    /// Check if the subject is the owner of the document.
    async fn is_owner(
        &self,
        subject: &Did,
        collection_id: &str,
        doc_id: &str,
    ) -> Result<bool> {
        let tuple = RelationTuple::owner(subject.clone(), collection_id, doc_id);
        self.store.has_tuple(&tuple).await
    }

    /// Check if subject has a specific relation to the document.
    async fn has_relation(
        &self,
        subject: &Did,
        collection_id: &str,
        doc_id: &str,
        relation: &str,
    ) -> Result<bool> {
        let tuple = RelationTuple::new(subject.clone(), relation, collection_id, doc_id);
        self.store.has_tuple(&tuple).await
    }
}

#[async_trait]
impl DocumentACP for LocalDocumentACP {
    async fn register_doc_object(
        &self,
        identity: &Did,
        _policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<()> {
        // Check if document is already registered
        if self.store.is_doc_registered(resource_name, doc_id).await? {
            return Err(Error::DocumentAlreadyRegistered(format!(
                "{}:{}",
                resource_name, doc_id
            )));
        }

        // Register owner relation
        let tuple = RelationTuple::owner(identity.clone(), resource_name, doc_id);
        self.store.put_tuple(&tuple).await
    }

    async fn is_doc_registered(
        &self,
        _policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<bool> {
        self.store.is_doc_registered(resource_name, doc_id).await
    }

    async fn check_doc_access(
        &self,
        identity: Option<&Did>,
        permission: DocumentPermission,
        _policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<bool> {
        // Check if document is registered
        if !self.store.is_doc_registered(resource_name, doc_id).await? {
            // Unregistered (public) documents allow all access
            return Ok(true);
        }

        // Document is registered, need identity to access
        let identity = match identity {
            Some(id) => id,
            None => return Ok(false), // Anonymous cannot access registered docs
        };

        // Owner always has all permissions (DPI rule: every permission starts with owner)
        if self.is_owner(identity, resource_name, doc_id).await? {
            return Ok(true);
        }

        // Check specific relations based on permission
        // DPI rule: permissions are unions (owner + relation)
        match permission {
            DocumentPermission::Read => {
                // reader OR updater OR deleter grants read (implied read)
                Ok(self.has_relation(identity, resource_name, doc_id, READER_RELATION).await?
                    || self.has_relation(identity, resource_name, doc_id, UPDATER_RELATION).await?
                    || self.has_relation(identity, resource_name, doc_id, DELETER_RELATION).await?)
            }
            DocumentPermission::Update => {
                // updater grants update
                self.has_relation(identity, resource_name, doc_id, UPDATER_RELATION).await
            }
            DocumentPermission::Delete => {
                // deleter grants delete
                self.has_relation(identity, resource_name, doc_id, DELETER_RELATION).await
            }
        }
    }

    async fn add_actor_relationship(
        &self,
        requestor: &Did,
        target: &Did,
        collection_id: &str,
        doc_id: &str,
        relation: &str,
    ) -> Result<bool> {
        // Only owner can add relationships
        if !self.is_owner(requestor, collection_id, doc_id).await? {
            return Err(Error::NotOwner {
                operation: "add actor relationship".to_string(),
            });
        }

        // Cannot add owner relation (it's immutable)
        if relation == OWNER_RELATION {
            return Err(Error::InvalidRelation(
                "cannot add owner relation".to_string(),
            ));
        }

        // Validate relation name against known valid relations
        if !is_valid_relation(relation) {
            return Err(Error::InvalidRelation(format!(
                "unknown relation '{}', valid relations are: reader, updater, deleter",
                relation
            )));
        }

        let tuple = RelationTuple::new(target.clone(), relation, collection_id, doc_id);

        // Check if already exists
        if self.store.has_tuple(&tuple).await? {
            return Ok(false);
        }

        self.store.put_tuple(&tuple).await?;
        Ok(true)
    }

    async fn delete_actor_relationship(
        &self,
        requestor: &Did,
        target: &Did,
        collection_id: &str,
        doc_id: &str,
        relation: &str,
    ) -> Result<bool> {
        // Only owner can delete relationships
        if !self.is_owner(requestor, collection_id, doc_id).await? {
            return Err(Error::NotOwner {
                operation: "delete actor relationship".to_string(),
            });
        }

        // Cannot delete owner relation (it's immutable)
        if relation == OWNER_RELATION {
            return Err(Error::InvalidRelation(
                "cannot delete owner relation".to_string(),
            ));
        }

        let tuple = RelationTuple::new(target.clone(), relation, collection_id, doc_id);

        // Check if exists
        if !self.store.has_tuple(&tuple).await? {
            return Ok(false);
        }

        self.store.delete_tuple(&tuple).await?;
        Ok(true)
    }
}

/// In-memory ACP store for local use and testing.
pub struct MemoryAcpStore {
    tuples: RwLock<HashMap<String, RelationTuple>>,
}

impl MemoryAcpStore {
    /// Create a new in-memory ACP store.
    pub fn new() -> Self {
        Self {
            tuples: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MemoryAcpStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AcpStore for MemoryAcpStore {
    async fn put_tuple(&self, tuple: &RelationTuple) -> Result<()> {
        self.tuples
            .write()
            .insert(tuple.storage_key(), tuple.clone());
        Ok(())
    }

    async fn delete_tuple(&self, tuple: &RelationTuple) -> Result<()> {
        self.tuples.write().remove(&tuple.storage_key());
        Ok(())
    }

    async fn has_tuple(&self, tuple: &RelationTuple) -> Result<bool> {
        Ok(self.tuples.read().contains_key(&tuple.storage_key()))
    }

    async fn get_doc_tuples(&self, collection_id: &str, doc_id: &str) -> Result<Vec<RelationTuple>> {
        let prefix = RelationTuple::doc_prefix(collection_id, doc_id);
        let tuples = self
            .tuples
            .read()
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, v)| v.clone())
            .collect();
        Ok(tuples)
    }

    async fn get_relation_subjects(
        &self,
        collection_id: &str,
        doc_id: &str,
        relation: &str,
    ) -> Result<Vec<Did>> {
        let prefix = RelationTuple::relation_prefix(collection_id, doc_id, relation);
        let subjects = self
            .tuples
            .read()
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, v)| v.subject().clone())
            .collect();
        Ok(subjects)
    }

    async fn get_subject_relations(
        &self,
        subject: &Did,
        collection_id: &str,
        doc_id: &str,
    ) -> Result<Vec<String>> {
        let prefix = RelationTuple::doc_prefix(collection_id, doc_id);
        let relations = self
            .tuples
            .read()
            .iter()
            .filter(|(k, v)| k.starts_with(&prefix) && v.subject() == subject)
            .map(|(_, v)| v.relation().to_string())
            .collect();
        Ok(relations)
    }

    async fn delete_doc_tuples(&self, collection_id: &str, doc_id: &str) -> Result<()> {
        let prefix = RelationTuple::doc_prefix(collection_id, doc_id);
        self.tuples.write().retain(|k, _| !k.starts_with(&prefix));
        Ok(())
    }

    async fn is_doc_registered(&self, collection_id: &str, doc_id: &str) -> Result<bool> {
        let prefix = RelationTuple::doc_prefix(collection_id, doc_id);
        Ok(self.tuples.read().keys().any(|k| k.starts_with(&prefix)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_did() -> Did {
        Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
    }

    fn test_did2() -> Did {
        Did::new("did:key:z6MkfXG2FkNy3u7Eg3jm8e2YQpGz7Z1JqWgHDAP1hLk9r2bR").unwrap()
    }

    fn create_acp() -> LocalDocumentACP {
        LocalDocumentACP::new(Arc::new(MemoryAcpStore::new()))
    }

    // Public Documents tests

    #[tokio::test]
    async fn test_unregistered_doc_allows_all_access() {
        let acp = create_acp();

        // Anonymous can access unregistered doc
        let access = acp
            .check_doc_access(None, DocumentPermission::Read, "policy1", "users", "doc1")
            .await
            .unwrap();
        assert!(access, "unregistered doc should allow anonymous read");

        // Any identity can access unregistered doc
        let access = acp
            .check_doc_access(
                Some(&test_did()),
                DocumentPermission::Update,
                "policy1",
                "users",
                "doc1",
            )
            .await
            .unwrap();
        assert!(access, "unregistered doc should allow any update");
    }

    // Registered Documents tests

    #[tokio::test]
    async fn test_register_doc_creates_owner() {
        let acp = create_acp();
        let owner = test_did();

        acp.register_doc_object(&owner, "policy1", "users", "doc1")
            .await
            .unwrap();

        assert!(acp
            .is_doc_registered("policy1", "users", "doc1")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_register_doc_twice_fails() {
        let acp = create_acp();
        let owner = test_did();

        acp.register_doc_object(&owner, "policy1", "users", "doc1")
            .await
            .unwrap();

        let result = acp
            .register_doc_object(&owner, "policy1", "users", "doc1")
            .await;
        assert!(matches!(result, Err(Error::DocumentAlreadyRegistered(_))));
    }

    #[tokio::test]
    async fn test_owner_has_all_permissions() {
        let acp = create_acp();
        let owner = test_did();

        acp.register_doc_object(&owner, "policy1", "users", "doc1")
            .await
            .unwrap();

        assert!(acp
            .check_doc_access(
                Some(&owner),
                DocumentPermission::Read,
                "policy1",
                "users",
                "doc1"
            )
            .await
            .unwrap());
        assert!(acp
            .check_doc_access(
                Some(&owner),
                DocumentPermission::Update,
                "policy1",
                "users",
                "doc1"
            )
            .await
            .unwrap());
        assert!(acp
            .check_doc_access(
                Some(&owner),
                DocumentPermission::Delete,
                "policy1",
                "users",
                "doc1"
            )
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_anonymous_cannot_access_registered_doc() {
        let acp = create_acp();
        let owner = test_did();

        acp.register_doc_object(&owner, "policy1", "users", "doc1")
            .await
            .unwrap();

        let access = acp
            .check_doc_access(None, DocumentPermission::Read, "policy1", "users", "doc1")
            .await
            .unwrap();
        assert!(!access, "anonymous should not read registered doc");
    }

    #[tokio::test]
    async fn test_non_owner_cannot_access_without_relation() {
        let acp = create_acp();
        let owner = test_did();
        let other = test_did2();

        acp.register_doc_object(&owner, "policy1", "users", "doc1")
            .await
            .unwrap();

        assert!(!acp
            .check_doc_access(
                Some(&other),
                DocumentPermission::Read,
                "policy1",
                "users",
                "doc1"
            )
            .await
            .unwrap());
        assert!(!acp
            .check_doc_access(
                Some(&other),
                DocumentPermission::Update,
                "policy1",
                "users",
                "doc1"
            )
            .await
            .unwrap());
        assert!(!acp
            .check_doc_access(
                Some(&other),
                DocumentPermission::Delete,
                "policy1",
                "users",
                "doc1"
            )
            .await
            .unwrap());
    }

    // Sharing (AddActorRelationship) tests

    #[tokio::test]
    async fn test_add_reader_grants_read_only() {
        let acp = create_acp();
        let owner = test_did();
        let reader = test_did2();

        acp.register_doc_object(&owner, "policy1", "users", "doc1")
            .await
            .unwrap();

        let added = acp
            .add_actor_relationship(&owner, &reader, "users", "doc1", READER_RELATION)
            .await
            .unwrap();
        assert!(added, "relationship should be added");

        // Reader can read
        assert!(acp
            .check_doc_access(
                Some(&reader),
                DocumentPermission::Read,
                "policy1",
                "users",
                "doc1"
            )
            .await
            .unwrap());

        // Reader cannot update
        assert!(!acp
            .check_doc_access(
                Some(&reader),
                DocumentPermission::Update,
                "policy1",
                "users",
                "doc1"
            )
            .await
            .unwrap());

        // Reader cannot delete
        assert!(!acp
            .check_doc_access(
                Some(&reader),
                DocumentPermission::Delete,
                "policy1",
                "users",
                "doc1"
            )
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_add_updater_grants_read_and_update() {
        let acp = create_acp();
        let owner = test_did();
        let updater = test_did2();

        acp.register_doc_object(&owner, "policy1", "users", "doc1")
            .await
            .unwrap();

        acp.add_actor_relationship(&owner, &updater, "users", "doc1", UPDATER_RELATION)
            .await
            .unwrap();

        // Updater can read (implied)
        assert!(acp
            .check_doc_access(
                Some(&updater),
                DocumentPermission::Read,
                "policy1",
                "users",
                "doc1"
            )
            .await
            .unwrap());

        // Updater can update
        assert!(acp
            .check_doc_access(
                Some(&updater),
                DocumentPermission::Update,
                "policy1",
                "users",
                "doc1"
            )
            .await
            .unwrap());

        // Updater cannot delete
        assert!(!acp
            .check_doc_access(
                Some(&updater),
                DocumentPermission::Delete,
                "policy1",
                "users",
                "doc1"
            )
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_non_owner_cannot_add_relationship() {
        let acp = create_acp();
        let owner = test_did();
        let other = test_did2();

        acp.register_doc_object(&owner, "policy1", "users", "doc1")
            .await
            .unwrap();

        let result = acp
            .add_actor_relationship(&other, &owner, "users", "doc1", READER_RELATION)
            .await;
        assert!(matches!(result, Err(Error::NotOwner { .. })));
    }

    #[tokio::test]
    async fn test_cannot_add_owner_relation() {
        let acp = create_acp();
        let owner = test_did();
        let other = test_did2();

        acp.register_doc_object(&owner, "policy1", "users", "doc1")
            .await
            .unwrap();

        let result = acp
            .add_actor_relationship(&owner, &other, "users", "doc1", OWNER_RELATION)
            .await;
        assert!(matches!(result, Err(Error::InvalidRelation(_))));
    }

    #[tokio::test]
    async fn test_cannot_add_unknown_relation() {
        let acp = create_acp();
        let owner = test_did();
        let other = test_did2();

        acp.register_doc_object(&owner, "policy1", "users", "doc1")
            .await
            .unwrap();

        // Try to add a typo/unknown relation
        let result = acp
            .add_actor_relationship(&owner, &other, "users", "doc1", "reador") // typo
            .await;
        assert!(
            matches!(result, Err(Error::InvalidRelation(msg)) if msg.contains("unknown relation")),
            "should reject unknown relation names"
        );

        // Try another unknown relation
        let result = acp
            .add_actor_relationship(&owner, &other, "users", "doc1", "admin")
            .await;
        assert!(
            matches!(result, Err(Error::InvalidRelation(msg)) if msg.contains("unknown relation")),
            "should reject 'admin' as unknown relation"
        );
    }

    #[tokio::test]
    async fn test_add_duplicate_relationship_returns_false() {
        let acp = create_acp();
        let owner = test_did();
        let reader = test_did2();

        acp.register_doc_object(&owner, "policy1", "users", "doc1")
            .await
            .unwrap();

        let added1 = acp
            .add_actor_relationship(&owner, &reader, "users", "doc1", READER_RELATION)
            .await
            .unwrap();
        assert!(added1);

        let added2 = acp
            .add_actor_relationship(&owner, &reader, "users", "doc1", READER_RELATION)
            .await
            .unwrap();
        assert!(!added2, "duplicate add should return false");
    }

    // Delete relationship tests

    #[tokio::test]
    async fn test_delete_relationship() {
        let acp = create_acp();
        let owner = test_did();
        let reader = test_did2();

        acp.register_doc_object(&owner, "policy1", "users", "doc1")
            .await
            .unwrap();

        acp.add_actor_relationship(&owner, &reader, "users", "doc1", READER_RELATION)
            .await
            .unwrap();

        // Verify reader has access
        assert!(acp
            .check_doc_access(
                Some(&reader),
                DocumentPermission::Read,
                "policy1",
                "users",
                "doc1"
            )
            .await
            .unwrap());

        // Delete relationship
        let deleted = acp
            .delete_actor_relationship(&owner, &reader, "users", "doc1", READER_RELATION)
            .await
            .unwrap();
        assert!(deleted);

        // Verify reader no longer has access
        assert!(!acp
            .check_doc_access(
                Some(&reader),
                DocumentPermission::Read,
                "policy1",
                "users",
                "doc1"
            )
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_delete_nonexistent_relationship_returns_false() {
        let acp = create_acp();
        let owner = test_did();
        let other = test_did2();

        acp.register_doc_object(&owner, "policy1", "users", "doc1")
            .await
            .unwrap();

        let deleted = acp
            .delete_actor_relationship(&owner, &other, "users", "doc1", READER_RELATION)
            .await
            .unwrap();
        assert!(!deleted);
    }

    // Deleter relation tests

    #[tokio::test]
    async fn test_add_deleter_grants_read_and_delete() {
        let acp = create_acp();
        let owner = test_did();
        let deleter = test_did2();

        acp.register_doc_object(&owner, "policy1", "users", "doc1")
            .await
            .unwrap();

        acp.add_actor_relationship(&owner, &deleter, "users", "doc1", DELETER_RELATION)
            .await
            .unwrap();

        // Deleter can read (implied by deleter relation)
        assert!(
            acp.check_doc_access(
                Some(&deleter),
                DocumentPermission::Read,
                "policy1",
                "users",
                "doc1"
            )
            .await
            .unwrap(),
            "deleter should have implied read permission"
        );

        // Deleter can delete
        assert!(
            acp.check_doc_access(
                Some(&deleter),
                DocumentPermission::Delete,
                "policy1",
                "users",
                "doc1"
            )
            .await
            .unwrap(),
            "deleter should have delete permission"
        );

        // Deleter CANNOT update
        assert!(
            !acp.check_doc_access(
                Some(&deleter),
                DocumentPermission::Update,
                "policy1",
                "users",
                "doc1"
            )
            .await
            .unwrap(),
            "deleter should NOT have update permission"
        );
    }

    #[tokio::test]
    async fn test_delete_deleter_relationship_revokes_access() {
        let acp = create_acp();
        let owner = test_did();
        let deleter = test_did2();

        acp.register_doc_object(&owner, "policy1", "users", "doc1")
            .await
            .unwrap();

        acp.add_actor_relationship(&owner, &deleter, "users", "doc1", DELETER_RELATION)
            .await
            .unwrap();

        // Verify deleter has access
        assert!(acp
            .check_doc_access(
                Some(&deleter),
                DocumentPermission::Delete,
                "policy1",
                "users",
                "doc1"
            )
            .await
            .unwrap());

        // Delete relationship
        acp.delete_actor_relationship(&owner, &deleter, "users", "doc1", DELETER_RELATION)
            .await
            .unwrap();

        // Verify deleter no longer has delete access
        assert!(!acp
            .check_doc_access(
                Some(&deleter),
                DocumentPermission::Delete,
                "policy1",
                "users",
                "doc1"
            )
            .await
            .unwrap());

        // Verify deleter also lost implied read access
        assert!(!acp
            .check_doc_access(
                Some(&deleter),
                DocumentPermission::Read,
                "policy1",
                "users",
                "doc1"
            )
            .await
            .unwrap());
    }

    // Non-owner cannot delete relationship test

    #[tokio::test]
    async fn test_non_owner_cannot_delete_relationship() {
        let acp = create_acp();
        let owner = test_did();
        let reader = test_did2();
        let attacker = Did::new("did:key:z6MkattackerAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA").unwrap();

        acp.register_doc_object(&owner, "policy1", "users", "doc1")
            .await
            .unwrap();

        acp.add_actor_relationship(&owner, &reader, "users", "doc1", READER_RELATION)
            .await
            .unwrap();

        // Attacker (non-owner) tries to delete reader relationship
        let result = acp
            .delete_actor_relationship(&attacker, &reader, "users", "doc1", READER_RELATION)
            .await;
        assert!(
            matches!(result, Err(Error::NotOwner { .. })),
            "non-owner should not be able to delete relationships"
        );
    }

    #[tokio::test]
    async fn test_cannot_delete_owner_relation() {
        let acp = create_acp();
        let owner = test_did();

        acp.register_doc_object(&owner, "policy1", "users", "doc1")
            .await
            .unwrap();

        // Owner tries to delete their own owner relation
        let result = acp
            .delete_actor_relationship(&owner, &owner, "users", "doc1", OWNER_RELATION)
            .await;
        assert!(
            matches!(result, Err(Error::InvalidRelation(_))),
            "should not be able to delete owner relation"
        );
    }

    // Cross-collection isolation test

    #[tokio::test]
    async fn test_cross_collection_isolation() {
        let acp = create_acp();
        let owner = test_did();
        let reader = test_did2();

        // Register same doc_id in two different collections
        acp.register_doc_object(&owner, "policy1", "users", "doc1")
            .await
            .unwrap();
        acp.register_doc_object(&owner, "policy1", "posts", "doc1")
            .await
            .unwrap();

        // Grant reader access to doc1 in "users" collection ONLY
        acp.add_actor_relationship(&owner, &reader, "users", "doc1", READER_RELATION)
            .await
            .unwrap();

        // Reader CAN access users/doc1
        assert!(
            acp.check_doc_access(
                Some(&reader),
                DocumentPermission::Read,
                "policy1",
                "users",
                "doc1"
            )
            .await
            .unwrap(),
            "reader should access users/doc1"
        );

        // Reader CANNOT access posts/doc1 (different collection, no permission)
        assert!(
            !acp.check_doc_access(
                Some(&reader),
                DocumentPermission::Read,
                "policy1",
                "posts",
                "doc1"
            )
            .await
            .unwrap(),
            "reader should NOT access posts/doc1 (cross-collection isolation)"
        );
    }
}
