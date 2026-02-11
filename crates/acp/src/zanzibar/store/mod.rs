//! Zanzibar storage trait and implementations.
//!
//! Defines the ZanzibarStore trait for storing policies and relationships,
//! with memory and persistent implementations.

mod memory;
mod persistent;
mod traits;

pub use memory::MemoryZanzibarStore;
pub use persistent::PersistentZanzibarStore;
pub use traits::ZanzibarStore;

/// Options for storing a policy.
#[derive(Debug, Clone, Default)]
pub struct StorePolicyOptions {
    /// If true, validate the policy structure before storing.
    pub validate: bool,
    /// If true, enforce DPI (DefraDB Policy Interface) compliance.
    /// DPI rules:
    /// - Every resource must have an 'owner' relation
    /// - Computed expressions must include 'owner'
    /// - Only union operations are allowed
    pub enforce_dpi: bool,
}

impl StorePolicyOptions {
    /// Create options with no validation.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable basic policy validation.
    pub fn with_validation(mut self) -> Self {
        self.validate = true;
        self
    }

    /// Enable DPI compliance enforcement.
    pub fn with_dpi_enforcement(mut self) -> Self {
        self.enforce_dpi = true;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zanzibar::expression::RelationExpression;
    use crate::zanzibar::types::{Policy, Relation, Relationship, Resource, Subject};
    use identity::Did;

    fn test_did() -> Did {
        Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
    }

    fn test_did2() -> Did {
        Did::new("did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH").unwrap()
    }

    #[tokio::test]
    async fn test_memory_store_policy() {
        let store = MemoryZanzibarStore::new();
        let policy = Policy::new("policy1", "Test Policy");

        store.store_policy(&policy).await.unwrap();

        let loaded = store.get_policy("policy1").await.unwrap();
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().id, "policy1");

        // Non-existent policy
        let missing = store.get_policy("missing").await.unwrap();
        assert!(missing.is_none());
    }

    #[tokio::test]
    async fn test_memory_store_relationship() {
        let store = MemoryZanzibarStore::new();
        let did = test_did();

        let rel = Relationship::with_entity("document", "doc1", "owner", did.clone());
        store.store_relationship("policy1", &rel).await.unwrap();

        // Check direct relationship
        let has = store
            .has_relationship(
                "policy1",
                "document",
                "doc1",
                "owner",
                &Subject::Entity(did.clone()),
            )
            .await
            .unwrap();
        assert!(has);

        // Check permission direct
        let perm = store
            .check_permission_direct("policy1", "document", "doc1", "owner", &did)
            .await
            .unwrap();
        assert!(perm);

        // Non-existent relationship
        let missing = store
            .has_relationship(
                "policy1",
                "document",
                "doc1",
                "reader",
                &Subject::Entity(did.clone()),
            )
            .await
            .unwrap();
        assert!(!missing);
    }

    #[tokio::test]
    async fn test_memory_store_wildcard() {
        let store = MemoryZanzibarStore::new();
        let did = test_did();

        // Store wildcard relationship
        let rel = Relationship::new("document", "doc1", "viewer", Subject::Wildcard);
        store.store_relationship("policy1", &rel).await.unwrap();

        // Any user should have permission via wildcard
        let perm = store
            .check_permission_direct("policy1", "document", "doc1", "viewer", &did)
            .await
            .unwrap();
        assert!(perm);
    }

    #[tokio::test]
    async fn test_memory_store_typed_wildcard() {
        let store = MemoryZanzibarStore::new();
        let did = test_did();

        // Store typed wildcard relationship (user:*)
        let rel = Relationship::new(
            "document",
            "doc1",
            "viewer",
            Subject::typed_wildcard("user"),
        );
        store.store_relationship("policy1", &rel).await.unwrap();

        // Any user should have permission via typed wildcard
        // (DIDs don't carry resource type, so typed wildcards match any entity)
        let perm = store
            .check_permission_direct("policy1", "document", "doc1", "viewer", &did)
            .await
            .unwrap();
        assert!(perm);

        // A different user should also match
        let did2 = test_did2();
        let perm2 = store
            .check_permission_direct("policy1", "document", "doc1", "viewer", &did2)
            .await
            .unwrap();
        assert!(perm2);
    }

    #[tokio::test]
    async fn test_memory_store_get_subjects() {
        let store = MemoryZanzibarStore::new();
        let did1 = test_did();
        let did2 = test_did2();

        let rel1 = Relationship::with_entity("document", "doc1", "reader", did1.clone());
        let rel2 = Relationship::with_entity("document", "doc1", "reader", did2.clone());

        store.store_relationship("policy1", &rel1).await.unwrap();
        store.store_relationship("policy1", &rel2).await.unwrap();

        let subjects = store
            .get_relation_subjects("policy1", "document", "doc1", "reader")
            .await
            .unwrap();

        assert_eq!(subjects.len(), 2);
    }

    #[tokio::test]
    async fn test_memory_store_delete_object() {
        let store = MemoryZanzibarStore::new();
        let did = test_did();

        let rel1 = Relationship::with_entity("document", "doc1", "owner", did.clone());
        let rel2 = Relationship::with_entity("document", "doc1", "reader", did.clone());

        store.store_relationship("policy1", &rel1).await.unwrap();
        store.store_relationship("policy1", &rel2).await.unwrap();

        // Delete all relationships for doc1
        store
            .delete_object_relationships("policy1", "document", "doc1")
            .await
            .unwrap();

        let has = store
            .has_relationship(
                "policy1",
                "document",
                "doc1",
                "owner",
                &Subject::Entity(did.clone()),
            )
            .await
            .unwrap();
        assert!(!has);
    }

    #[tokio::test]
    async fn test_memory_store_entity_set_subject() {
        let store = MemoryZanzibarStore::new();

        // File has parent relation to folder (entity set)
        let rel = Relationship::new(
            "file",
            "file1",
            "parent",
            Subject::entity_set("folder", "folder1", "owner"),
        );
        store.store_relationship("policy1", &rel).await.unwrap();

        let targets = store
            .get_relation_targets("policy1", "file", "file1", "parent")
            .await
            .unwrap();

        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].resource, "folder");
        assert_eq!(targets[0].object_id, "folder1");
    }

    #[tokio::test]
    async fn test_store_policy_with_validation() {
        let store = MemoryZanzibarStore::new();

        // Valid policy
        let policy = Policy::new("policy1", "Test")
            .with_resource(Resource::new("document").with_relation(Relation::direct("owner")));

        // Should succeed with validation
        let options = StorePolicyOptions::new().with_validation();
        store
            .store_policy_with_options(&policy, &options)
            .await
            .unwrap();

        // Verify policy was stored
        let loaded = store.get_policy("policy1").await.unwrap();
        assert!(loaded.is_some());
    }

    #[tokio::test]
    async fn test_store_policy_with_dpi_enforcement_valid() {
        let store = MemoryZanzibarStore::new();

        // DPI-compliant policy
        let policy = Policy::new("policy1", "Test").with_resource(
            Resource::new("document")
                .with_relation(Relation::direct("owner"))
                .with_relation(Relation::computed(
                    "reader",
                    RelationExpression::union(vec![
                        RelationExpression::this(),
                        RelationExpression::computed_userset("owner"),
                    ]),
                )),
        );

        // Should succeed with DPI enforcement
        let options = StorePolicyOptions::new()
            .with_validation()
            .with_dpi_enforcement();
        store
            .store_policy_with_options(&policy, &options)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_store_policy_with_dpi_enforcement_rejects_missing_owner() {
        let store = MemoryZanzibarStore::new();

        // Policy without owner relation (violates DPI)
        let policy = Policy::new("policy1", "Test")
            .with_resource(Resource::new("document").with_relation(Relation::direct("reader")));

        let options = StorePolicyOptions::new()
            .with_validation()
            .with_dpi_enforcement();

        let result = store.store_policy_with_options(&policy, &options).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::error::Error::DpiMissingOwner { .. }
        ));
    }

    #[tokio::test]
    async fn test_store_policy_with_dpi_enforcement_rejects_intersection() {
        let store = MemoryZanzibarStore::new();

        // Policy with intersection (violates DPI)
        let policy = Policy::new("policy1", "Test").with_resource(
            Resource::new("document")
                .with_relation(Relation::direct("owner"))
                .with_relation(Relation::direct("approved"))
                .with_relation(Relation::computed(
                    "editor",
                    RelationExpression::intersection(vec![
                        RelationExpression::computed_userset("owner"),
                        RelationExpression::computed_userset("approved"),
                    ]),
                )),
        );

        let options = StorePolicyOptions::new()
            .with_validation()
            .with_dpi_enforcement();

        let result = store.store_policy_with_options(&policy, &options).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::error::Error::DpiDisallowedOperation { .. }
        ));
    }

    #[tokio::test]
    async fn test_store_policy_without_dpi_allows_intersection() {
        let store = MemoryZanzibarStore::new();

        // Policy with intersection (allowed without DPI enforcement)
        let policy = Policy::new("policy1", "Test").with_resource(
            Resource::new("document")
                .with_relation(Relation::direct("owner"))
                .with_relation(Relation::direct("approved"))
                .with_relation(Relation::computed(
                    "editor",
                    RelationExpression::intersection(vec![
                        RelationExpression::computed_userset("owner"),
                        RelationExpression::computed_userset("approved"),
                    ]),
                )),
        );

        // Without DPI enforcement, this should succeed
        let options = StorePolicyOptions::new().with_validation();
        store
            .store_policy_with_options(&policy, &options)
            .await
            .unwrap();
    }
}
