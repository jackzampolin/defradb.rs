//! Zanzibar storage trait and implementations.
//!
//! Defines the ZanzibarStore trait for storing policies and relationships,
//! with memory and persistent implementations.

use async_trait::async_trait;
use identity::Did;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

use storage::corekv::{IterOptions, Reader, Store, Writer};
use storage::namespace::{Namespace, NamespacedStore};
use storage::RedbStore;

use super::types::{ObjectRef, Policy, Relationship, Subject};
use crate::error::{Error, Result};

/// Trait for Zanzibar policy and relationship storage.
///
/// Provides operations for:
/// - Policy storage and retrieval
/// - Relationship tuple storage
/// - Relationship queries (for permission evaluation)
#[async_trait]
pub trait ZanzibarStore: Send + Sync {
    /// Store a policy.
    async fn store_policy(&self, policy: &Policy) -> Result<()>;

    /// Get a policy by ID.
    async fn get_policy(&self, policy_id: &str) -> Result<Option<Policy>>;

    /// Delete a policy.
    async fn delete_policy(&self, policy_id: &str) -> Result<bool>;

    /// Store a relationship tuple.
    async fn store_relationship(&self, policy_id: &str, rel: &Relationship) -> Result<()>;

    /// Delete a relationship tuple.
    async fn delete_relationship(&self, policy_id: &str, rel: &Relationship) -> Result<bool>;

    /// Check if a specific relationship exists.
    ///
    /// For direct entity subjects, checks for exact match.
    /// For wildcard subjects, this checks for the wildcard tuple.
    async fn has_relationship(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
        subject: &Subject,
    ) -> Result<bool>;

    /// Check if subject has the relation either directly or via wildcard.
    ///
    /// Returns true if:
    /// - Direct tuple exists: (resource, object_id, relation, subject)
    /// - Wildcard tuple exists: (resource, object_id, relation, *)
    async fn check_permission_direct(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
        subject: &Did,
    ) -> Result<bool>;

    /// Get all subjects with a specific relation to an object.
    ///
    /// Returns entity subjects and entity set subjects.
    async fn get_relation_subjects(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
    ) -> Result<Vec<Subject>>;

    /// Get objects that the subject has a specific relation to.
    ///
    /// Used for tuple-to-userset: find objects where we have the tuple relation.
    async fn get_relation_targets(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
    ) -> Result<Vec<ObjectRef>>;

    /// Delete all relationships for an object.
    async fn delete_object_relationships(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
    ) -> Result<()>;
}

/// In-memory Zanzibar store for testing.
pub struct MemoryZanzibarStore {
    policies: RwLock<HashMap<String, Policy>>,
    // Key: (policy_id, storage_key)
    relationships: RwLock<HashMap<String, HashMap<String, Relationship>>>,
}

impl MemoryZanzibarStore {
    pub fn new() -> Self {
        Self {
            policies: RwLock::new(HashMap::new()),
            relationships: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MemoryZanzibarStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ZanzibarStore for MemoryZanzibarStore {
    async fn store_policy(&self, policy: &Policy) -> Result<()> {
        self.policies
            .write()
            .insert(policy.id.clone(), policy.clone());
        Ok(())
    }

    async fn get_policy(&self, policy_id: &str) -> Result<Option<Policy>> {
        Ok(self.policies.read().get(policy_id).cloned())
    }

    async fn delete_policy(&self, policy_id: &str) -> Result<bool> {
        let removed = self.policies.write().remove(policy_id).is_some();
        if removed {
            self.relationships.write().remove(policy_id);
        }
        Ok(removed)
    }

    async fn store_relationship(&self, policy_id: &str, rel: &Relationship) -> Result<()> {
        let key = rel.storage_key();
        self.relationships
            .write()
            .entry(policy_id.to_string())
            .or_default()
            .insert(key, rel.clone());
        Ok(())
    }

    async fn delete_relationship(&self, policy_id: &str, rel: &Relationship) -> Result<bool> {
        let key = rel.storage_key();
        let mut guard = self.relationships.write();
        if let Some(rels) = guard.get_mut(policy_id) {
            return Ok(rels.remove(&key).is_some());
        }
        Ok(false)
    }

    async fn has_relationship(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
        subject: &Subject,
    ) -> Result<bool> {
        let rel = Relationship::new(resource, object_id, relation, subject.clone());
        let key = rel.storage_key();

        let guard = self.relationships.read();
        if let Some(rels) = guard.get(policy_id) {
            return Ok(rels.contains_key(&key));
        }
        Ok(false)
    }

    async fn check_permission_direct(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
        subject: &Did,
    ) -> Result<bool> {
        // Check direct relationship
        let direct = Relationship::with_entity(resource, object_id, relation, subject.clone());
        let direct_key = direct.storage_key();

        // Check untyped wildcard relationship
        let wildcard = Relationship::new(resource, object_id, relation, Subject::Wildcard);
        let wildcard_key = wildcard.storage_key();

        let guard = self.relationships.read();
        if let Some(rels) = guard.get(policy_id) {
            // Direct match or untyped wildcard
            if rels.contains_key(&direct_key) || rels.contains_key(&wildcard_key) {
                return Ok(true);
            }

            // Check for any typed wildcard on this relation
            // TypedWildcard matches any entity (DIDs don't carry resource type info)
            let prefix = Relationship::relation_prefix(resource, object_id, relation);
            for (key, rel) in rels.iter() {
                if key.starts_with(&prefix) && rel.subject.is_typed_wildcard() {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    async fn get_relation_subjects(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
    ) -> Result<Vec<Subject>> {
        let prefix = Relationship::relation_prefix(resource, object_id, relation);

        let guard = self.relationships.read();
        if let Some(rels) = guard.get(policy_id) {
            let subjects: Vec<_> = rels
                .iter()
                .filter(|(k, _)| k.starts_with(&prefix))
                .map(|(_, v)| v.subject.clone())
                .collect();
            return Ok(subjects);
        }
        Ok(Vec::new())
    }

    async fn get_relation_targets(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
    ) -> Result<Vec<ObjectRef>> {
        // Find entity set subjects that reference other objects
        let prefix = Relationship::relation_prefix(resource, object_id, relation);

        let guard = self.relationships.read();
        if let Some(rels) = guard.get(policy_id) {
            let targets: Vec<_> = rels
                .iter()
                .filter(|(k, _)| k.starts_with(&prefix))
                .filter_map(|(_, v)| match &v.subject {
                    Subject::EntitySet {
                        resource,
                        object_id,
                        ..
                    } => Some(ObjectRef::new(resource, object_id)),
                    _ => None,
                })
                .collect();
            return Ok(targets);
        }
        Ok(Vec::new())
    }

    async fn delete_object_relationships(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
    ) -> Result<()> {
        let prefix = Relationship::object_prefix(resource, object_id);

        let mut guard = self.relationships.write();
        if let Some(rels) = guard.get_mut(policy_id) {
            rels.retain(|k, _| !k.starts_with(&prefix));
        }
        Ok(())
    }
}

/// Persistent Zanzibar store backed by any Store implementation.
pub struct PersistentZanzibarStore<S: Store> {
    store: NamespacedStore<S>,
}

impl<S: Store> PersistentZanzibarStore<S> {
    /// Create from an existing Store with ACP namespace isolation.
    pub fn from_store(store: Arc<S>) -> Self {
        Self {
            store: NamespacedStore::new(store, Namespace::Acpstore),
        }
    }
}

impl PersistentZanzibarStore<RedbStore> {
    /// Open a persistent store at the given path.
    pub fn open(path: &std::path::Path) -> Result<Self> {
        let store = RedbStore::open(path).map_err(|e| Error::Storage(e.to_string()))?;
        Ok(Self::from_store(Arc::new(store)))
    }
}

impl<S: Store> PersistentZanzibarStore<S> {
    fn policy_key(policy_id: &str) -> String {
        format!("/zanzibar/policy/{}", policy_id)
    }

    fn relationship_key(policy_id: &str, rel: &Relationship) -> String {
        format!("/zanzibar/{}{}", policy_id, rel.storage_key())
    }

    fn relationship_prefix(policy_id: &str, resource: &str, object_id: &str) -> String {
        format!(
            "/zanzibar/{}{}",
            policy_id,
            Relationship::object_prefix(resource, object_id)
        )
    }

    fn relation_prefix(policy_id: &str, resource: &str, object_id: &str, relation: &str) -> String {
        format!(
            "/zanzibar/{}{}",
            policy_id,
            Relationship::relation_prefix(resource, object_id, relation)
        )
    }
}

#[async_trait]
impl<S: Store> ZanzibarStore for PersistentZanzibarStore<S> {
    async fn store_policy(&self, policy: &Policy) -> Result<()> {
        let mut txn = self
            .store
            .new_txn(false)
            .await
            .map_err(|e| Error::storage_txn("store_policy: create transaction", e))?;

        let key = Self::policy_key(&policy.id);
        let value = serde_json::to_vec(policy)?;

        txn.set(key.as_bytes(), &value)
            .await
            .map_err(|e| Error::storage_write(format!("store_policy: set key {}", key), e))?;

        txn.commit()
            .await
            .map_err(|e| Error::storage_txn("store_policy: commit", e))?;

        Ok(())
    }

    async fn get_policy(&self, policy_id: &str) -> Result<Option<Policy>> {
        let txn = self
            .store
            .new_txn(true)
            .await
            .map_err(|e| Error::storage_txn("get_policy: create transaction", e))?;

        let key = Self::policy_key(policy_id);

        match txn
            .get(key.as_bytes())
            .await
            .map_err(|e| Error::storage_read(format!("get_policy: get key {}", key), e))?
        {
            Some(data) => {
                let policy: Policy = serde_json::from_slice(&data)?;
                Ok(Some(policy))
            }
            None => Ok(None),
        }
    }

    async fn delete_policy(&self, policy_id: &str) -> Result<bool> {
        let mut txn = self
            .store
            .new_txn(false)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let key = Self::policy_key(policy_id);

        let exists = txn
            .has(key.as_bytes())
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        if exists {
            // Delete the policy
            txn.delete(key.as_bytes())
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;

            // Cascade delete: remove all relationships for this policy
            // Relationships are stored with prefix /zanzibar/{policy_id}/rel/
            let rel_prefix = format!("/zanzibar/{}/rel/", policy_id);
            let iter_opts = IterOptions::new().with_prefix(rel_prefix.into_bytes());

            let mut keys_to_delete = Vec::new();
            {
                let mut iter = txn
                    .iterator(iter_opts)
                    .await
                    .map_err(|e| Error::Storage(e.to_string()))?;

                while let Some(kv) = iter
                    .next()
                    .await
                    .map_err(|e| Error::Storage(e.to_string()))?
                {
                    keys_to_delete.push(kv.key);
                }
            }

            // Delete collected relationship keys
            for key in keys_to_delete {
                txn.delete(&key)
                    .await
                    .map_err(|e| Error::Storage(e.to_string()))?;
            }
        }

        txn.commit()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(exists)
    }

    async fn store_relationship(&self, policy_id: &str, rel: &Relationship) -> Result<()> {
        let mut txn = self
            .store
            .new_txn(false)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let key = Self::relationship_key(policy_id, rel);
        let value = serde_json::to_vec(rel)?;

        txn.set(key.as_bytes(), &value)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        txn.commit()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(())
    }

    async fn delete_relationship(&self, policy_id: &str, rel: &Relationship) -> Result<bool> {
        let mut txn = self
            .store
            .new_txn(false)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let key = Self::relationship_key(policy_id, rel);

        let exists = txn
            .has(key.as_bytes())
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        if exists {
            txn.delete(key.as_bytes())
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
        }

        txn.commit()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(exists)
    }

    async fn has_relationship(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
        subject: &Subject,
    ) -> Result<bool> {
        let txn = self
            .store
            .new_txn(true)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let rel = Relationship::new(resource, object_id, relation, subject.clone());
        let key = Self::relationship_key(policy_id, &rel);

        txn.has(key.as_bytes())
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    async fn check_permission_direct(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
        subject: &Did,
    ) -> Result<bool> {
        let txn = self
            .store
            .new_txn(true)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        // Check direct relationship
        let direct = Relationship::with_entity(resource, object_id, relation, subject.clone());
        let direct_key = Self::relationship_key(policy_id, &direct);

        if txn
            .has(direct_key.as_bytes())
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
        {
            return Ok(true);
        }

        // Check untyped wildcard relationship
        let wildcard = Relationship::new(resource, object_id, relation, Subject::Wildcard);
        let wildcard_key = Self::relationship_key(policy_id, &wildcard);

        if txn
            .has(wildcard_key.as_bytes())
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
        {
            return Ok(true);
        }

        // Check for any typed wildcard on this relation
        // TypedWildcard matches any entity (DIDs don't carry resource type info)
        let prefix = Self::relation_prefix(policy_id, resource, object_id, relation);
        let iter_opts = IterOptions::new().with_prefix(prefix.into_bytes());

        let mut iter = txn
            .iterator(iter_opts)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        while let Some(kv) = iter
            .next()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
        {
            let rel: Relationship = serde_json::from_slice(&kv.value)?;
            if rel.subject.is_typed_wildcard() {
                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn get_relation_subjects(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
    ) -> Result<Vec<Subject>> {
        let txn = self
            .store
            .new_txn(true)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let prefix = Self::relation_prefix(policy_id, resource, object_id, relation);
        let iter_opts = IterOptions::new().with_prefix(prefix.into_bytes());

        let mut iter = txn
            .iterator(iter_opts)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let mut subjects = Vec::new();

        while let Some(kv) = iter
            .next()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
        {
            let rel: Relationship = serde_json::from_slice(&kv.value)?;
            subjects.push(rel.subject);
        }

        Ok(subjects)
    }

    async fn get_relation_targets(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
    ) -> Result<Vec<ObjectRef>> {
        let subjects = self
            .get_relation_subjects(policy_id, resource, object_id, relation)
            .await?;

        let targets: Vec<_> = subjects
            .into_iter()
            .filter_map(|s| match s {
                Subject::EntitySet {
                    resource,
                    object_id,
                    ..
                } => Some(ObjectRef::new(resource, object_id)),
                _ => None,
            })
            .collect();

        Ok(targets)
    }

    async fn delete_object_relationships(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
    ) -> Result<()> {
        let mut txn = self
            .store
            .new_txn(false)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let prefix = Self::relationship_prefix(policy_id, resource, object_id);
        let iter_opts = IterOptions::new().with_prefix(prefix.clone().into_bytes());

        // Collect keys to delete
        let mut keys_to_delete = Vec::new();
        {
            let mut iter = txn
                .iterator(iter_opts)
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;

            while let Some(kv) = iter
                .next()
                .await
                .map_err(|e| Error::Storage(e.to_string()))?
            {
                keys_to_delete.push(kv.key);
            }
        }

        // Delete collected keys
        for key in keys_to_delete {
            txn.delete(&key)
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
        }

        txn.commit()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
