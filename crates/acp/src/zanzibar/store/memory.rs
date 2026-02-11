//! In-memory Zanzibar store implementation.

use async_trait::async_trait;
use identity::Did;
use parking_lot::RwLock;
use std::collections::HashMap;

use super::traits::ZanzibarStore;
use crate::error::Result;
use crate::zanzibar::types::{ObjectRef, Policy, Relationship, Subject};

/// In-memory Zanzibar store for testing.
pub struct MemoryZanzibarStore {
    policies: RwLock<HashMap<String, Policy>>,
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
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

    async fn list_policies(&self) -> Result<Vec<Policy>> {
        Ok(self.policies.read().values().cloned().collect())
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
        let direct = Relationship::with_entity(resource, object_id, relation, subject.clone());
        let direct_key = direct.storage_key();

        let wildcard = Relationship::new(resource, object_id, relation, Subject::Wildcard);
        let wildcard_key = wildcard.storage_key();

        let guard = self.relationships.read();
        if let Some(rels) = guard.get(policy_id) {
            if rels.contains_key(&direct_key) || rels.contains_key(&wildcard_key) {
                return Ok(true);
            }

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
