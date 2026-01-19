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
use crate::identity::Identity;
use crate::permission::DocumentPermission;
use crate::relation::{
    RelationTuple, DELETER_RELATION, OWNER_RELATION, READER_RELATION, UPDATER_RELATION,
};
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
    async fn is_owner(&self, subject: &Did, collection_id: &str, doc_id: &str) -> Result<bool> {
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
        identity: &Identity,
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

        // Document is registered, need authenticated identity to access
        let did = match identity {
            Identity::Authenticated(did) => did,
            Identity::Anonymous => return Ok(false), // Anonymous cannot access registered docs
        };

        // Owner always has all permissions (DPI rule: every permission starts with owner)
        if self.is_owner(did, resource_name, doc_id).await? {
            return Ok(true);
        }

        // Check specific relations based on permission
        // DPI rule: permissions are unions (owner + relation)
        match permission {
            DocumentPermission::Read => {
                // reader OR updater OR deleter grants read (implied read)
                Ok(self
                    .has_relation(did, resource_name, doc_id, READER_RELATION)
                    .await?
                    || self
                        .has_relation(did, resource_name, doc_id, UPDATER_RELATION)
                        .await?
                    || self
                        .has_relation(did, resource_name, doc_id, DELETER_RELATION)
                        .await?)
            }
            DocumentPermission::Update => {
                // updater grants update
                self.has_relation(did, resource_name, doc_id, UPDATER_RELATION)
                    .await
            }
            DocumentPermission::Delete => {
                // deleter grants delete
                self.has_relation(did, resource_name, doc_id, DELETER_RELATION)
                    .await
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

    async fn unregister_doc_object(
        &self,
        _policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<()> {
        // Delete all tuples for this document (owner, reader, updater, deleter, etc.)
        self.store.delete_doc_tuples(resource_name, doc_id).await
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

    async fn get_doc_tuples(
        &self,
        collection_id: &str,
        doc_id: &str,
    ) -> Result<Vec<RelationTuple>> {
        // Validate inputs to prevent path traversal
        RelationTuple::validate_prefix(collection_id, doc_id)?;

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
        // Validate inputs to prevent path traversal
        RelationTuple::validate_relation_prefix(collection_id, doc_id, relation)?;

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
        // Validate inputs to prevent path traversal
        RelationTuple::validate_prefix(collection_id, doc_id)?;

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
        // Validate inputs to prevent path traversal
        RelationTuple::validate_prefix(collection_id, doc_id)?;

        let prefix = RelationTuple::doc_prefix(collection_id, doc_id);
        self.tuples.write().retain(|k, _| !k.starts_with(&prefix));
        Ok(())
    }

    async fn is_doc_registered(&self, collection_id: &str, doc_id: &str) -> Result<bool> {
        // Validate inputs to prevent path traversal
        RelationTuple::validate_prefix(collection_id, doc_id)?;

        let prefix = RelationTuple::doc_prefix(collection_id, doc_id);
        Ok(self.tuples.read().keys().any(|k| k.starts_with(&prefix)))
    }
}
// Tests extracted to crates/acp/tests/local_tests.rs
