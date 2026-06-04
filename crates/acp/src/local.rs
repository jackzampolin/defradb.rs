//! Local DocumentACP implementation.
//!
//! This implementation stores relation tuples locally and evaluates
//! permission checks against them. SourceHub integration is deferred.
//!
//! # Audit Logging
//!
//! All ACP operations are logged for security auditing:
//! - Permission grants (debug level)
//! - Permission denials (info level)
//! - Document registrations (info level)
//! - Relationship changes (info level)

use async_trait::async_trait;
use identity::Did;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

use crate::dac::DocumentACP;
use crate::error::{Error, Result};
use crate::identity::Identity;
use crate::permission::DocumentPermission;
use crate::relation::{RelationTuple, OWNER_RELATION};
use crate::store::AcpStore;

// Relation validation is done at the FFI/caller layer against the policy definition.
// LocalDocumentACP only rejects the immutable "owner" relation.

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

    /// Namespace a collection_id by policy_id to isolate policies in storage.
    fn namespaced_collection(policy_id: &str, collection_id: &str) -> String {
        format!("{policy_id}:{collection_id}")
    }

    /// Check if the subject is the owner of the document.
    async fn is_owner(&self, subject: &Did, collection_id: &str, doc_id: &str) -> Result<bool> {
        let tuple = RelationTuple::owner(subject.clone(), collection_id, doc_id);
        self.store.has_tuple(&tuple).await
    }

    /// Check if subject has a specific relation to the document.
    ///
    /// Checks both direct and wildcard ("*") entries.
    async fn has_relation(
        &self,
        subject: &Did,
        collection_id: &str,
        doc_id: &str,
        relation: &str,
    ) -> Result<bool> {
        // Check direct relationship
        let tuple = RelationTuple::new(subject.clone(), relation, collection_id, doc_id);
        if self.store.has_tuple(&tuple).await? {
            return Ok(true);
        }
        // Check wildcard relationship (target="*" means all actors)
        let wildcard_tuple = RelationTuple::new(Did::wildcard(), relation, collection_id, doc_id);
        self.store.has_tuple(&wildcard_tuple).await
    }

    /// Check whether `subject` has any relation that grants `permission`.
    ///
    /// For a `Read` permission, this fans out to check all relations in
    /// `DocumentPermission::implies_read_permissions()` (reader, updater,
    /// deleter) — matches Go's `ImplyDocumentReadPerm` semantics. For
    /// `Update` / `Delete`, checks only the single corresponding relation.
    async fn check_any_implies_read_relation(
        &self,
        subject: &Did,
        collection_id: &str,
        doc_id: &str,
        permission: DocumentPermission,
    ) -> Result<bool> {
        if permission == DocumentPermission::Read {
            for perm in DocumentPermission::implies_read_permissions() {
                if self
                    .has_relation(subject, collection_id, doc_id, perm.relation_name())
                    .await?
                {
                    return Ok(true);
                }
            }
            Ok(false)
        } else {
            self.has_relation(subject, collection_id, doc_id, permission.relation_name())
                .await
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl DocumentACP for LocalDocumentACP {
    async fn register_doc_object(
        &self,
        identity: &Did,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<()> {
        let ns_collection = Self::namespaced_collection(policy_id, resource_name);
        // Use atomic registration to prevent TOCTOU race conditions
        // where concurrent registrations could overwrite each other
        match self
            .store
            .register_doc_atomic(identity, &ns_collection, doc_id)
            .await?
        {
            true => {
                tracing::info!(
                    target: "acp::audit",
                    event = "document_registered",
                    owner = %identity,
                    collection = %resource_name,
                    doc_id = %doc_id,
                    "Document registered with ACP"
                );
                Ok(())
            }
            false => {
                tracing::warn!(
                    target: "acp::audit",
                    event = "document_registration_failed",
                    owner = %identity,
                    collection = %resource_name,
                    doc_id = %doc_id,
                    reason = "already_registered",
                    "Document registration failed: already registered"
                );
                Err(Error::DocumentAlreadyRegistered(format!(
                    "{}:{}",
                    resource_name, doc_id
                )))
            }
        }
    }

    async fn is_doc_registered(
        &self,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<bool> {
        let ns_collection = Self::namespaced_collection(policy_id, resource_name);
        self.store.is_doc_registered(&ns_collection, doc_id).await
    }

    async fn get_doc_owner(
        &self,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<Option<Did>> {
        let ns_collection = Self::namespaced_collection(policy_id, resource_name);
        Ok(self
            .store
            .get_relation_subjects(&ns_collection, doc_id, OWNER_RELATION)
            .await?
            .into_iter()
            .next())
    }

    async fn check_doc_access(
        &self,
        identity: &Identity,
        permission: DocumentPermission,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<bool> {
        let ns_collection = Self::namespaced_collection(policy_id, resource_name);
        // Check if document is registered
        if !self.store.is_doc_registered(&ns_collection, doc_id).await? {
            // Document is NOT registered — it was created without an identity
            // on an ACP-protected collection, making it a public document.
            // Anyone can read/update/delete it.
            return Ok(true);
        }

        // Document is registered, need authenticated identity to access
        // (unless a wildcard "*" grant exists for the requested permission)
        let did = match identity {
            Identity::Authenticated(did) => did,
            Identity::Anonymous => {
                // Check if wildcard grants exist for the requested permission.
                let wildcard = Did::wildcard();
                let granted = self
                    .check_any_implies_read_relation(&wildcard, &ns_collection, doc_id, permission)
                    .await?;
                return Ok(granted);
            }
        };

        // Owner always has all permissions (DPI rule: every permission starts with owner)
        if self.is_owner(did, &ns_collection, doc_id).await? {
            return Ok(true);
        }

        // Check specific relations based on permission.
        let granted = self
            .check_any_implies_read_relation(did, &ns_collection, doc_id, permission)
            .await?;

        if granted {
            tracing::debug!(
                target: "acp::audit",
                event = "permission_granted",
                subject = %did,
                permission = ?permission,
                collection = %resource_name,
                doc_id = %doc_id,
                "Permission granted via relation"
            );
        } else {
            tracing::info!(
                target: "acp::audit",
                event = "permission_denied",
                subject = %did,
                permission = ?permission,
                collection = %resource_name,
                doc_id = %doc_id,
                "Permission denied: no matching relation"
            );
        }

        Ok(granted)
    }

    async fn add_actor_relationship(
        &self,
        requestor: &Did,
        target: &Did,
        policy_id: &str,
        collection_id: &str,
        doc_id: &str,
        relation: &str,
        managing_relations: &[String],
    ) -> Result<bool> {
        let ns_collection = Self::namespaced_collection(policy_id, collection_id);
        // Owner or manager with a managing relation can add relationships
        if !self.is_owner(requestor, &ns_collection, doc_id).await? {
            let mut is_manager = false;
            for mgr_relation in managing_relations {
                if self
                    .has_relation(requestor, &ns_collection, doc_id, mgr_relation)
                    .await?
                {
                    is_manager = true;
                    break;
                }
            }
            if !is_manager {
                if !managing_relations.is_empty() {
                    return Err(Error::NotManager {
                        operation: "create relationship".to_string(),
                    });
                }
                return Err(Error::NotOwner {
                    operation: "add actor relationship".to_string(),
                });
            }
        }

        // Cannot add owner relation (it's immutable)
        if relation == OWNER_RELATION {
            return Err(Error::InvalidRelation(
                "cannot add owner relation".to_string(),
            ));
        }

        let tuple = RelationTuple::new(target.clone(), relation, &ns_collection, doc_id);

        // Check if already exists
        if self.store.has_tuple(&tuple).await? {
            tracing::debug!(
                target: "acp::audit",
                event = "relationship_exists",
                requestor = %requestor,
                target = %target,
                relation = %relation,
                collection = %ns_collection,
                doc_id = %doc_id,
                "Relationship already exists"
            );
            return Ok(false);
        }

        self.store.put_tuple(&tuple).await?;
        tracing::info!(
            target: "acp::audit",
            event = "relationship_added",
            requestor = %requestor,
            target = %target,
            relation = %relation,
            collection = %ns_collection,
            doc_id = %doc_id,
            "Actor relationship added"
        );
        Ok(true)
    }

    async fn delete_actor_relationship(
        &self,
        requestor: &Did,
        target: &Did,
        policy_id: &str,
        collection_id: &str,
        doc_id: &str,
        relation: &str,
        managing_relations: &[String],
    ) -> Result<bool> {
        let ns_collection = Self::namespaced_collection(policy_id, collection_id);
        // Owner or manager with a managing relation can delete relationships
        if !self.is_owner(requestor, &ns_collection, doc_id).await? {
            let mut is_manager = false;
            for mgr_relation in managing_relations {
                if self
                    .has_relation(requestor, &ns_collection, doc_id, mgr_relation)
                    .await?
                {
                    is_manager = true;
                    break;
                }
            }
            if !is_manager {
                if !managing_relations.is_empty() {
                    return Err(Error::NotManager {
                        operation: "delete relationship".to_string(),
                    });
                }
                return Err(Error::NotOwner {
                    operation: "delete actor relationship".to_string(),
                });
            }
        }

        // Cannot delete owner relation (it's immutable)
        if relation == OWNER_RELATION {
            return Err(Error::InvalidRelation(
                "cannot delete owner relation".to_string(),
            ));
        }

        let tuple = RelationTuple::new(target.clone(), relation, &ns_collection, doc_id);

        // Check if exists
        if !self.store.has_tuple(&tuple).await? {
            tracing::debug!(
                target: "acp::audit",
                event = "relationship_not_found",
                requestor = %requestor,
                target = %target,
                relation = %relation,
                collection = %ns_collection,
                doc_id = %doc_id,
                "Relationship does not exist"
            );
            return Ok(false);
        }

        self.store.delete_tuple(&tuple).await?;
        tracing::info!(
            target: "acp::audit",
            event = "relationship_deleted",
            requestor = %requestor,
            target = %target,
            relation = %relation,
            collection = %ns_collection,
            doc_id = %doc_id,
            "Actor relationship deleted"
        );
        Ok(true)
    }

    async fn unregister_doc_object(
        &self,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<()> {
        let ns_collection = Self::namespaced_collection(policy_id, resource_name);
        // Delete all tuples for this document (owner, reader, updater, deleter, etc.)
        self.store.delete_doc_tuples(&ns_collection, doc_id).await?;
        tracing::info!(
            target: "acp::audit",
            event = "document_unregistered",
            collection = %resource_name,
            doc_id = %doc_id,
            "Document unregistered from ACP"
        );
        Ok(())
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
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

    async fn register_doc_atomic(
        &self,
        owner: &Did,
        collection_id: &str,
        doc_id: &str,
    ) -> Result<bool> {
        // Validate inputs to prevent path traversal
        RelationTuple::validate_prefix(collection_id, doc_id)?;

        let prefix = RelationTuple::doc_prefix(collection_id, doc_id);
        let tuple = RelationTuple::owner(owner.clone(), collection_id, doc_id);
        let key = tuple.storage_key();

        // Hold write lock for entire operation - this is the atomic guarantee
        let mut guard = self.tuples.write();

        // Check if document is already registered (any tuple with this prefix)
        if guard.keys().any(|k| k.starts_with(&prefix)) {
            return Ok(false); // Already registered, no change
        }

        // Document not registered, insert owner tuple
        guard.insert(key, tuple);
        Ok(true)
    }
}
