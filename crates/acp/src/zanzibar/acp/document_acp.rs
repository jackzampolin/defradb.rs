//! DocumentACP trait implementation for ZanzibarDocumentACP.

use async_trait::async_trait;

use identity::Did;
use zanzibar::store::ZanzibarStore;
use zanzibar::types::{Relationship, Subject};

use super::{ZanzibarDocumentACP, OWNER_RELATION};
use crate::dac::DocumentACP;
use crate::error::{Error, Result};
use crate::identity::Identity;
use crate::permission::DocumentPermission;

fn actor_subject(actor: &Did) -> Subject {
    if actor.is_wildcard() {
        Subject::Wildcard
    } else {
        Subject::Entity(actor.clone())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: ZanzibarStore + ?Sized + 'static> DocumentACP for ZanzibarDocumentACP<S> {
    async fn register_doc_object(
        &self,
        identity: &Did,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<()> {
        self.ensure_policy(policy_id, resource_name).await?;

        let subjects = self
            .store
            .get_relation_subjects(policy_id, resource_name, doc_id, OWNER_RELATION)
            .await?;

        if !subjects.is_empty() {
            tracing::warn!(
                target: "acp::audit",
                event = "document_registration_failed",
                owner = %identity,
                collection = %resource_name,
                doc_id = %doc_id,
                reason = "already_registered",
                "Document registration failed: already registered"
            );
            return Err(Error::DocumentAlreadyRegistered(format!(
                "{}:{}",
                resource_name, doc_id
            )));
        }

        let rel =
            Relationship::with_entity(resource_name, doc_id, OWNER_RELATION, identity.clone());
        self.store.store_relationship(policy_id, &rel).await?;

        tracing::info!(
            target: "acp::audit",
            event = "document_registered",
            owner = %identity,
            collection = %resource_name,
            doc_id = %doc_id,
            "Document registered with Zanzibar ACP"
        );

        Ok(())
    }

    async fn is_doc_registered(
        &self,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<bool> {
        let subjects = self
            .store
            .get_relation_subjects(policy_id, resource_name, doc_id, OWNER_RELATION)
            .await?;

        Ok(!subjects.is_empty())
    }

    async fn get_doc_owner(
        &self,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<Option<Did>> {
        let owner = self
            .store
            .get_relation_subjects(policy_id, resource_name, doc_id, OWNER_RELATION)
            .await?
            .into_iter()
            .next();

        Ok(owner.and_then(|subject| subject.as_entity().cloned()))
    }

    async fn check_doc_access(
        &self,
        identity: &Identity,
        permission: DocumentPermission,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<bool> {
        if !self
            .is_doc_registered(policy_id, resource_name, doc_id)
            .await?
        {
            return Ok(true);
        }

        let subject = match identity {
            Identity::Authenticated(did) => did.clone(),
            Identity::Anonymous => Did::wildcard(),
        };

        self.ensure_policy(policy_id, resource_name).await?;

        let granted = if permission == DocumentPermission::Read {
            // Read access is granted if the caller has any of the
            // implies-read permissions — matches Go's ImplyDocumentReadPerm.
            let engine = self.engine.read().await;
            let mut result = false;
            for perm in DocumentPermission::implies_read_permissions() {
                match engine
                    .check(policy_id, resource_name, doc_id, perm.as_str(), &subject)
                    .await
                {
                    Ok(true) => {
                        result = true;
                        break;
                    }
                    Ok(false) => continue,
                    Err(e) => return Err(Error::from(e)),
                }
            }
            result
        } else {
            let relation = Self::permission_to_relation(permission);
            let engine = self.engine.read().await;
            engine
                .check(policy_id, resource_name, doc_id, relation, &subject)
                .await?
        };

        if granted {
            tracing::debug!(
                target: "acp::audit",
                event = "permission_granted",
                subject = %subject,
                permission = ?permission,
                collection = %resource_name,
                doc_id = %doc_id,
                "Permission granted via Zanzibar"
            );
        } else {
            tracing::info!(
                target: "acp::audit",
                event = "permission_denied",
                subject = %subject,
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
        // The actor path is a thin wrapper over the general subject path, so
        // authority and floor validation run through a single implementation.
        self.add_relationship(
            requestor,
            actor_subject(target),
            policy_id,
            collection_id,
            doc_id,
            relation,
            managing_relations,
        )
        .await
    }

    async fn add_relationship(
        &self,
        requestor: &Did,
        target: Subject,
        policy_id: &str,
        collection_id: &str,
        doc_id: &str,
        relation: &str,
        _managing_relations: &[String],
    ) -> Result<bool> {
        self.ensure_policy(policy_id, collection_id).await?;

        if relation == OWNER_RELATION {
            return Err(Error::InvalidRelation(
                "cannot add owner relation".to_string(),
            ));
        }

        self.check_manage_relation(
            requestor,
            policy_id,
            collection_id,
            doc_id,
            relation,
            "create",
        )
        .await?;

        // Soundness floor: a declared relation's subject must satisfy its
        // `types:` (and any cross-object reference must point at a declared
        // resource) before it is stored. Undeclared relations are still accepted
        // — relation-vs-policy validation is an adapter-layer concern in Go, and
        // an undeclared relation carries no type contract to enforce.
        let rel = Relationship::new(collection_id, doc_id, relation, target);
        let policy = self
            .store
            .get_policy(policy_id)
            .await?
            .ok_or_else(|| Error::InvalidPolicy(format!("policy '{}' not found", policy_id)))?;
        if policy.get_relation(collection_id, relation).is_some() {
            rel.validate(&policy)?;
        }

        let has = self
            .store
            .has_relationship(policy_id, collection_id, doc_id, relation, &rel.subject)
            .await?;
        if has {
            return Ok(false);
        }

        self.store.store_relationship(policy_id, &rel).await?;

        tracing::info!(
            target: "acp::audit",
            event = "relationship_added",
            requestor = %requestor,
            subject = %rel.subject,
            relation = %relation,
            collection = %collection_id,
            doc_id = %doc_id,
            "Relationship added via Zanzibar"
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
        // The actor path is a thin wrapper over the general subject path, so
        // authority runs through a single implementation.
        self.delete_relationship(
            requestor,
            actor_subject(target),
            policy_id,
            collection_id,
            doc_id,
            relation,
            managing_relations,
        )
        .await
    }

    async fn delete_relationship(
        &self,
        requestor: &Did,
        target: Subject,
        policy_id: &str,
        collection_id: &str,
        doc_id: &str,
        relation: &str,
        _managing_relations: &[String],
    ) -> Result<bool> {
        self.ensure_policy(policy_id, collection_id).await?;

        if relation == OWNER_RELATION {
            return Err(Error::InvalidRelation(
                "cannot delete owner relation".to_string(),
            ));
        }

        self.check_manage_relation(
            requestor,
            policy_id,
            collection_id,
            doc_id,
            relation,
            "delete",
        )
        .await?;

        // No floor validation on the revoke path: type validation guards what a
        // grant introduces, but a revoke only removes an existing edge.
        let rel = Relationship::new(collection_id, doc_id, relation, target);
        let deleted = self.store.delete_relationship(policy_id, &rel).await?;

        if deleted {
            tracing::info!(
                target: "acp::audit",
                event = "relationship_deleted",
                requestor = %requestor,
                subject = %rel.subject,
                relation = %relation,
                collection = %collection_id,
                doc_id = %doc_id,
                "Relationship deleted via Zanzibar"
            );
        } else {
            tracing::debug!(
                target: "acp::audit",
                event = "relationship_not_found",
                requestor = %requestor,
                subject = %rel.subject,
                relation = %relation,
                collection = %collection_id,
                doc_id = %doc_id,
                "Relationship does not exist"
            );
        }

        Ok(deleted)
    }

    async fn unregister_doc_object(
        &self,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<()> {
        self.store
            .delete_object_relationships(policy_id, resource_name, doc_id)
            .await?;

        tracing::info!(
            target: "acp::audit",
            event = "document_unregistered",
            collection = %resource_name,
            doc_id = %doc_id,
            "Document unregistered from Zanzibar ACP"
        );

        Ok(())
    }
}
