//! DocumentACP trait implementation for ZanzibarDocumentACP.

use async_trait::async_trait;

use identity::Did;
use zanzibar::store::ZanzibarStore;
use zanzibar::types::{Relationship, Subject};

use super::{ZanzibarDocumentACP, OWNER_RELATION};
use crate::dac::{DocumentACP, ReplicatedActorRelationship};
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

        let subject = actor_subject(target);
        let has = self
            .store
            .has_relationship(policy_id, collection_id, doc_id, relation, &subject)
            .await?;

        if has {
            tracing::debug!(
                target: "acp::audit",
                event = "relationship_exists",
                requestor = %requestor,
                target = %target,
                relation = %relation,
                collection = %collection_id,
                doc_id = %doc_id,
                "Relationship already exists"
            );
            return Ok(false);
        }

        let rel = Relationship::new(collection_id, doc_id, relation, subject);
        self.store.store_relationship(policy_id, &rel).await?;

        tracing::info!(
            target: "acp::audit",
            event = "relationship_added",
            requestor = %requestor,
            target = %target,
            relation = %relation,
            collection = %collection_id,
            doc_id = %doc_id,
            "Actor relationship added via Zanzibar"
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

        let rel = Relationship::new(collection_id, doc_id, relation, actor_subject(target));
        let deleted = self.store.delete_relationship(policy_id, &rel).await?;

        if deleted {
            tracing::info!(
                target: "acp::audit",
                event = "relationship_deleted",
                requestor = %requestor,
                target = %target,
                relation = %relation,
                collection = %collection_id,
                doc_id = %doc_id,
                "Actor relationship deleted via Zanzibar"
            );
        } else {
            tracing::debug!(
                target: "acp::audit",
                event = "relationship_not_found",
                requestor = %requestor,
                target = %target,
                relation = %relation,
                collection = %collection_id,
                doc_id = %doc_id,
                "Relationship does not exist"
            );
        }

        Ok(deleted)
    }

    async fn export_actor_relationships(
        &self,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<Vec<ReplicatedActorRelationship>> {
        self.ensure_policy(policy_id, resource_name).await?;

        let Some(policy) = self.store.get_policy(policy_id).await? else {
            return Ok(Vec::new());
        };
        let Some(resource) = policy.get_resource(resource_name) else {
            return Ok(Vec::new());
        };

        let mut relationships = Vec::new();
        for relation in &resource.relations {
            if relation.name == OWNER_RELATION {
                continue;
            }

            let subjects = self
                .store
                .get_relation_subjects(policy_id, resource_name, doc_id, &relation.name)
                .await?;

            for subject in subjects {
                let actor = match subject {
                    Subject::Entity(did) => did.to_string(),
                    Subject::Wildcard => "*".to_string(),
                    _ => continue,
                };
                relationships.push(ReplicatedActorRelationship {
                    relation: relation.name.clone(),
                    actor,
                });
            }
        }

        Ok(relationships)
    }

    async fn replace_actor_relationships(
        &self,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
        relationships: &[ReplicatedActorRelationship],
    ) -> Result<()> {
        self.ensure_policy(policy_id, resource_name).await?;

        let Some(policy) = self.store.get_policy(policy_id).await? else {
            return Ok(());
        };
        let Some(resource) = policy.get_resource(resource_name) else {
            return Ok(());
        };

        for relation in &resource.relations {
            if relation.name == OWNER_RELATION {
                continue;
            }

            let subjects = self
                .store
                .get_relation_subjects(policy_id, resource_name, doc_id, &relation.name)
                .await?;

            for subject in subjects {
                if !matches!(subject, Subject::Entity(_) | Subject::Wildcard) {
                    continue;
                }
                let rel = Relationship::new(resource_name, doc_id, &relation.name, subject);
                let _ = self.store.delete_relationship(policy_id, &rel).await?;
            }
        }

        for relationship in relationships {
            let subject = if relationship.actor == "*" {
                Subject::Wildcard
            } else {
                Subject::Entity(Did::new(&relationship.actor).map_err(|error| {
                    Error::Storage(format!(
                        "invalid replicated actor DID '{}': {}",
                        relationship.actor, error
                    ))
                })?)
            };

            let rel = Relationship::new(resource_name, doc_id, &relationship.relation, subject);
            self.store.store_relationship(policy_id, &rel).await?;
        }

        Ok(())
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
