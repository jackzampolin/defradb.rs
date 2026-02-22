//! DocumentACP trait implementation for ZanzibarDocumentACP.

use async_trait::async_trait;

use zanzibar::did::Did;
use zanzibar::store::ZanzibarStore;
use zanzibar::types::{Relationship, Subject};

use super::{ZanzibarDocumentACP, OWNER_RELATION};
use crate::dac::DocumentACP;
use crate::error::{Error, Result};
use crate::identity::Identity;
use crate::permission::DocumentPermission;

/// Convert an identity::Did to zanzibar::Did.
fn to_zdid(did: &identity::Did) -> Did {
    Did::new_unchecked(did.to_string())
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: ZanzibarStore + 'static> DocumentACP for ZanzibarDocumentACP<S> {
    async fn register_doc_object(
        &self,
        identity: &identity::Did,
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

        let zdid = to_zdid(identity);
        let rel = Relationship::with_entity(resource_name, doc_id, OWNER_RELATION, zdid);
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

        let did = match identity {
            Identity::Authenticated(did) => did,
            Identity::Anonymous => {
                tracing::info!(
                    target: "acp::audit",
                    event = "permission_denied",
                    subject = "anonymous",
                    permission = ?permission,
                    collection = %resource_name,
                    doc_id = %doc_id,
                    "Permission denied: anonymous cannot access registered docs"
                );
                return Ok(false);
            }
        };

        self.ensure_policy(policy_id, resource_name).await?;

        let zdid = to_zdid(did);

        let granted = if permission == DocumentPermission::Read {
            let engine = self.engine.read().await;
            let mut result = false;
            for perm_name in &["read", "update", "delete"] {
                match engine
                    .check(policy_id, resource_name, doc_id, perm_name, &zdid)
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
                .check(policy_id, resource_name, doc_id, relation, &zdid)
                .await?
        };

        if granted {
            tracing::debug!(
                target: "acp::audit",
                event = "permission_granted",
                subject = %did,
                permission = ?permission,
                collection = %resource_name,
                doc_id = %doc_id,
                "Permission granted via Zanzibar"
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
        requestor: &identity::Did,
        target: &identity::Did,
        _policy_id: &str,
        collection_id: &str,
        doc_id: &str,
        relation: &str,
        _managing_relations: &[String],
    ) -> Result<bool> {
        self.ensure_policy(collection_id, collection_id).await?;

        if relation == OWNER_RELATION {
            return Err(Error::InvalidRelation(
                "cannot add owner relation".to_string(),
            ));
        }

        let zrequestor = to_zdid(requestor);
        let ztarget = to_zdid(target);

        self.check_manage_relation(
            &zrequestor,
            collection_id,
            collection_id,
            doc_id,
            relation,
            "create",
        )
        .await?;

        let has = self
            .store
            .has_relationship(
                collection_id,
                collection_id,
                doc_id,
                relation,
                &Subject::Entity(ztarget.clone()),
            )
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

        let rel = Relationship::with_entity(collection_id, doc_id, relation, ztarget);
        self.store.store_relationship(collection_id, &rel).await?;

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
        requestor: &identity::Did,
        target: &identity::Did,
        _policy_id: &str,
        collection_id: &str,
        doc_id: &str,
        relation: &str,
        _managing_relations: &[String],
    ) -> Result<bool> {
        self.ensure_policy(collection_id, collection_id).await?;

        if relation == OWNER_RELATION {
            return Err(Error::InvalidRelation(
                "cannot delete owner relation".to_string(),
            ));
        }

        let zrequestor = to_zdid(requestor);
        let ztarget = to_zdid(target);

        self.check_manage_relation(
            &zrequestor,
            collection_id,
            collection_id,
            doc_id,
            relation,
            "delete",
        )
        .await?;

        let rel = Relationship::with_entity(collection_id, doc_id, relation, ztarget);
        let deleted = self.store.delete_relationship(collection_id, &rel).await?;

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
