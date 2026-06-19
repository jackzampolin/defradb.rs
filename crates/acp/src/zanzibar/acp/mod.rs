//! Zanzibar-based DocumentACP implementation.

mod document_acp;

use async_lock::RwLock;
use std::sync::Arc;

use identity::Did;
use zanzibar::engine::PermissionEngine;
use zanzibar::expression::RelationExpression;
use zanzibar::store::ZanzibarStore;
use zanzibar::types::{Policy, Relation, Relationship, Resource, Subject};

use crate::error::{Error, Result};
use crate::permission::DocumentPermission;

pub const OWNER_RELATION: &str = "owner";

/// Parse a relationship-target string into a [`Subject`], supporting the full
/// document-ACP language rather than only actor DIDs. This is the collection-
/// level-ACP seam: it lets a document relation point at another object (a TTU
/// parent edge) or at a userset, mirroring what the Zanzibar engine and store
/// already resolve.
///
/// Forms:
/// - `*`                     → [`Subject::Wildcard`] (all actors)
/// - `did:...`               → [`Subject::Entity`] (a single actor)
/// - `resource:id#relation`  → [`Subject::EntitySet`] (a userset)
/// - `resource:id`           → [`Subject::EntitySet`] with an empty relation
///   (a cross-object / TTU edge; the engine keys object edges on the
///   (resource, object_id) pair and ignores the relation)
pub fn parse_target_subject(target: &str) -> Result<Subject> {
    if target == "*" {
        return Ok(Subject::Wildcard);
    }
    if target.starts_with("did:") {
        let did = Did::new(target).map_err(|e| {
            Error::InvalidRelation(format!("invalid actor DID '{}': {}", target, e))
        })?;
        return Ok(Subject::Entity(did));
    }

    // Object form: `resource:object_id` with an optional `#relation` suffix.
    let (object, relation) = match target.split_once('#') {
        Some((object, relation)) => (object, relation),
        None => (target, ""),
    };
    let (resource, object_id) = object.split_once(':').ok_or_else(|| {
        Error::InvalidRelation(format!(
            "invalid relationship target '{}': expected 'did:...', '*', \
             'resource:id', or 'resource:id#relation'",
            target
        ))
    })?;
    if resource.is_empty() || object_id.is_empty() {
        return Err(Error::InvalidRelation(format!(
            "invalid relationship target '{}': empty resource or object id",
            target
        )));
    }
    Ok(Subject::entity_set(resource, object_id, relation))
}
pub const READER_RELATION: &str = "reader";
pub const UPDATER_RELATION: &str = "updater";
pub const DELETER_RELATION: &str = "deleter";
pub const ADMIN_RELATION: &str = "admin";

pub struct ZanzibarDocumentACP<S: ZanzibarStore + ?Sized> {
    store: Arc<S>,
    engine: RwLock<PermissionEngine<S>>,
}

impl<S: ZanzibarStore + ?Sized> ZanzibarDocumentACP<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store: store.clone(),
            engine: RwLock::new(PermissionEngine::new(store)),
        }
    }

    pub fn create_default_policy(policy_id: &str, resource_name: &str) -> Policy {
        Policy::new(policy_id, format!("Policy for {}", resource_name)).with_resource(
            Resource::new(resource_name)
                .with_relation(Relation::direct(OWNER_RELATION))
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
                .with_relation(Relation::direct(READER_RELATION))
                .with_relation(Relation::direct(UPDATER_RELATION))
                .with_relation(Relation::direct(DELETER_RELATION))
                .with_relation(Relation::computed(
                    "read",
                    RelationExpression::union(vec![
                        RelationExpression::computed_userset(OWNER_RELATION),
                        RelationExpression::computed_userset(ADMIN_RELATION),
                        RelationExpression::computed_userset(READER_RELATION),
                        RelationExpression::computed_userset(UPDATER_RELATION),
                        RelationExpression::computed_userset(DELETER_RELATION),
                    ]),
                ))
                .with_relation(Relation::computed(
                    "update",
                    RelationExpression::union(vec![
                        RelationExpression::computed_userset(OWNER_RELATION),
                        RelationExpression::computed_userset(ADMIN_RELATION),
                        RelationExpression::computed_userset(UPDATER_RELATION),
                    ]),
                ))
                .with_relation(Relation::computed(
                    "delete",
                    RelationExpression::union(vec![
                        RelationExpression::computed_userset(OWNER_RELATION),
                        RelationExpression::computed_userset(ADMIN_RELATION),
                        RelationExpression::computed_userset(DELETER_RELATION),
                    ]),
                )),
        )
    }

    async fn ensure_policy(&self, policy_id: &str, resource_name: &str) -> Result<()> {
        let exists = {
            let engine = self.engine.read().await;
            engine.lookup.has_policy(policy_id)
        };

        if !exists {
            if let Some(policy) = self.store.get_policy(policy_id).await? {
                let mut engine = self.engine.write().await;
                engine.add_policy(&policy);
            } else {
                let policy = Self::create_default_policy(policy_id, resource_name);
                self.store.store_policy(&policy).await?;
                let mut engine = self.engine.write().await;
                engine.add_policy(&policy);
            }
        }

        Ok(())
    }

    async fn is_owner(
        &self,
        subject: &Did,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<bool> {
        Ok(self
            .store
            .check_permission_direct(policy_id, resource_name, doc_id, OWNER_RELATION, subject)
            .await?)
    }

    async fn check_manage_relation(
        &self,
        subject: &Did,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
        target_relation: &str,
        operation: &str,
    ) -> Result<()> {
        if self
            .is_owner(subject, policy_id, resource_name, doc_id)
            .await?
        {
            return Ok(());
        }

        let policy = match self.store.get_policy(policy_id).await? {
            Some(p) => p,
            None => {
                return Err(Error::NotOwner {
                    operation: format!("{} actor relationship", operation),
                });
            }
        };

        let managers = policy.get_managers_for_relation(resource_name, target_relation);
        let has_managers = !managers.is_empty();

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

    /// Seed a relationship whose subject may be a userset or a cross-object
    /// edge, not only an actor DID — the collection-level-ACP seam.
    ///
    /// Authority follows the same rule as `add_actor_relationship`: the
    /// requestor must own the document or hold a managing relation for
    /// `relation`. The `target` subject is stored verbatim, so a caller can pass
    /// a [`Subject::EntitySet`] (userset / TTU parent edge) that the engine then
    /// resolves through `TupleToUserset`.
    pub async fn add_subject_relationship(
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

        let has = self
            .store
            .has_relationship(policy_id, collection_id, doc_id, relation, &target)
            .await?;
        if has {
            return Ok(false);
        }

        let rel = Relationship::new(collection_id, doc_id, relation, target);
        self.store.store_relationship(policy_id, &rel).await?;
        Ok(true)
    }

    fn permission_to_relation(permission: DocumentPermission) -> &'static str {
        match permission {
            DocumentPermission::Read => "read",
            DocumentPermission::Update => "update",
            DocumentPermission::Delete => "delete",
        }
    }

    pub async fn invalidate_policy_cache(&self, policy_id: &str) {
        let mut engine = self.engine.write().await;
        engine.remove_policy(policy_id);
    }

    pub async fn reload_policy(&self, policy_id: &str) -> Result<()> {
        let mut engine = self.engine.write().await;
        Ok(engine.reload_policy(policy_id).await?)
    }

    pub async fn clear_policy_cache(&self) {
        let mut engine = self.engine.write().await;
        engine.clear_cache();
    }
}

#[cfg(test)]
mod parse_target_subject_tests {
    use super::*;

    #[test]
    fn parses_wildcard() {
        assert!(matches!(
            parse_target_subject("*").unwrap(),
            Subject::Wildcard
        ));
    }

    #[test]
    fn parses_actor_did() {
        let subject =
            parse_target_subject("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK")
                .unwrap();
        assert!(subject.is_entity());
    }

    #[test]
    fn parses_userset() {
        match parse_target_subject("group:hr#participant").unwrap() {
            Subject::EntitySet {
                resource,
                object_id,
                relation,
            } => {
                assert_eq!(resource, "group");
                assert_eq!(object_id, "hr");
                assert_eq!(relation, "participant");
            }
            other => panic!("expected EntitySet userset, got {:?}", other),
        }
    }

    #[test]
    fn parses_cross_object_edge() {
        match parse_target_subject("directory:teamdir").unwrap() {
            Subject::EntitySet {
                resource,
                object_id,
                relation,
            } => {
                assert_eq!(resource, "directory");
                assert_eq!(object_id, "teamdir");
                assert_eq!(relation, "", "an object edge carries no subject relation");
            }
            other => panic!("expected EntitySet object edge, got {:?}", other),
        }
    }

    #[test]
    fn rejects_unqualified_target() {
        assert!(parse_target_subject("not-a-target").is_err());
    }

    #[test]
    fn rejects_malformed_did() {
        assert!(parse_target_subject("did:bogus").is_err());
    }
}
