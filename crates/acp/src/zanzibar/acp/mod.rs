//! Zanzibar-based DocumentACP implementation.

mod document_acp;

use async_lock::RwLock;
use identity::Did;
use std::sync::Arc;

use super::engine::PermissionEngine;
use super::expression::RelationExpression;
use super::store::ZanzibarStore;
use super::types::{Policy, Relation, Resource};
use crate::error::{Error, Result};
use crate::permission::DocumentPermission;

pub const OWNER_RELATION: &str = "owner";
pub const READER_RELATION: &str = "reader";
pub const UPDATER_RELATION: &str = "updater";
pub const DELETER_RELATION: &str = "deleter";
pub const ADMIN_RELATION: &str = "admin";

/// DocumentACP implementation using the Zanzibar permission model.
///
/// Uses computed usersets to model permission inheritance:
/// - `owner` implies all permissions
/// - `reader` = direct readers + owner + admin + updater + deleter
/// - `updater` = direct updaters + owner + admin
/// - `deleter` = direct deleters + owner + admin
pub struct ZanzibarDocumentACP<S: ZanzibarStore> {
    store: Arc<S>,
    engine: RwLock<PermissionEngine<S>>,
}

impl<S: ZanzibarStore> ZanzibarDocumentACP<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store: store.clone(),
            engine: RwLock::new(PermissionEngine::new(store)),
        }
    }

    /// Create a default document policy for a collection.
    ///
    /// Creates a policy with the standard DPI relations:
    /// - owner: direct relation (base case)
    /// - admin: direct relation that manages [reader, updater, deleter]
    /// - reader: owner + admin + direct readers + updater + deleter
    /// - updater: owner + admin + direct updaters
    /// - deleter: owner + admin + direct deleters
    pub fn create_default_policy(policy_id: &str, resource_name: &str) -> Policy {
        Policy::new(policy_id, format!("Policy for {}", resource_name)).with_resource(
            Resource::new(resource_name)
                .with_relation(Relation::direct(OWNER_RELATION))
                // Admin relation with manages capability
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
                .with_relation(Relation::computed(
                    READER_RELATION,
                    RelationExpression::union(vec![
                        RelationExpression::this(),
                        RelationExpression::computed_userset(OWNER_RELATION),
                        RelationExpression::computed_userset(ADMIN_RELATION),
                        // Updater and deleter also imply read
                        RelationExpression::computed_userset(UPDATER_RELATION),
                        RelationExpression::computed_userset(DELETER_RELATION),
                    ]),
                ))
                .with_relation(Relation::computed(
                    UPDATER_RELATION,
                    RelationExpression::union(vec![
                        RelationExpression::this(),
                        RelationExpression::computed_userset(OWNER_RELATION),
                        RelationExpression::computed_userset(ADMIN_RELATION),
                    ]),
                ))
                .with_relation(Relation::computed(
                    DELETER_RELATION,
                    RelationExpression::union(vec![
                        RelationExpression::this(),
                        RelationExpression::computed_userset(OWNER_RELATION),
                        RelationExpression::computed_userset(ADMIN_RELATION),
                    ]),
                )),
        )
    }

    /// Ensure a policy exists for the given policy_id and resource.
    /// Creates a default policy if one doesn't exist.
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
        self.store
            .check_permission_direct(policy_id, resource_name, doc_id, OWNER_RELATION, subject)
            .await
    }

    /// Check if subject can manage a given relation (is owner OR has a managing relation).
    ///
    /// DefraDB pattern: actors can manage relationships if they are either:
    /// 1. The owner of the object, OR
    /// 2. Have a relation that has the target relation in its `manages` list
    async fn check_manage_relation(
        &self,
        subject: &Did,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
        target_relation: &str,
        operation: &str,
    ) -> Result<()> {
        // Owner check first (fast path)
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

    fn permission_to_relation(permission: DocumentPermission) -> &'static str {
        match permission {
            DocumentPermission::Read => READER_RELATION,
            DocumentPermission::Update => UPDATER_RELATION,
            DocumentPermission::Delete => DELETER_RELATION,
        }
    }

    /// Invalidate cached policy, forcing reload on next access.
    pub async fn invalidate_policy_cache(&self, policy_id: &str) {
        let mut engine = self.engine.write().await;
        engine.remove_policy(policy_id);
    }

    /// Reload a policy from the store, updating the cache.
    pub async fn reload_policy(&self, policy_id: &str) -> Result<()> {
        let mut engine = self.engine.write().await;
        engine.reload_policy(policy_id).await
    }

    /// Clear all cached policies.
    pub async fn clear_policy_cache(&self) {
        let mut engine = self.engine.write().await;
        engine.clear_cache();
    }
}
