//! Adapter to bridge document-level ACP operations to HTTP's DocumentAcpOperations trait.

use std::sync::Arc;

use async_trait::async_trait;

use defra_http::router::DocumentAcpOperations;
use storage::corekv::Store;

/// Adapter that implements DocumentAcpOperations using the database and ACP store.
pub struct DocumentAcpAdapter<S: Store> {
    database: Arc<db::DB<S>>,
    acp: Arc<dyn acp::DocumentACP>,
    store: Arc<dyn acp::ZanzibarStore>,
    p2p: Option<Arc<dyn defra_http::router::P2POperations>>,
}

impl<S: Store + 'static> DocumentAcpAdapter<S> {
    /// Create a new adapter.
    pub fn new(
        database: Arc<db::DB<S>>,
        acp: Arc<dyn acp::DocumentACP>,
        store: Arc<dyn acp::ZanzibarStore>,
        p2p: Option<Arc<dyn defra_http::router::P2POperations>>,
    ) -> Self {
        Self {
            database,
            acp,
            store,
            p2p,
        }
    }

    /// Create an Arc-wrapped adapter.
    pub fn new_arc(
        database: Arc<db::DB<S>>,
        acp: Arc<dyn acp::DocumentACP>,
        store: Arc<dyn acp::ZanzibarStore>,
        p2p: Option<Arc<dyn defra_http::router::P2POperations>>,
    ) -> Arc<dyn DocumentAcpOperations> {
        Arc::new(Self::new(database, acp, store, p2p))
    }

    /// Look up policy info for a collection by name.
    fn get_policy_info(&self, collection: &str) -> Result<(String, String), String> {
        let col = self
            .database
            .get_collection(collection)
            .map_err(|e| format!("{}", e))?
            .ok_or_else(|| format!("collection '{}' not found", collection))?;

        let policy = col
            .schema()
            .policy
            .as_ref()
            .ok_or_else(|| format!("collection '{}' has no ACP policy", collection))?;

        Ok((policy.id.clone(), policy.resource_name.clone()))
    }

    /// Validate the relation exists in the policy and compute managing relations.
    async fn validate_and_get_managing_relations(
        &self,
        policy_id: &str,
        resource_name: &str,
        relation: &str,
    ) -> Result<Vec<String>, String> {
        let policy = self
            .store
            .get_policy(policy_id)
            .await
            .map_err(|e| format!("failed to get policy: {}", e))?
            .ok_or_else(|| format!("policy '{}' not found", policy_id))?;

        let resource = policy
            .get_resource(resource_name)
            .ok_or_else(|| format!("resource '{}' not found in policy", resource_name))?;

        if resource.get_relation(relation).is_none() {
            return Err(format!(
                "relation '{}' not found in policy resource '{}'",
                relation, resource_name
            ));
        }

        Ok(policy
            .get_managers_for_relation(resource_name, relation)
            .into_iter()
            .map(|s| s.to_string())
            .collect())
    }
}

#[async_trait]
impl<S: Store + 'static> DocumentAcpOperations for DocumentAcpAdapter<S> {
    async fn check_doc_access(
        &self,
        actor: &identity::Did,
        permission: acp::DocumentPermission,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<bool, String> {
        self.acp
            .check_doc_access(
                &acp::Identity::Authenticated(actor.clone()),
                permission,
                policy_id,
                resource_name,
                doc_id,
            )
            .await
            .map_err(|e| format!("{}", e))
    }

    async fn add_doc_relationship(
        &self,
        requestor: &identity::Did,
        target_actor: &str,
        collection: &str,
        doc_id: &str,
        relation: &str,
    ) -> Result<bool, String> {
        if relation == "owner" {
            return Err("OPERATION_FORBIDDEN: cannot add owner relation".into());
        }

        let (policy_id, resource_name) = self.get_policy_info(collection)?;

        let target: identity::Did = target_actor
            .parse()
            .map_err(|e| format!("invalid target actor DID: {}", e))?;

        let managing = self
            .validate_and_get_managing_relations(&policy_id, &resource_name, relation)
            .await?;

        let added = self
            .acp
            .add_actor_relationship(
                requestor,
                &target,
                &policy_id,
                &resource_name,
                doc_id,
                relation,
                &managing,
            )
            .await
            .map_err(|e| format!("{}", e))?;

        if added {
            if let Some(p2p) = &self.p2p {
                if let Err(error) = p2p.republish_document(collection, doc_id).await {
                    tracing::debug!(
                        error = %error,
                        collection,
                        doc_id,
                        "best-effort DAC republish after grant failed"
                    );
                }
            }
        }

        Ok(added)
    }

    async fn delete_doc_relationship(
        &self,
        requestor: &identity::Did,
        target_actor: &str,
        collection: &str,
        doc_id: &str,
        relation: &str,
    ) -> Result<bool, String> {
        if relation == "owner" {
            return Err("OPERATION_FORBIDDEN: cannot delete owner relation".into());
        }

        let (policy_id, resource_name) = self.get_policy_info(collection)?;

        let target: identity::Did = target_actor
            .parse()
            .map_err(|e| format!("invalid target actor DID: {}", e))?;

        let managing = self
            .validate_and_get_managing_relations(&policy_id, &resource_name, relation)
            .await?;

        let deleted = self
            .acp
            .delete_actor_relationship(
                requestor,
                &target,
                &policy_id,
                &resource_name,
                doc_id,
                relation,
                &managing,
            )
            .await
            .map_err(|e| format!("{}", e))?;

        if deleted {
            if let Some(p2p) = &self.p2p {
                if let Err(error) = p2p.republish_document(collection, doc_id).await {
                    tracing::debug!(
                        error = %error,
                        collection,
                        doc_id,
                        "best-effort DAC republish after revoke failed"
                    );
                }
            }
        }

        Ok(deleted)
    }
}
