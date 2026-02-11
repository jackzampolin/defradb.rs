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
}

impl<S: Store + 'static> DocumentAcpAdapter<S> {
    /// Create a new adapter.
    pub fn new(
        database: Arc<db::DB<S>>,
        acp: Arc<dyn acp::DocumentACP>,
        store: Arc<dyn acp::ZanzibarStore>,
    ) -> Self {
        Self {
            database,
            acp,
            store,
        }
    }

    /// Create an Arc-wrapped adapter.
    pub fn new_arc(
        database: Arc<db::DB<S>>,
        acp: Arc<dyn acp::DocumentACP>,
        store: Arc<dyn acp::ZanzibarStore>,
    ) -> Arc<dyn DocumentAcpOperations> {
        Arc::new(Self::new(database, acp, store))
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

    /// Compute which relations can manage (grant/revoke) the given relation.
    async fn get_managing_relations(
        &self,
        policy_id: &str,
        resource_name: &str,
        relation: &str,
    ) -> Vec<String> {
        match self.store.get_policy(policy_id).await {
            Ok(Some(policy)) => policy
                .get_managers_for_relation(resource_name, relation)
                .into_iter()
                .map(|s| s.to_string())
                .collect(),
            _ => vec![],
        }
    }
}

#[async_trait]
impl<S: Store + 'static> DocumentAcpOperations for DocumentAcpAdapter<S> {
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
            .get_managing_relations(&policy_id, &resource_name, relation)
            .await;

        self.acp
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
            .map_err(|e| format!("{}", e))
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
            .get_managing_relations(&policy_id, &resource_name, relation)
            .await;

        self.acp
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
            .map_err(|e| format!("{}", e))
    }
}
