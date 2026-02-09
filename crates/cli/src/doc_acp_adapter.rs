//! Adapter to bridge document-level ACP operations to HTTP's DocumentAcpOperations trait.

use std::sync::Arc;

use async_trait::async_trait;

use defra_http::router::DocumentAcpOperations;
use storage::corekv::Store;

/// Adapter that implements DocumentAcpOperations using the database and ACP store.
pub struct DocumentAcpAdapter<S: Store> {
    database: Arc<db::DB<S>>,
    acp: Arc<dyn acp::DocumentACP>,
}

impl<S: Store + 'static> DocumentAcpAdapter<S> {
    /// Create a new adapter.
    pub fn new(database: Arc<db::DB<S>>, acp: Arc<dyn acp::DocumentACP>) -> Self {
        Self { database, acp }
    }

    /// Create an Arc-wrapped adapter.
    pub fn new_arc(
        database: Arc<db::DB<S>>,
        acp: Arc<dyn acp::DocumentACP>,
    ) -> Arc<dyn DocumentAcpOperations> {
        Arc::new(Self::new(database, acp))
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
        let (policy_id, resource_name) = self.get_policy_info(collection)?;

        let target: identity::Did = target_actor
            .parse()
            .map_err(|e| format!("invalid target actor DID: {}", e))?;

        self.acp
            .add_actor_relationship(
                requestor,
                &target,
                &policy_id,
                &resource_name,
                doc_id,
                relation,
                &[],
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
        let (policy_id, resource_name) = self.get_policy_info(collection)?;

        let target: identity::Did = target_actor
            .parse()
            .map_err(|e| format!("invalid target actor DID: {}", e))?;

        self.acp
            .delete_actor_relationship(
                requestor,
                &target,
                &policy_id,
                &resource_name,
                doc_id,
                relation,
                &[],
            )
            .await
            .map_err(|e| format!("{}", e))
    }
}
