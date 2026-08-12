//! Type-erased document-ACP operations bridging DB to EmbeddedNode's public DAC API.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, bail};
use identity::Did;

#[async_trait::async_trait]
pub(crate) trait AcpOps: Send + Sync {
    async fn add_dac_policy(&self, identity: &str, policy: &str) -> anyhow::Result<String>;
    async fn add_dac_actor_relationship(
        &self,
        identity: &str,
        collection: &str,
        doc_id: &str,
        relation: &str,
        target: &str,
    ) -> anyhow::Result<bool>;
    async fn delete_dac_actor_relationship(
        &self,
        identity: &str,
        collection: &str,
        doc_id: &str,
        relation: &str,
        target: &str,
    ) -> anyhow::Result<bool>;
}

pub(crate) struct DbAcpOps<S: storage::corekv::Store + 'static> {
    database: Arc<db::DB<S>>,
    document_acp: Arc<dyn acp::DocumentACP>,
    local_zanzibar_store: Option<Arc<dyn acp::ZanzibarStore>>,
    sourcehub_acp: Option<Arc<sourcehub::SourceHubDocumentACP>>,
    event_bus: Arc<dyn events::Bus>,
    policy_counter: AtomicU64,
}

/// Everything a relationship call needs from the collection and its policy.
struct RelationshipTarget {
    target: acp::Subject,
    policy_id: String,
    resource_name: String,
    collection_id: String,
    managing_relations: Vec<String>,
}

impl<S: storage::corekv::Store + 'static> DbAcpOps<S> {
    pub(crate) fn new(
        database: Arc<db::DB<S>>,
        document_acp: Arc<dyn acp::DocumentACP>,
        local_zanzibar_store: Option<Arc<dyn acp::ZanzibarStore>>,
        sourcehub_acp: Option<Arc<sourcehub::SourceHubDocumentACP>>,
        event_bus: Arc<dyn events::Bus>,
    ) -> Self {
        Self {
            database,
            document_acp,
            local_zanzibar_store,
            sourcehub_acp,
            event_bus,
            policy_counter: AtomicU64::new(1),
        }
    }

    /// Resolve the caller's DID and check the node-level permission it needs.
    async fn authorize(
        &self,
        identity: &str,
        permission: acp::nac::NodePermission,
    ) -> anyhow::Result<Did> {
        let did = Did::new(identity).map_err(|error| anyhow!("invalid identity DID: {error}"))?;
        self.database
            .check_node_access(Some(&did), permission)
            .await
            .map_err(|error| anyhow!("{error}"))?;
        Ok(did)
    }

    /// Load a policy from whichever ACP backend this node was built with.
    async fn get_policy(&self, policy_id: &str) -> anyhow::Result<acp::Policy> {
        let policy = if let Some(store) = &self.local_zanzibar_store {
            store
                .get_policy(policy_id)
                .await
                .map_err(|error| anyhow!("failed to get policy: {error}"))?
        } else if let Some(sourcehub_acp) = &self.sourcehub_acp {
            sourcehub_acp
                .get_policy(policy_id)
                .await
                .map_err(|error| anyhow!("failed to get policy: {error}"))?
        } else {
            bail!("operation requires ACP, but ACP not available");
        };
        policy.ok_or_else(|| anyhow!("policy '{policy_id}' not found"))
    }

    async fn relationship_target(
        &self,
        collection: &str,
        relation: &str,
        target: &str,
    ) -> anyhow::Result<RelationshipTarget> {
        let collection_version = self
            .database
            .get_collection(collection)
            .map_err(|error| anyhow!("failed to get collection '{collection}': {error}"))?
            .ok_or_else(|| anyhow!("collection '{collection}' does not exist"))?;
        let Some(policy) = collection_version.schema().policy.clone() else {
            bail!("operation requires ACP, but collection has no policy");
        };

        // Parse the target into a structured subject once, at the edge: an actor
        // DID, all-actors `*`, a cross-object edge, or a userset.
        let target = acp::parse_target_subject(target)
            .map_err(|error| anyhow!("invalid target: {error}"))?;

        let loaded = self.get_policy(&policy.id).await?;
        let resource = loaded
            .get_resource(&policy.resource_name)
            .ok_or_else(|| anyhow!("resource '{}' not found in policy", policy.resource_name))?;
        if resource.get_relation(relation).is_none() {
            bail!(
                "relation '{relation}' not found in policy resource '{}'",
                policy.resource_name
            );
        }
        let managing_relations = loaded
            .get_managers_for_relation(&policy.resource_name, relation)
            .into_iter()
            .map(str::to_string)
            .collect();

        Ok(RelationshipTarget {
            target,
            policy_id: policy.id,
            resource_name: policy.resource_name,
            collection_id: collection_version.collection_id().to_string(),
            managing_relations,
        })
    }

    /// Announce the document's latest state so subscribers re-evaluate access.
    ///
    /// A failure here leaves the grant in place: the relationship is already
    /// stored, and the event is only a notification (matches the FFI path).
    async fn publish_document_update(&self, collection_id: &str, doc_id: &str) {
        match db::block_reader::read_latest_composite_block(&self.database, doc_id).await {
            Ok(result) => {
                self.event_bus
                    .publish(events::Message::update(events::Update::new(
                        doc_id.to_string(),
                        result.cid,
                        collection_id.to_string(),
                        result.block,
                        false,
                        false,
                    )));
            }
            Err(error) => tracing::warn!(
                doc_id,
                error,
                "failed to publish document update after DAC grant"
            ),
        }
    }
}

#[async_trait::async_trait]
impl<S: storage::corekv::Store + 'static> AcpOps for DbAcpOps<S> {
    async fn add_dac_policy(&self, identity: &str, policy: &str) -> anyhow::Result<String> {
        if identity.is_empty() {
            bail!("policy creator can not be empty");
        }
        self.authorize(identity, acp::nac::NodePermission::DacPolicyAdd)
            .await?;

        if policy.is_empty() {
            bail!("policy data can not be empty");
        }

        acp::policy_yaml::check_duplicate_yaml_keys(policy).map_err(|error| anyhow!(error))?;
        let parsed = acp::policy_yaml::parse_policy_yaml(policy).map_err(|error| anyhow!(error))?;
        if parsed.name.is_empty() {
            bail!("name required");
        }
        acp::policy_yaml::validate_policy_expressions(&parsed).map_err(|error| anyhow!(error))?;

        if let Some(sourcehub_acp) = &self.sourcehub_acp {
            return sourcehub_acp
                .add_policy(identity, policy)
                .await
                .map_err(|error| anyhow!("SourceHub create policy failed: {error}"));
        }

        let Some(store) = &self.local_zanzibar_store else {
            bail!("operation requires ACP, but ACP not available");
        };
        let counter = self.policy_counter.fetch_add(1, Ordering::SeqCst);
        let built = acp::policy_yaml::build_policy(&parsed, counter)
            .map_err(|error| anyhow!("invalid policy: {error}"))?;
        let options = acp::StorePolicyOptions::new()
            .with_validation()
            .with_dpi_enforcement();
        store
            .store_policy_with_options(&built, &options)
            .await
            .map_err(|error| anyhow!("failed to store policy: {error}"))?;

        Ok(built.id)
    }

    async fn add_dac_actor_relationship(
        &self,
        identity: &str,
        collection: &str,
        doc_id: &str,
        relation: &str,
        target: &str,
    ) -> anyhow::Result<bool> {
        let requestor = self
            .authorize(identity, acp::nac::NodePermission::DacRelationAdd)
            .await?;
        if relation == acp::OWNER_RELATION {
            bail!("OPERATION_FORBIDDEN: cannot add owner relation");
        }
        let resolved = self
            .relationship_target(collection, relation, target)
            .await?;

        let added = self
            .document_acp
            .add_relationship(
                &requestor,
                resolved.target,
                &resolved.policy_id,
                &resolved.resource_name,
                doc_id,
                relation,
                &resolved.managing_relations,
            )
            .await
            .map_err(|error| anyhow!("{error}"))?;

        if added {
            self.publish_document_update(&resolved.collection_id, doc_id)
                .await;
        }

        // Local ACP relationships are node-local (matches Go): a grant is not
        // propagated to peers. Cross-node access control is SourceHub's role.
        Ok(!added)
    }

    async fn delete_dac_actor_relationship(
        &self,
        identity: &str,
        collection: &str,
        doc_id: &str,
        relation: &str,
        target: &str,
    ) -> anyhow::Result<bool> {
        let requestor = self
            .authorize(identity, acp::nac::NodePermission::DacRelationDelete)
            .await?;
        if relation == acp::OWNER_RELATION {
            bail!("OPERATION_FORBIDDEN: cannot delete owner relation");
        }
        let resolved = self
            .relationship_target(collection, relation, target)
            .await?;

        // Local ACP relationships are node-local (matches Go): a revoke is not
        // propagated to peers. Cross-node access control is SourceHub's role.
        self.document_acp
            .delete_relationship(
                &requestor,
                resolved.target,
                &resolved.policy_id,
                &resolved.resource_name,
                doc_id,
                relation,
                &resolved.managing_relations,
            )
            .await
            .map_err(|error| anyhow!("{error}"))
    }
}
