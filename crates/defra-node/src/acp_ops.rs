//! Type-erased document-ACP operations bridging DB to EmbeddedNode's public DAC API.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, bail};
use identity::Did;

#[async_trait::async_trait]
pub(crate) trait AcpOps: Send + Sync {
    async fn add_dac_policy(&self, identity: &str, policy: &str) -> anyhow::Result<String>;
}

pub(crate) struct DbAcpOps<S: storage::corekv::Store + 'static> {
    database: Arc<db::DB<S>>,
    local_zanzibar_store: Option<Arc<dyn acp::ZanzibarStore>>,
    sourcehub_acp: Option<Arc<sourcehub::SourceHubDocumentACP>>,
    policy_counter: AtomicU64,
}

impl<S: storage::corekv::Store + 'static> DbAcpOps<S> {
    pub(crate) fn new(
        database: Arc<db::DB<S>>,
        local_zanzibar_store: Option<Arc<dyn acp::ZanzibarStore>>,
        sourcehub_acp: Option<Arc<sourcehub::SourceHubDocumentACP>>,
    ) -> Self {
        Self {
            database,
            local_zanzibar_store,
            sourcehub_acp,
            policy_counter: AtomicU64::new(1),
        }
    }
}

#[async_trait::async_trait]
impl<S: storage::corekv::Store + 'static> AcpOps for DbAcpOps<S> {
    async fn add_dac_policy(&self, identity: &str, policy: &str) -> anyhow::Result<String> {
        if identity.is_empty() {
            bail!("policy creator can not be empty");
        }
        let did = Did::new(identity).map_err(|error| anyhow!("invalid identity DID: {error}"))?;
        self.database
            .check_node_access(Some(&did), acp::nac::NodePermission::DacPolicyAdd)
            .await
            .map_err(|error| anyhow!("{error}"))?;

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
}
