use std::sync::Arc;

use anyhow::{anyhow, Result};

use crate::Persistence;

pub(crate) async fn create_document_acp<S>(
    store: Arc<S>,
    persistence: Persistence,
    config: &crate::DocumentAcpConfig,
) -> Result<(
    Arc<dyn acp::DocumentACP>,
    Option<Arc<sourcehub::SourceHubDocumentACP>>,
)>
where
    S: storage::corekv::Store + 'static,
{
    match config {
        crate::DocumentAcpConfig::SourceHub(sourcehub_config) => {
            let tuning = sourcehub::AcpTuning::default();
            let provider = Arc::new(
                sourcehub::CosmosProvider::new(
                    sourcehub_config.grpc_address.clone(),
                    sourcehub_config.comet_rpc_address.clone(),
                    &sourcehub_config.signer_key,
                    &sourcehub_config.chain_id,
                    &tuning,
                )
                .map_err(|error| anyhow!("failed to create SourceHub provider: {error}"))?,
            );
            let sh_acp = Arc::new(sourcehub::SourceHubDocumentACP::new(
                provider,
                tuning.cache_ttl,
            ));
            Ok((sh_acp.clone(), Some(sh_acp)))
        }
        crate::DocumentAcpConfig::Local => match persistence {
            Persistence::Persistent => {
                let acp_store: Arc<dyn acp::AcpStore> =
                    Arc::new(acp::PersistentAcpStore::from_store(store));
                Ok((Arc::new(acp::LocalDocumentACP::new(acp_store)), None))
            }
            Persistence::Memory => {
                let acp_store: Arc<dyn acp::AcpStore> = Arc::new(acp::MemoryAcpStore::new());
                Ok((Arc::new(acp::LocalDocumentACP::new(acp_store)), None))
            }
        },
    }
}

pub(crate) async fn create_nac_manager<S>(
    store: Arc<S>,
    persistence: Persistence,
) -> Result<Arc<dyn db::NacManagerApi>>
where
    S: storage::corekv::Store + 'static,
{
    match persistence {
        Persistence::Persistent => {
            let nac_store = Arc::new(acp::PersistentZanzibarStore::from_store(store));
            let nac_config = db::NacConfig::new().with_dev_mode();
            let manager = Arc::new(db::NacManager::new(nac_store, nac_config));
            manager.initialize(None).await.map_err(|error| {
                anyhow!("failed to initialize NAC from persistent store: {error}")
            })?;
            Ok(manager)
        }
        Persistence::Memory => {
            let nac_store = Arc::new(acp::MemoryZanzibarStore::new());
            let nac_config = db::NacConfig::new().with_dev_mode();
            Ok(Arc::new(db::NacManager::new(nac_store, nac_config)))
        }
    }
}
