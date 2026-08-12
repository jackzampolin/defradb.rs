use std::sync::Arc;

#[cfg(feature = "sourcehub")]
use anyhow::anyhow;
use anyhow::Result;

pub(crate) async fn create_document_acp(
    acp_store: Arc<dyn acp::AcpStore>,
    config: &crate::DocumentAcpConfig,
) -> Result<(Arc<dyn acp::DocumentACP>, bool)> {
    match config {
        #[cfg(feature = "sourcehub")]
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
            Ok((sh_acp, true))
        }
        crate::DocumentAcpConfig::Local => {
            let document_acp = Arc::new(acp::LocalDocumentACP::new(acp_store));
            Ok((document_acp, false))
        }
    }
}
