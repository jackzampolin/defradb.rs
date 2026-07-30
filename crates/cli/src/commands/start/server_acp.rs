//! ACP and NAC initialization helpers for Node startup.

use std::sync::Arc;

use tracing::info;

use super::node::Node;
#[cfg(feature = "sourcehub")]
use crate::config::AcpDocumentType;
use crate::config::Config;
use crate::error::{Error, Result};
use identity::Did;

pub(super) struct DocumentAcpSetup {
    pub(super) document_acp: Arc<dyn acp::DocumentACP>,
    pub(super) http_adapter: Option<Arc<dyn defra_http::router::AcpOperations>>,
}

impl Node {
    #[cfg_attr(not(feature = "sourcehub"), allow(unused_variables))]
    pub(super) async fn setup_document_acp(
        config: &Config,
        identity_key_bytes: Option<&[u8]>,
        _acp_store: Arc<dyn acp::AcpStore>,
        zanzibar_store: Arc<dyn acp::ZanzibarStore>,
        event_bus: Arc<dyn events::Bus>,
        nac_checker: Arc<dyn db::NodeAccessChecker>,
    ) -> Result<DocumentAcpSetup> {
        #[cfg(feature = "sourcehub")]
        if config.acp.document_type == AcpDocumentType::SourceHub {
            if config.acp.sourcehub_address.is_empty() {
                return Err(Error::InvalidConfig(
                    "sourcehub_address required when document_type is source-hub".into(),
                ));
            }

            let signer_key_bytes = identity_key_bytes.ok_or_else(|| {
                Error::InvalidConfig(
                    "node identity required for SourceHub ACP (use --identity)".into(),
                )
            })?;

            let tuning = sourcehub::AcpTuning {
                request_timeout: std::time::Duration::from_secs(config.acp.request_timeout),
                circuit_breaker_threshold: config.acp.circuit_breaker_threshold,
                circuit_breaker_reset_timeout: std::time::Duration::from_secs(
                    config.acp.circuit_breaker_reset_timeout,
                ),
                cache_ttl: std::time::Duration::from_secs(config.acp.cache_ttl),
                receipt_timeout: std::time::Duration::from_secs(config.acp.receipt_timeout),
            };

            info!(
                request_timeout_s = config.acp.request_timeout,
                circuit_breaker_threshold = config.acp.circuit_breaker_threshold,
                circuit_breaker_reset_timeout_s = config.acp.circuit_breaker_reset_timeout,
                cache_ttl_s = config.acp.cache_ttl,
                receipt_timeout_s = config.acp.receipt_timeout,
                "Resolved ACP tuning (SourceHub)"
            );

            let provider = Arc::new(
                sourcehub::CosmosProvider::new(
                    config.acp.sourcehub_address.clone(),
                    config.acp.sourcehub_comet_address.clone(),
                    signer_key_bytes,
                    &config.acp.sourcehub_chain_id,
                    &tuning,
                )
                .map_err(|e| Error::InvalidConfig(format!("SourceHub provider: {}", e)))?,
            );

            let document_acp = Arc::new(sourcehub::SourceHubDocumentACP::new(
                provider,
                tuning.cache_ttl,
            ));
            let http_adapter = crate::sourcehub_acp_adapter::SourceHubAcpAdapter::new_arc(
                document_acp.clone(),
                zanzibar_store,
                nac_checker,
            );

            info!("Document ACP configured (SourceHub)");
            return Ok(DocumentAcpSetup {
                document_acp,
                http_adapter: Some(http_adapter),
            });
        }

        #[cfg(feature = "sourcehub")]
        if config.acp.document_type == AcpDocumentType::HubRs {
            if config.acp.hub_rs_address.is_empty() {
                return Err(Error::InvalidConfig(
                    "hub_rs_address required when document_type is hub-rs".into(),
                ));
            }

            let signer_key_bytes = identity_key_bytes.ok_or_else(|| {
                Error::InvalidConfig(
                    "node identity required for hub.rs ACP (use --identity)".into(),
                )
            })?;

            let tuning = sourcehub::AcpTuning {
                request_timeout: std::time::Duration::from_secs(config.acp.request_timeout),
                circuit_breaker_threshold: config.acp.circuit_breaker_threshold,
                circuit_breaker_reset_timeout: std::time::Duration::from_secs(
                    config.acp.circuit_breaker_reset_timeout,
                ),
                cache_ttl: std::time::Duration::from_secs(config.acp.cache_ttl),
                receipt_timeout: std::time::Duration::from_secs(config.acp.receipt_timeout),
            };

            info!(
                request_timeout_s = config.acp.request_timeout,
                circuit_breaker_threshold = config.acp.circuit_breaker_threshold,
                circuit_breaker_reset_timeout_s = config.acp.circuit_breaker_reset_timeout,
                receipt_timeout_s = config.acp.receipt_timeout,
                "Resolved ACP tuning (hub.rs; access decision cache disabled)"
            );

            let provider = Arc::new(
                sourcehub::HubRsProvider::new(
                    config.acp.hub_rs_address.clone(),
                    signer_key_bytes,
                    &tuning,
                    Some(event_bus),
                )
                .await
                .map_err(|e| Error::InvalidConfig(format!("hub.rs provider: {}", e)))?,
            );

            let document_acp = Arc::new(sourcehub::SourceHubDocumentACP::without_access_cache(
                provider,
            ));
            let http_adapter = crate::sourcehub_acp_adapter::SourceHubAcpAdapter::new_arc(
                document_acp.clone(),
                zanzibar_store,
                nac_checker,
            );

            info!("Document ACP configured (hub.rs)");
            return Ok(DocumentAcpSetup {
                document_acp,
                http_adapter: Some(http_adapter),
            });
        }

        info!("Document ACP configured (local)");
        let document_acp = Arc::new(acp::ZanzibarDocumentACP::new(zanzibar_store.clone()));
        Ok(DocumentAcpSetup {
            document_acp,
            http_adapter: Some(crate::acp_adapter::AcpAdapter::new_arc(
                zanzibar_store,
                nac_checker,
            )),
        })
    }

    pub(super) async fn setup_nac_manager(
        config: &Config,
        user_did: Option<&Did>,
    ) -> Result<Option<Arc<crate::nac_adapter::NacAdapter>>> {
        if !config.acp.node_enable {
            return Ok(None);
        }

        let nac_config = db::NacConfig::new().with_enabled();
        let nac_manager: Arc<dyn db::NacManagerApi> =
            Arc::new(db::create_memory_nac_manager(nac_config));
        nac_manager
            .initialize(user_did)
            .await
            .map_err(|e| Error::InvalidConfig(format!("failed to initialize NAC: {}", e)))?;

        Ok(Some(Arc::new(crate::nac_adapter::NacAdapter::new(
            nac_manager,
        ))))
    }
}
