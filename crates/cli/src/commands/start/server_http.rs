//! HTTP and PG server initialization helpers for Node startup.

use std::net::SocketAddr;
use std::sync::Arc;

use tracing::info;

use super::node::Node;
use super::server_acp::DocumentAcpSetup;
use super::server_query::QueryRunnerSetup;
use crate::config::{AcpDocumentType, Config, TransportType};
use crate::error::{Error, Result};
use identity::Did;

pub(super) struct HttpServerArgs<'a, S: storage::corekv::Store + 'static> {
    pub(super) database: Arc<db::DB<S>>,
    pub(super) config: &'a Config,
    pub(super) event_bus: Arc<dyn events::Bus>,
    pub(super) query_setup: &'a QueryRunnerSetup<S>,
    pub(super) p2p_adapter: Option<Arc<dyn defra_http::router::P2POperations>>,
    pub(super) nac_adapter: Option<Arc<crate::nac_adapter::NacAdapter>>,
    pub(super) acp_setup: &'a DocumentAcpSetup,
    pub(super) zanzibar_store: Arc<dyn acp::ZanzibarStore>,
    pub(super) user_did: Option<&'a Did>,
    pub(super) node_identity_did: Option<String>,
}

impl Node {
    pub(super) fn build_http_server<S>(args: HttpServerArgs<'_, S>) -> Result<defra_http::Server>
    where
        S: storage::corekv::Store + 'static,
    {
        let HttpServerArgs {
            database,
            config,
            event_bus,
            query_setup,
            p2p_adapter,
            nac_adapter,
            acp_setup,
            zanzibar_store,
            user_did,
            node_identity_did,
        } = args;

        let api_address: SocketAddr =
            config
                .api
                .address
                .parse()
                .map_err(|e: std::net::AddrParseError| {
                    Error::InvalidApiAddress(config.api.address.clone(), e.to_string())
                })?;

        let server_config = defra_http::ServerConfig {
            address: api_address,
            allowed_origins: config.api.allowed_origins.clone(),
            max_body_size: config.api.max_body_size,
            max_schema_size: config.api.max_schema_size,
            max_backup_size: config.api.max_backup_size,
            request_timeout: config.api.request_timeout,
            max_concurrent_requests: config.api.max_concurrent_requests,
        };

        let mut server =
            defra_http::Server::from_arc_with_config(query_setup.runner.clone(), server_config)
                .with_rest_arc(query_setup.rest_ops.clone())
                .with_dev_mode(config.development);

        let remote_signer_did = defra_core::signing::find_remote_signer_did();
        let signing_did = remote_signer_did
            .clone()
            .or_else(|| user_did.map(ToString::to_string))
            .or(node_identity_did);
        if let Some(did) = signing_did {
            info!(
                signing_did = %did,
                remote_signer_fallback = remote_signer_did.is_some(),
                "Configured HTTP signing fallback identity"
            );
            server = server.with_node_identity_did(did);
        }

        if let Some(adapter) = p2p_adapter {
            server = server.with_p2p_arc(adapter);
            if config.net.transport == TransportType::Iroh {
                info!("P2P HTTP endpoints enabled (iroh)");
            } else {
                info!("P2P HTTP endpoints enabled");
            }
        }

        let lens_adapter = crate::lens_adapter::LensAdapter::new_arc(database.clone());
        server = server.with_lens_arc(lens_adapter);
        info!("Lens HTTP endpoint enabled");

        if let Some(adapter) = &nac_adapter {
            server = server
                .with_nac_arc(adapter.clone() as Arc<dyn defra_http::router::NodeAcpOperations>);
            info!("NAC HTTP endpoints enabled");
        } else {
            info!("NAC disabled (use --node-acp-enable to enable)");
        }

        // Build the ACP adapter first (if enabled) so SchemaAdapter can use
        // it for schema-time DRI validation (#746).
        let acp_adapter_for_schema: Option<Arc<dyn defra_http::router::AcpOperations>> =
            if config.acp.document_type != AcpDocumentType::None {
                let acp_adapter = acp_setup.http_adapter.clone().unwrap_or_else(|| {
                    crate::acp_adapter::AcpAdapter::new_arc(zanzibar_store.clone())
                });
                server = server.with_acp_arc(acp_adapter.clone());
                info!(
                    "ACP policy HTTP endpoints enabled (type: {})",
                    config.acp.document_type
                );

                let doc_acp_adapter = crate::doc_acp_adapter::DocumentAcpAdapter::new_arc(
                    database.clone(),
                    acp_setup.document_acp.clone(),
                    zanzibar_store,
                );
                server = server.with_doc_acp_arc(doc_acp_adapter);
                info!("Document ACP HTTP endpoints enabled");

                Some(acp_adapter)
            } else {
                info!("Document ACP disabled (use --document-acp-type to enable)");
                None
            };

        let schema_adapter = match &acp_adapter_for_schema {
            Some(acp) => crate::schema_adapter::SchemaAdapter::new_arc_with_acp(
                database.clone(),
                acp.clone(),
            ),
            None => crate::schema_adapter::SchemaAdapter::new_arc(database.clone()),
        };
        server = server.with_schema_arc(schema_adapter);
        info!("Schema HTTP endpoint enabled");

        let view_adapter = crate::view_adapter::ViewAdapter::new_arc(database.clone());
        server = server.with_view_arc(view_adapter);
        info!("View HTTP endpoints enabled");

        let dump_adapter = crate::dump_adapter::DumpAdapter::new_arc(database.clone());
        server = server.with_dump_arc(dump_adapter);
        info!("Dump HTTP endpoint enabled");

        let collection_mgmt_adapter =
            crate::collection_mgmt_adapter::CollectionManagementAdapter::new_arc(database.clone());
        server = server.with_collection_mgmt_arc(collection_mgmt_adapter);
        info!("Collection management HTTP endpoints enabled");

        let txn_adapter =
            crate::txn_adapter::TxnRegistryAdapter::new_arc(query_setup.registry.clone());
        server = server.with_txn_ops_arc(txn_adapter);
        info!("Transaction-scoped HTTP endpoints enabled");

        let index_adapter = crate::index_adapter::IndexAdapter::new_arc(database.clone());
        server = server.with_index_arc(index_adapter);
        info!("Index HTTP endpoints enabled");

        let encrypted_index_adapter =
            crate::encrypted_index_adapter::EncryptedIndexAdapter::new_arc(database.clone());
        server = server.with_encrypted_index_arc(encrypted_index_adapter);
        info!("Encrypted index HTTP endpoints enabled");

        let backup_adapter = crate::backup_adapter::BackupAdapter::new_arc(
            database.clone(),
            query_setup.runner.clone(),
        );
        server = server.with_backup_arc(backup_adapter);
        info!("Backup HTTP endpoints enabled");

        let block_adapter = crate::block_adapter::BlockAdapter::new_arc(
            database.clone(),
            acp_setup.document_acp.clone(),
        );
        server = server.with_block_arc(block_adapter);
        info!("Block HTTP endpoints enabled");

        server = server.with_event_bus_arc(event_bus);
        info!("Subscription event bus enabled");

        info!(
            "HTTP server configured on {} with REST endpoints enabled",
            api_address
        );

        Ok(server)
    }

    pub(super) fn build_pg_server<S>(
        database: Arc<db::DB<S>>,
        config: &Config,
        query_setup: &QueryRunnerSetup<S>,
    ) -> Result<Option<pg_compat::PgServer>>
    where
        S: storage::corekv::Store + 'static,
    {
        if config.api.pg_address.is_empty() {
            return Ok(None);
        }

        let pg_addr: SocketAddr =
            config
                .api
                .pg_address
                .parse()
                .map_err(|e: std::net::AddrParseError| {
                    Error::InvalidApiAddress(config.api.pg_address.clone(), e.to_string())
                })?;
        let pg_schema_manager = crate::schema_adapter::SchemaAdapter::new_pg_arc(database);

        Ok(Some(pg_compat::PgServer::new(
            pg_addr,
            query_setup.runner.clone(),
            query_setup.collection_provider.clone(),
            Some(pg_schema_manager),
        )))
    }
}
