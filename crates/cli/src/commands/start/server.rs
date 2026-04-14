//! Store and server initialization

use std::sync::Arc;

use tracing::{info, warn};

use super::node::{Node, P2PTasks};
use super::server_http::HttpServerArgs;
use crate::config::Config;
use crate::error::{Error, Result};
use identity::Identity;

impl Node {
    /// Initialize store, database, P2P, and HTTP server.
    ///
    /// This function creates the database, loads collections, sets up the query
    /// runner with proper transaction support, and returns the HTTP server.
    ///
    /// Returns a tuple of (P2PHostHandle, P2PTasks, downsample task, HTTP Server)
    /// where the background tasks are tracked for graceful shutdown.
    pub(super) async fn init_store_and_server<S>(
        store: Arc<S>,
        config: &Config,
        peer_keypair: Option<p2p::Keypair>,
        user_identity: Option<std::sync::Arc<identity::RawIdentity>>,
        acp_store: Arc<dyn acp::AcpStore>,
        zanzibar_store: Arc<dyn acp::ZanzibarStore>,
        node_identity_did: Option<String>,
    ) -> Result<(
        Option<p2p::P2PHostHandle>,
        Option<P2PTasks>,
        Option<tokio::task::JoinHandle<()>>,
        defra_http::Server,
        Option<pg_compat::PgServer>,
    )>
    where
        S: storage::corekv::Store + 'static,
    {
        let user_did = match &user_identity {
            Some(identity) => Some(identity.did().map_err(|e| {
                Error::InvalidIdentity(format!(
                    "failed to derive DID from --identity flag: {}. \
                     Verify your key is valid and matches --identity-key-type. \
                     Remove --identity flag to run without authentication.",
                    e
                ))
            })?),
            None => None,
        };

        let identity_key_bytes = user_identity.as_ref().map(|id| id.private_key_bytes());

        if let (Some(did), Some(identity)) = (&user_did, &user_identity) {
            defra_core::signing::store_identity(
                did.as_ref(),
                defra_core::signing::SigningConfig {
                    key_type: match identity.identity_key_type() {
                        identity::IdentityKeyType::Ed25519 => {
                            defra_core::signing::SigningKeyType::Ed25519
                        }
                        identity::IdentityKeyType::Secp256k1 => {
                            defra_core::signing::SigningKeyType::Secp256k1
                        }
                        identity::IdentityKeyType::Secp256r1 => {
                            defra_core::signing::SigningKeyType::Secp256r1
                        }
                        other => {
                            return Err(Error::InvalidIdentity(format!(
                                "unsupported identity key type for node signing: {}",
                                other
                            )))
                        }
                    },
                    private_key_bytes:
                        defra_core::signing::SigningConfig::private_key_bytes_from_vec(
                            identity.private_key_bytes(),
                        ),
                    public_key_bytes: identity.public_key_bytes(),
                    public_key_hex: hex::encode(identity.public_key_bytes()),
                    remote_signer: None,
                    signing_authorization: None,
                },
            );
            info!("Stored node identity signing config for DID {}", did);
        }

        let mut db_options = db::DbOptions::new();
        if let Some(identity) = user_identity {
            db_options = db_options.with_node_identity_arc(identity);
            info!("Database configured with user identity");
        }
        let embedding_api_key = if config.embedding.api_key_env.is_empty() {
            String::new()
        } else {
            match std::env::var(&config.embedding.api_key_env) {
                Ok(value) => value,
                Err(std::env::VarError::NotPresent) => {
                    if !config.embedding.url.is_empty() || !config.embedding.model.is_empty() {
                        warn!(
                            env_var = %config.embedding.api_key_env,
                            "embedding API key env var is not set; requests will be sent without Authorization header"
                        );
                    }
                    String::new()
                }
                Err(std::env::VarError::NotUnicode(_)) => {
                    warn!(
                        env_var = %config.embedding.api_key_env,
                        "embedding API key env var is not valid Unicode; requests will be sent without Authorization header"
                    );
                    String::new()
                }
            }
        };
        db_options = db_options
            .with_embedding_url(config.embedding.url.clone())
            .with_embedding_model(config.embedding.model.clone())
            .with_embedding_api_key(embedding_api_key);

        let mut database = db::DB::open_from_arc_with_options(store.clone(), db_options)
            .await
            .map_err(|e| Error::Storage(storage::Error::Other(e.to_string())))?;

        let event_bus: Arc<dyn events::Bus> = Arc::new(events::ChannelBus::new());
        database.set_event_bus(event_bus.clone());
        info!("Event bus configured for subscriptions");

        let database = Arc::new(database);

        let collection_count = database
            .list_collections()
            .map_err(|e| Error::Storage(storage::Error::Other(e.to_string())))?
            .len();
        info!("Loaded {} collection schema(s)", collection_count);

        let downsample_task = Some(database.clone().start_downsample_task());
        info!("Downsample worker enabled");

        let mut p2p_setup = Self::setup_p2p(
            store.clone(),
            database.clone(),
            event_bus.clone(),
            config,
            peer_keypair,
        )
        .await?;

        let acp_setup = Self::setup_document_acp(
            config,
            identity_key_bytes.as_deref(),
            acp_store,
            zanzibar_store.clone(),
            event_bus.clone(),
        )
        .await?;

        if let Some(set_acp) = p2p_setup.wire_merge_acp.take() {
            set_acp(acp_setup.document_acp.clone());
        }
        if let Some(set_acp) = p2p_setup.wire_doc_pusher_acp.take() {
            set_acp(acp_setup.document_acp.clone());
        }

        let nac_adapter = Self::setup_nac_manager(config, user_did.as_ref()).await?;
        let query_setup = Self::setup_query_runner(
            database.clone(),
            config,
            user_did.as_ref(),
            acp_setup.document_acp.clone(),
            nac_adapter.clone(),
            p2p_setup.mutator.clone(),
        );

        let zanzibar_store_for_pg = zanzibar_store.clone();
        let http_server = Self::build_http_server(HttpServerArgs {
            database: database.clone(),
            config,
            event_bus,
            query_setup: &query_setup,
            p2p_adapter: p2p_setup.http_adapter.clone(),
            nac_adapter,
            acp_setup: &acp_setup,
            zanzibar_store,
            user_did: user_did.as_ref(),
            node_identity_did,
        })?;
        let pg_server = Self::build_pg_server(
            database,
            config,
            &query_setup,
            &acp_setup,
            zanzibar_store_for_pg,
        )?;

        Ok((
            p2p_setup.host_handle,
            p2p_setup.p2p_tasks,
            downsample_task,
            http_server,
            pg_server,
        ))
    }
}
