//! Store and server initialization

use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use super::node::{Node, ServerSetup};
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
    /// Returns a [`ServerSetup`] holding the servers and background tasks
    /// tracked for graceful shutdown.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn init_store_and_server(
        store: Arc<storage::DynStore>,
        config: &Config,
        peer_keypair: Option<p2p::Keypair>,
        user_identity: Option<std::sync::Arc<identity::RawIdentity>>,
        acp_store: Arc<dyn acp::AcpStore>,
        zanzibar_store: Arc<dyn acp::ZanzibarStore>,
        node_identity_did: Option<String>,
        se_key: Option<[u8; 32]>,
    ) -> Result<ServerSetup> {
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

        let mut db_options =
            db::DbOptions::new().with_max_txn_retries(config.datastore.max_txn_retries);
        if let Some(identity) = user_identity.as_ref() {
            db_options = db_options.with_node_identity_arc(identity.clone());
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
            user_identity,
            se_key,
        )
        .await?;

        let acp_setup = Self::setup_document_acp(
            config,
            identity_key_bytes.as_deref(),
            acp_store,
            zanzibar_store.clone(),
            event_bus.clone(),
            db::node_access_checker(database.clone()),
        )
        .await?;

        if let Some(set_acp) = p2p_setup.wire_merge_acp.take() {
            set_acp(acp_setup.document_acp.clone());
        }
        if let Some(set_acp) = p2p_setup.wire_doc_pusher_acp.take() {
            set_acp(acp_setup.document_acp.clone());
        }

        let nac_adapter = Self::setup_nac_manager(config, user_did.as_ref()).await?;

        // Wire the NAC manager into the DB so DB-layer `check_node_access` calls
        // go live (first-call-wins via the DB's OnceLock). Without this the CLI
        // server's DB-layer NAC checks are inert.
        if let Some(adapter) = nac_adapter.as_ref() {
            database.set_nac_manager(adapter.nac_manager());
        }

        // Populate the manage-channel serve deps now that the controller exists;
        // until this fires the event loop drops inbound manage requests rather
        // than serving them. The NAC handle gates authorization: when NAC is
        // enabled we use the real adapter's manager, otherwise we supply a
        // disabled NacManager whose check_permission returns Ok(true) (parity
        // with the embedded node, which always populates these hooks).
        if let (Some(hooks), Some(controller), Some(correlator), Some(query_correlator)) = (
            p2p_setup.manage_hooks.as_ref(),
            p2p_setup.manage_controller.as_ref(),
            p2p_setup.manage_correlator.as_ref(),
            p2p_setup.manage_query_correlator.as_ref(),
        ) {
            let nac: Arc<dyn db::NacManagerApi> = match nac_adapter.as_ref() {
                Some(adapter) => adapter.nac_manager(),
                None => Arc::new(db::create_memory_nac_manager(db::NacConfig::new())),
            };
            let _ = hooks.set(defra_p2p_adapter::manage::hooks::ManageHooks {
                ops: controller.clone(),
                nac,
                correlator: correlator.clone(),
                query_correlator: query_correlator.clone(),
            });
        }

        // Build + wire the KMS (mirrors crates/embedded/src/node.rs). The P2P
        // transport was created earlier; the NacDacPolicy needs document_acp +
        // NAC which exist here (PR #4778 ordering).
        {
            // Blockstore-backed KeyStore (mirrors Go's internal/kms/enc_store.go):
            // the KMS serves DEKs for ANY encrypted write by reading/writing the
            // node's durable encstore→blockstore, not a RAM-only map. The DB owns
            // the blockstore Arc (set_kms_blockstore) so the adapter can hold a
            // Weak and avoid the lock-pinning cycle (#976) while sharing the cache.
            let kms_blockstore = database.set_kms_blockstore(Arc::new(
                blockstore::DefraBlockstore::new(store.clone(), true),
            ));
            let enc_block_store: Arc<dyn kms::EncBlockStore> =
                Arc::new(db::DbEncBlockStore::new(database.clone(), kms_blockstore));
            let kms_store: Arc<dyn kms::KeyStore> =
                Arc::new(kms::BlockstoreKeyStore::new(enc_block_store));
            let doc_lookup: Arc<dyn kms::DocCollectionLookup> =
                Arc::new(db::DbDocCollectionLookup::new(database.clone()));
            let policy = Arc::new(kms::NacDacPolicy::new(
                acp_setup.document_acp.clone(),
                doc_lookup,
            ));
            if let Some(adapter) = nac_adapter.as_ref() {
                policy.set_node_acp(Arc::new(db::DbNodeAcpRead::new(adapter.nac_manager())));
            }

            // Node identity used for cross-peer DEK requests. Anonymous nodes
            // use a stable placeholder DID.
            let node_did = database.node_did().unwrap_or_else(|| {
                identity::Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK")
                    .expect("static anonymous DID parses")
            });

            let transports: Vec<Arc<dyn kms::KeyTransport>> = match p2p_setup.kms_transport.clone()
            {
                Some(t) => vec![t],
                None => vec![],
            };
            let kms_service: Arc<dyn kms::KmsService> = Arc::new(kms::DefraKms::new(
                kms_store,
                transports,
                policy as Arc<dyn kms::AccessPolicy>,
                Arc::new(db::DbBlockDocIDResolver::new(database.clone())),
                node_did,
            ));
            kms_service.set_local_peer_id(p2p_setup.local_peer_id.clone());

            // Serve handler with a Weak to break the transport↔kms Arc cycle.
            if let Some(kt) = p2p_setup.kms_transport.as_ref() {
                struct KmsServeHandler {
                    kms: std::sync::Weak<dyn kms::KmsService>,
                }
                #[async_trait::async_trait]
                impl kms::IncomingHandler for KmsServeHandler {
                    async fn handle(
                        &self,
                        from: kms::PeerIdentity,
                        req: kms::FetchEncryptionKeyRequest,
                    ) -> kms::Result<kms::FetchEncryptionKeyReply> {
                        match self.kms.upgrade() {
                            Some(k) => k.serve_request(from, req).await,
                            None => Err(kms::Error::Internal("kms dropped".into())),
                        }
                    }
                }
                kt.install_handler(Arc::new(KmsServeHandler {
                    kms: Arc::downgrade(&kms_service),
                }));
            }
            if let Some(wire_kms) = p2p_setup.wire_kms.take() {
                wire_kms(kms_service.clone());
            }
            database.set_kms(kms_service.clone());
        }

        let query_setup = Self::setup_query_runner(
            database.clone(),
            config,
            user_did.as_ref(),
            acp_setup.document_acp.clone(),
            nac_adapter.clone(),
            p2p_setup.mutator.clone(),
            p2p_setup.txn_broadcaster.clone(),
            p2p_setup.se_transport.take(),
        );

        let txn_cleanup_task = if config.api.transaction_idle_timeout > 0 {
            if config.api.transaction_cleanup_interval == 0 {
                return Err(Error::InvalidConfig(
                    "api.transaction_cleanup_interval must be > 0 when transaction_idle_timeout is enabled"
                        .to_string(),
                ));
            }

            let max_idle_age = Duration::from_secs(config.api.transaction_idle_timeout);
            let sweep_interval = Duration::from_secs(config.api.transaction_cleanup_interval);
            info!(
                max_idle_age_secs = config.api.transaction_idle_timeout,
                sweep_interval_secs = config.api.transaction_cleanup_interval,
                "Transaction idle cleanup worker enabled"
            );
            Some(
                query_setup
                    .registry
                    .start_stale_transaction_cleanup(max_idle_age, sweep_interval),
            )
        } else {
            info!("Transaction idle cleanup worker disabled");
            None
        };

        #[cfg(feature = "postgres")]
        let zanzibar_store_for_pg = zanzibar_store.clone();
        let http_server = Self::build_http_server(HttpServerArgs {
            database: database.clone(),
            config,
            event_bus,
            query_setup: &query_setup,
            p2p_adapter: p2p_setup.http_adapter.clone(),
            manage_requester: p2p_setup.manage_requester.clone(),
            nac_adapter,
            txn_broadcaster: p2p_setup.txn_broadcaster.clone(),
            acp_setup: &acp_setup,
            zanzibar_store,
            user_did: user_did.as_ref(),
            node_identity_did,
        })?;
        #[cfg(feature = "postgres")]
        let pg_server = Self::build_pg_server(
            database,
            config,
            &query_setup,
            &acp_setup,
            zanzibar_store_for_pg,
        )?;

        Ok(ServerSetup {
            p2p_handle: p2p_setup.host_handle,
            p2p_tasks: p2p_setup.p2p_tasks,
            downsample_task,
            txn_cleanup_task,
            http_server,
            #[cfg(feature = "postgres")]
            pg_server,
        })
    }
}
