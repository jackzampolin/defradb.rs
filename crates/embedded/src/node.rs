use std::path::PathBuf;
use std::sync::Arc;

use crate::node_acp::{create_document_acp, create_nac_manager};
use crate::node_identity::create_node_identity;
use crate::node_tasks::BackgroundTasks;
#[cfg(feature = "iroh")]
use crate::IrohConfig;
use crate::{
    DocumentAcpConfig, EmbeddedNodeConfig, EmbeddedStore, Libp2pConfig, ManagedP2PSystem,
    Persistence, SigningConfig, SigningKey, SourceHubConfig, TransportConfig,
};
use anyhow::{anyhow, Context, Result};
use p2p::sync::SyncConfig;

pub(crate) type EmbeddedBlockstore<S> = blockstore::DefraBlockstore<S>;
pub(crate) type EmbeddedMergeHandler<S> = db_merge::AcpMergeHandler<S, EmbeddedBlockstore<S>>;
type EmbeddedTxnRegistry<S> = db::DbTransactionRegistry<S>;
pub(crate) type WireDocumentAcpCallback = Box<dyn FnOnce(Arc<dyn acp::DocumentACP>)>;

/// Embedded DefraDB node assembled for native/mobile embedding.
pub struct EmbeddedNode<S: storage::corekv::Store> {
    pub database: Arc<db::DB<S>>,
    background_tasks: Arc<BackgroundTasks>,
    pub txn_registry: Arc<EmbeddedTxnRegistry<S>>,
    pub query_runner: Arc<dyn query::QueryExecutor>,
    pub nac_manager: Arc<dyn db::NacManagerApi>,
    pub document_acp: Arc<dyn acp::DocumentACP>,
    pub local_zanzibar_store: Option<Arc<dyn acp::ZanzibarStore>>,
    pub event_bus: Arc<dyn events::Bus>,
    pub node_identity_did: Option<String>,
    pub sourcehub_acp: Option<Arc<sourcehub::SourceHubDocumentACP>>,
    pub p2p: Option<Arc<ManagedP2PSystem>>,
}

impl<S: storage::corekv::Store + 'static> EmbeddedNode<S> {
    pub fn builder() -> NodeBuilder {
        NodeBuilder::default()
    }

    pub fn p2p(&self) -> Option<&Arc<ManagedP2PSystem>> {
        self.p2p.as_ref()
    }

    pub fn background_tasks(&self) -> Arc<BackgroundTasks> {
        self.background_tasks.clone()
    }

    pub async fn execute(&self, query_str: &str) -> query::QueryResponse {
        self.query_runner
            .execute(query::QueryRequest::new(query_str))
            .await
    }

    pub async fn add_schema(&self, sdl: &str) -> Result<()> {
        let collections =
            query::parse_sdl(sdl).map_err(|error| anyhow!("SDL parse error: {error}"))?;
        schema::definition_validation::validate_new_collections(&collections)
            .map_err(|error| anyhow!("schema validation error: {error}"))?;

        for collection in collections {
            self.database
                .create_collection(collection)
                .await
                .map_err(|error| anyhow!("create collection error: {error}"))?;
        }

        Ok(())
    }
}

/// Builder for memory/redb embedded nodes.
#[derive(Default)]
pub struct NodeBuilder {
    data_path: Option<PathBuf>,
    config: EmbeddedNodeConfig,
}

impl NodeBuilder {
    pub fn data_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.data_path = Some(path.into());
        self
    }

    pub fn with_transport(mut self, transport: TransportConfig) -> Self {
        self.config.transport = transport;
        self
    }

    pub fn with_libp2p(mut self, listen_addr: impl Into<String>) -> Self {
        self.config.transport = TransportConfig::Libp2p(Libp2pConfig {
            listen_addr: listen_addr.into(),
        });
        self
    }

    #[cfg(feature = "iroh")]
    pub fn with_iroh(mut self, config: IrohConfig) -> Self {
        self.config.transport = TransportConfig::Iroh(config);
        self
    }

    pub fn enable_signing(mut self) -> Self {
        self.config.signing = SigningConfig::Enabled { key: None };
        self
    }

    pub fn with_signing_key(mut self, key: SigningKey) -> Self {
        self.config.signing = SigningConfig::Enabled { key: Some(key) };
        self
    }

    pub fn with_signing_identity_did(mut self, did: impl Into<String>) -> Self {
        self.config.signing = SigningConfig::RegisteredIdentity { did: did.into() };
        self
    }

    pub fn with_sourcehub(mut self, config: SourceHubConfig) -> Self {
        self.config.document_acp = DocumentAcpConfig::SourceHub(config);
        self
    }

    pub async fn build(mut self) -> Result<EmbeddedNode<EmbeddedStore>> {
        let (store, persistence) = if let Some(path) = self.data_path.take() {
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await.with_context(|| {
                    format!("failed to create parent directory for '{}'", path.display())
                })?;
            }

            #[cfg(feature = "iroh")]
            if let TransportConfig::Iroh(iroh) = &mut self.config.transport {
                if iroh.secret_key_path.is_none() {
                    iroh.secret_key_path =
                        Some(PathBuf::from(format!("{}.iroh.key", path.display())));
                }
            }

            tracing::info!(
                storage_backend = "redb",
                data_path = %path.display(),
                "embedded node starting"
            );

            let redb = storage::RedbStore::open(
                path.to_str()
                    .ok_or_else(|| anyhow!("data path contains non-UTF-8 characters"))?,
            )
            .with_context(|| format!("failed to open redb store at '{}'", path.display()))?;

            (Arc::new(EmbeddedStore::Redb(redb)), Persistence::Persistent)
        } else {
            tracing::info!(
                storage_backend = "memory",
                "embedded node starting (ephemeral, no data_path)"
            );
            (
                Arc::new(EmbeddedStore::Memory(storage::MemoryStore::new())),
                Persistence::Memory,
            )
        };

        self.config.persistence = persistence;
        build_with_store(store, self.config).await
    }
}

pub struct ShutdownHandle {
    inner: ShutdownKind,
}

enum ShutdownKind {
    Libp2p {
        handle: Box<p2p::P2PHostHandle>,
        aborts: Vec<tokio::task::AbortHandle>,
    },
    #[cfg(feature = "iroh")]
    Iroh {
        transport: p2p::iroh::IrohTransport,
        aborts: Vec<tokio::task::AbortHandle>,
    },
}

impl ShutdownHandle {
    pub(crate) fn libp2p(
        handle: p2p::P2PHostHandle,
        aborts: Vec<tokio::task::AbortHandle>,
    ) -> Self {
        Self {
            inner: ShutdownKind::Libp2p {
                handle: Box::new(handle),
                aborts,
            },
        }
    }

    #[cfg(feature = "iroh")]
    pub(crate) fn iroh(
        transport: p2p::iroh::IrohTransport,
        aborts: Vec<tokio::task::AbortHandle>,
    ) -> Self {
        Self {
            inner: ShutdownKind::Iroh { transport, aborts },
        }
    }

    pub async fn shutdown(&self) {
        match &self.inner {
            ShutdownKind::Libp2p { handle, aborts } => {
                for abort in aborts {
                    abort.abort();
                }
                let _ = handle.shutdown().await;
            }
            #[cfg(feature = "iroh")]
            ShutdownKind::Iroh { transport, aborts } => {
                let _ = transport.shutdown().await;
                for abort in aborts {
                    abort.abort();
                }
            }
        }

        defra_core::signing::clear_identity_store();
    }
}

pub async fn build_with_store<S>(
    store: Arc<S>,
    config: EmbeddedNodeConfig,
) -> Result<EmbeddedNode<S>>
where
    S: storage::corekv::Store + 'static,
{
    let event_bus: Arc<dyn events::Bus> = Arc::new(events::ChannelBus::default());

    let (raw_identity, node_identity_did) = create_node_identity(&config.signing)?;
    let mut db_options = db::DbOptions::default();
    if let Some(identity) = raw_identity {
        db_options = db_options.with_node_identity(identity);
    }

    let mut database = db::DB::open_from_arc_with_options(store.clone(), db_options)
        .await
        .map_err(|error| anyhow!("failed to open database: {error}"))?;
    database.set_event_bus(event_bus.clone());
    let database = Arc::new(database);
    let background_tasks = Arc::new(BackgroundTasks::new(Some(
        database.clone().start_downsample_task(),
    )));

    let sync_config = SyncConfig {
        max_concurrent_dag_fetches: config
            .max_concurrent_dag_fetches
            .unwrap_or(p2p::sync::DEFAULT_MAX_CONCURRENT_DAG_FETCHES),
        max_concurrent_push_tasks: config
            .max_concurrent_push_tasks
            .unwrap_or(p2p::sync::DEFAULT_MAX_CONCURRENT_PUSH_TASKS),
        rate_limit_burst: config
            .rate_limit_burst
            .unwrap_or(p2p::sync::DEFAULT_RATE_LIMIT_BURST),
        rate_limit_rate: config
            .rate_limit_rate
            .unwrap_or(p2p::sync::DEFAULT_RATE_LIMIT_RATE),
        ..Default::default()
    };

    let mut p2p_setup = match &config.transport {
        TransportConfig::None => None,
        TransportConfig::Libp2p(libp2p) => Some(
            crate::node_p2p::setup_libp2p(
                store.clone(),
                database.clone(),
                event_bus.clone(),
                libp2p,
                sync_config.clone(),
            )
            .await?,
        ),
        #[cfg(feature = "iroh")]
        TransportConfig::Iroh(iroh) => Some(
            crate::node_p2p::setup_iroh(
                store.clone(),
                database.clone(),
                event_bus.clone(),
                iroh,
                sync_config.clone(),
            )
            .await?,
        ),
    };

    let (document_acp, local_zanzibar_store, sourcehub_acp) =
        create_document_acp(store.clone(), config.persistence, &config.document_acp).await?;
    let nac_manager = create_nac_manager(store.clone(), config.persistence).await?;

    if let Some(ref mut setup) = p2p_setup {
        setup.merge_handler.set_document_acp(document_acp.clone());
        setup
            .merge_handler
            .set_strict_replicated_doc_access(sourcehub_acp.is_some());
        if let Some(wire_document_acp) = setup.wire_document_acp.take() {
            wire_document_acp(document_acp.clone());
        }
    }

    let fetcher = db::LensedAutoCommitFetcher::new(database.clone());
    let collection_provider: Arc<dyn query::CollectionProvider> =
        db::DbCollectionProvider::new_arc(database.clone());
    let txn_registry = Arc::new(db::DbTransactionRegistry::new(database.clone()));

    let query_runner = query::QueryRunner::with_arc_registry_and_provider(
        fetcher,
        collection_provider,
        txn_registry.clone(),
    )
    .with_mutator(
        p2p_setup
            .as_ref()
            .map(|setup| setup.mutator.clone())
            .unwrap_or_else(|| Arc::new(db::AutoCommitMutator::new(database.clone()))),
    )
    .with_acp(document_acp.clone())
    .with_lens_store(database.lens_store().clone());

    let query_runner: Arc<dyn query::QueryExecutor> = Arc::new(query_runner);

    Ok(EmbeddedNode {
        database,
        background_tasks,
        txn_registry,
        query_runner,
        nac_manager,
        document_acp,
        local_zanzibar_store,
        event_bus,
        node_identity_did,
        sourcehub_acp,
        p2p: p2p_setup.map(|setup| setup.system),
    })
}
