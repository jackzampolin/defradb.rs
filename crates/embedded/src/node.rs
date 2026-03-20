use std::path::PathBuf;
use std::sync::Arc;

use crate::libp2p_adapter::{DbDocPusher, P2PAdapter};
#[cfg(feature = "iroh")]
use crate::transport_doc_pusher::DbTransportDocPusher;
#[cfg(feature = "iroh")]
use crate::transport_version_syncer::DbTransportVersionSyncer;
use crate::version_syncer::DbVersionSyncer;
#[cfg(feature = "iroh")]
use crate::IrohConfig;
use crate::{
    DocumentAcpConfig, EmbeddedNodeConfig, EmbeddedStore, Libp2pConfig, ManagedP2PSystem,
    P2POperations, Persistence, SigningConfig, SigningKey, SourceHubConfig, TransportConfig,
    TransportKind,
};
use anyhow::{anyhow, Context, Result};
use identity::Identity;
use p2p::sync::{PushFailure, ReplicationConfig, ReplicationLoop, ReplicationResult, SyncConfig};
use p2p::topics::DefraTopic;
#[cfg(feature = "iroh")]
use p2p::P2PTransport;

type EmbeddedBlockstore<S> = blockstore::DefraBlockstore<S>;
type EmbeddedMergeHandler<S> = db::AcpMergeHandler<S, EmbeddedBlockstore<S>>;
type EmbeddedTxnRegistry<S> = db::DbTransactionRegistry<S>;
type WireDocumentAcpCallback = Box<dyn FnOnce(Arc<dyn acp::DocumentACP>)>;

/// Embedded DefraDB node assembled for native/mobile embedding.
pub struct EmbeddedNode<S: storage::corekv::Store> {
    pub database: Arc<db::DB<S>>,
    background_tasks: Arc<BackgroundTasks>,
    pub txn_registry: Arc<EmbeddedTxnRegistry<S>>,
    pub query_runner: Arc<dyn query::QueryExecutor>,
    pub nac_manager: Arc<dyn db::NacManagerApi>,
    pub document_acp: Arc<dyn acp::DocumentACP>,
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
        db::definition_validation::validate_new_collections(&collections)
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

pub struct BackgroundTasks {
    downsample_task: Option<tokio::task::JoinHandle<()>>,
}

impl BackgroundTasks {
    fn new(downsample_task: Option<tokio::task::JoinHandle<()>>) -> Self {
        Self { downsample_task }
    }
}

impl Drop for BackgroundTasks {
    fn drop(&mut self) {
        if let Some(task) = self.downsample_task.take() {
            task.abort();
        }
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

    pub fn with_encryption_key(mut self, key: Vec<u8>) -> Self {
        self.config.encryption_key = Some(key);
        self
    }

    pub async fn build(mut self) -> Result<EmbeddedNode<EmbeddedStore>> {
        let (store, persistence) = if let Some(path) = self.data_path.take() {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).with_context(|| {
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

            let redb = storage::RedbStore::open(
                path.to_str()
                    .ok_or_else(|| anyhow!("data path contains non-UTF-8 characters"))?,
            )
            .with_context(|| format!("failed to open redb store at '{}'", path.display()))?;

            (Arc::new(EmbeddedStore::Redb(redb)), Persistence::Persistent)
        } else {
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
    fn libp2p(handle: p2p::P2PHostHandle, aborts: Vec<tokio::task::AbortHandle>) -> Self {
        Self {
            inner: ShutdownKind::Libp2p {
                handle: Box::new(handle),
                aborts,
            },
        }
    }

    #[cfg(feature = "iroh")]
    fn iroh(transport: p2p::iroh::IrohTransport, aborts: Vec<tokio::task::AbortHandle>) -> Self {
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
    }
}

struct P2PSetup<S: storage::corekv::Store + 'static> {
    system: Arc<ManagedP2PSystem>,
    mutator: Arc<dyn query::DocMutator>,
    merge_handler: Arc<EmbeddedMergeHandler<S>>,
    wire_document_acp: Option<WireDocumentAcpCallback>,
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
        ..Default::default()
    };

    let mut p2p_setup = match &config.transport {
        TransportConfig::None => None,
        TransportConfig::Libp2p(libp2p) => Some(
            setup_libp2p(
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
            setup_iroh(
                store.clone(),
                database.clone(),
                event_bus.clone(),
                iroh,
                sync_config.clone(),
            )
            .await?,
        ),
    };

    let (document_acp, sourcehub_acp) =
        create_document_acp(store.clone(), config.persistence, &config.document_acp).await?;
    let nac_manager = create_nac_manager(store.clone(), config.persistence).await?;

    if let Some(ref mut setup) = p2p_setup {
        setup.merge_handler.set_document_acp(document_acp.clone());
        if let Some(wire_document_acp) = setup.wire_document_acp.take() {
            wire_document_acp(document_acp.clone());
        }
    }

    let fetcher = db::LensedAutoCommitFetcher::new(database.clone());
    let collection_provider: Arc<dyn query::CollectionProvider> =
        db::DbCollectionProvider::new_arc(database.clone());
    let txn_registry = Arc::new(db::DbTransactionRegistry::new(database.clone()));

    let mut query_runner = query::QueryRunner::with_arc_registry_and_provider(
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

    if let Some(key) = config.encryption_key {
        query_runner = query_runner.with_encryption_key(key);
    }

    let query_runner: Arc<dyn query::QueryExecutor> = Arc::new(query_runner);

    Ok(EmbeddedNode {
        database,
        background_tasks,
        txn_registry,
        query_runner,
        nac_manager,
        document_acp,
        event_bus,
        node_identity_did,
        sourcehub_acp,
        p2p: p2p_setup.map(|setup| setup.system),
    })
}

fn create_node_identity(
    config: &SigningConfig,
) -> Result<(Option<identity::RawIdentity>, Option<String>)> {
    match config {
        SigningConfig::Disabled => Ok((None, None)),
        SigningConfig::Enabled { key } => {
            let raw_identity = match key {
                Some(SigningKey::Secp256k1(bytes)) => {
                    let private_key = crypto::Secp256k1PrivateKey::from_bytes(bytes)
                        .map_err(|error| anyhow!("failed to load secp256k1 key: {error}"))?;
                    identity::RawIdentity::from_secp256k1(private_key)
                        .map_err(|error| anyhow!("failed to create node identity: {error}"))?
                }
                Some(SigningKey::Secp256r1(bytes)) => {
                    let private_key = crypto::Secp256r1PrivateKey::from_bytes(bytes)
                        .map_err(|error| anyhow!("failed to load secp256r1 key: {error}"))?;
                    identity::RawIdentity::from_secp256r1(private_key)
                        .map_err(|error| anyhow!("failed to create node identity: {error}"))?
                }
                Some(SigningKey::Ed25519(bytes)) => {
                    let private_key = crypto::Ed25519PrivateKey::from_bytes(bytes)
                        .map_err(|error| anyhow!("failed to load ed25519 key: {error}"))?;
                    identity::RawIdentity::from_ed25519(private_key)
                        .map_err(|error| anyhow!("failed to create node identity: {error}"))?
                }
                None => {
                    let private_key = crypto::generate_secp256k1()
                        .map_err(|error| anyhow!("failed to generate node signing key: {error}"))?;
                    identity::RawIdentity::from_secp256k1(private_key)
                        .map_err(|error| anyhow!("failed to create node identity: {error}"))?
                }
            };

            let did = raw_identity
                .did()
                .map_err(|error| anyhow!("failed to derive node DID: {error}"))?;
            let did_str = did.to_string();
            let key_type = match key {
                Some(SigningKey::Ed25519(_)) => "ed25519".to_string(),
                Some(SigningKey::Secp256r1(_)) => "secp256r1".to_string(),
                _ => "secp256k1".to_string(),
            };

            defra_core::signing::store_identity(
                &did_str,
                defra_core::signing::SigningConfig {
                    key_type,
                    private_key_bytes: raw_identity.private_key_bytes().to_vec(),
                    public_key_bytes: raw_identity.public_key_bytes().to_vec(),
                    public_key_hex: hex::encode(raw_identity.public_key_bytes()),
                    remote_signer: None,
                    signing_authorization: None,
                },
            );

            Ok((Some(raw_identity), Some(did_str)))
        }
        SigningConfig::RegisteredIdentity { did } => {
            let stored = defra_core::signing::get_identity(did)
                .ok_or_else(|| anyhow!("no signing identity registered for DID: {}", did))?;
            if !stored.has_local_private_key() && !stored.has_remote_signer() {
                return Err(anyhow!(
                    "registered identity {} has neither a local key nor a remote signer",
                    did
                ));
            }

            let raw_identity = if stored.has_local_private_key() {
                let raw_identity = raw_identity_from_stored_config(&stored)?;
                let derived_did = raw_identity
                    .did()
                    .map_err(|error| anyhow!("failed to derive stored identity DID: {error}"))?;
                if derived_did.as_str() != did {
                    return Err(anyhow!(
                        "registered identity DID mismatch: expected {}, derived {}",
                        did,
                        derived_did
                    ));
                }
                Some(raw_identity)
            } else {
                None
            };

            Ok((raw_identity, Some(did.clone())))
        }
    }
}

fn raw_identity_from_stored_config(
    config: &defra_core::signing::SigningConfig,
) -> Result<identity::RawIdentity> {
    match config.key_type.as_str() {
        "ed25519" => {
            let private_key = crypto::Ed25519PrivateKey::from_bytes(&config.private_key_bytes)
                .map_err(|error| anyhow!("failed to load stored ed25519 key: {error}"))?;
            identity::RawIdentity::from_ed25519(private_key)
                .map_err(|error| anyhow!("failed to create stored ed25519 identity: {error}"))
        }
        "secp256k1" => {
            let private_key = crypto::Secp256k1PrivateKey::from_bytes(&config.private_key_bytes)
                .map_err(|error| anyhow!("failed to load stored secp256k1 key: {error}"))?;
            identity::RawIdentity::from_secp256k1(private_key)
                .map_err(|error| anyhow!("failed to create stored secp256k1 identity: {error}"))
        }
        "secp256r1" => {
            let private_key = crypto::Secp256r1PrivateKey::from_bytes(&config.private_key_bytes)
                .map_err(|error| anyhow!("failed to load stored secp256r1 key: {error}"))?;
            identity::RawIdentity::from_secp256r1(private_key)
                .map_err(|error| anyhow!("failed to create stored secp256r1 identity: {error}"))
        }
        other => Err(anyhow!(
            "stored identity {} cannot be used as a node identity",
            other
        )),
    }
}

async fn create_document_acp<S>(
    store: Arc<S>,
    persistence: Persistence,
    config: &DocumentAcpConfig,
) -> Result<(
    Arc<dyn acp::DocumentACP>,
    Option<Arc<sourcehub::SourceHubDocumentACP>>,
)>
where
    S: storage::corekv::Store + 'static,
{
    match config {
        DocumentAcpConfig::SourceHub(sourcehub_config) => {
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
        DocumentAcpConfig::Local => match persistence {
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

async fn create_nac_manager<S>(
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

async fn setup_libp2p<S>(
    store: Arc<S>,
    database: Arc<db::DB<S>>,
    event_bus: Arc<dyn events::Bus>,
    config: &Libp2pConfig,
    sync_config: SyncConfig,
) -> Result<P2PSetup<S>>
where
    S: storage::corekv::Store + 'static,
{
    use p2p::bitswap::BitswapStoreAdapter;
    use p2p::sync::DocumentHeadProvider;
    use storage::stores::Peerstore;

    let blockstore = Arc::new(blockstore::DefraBlockstore::new(store.clone(), true));
    let bitswap_store = BitswapStoreAdapter::new(blockstore.clone());

    let p2p_keypair = {
        let peerstore = Peerstore::new(store.clone());
        let key_id = "__local_p2p_identity__";
        match peerstore.get_replicator(key_id).await {
            Ok(Some(bytes)) => match libp2p::identity::Keypair::from_protobuf_encoding(&bytes) {
                Ok(keypair) => keypair,
                Err(_) => {
                    let keypair = libp2p::identity::Keypair::generate_ed25519();
                    if let Ok(encoded) = keypair.to_protobuf_encoding() {
                        let _ = peerstore.create_replicator(key_id, &encoded).await;
                    }
                    keypair
                }
            },
            _ => {
                let keypair = libp2p::identity::Keypair::generate_ed25519();
                if let Ok(encoded) = keypair.to_protobuf_encoding() {
                    let _ = peerstore.create_replicator(key_id, &encoded).await;
                }
                keypair
            }
        }
    };

    let (host, handle, event_rx, _replicator_registry) =
        p2p::P2PHost::with_keypair(p2p_keypair, bitswap_store)
            .await
            .map_err(|error| anyhow!("failed to create P2P host: {error}"))?;
    tokio::spawn(async move {
        host.run().await;
    });

    let listen_addr = config
        .listen_addr
        .parse()
        .map_err(|error| anyhow!("invalid multiaddr '{}': {error}", config.listen_addr))?;
    handle
        .listen(listen_addr)
        .await
        .map_err(|error| anyhow!("failed to start listening: {error}"))?;

    for topic in [
        DefraTopic::DocSync,
        DefraTopic::Encryption,
        DefraTopic::Custom("sync-branchable".to_string()),
    ] {
        if let Err(error) = handle.subscribe(topic.clone()).await {
            tracing::warn!(topic = %topic, error = %error, "failed to subscribe to default topic");
        }
    }

    let collection_store: Arc<dyn p2p::sync::P2PCollectionStorage> =
        Arc::new(p2p::sync::P2PCollectionStore::new(store.clone()));
    let head_provider: Arc<dyn DocumentHeadProvider> =
        Arc::new(db::DbHeadProvider::new(database.clone()));
    let (mut coordinator, sync_events_rx) = p2p::sync::SyncCoordinator::with_head_provider(
        p2p::Libp2pTransport::new(handle.clone()),
        blockstore.clone(),
        sync_config,
        p2p::bitswap::AccessMode::Controlled,
        Arc::new(p2p::ReplicatorRegistry::new()),
        collection_store,
        head_provider,
    )
    .await
    .map_err(|error| anyhow!("failed to create sync coordinator: {error}"))?;

    let (failure_tx, failure_rx) = tokio::sync::mpsc::unbounded_channel::<PushFailure>();
    coordinator.set_failure_channel(failure_tx);
    let coordinator = Arc::new(coordinator);
    let merge_handler_inner = Arc::new(db::DbMergeHandler::new(
        database.clone(),
        blockstore.clone(),
    ));
    let merge_handler = Arc::new(db::AcpMergeHandler::new(merge_handler_inner.clone()));

    match coordinator.load_p2p_collections().await {
        Ok(count) if count > 0 => tracing::debug!(count, "loaded persisted P2P collections"),
        Ok(_) => {}
        Err(error) => tracing::warn!(error = %error, "failed to load persisted P2P collections"),
    }

    let host_event_task =
        spawn_libp2p_event_handler(event_rx, coordinator.clone(), event_bus.clone());
    let replication_task = spawn_replication_loop(
        coordinator.clone(),
        sync_events_rx,
        merge_handler.clone(),
        event_bus.clone(),
    );
    let failure_recorder_task = spawn_failure_recorder(store.clone(), failure_rx);

    let doc_pusher_impl = Arc::new(DbDocPusher::new(database.clone()));
    let doc_pusher_for_acp = doc_pusher_impl.clone();
    let doc_pusher: Arc<dyn crate::DocPusher> = doc_pusher_impl;
    let version_syncer = Some(DbVersionSyncer::new_arc(
        blockstore.clone(),
        merge_handler_inner,
        database.clone(),
    ));
    let retry_loop_task =
        spawn_libp2p_retry_loop(store.clone(), handle.clone(), doc_pusher.clone());

    let restore_peerstore = storage::stores::Peerstore::new(store.clone());
    restore_libp2p_replicators(&handle, &restore_peerstore).await;
    let restored_doc_ids = restore_libp2p_documents(&handle, &restore_peerstore).await;

    let adapter = P2PAdapter::with_full_context(
        handle.clone(),
        coordinator.clone(),
        doc_pusher,
        event_bus,
        version_syncer,
    );
    adapter.set_initial_tracked_documents(restored_doc_ids);
    let system = Arc::new(ManagedP2PSystem::new(
        TransportKind::Libp2p,
        Arc::new(adapter) as Arc<dyn P2POperations>,
        ShutdownHandle::libp2p(
            handle.clone(),
            vec![
                host_event_task.abort_handle(),
                replication_task.abort_handle(),
                failure_recorder_task.abort_handle(),
                retry_loop_task.abort_handle(),
            ],
        ),
    ));

    Ok(P2PSetup {
        system,
        mutator: Arc::new(db::BroadcastMutator::new(database, coordinator)),
        merge_handler,
        wire_document_acp: Some(Box::new(move |acp| {
            doc_pusher_for_acp.set_document_acp(acp);
        })),
    })
}

#[cfg(feature = "iroh")]
async fn setup_iroh<S>(
    store: Arc<S>,
    database: Arc<db::DB<S>>,
    event_bus: Arc<dyn events::Bus>,
    config: &IrohConfig,
    sync_config: SyncConfig,
) -> Result<P2PSetup<S>>
where
    S: storage::corekv::Store + 'static,
{
    use crate::IrohP2PAdapter;
    use storage::stores::Peerstore;

    let secret_key = load_or_generate_iroh_secret_key(config.secret_key_path.as_deref())?;
    let iroh_config = p2p::iroh::IrohEndpointConfig {
        secret_key: secret_key.clone(),
        relay_mode: config.relay_mode.clone(),
        discovery: config.discovery.clone(),
        bind_port: config.bind_port,
        bind_addr: config.bind_addr,
    };
    let (command_tx, event_rx, endpoint_task) = p2p::iroh::spawn_endpoint(iroh_config)
        .await
        .map_err(|error| anyhow!("failed to spawn iroh endpoint: {error}"))?;

    let transport = p2p::iroh::IrohTransport::new(command_tx, secret_key);
    let blockstore = Arc::new(blockstore::DefraBlockstore::new(store.clone(), true));
    let collection_store: Arc<dyn p2p::sync::P2PCollectionStorage> =
        Arc::new(p2p::sync::P2PCollectionStore::new(store.clone()));
    let head_provider: Arc<dyn p2p::sync::DocumentHeadProvider> =
        Arc::new(db::DbHeadProvider::new(database.clone()));
    let (mut coordinator, sync_events_rx) = p2p::sync::SyncCoordinator::with_head_provider(
        transport.clone(),
        blockstore.clone(),
        sync_config,
        p2p::bitswap::AccessMode::Controlled,
        Arc::new(p2p::ReplicatorRegistry::new()),
        collection_store,
        head_provider,
    )
    .await
    .map_err(|error| anyhow!("failed to create iroh sync coordinator: {error}"))?;

    let (failure_tx, failure_rx) = tokio::sync::mpsc::unbounded_channel::<PushFailure>();
    coordinator.set_failure_channel(failure_tx);
    let coordinator = Arc::new(coordinator);
    let merge_handler_inner = Arc::new(db::DbMergeHandler::new(
        database.clone(),
        blockstore.clone(),
    ));
    let merge_handler = Arc::new(db::AcpMergeHandler::new(merge_handler_inner.clone()));

    match coordinator.load_p2p_collections().await {
        Ok(count) if count > 0 => tracing::debug!(count, "loaded persisted P2P collections"),
        Ok(_) => {}
        Err(error) => tracing::warn!(error = %error, "failed to load persisted P2P collections"),
    }

    let event_handler_task =
        spawn_iroh_event_handler(event_rx, coordinator.clone(), event_bus.clone());
    let replication_task = spawn_replication_loop(
        coordinator.clone(),
        sync_events_rx,
        merge_handler.clone(),
        event_bus.clone(),
    );
    let failure_recorder_task = spawn_failure_recorder(store.clone(), failure_rx);

    let doc_pusher_impl = Arc::new(DbTransportDocPusher::new(
        database.clone(),
        transport.clone(),
    ));
    let doc_pusher_for_acp = doc_pusher_impl.clone();
    let doc_pusher: Arc<dyn crate::TransportDocPusher> = doc_pusher_impl;
    let version_syncer = Some(DbTransportVersionSyncer::new_arc(
        blockstore.clone(),
        merge_handler_inner,
        database.clone(),
        transport.clone(),
    ));
    let retry_loop_task =
        spawn_iroh_retry_loop(store.clone(), transport.clone(), doc_pusher.clone());

    let restore_peerstore = Peerstore::new(store.clone());
    restore_iroh_replicators(&coordinator, &restore_peerstore).await;
    let restored_doc_ids = restore_iroh_documents(&transport, &restore_peerstore).await;

    let adapter = IrohP2PAdapter::with_full_context(
        transport.clone(),
        coordinator.clone(),
        doc_pusher,
        event_bus,
        version_syncer,
    );
    adapter.set_initial_tracked_documents(restored_doc_ids);
    let system = Arc::new(ManagedP2PSystem::new(
        TransportKind::Iroh,
        Arc::new(adapter) as Arc<dyn P2POperations>,
        ShutdownHandle::iroh(
            transport.clone(),
            vec![
                endpoint_task.abort_handle(),
                event_handler_task.abort_handle(),
                replication_task.abort_handle(),
                failure_recorder_task.abort_handle(),
                retry_loop_task.abort_handle(),
            ],
        ),
    ));

    Ok(P2PSetup {
        system,
        mutator: Arc::new(db::BroadcastMutator::new(database, coordinator)),
        merge_handler,
        wire_document_acp: Some(Box::new(move |acp| {
            doc_pusher_for_acp.set_document_acp(acp);
        })),
    })
}

fn spawn_libp2p_event_handler<B: blockstore::Blockstore + 'static>(
    mut events: tokio::sync::mpsc::Receiver<p2p::HostEvent>,
    coordinator: Arc<p2p::sync::Libp2pSyncCoordinator<B>>,
    event_bus: Arc<dyn events::Bus>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(32));
        while let Some(event) = events.recv().await {
            match &event {
                p2p::HostEvent::PeerSubscribed { peer_id, topic } => {
                    event_bus.publish(events::Message::topic_peer_event(
                        events::TopicPeerEventData {
                            peer_id: peer_id.to_string(),
                            topic: topic.clone(),
                            event_type: "JOINED".to_string(),
                        },
                    ));
                }
                p2p::HostEvent::PeerUnsubscribed { peer_id, topic } => {
                    event_bus.publish(events::Message::topic_peer_event(
                        events::TopicPeerEventData {
                            peer_id: peer_id.to_string(),
                            topic: topic.clone(),
                            event_type: "LEFT".to_string(),
                        },
                    ));
                }
                _ => {}
            }

            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let coordinator = coordinator.clone();
            tokio::spawn(async move {
                if let Err(error) = coordinator
                    .handle_transport_event(p2p::convert_host_event(event))
                    .await
                {
                    tracing::error!(error = %error, "error handling libp2p event");
                }
                drop(permit);
            });
        }
    })
}

#[cfg(feature = "iroh")]
fn spawn_iroh_event_handler<B: blockstore::Blockstore + 'static>(
    mut events: tokio::sync::mpsc::Receiver<p2p::TransportEvent>,
    coordinator: Arc<p2p::sync::IrohSyncCoordinator<B>>,
    event_bus: Arc<dyn events::Bus>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(32));
        while let Some(event) = events.recv().await {
            match &event {
                p2p::TransportEvent::PeerSubscribed { peer_id, topic } => {
                    event_bus.publish(events::Message::topic_peer_event(
                        events::TopicPeerEventData {
                            peer_id: peer_id.to_string(),
                            topic: topic.clone(),
                            event_type: "JOINED".to_string(),
                        },
                    ));
                }
                p2p::TransportEvent::PeerUnsubscribed { peer_id, topic } => {
                    event_bus.publish(events::Message::topic_peer_event(
                        events::TopicPeerEventData {
                            peer_id: peer_id.to_string(),
                            topic: topic.clone(),
                            event_type: "LEFT".to_string(),
                        },
                    ));
                }
                _ => {}
            }

            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let coordinator = coordinator.clone();
            tokio::spawn(async move {
                if let Err(error) = coordinator.handle_transport_event(event).await {
                    tracing::error!(error = %error, "error handling iroh event");
                }
                drop(permit);
            });
        }
    })
}

fn spawn_replication_loop<B, T, S>(
    coordinator: Arc<p2p::sync::SyncCoordinator<B, T>>,
    sync_events_rx: tokio::sync::mpsc::Receiver<p2p::sync::SyncEvent>,
    merge_handler: Arc<EmbeddedMergeHandler<S>>,
    event_bus: Arc<dyn events::Bus>,
) -> tokio::task::JoinHandle<()>
where
    B: blockstore::Blockstore + 'static,
    T: p2p::P2PTransport,
    S: storage::corekv::Store + 'static,
{
    tokio::spawn(async move {
        let local_peer = coordinator.local_peer_id().to_string();
        ReplicationLoop::run_parallel(
            coordinator,
            sync_events_rx,
            merge_handler,
            ReplicationConfig::default(),
            move |result| match &result {
                ReplicationResult::Merged {
                    cid,
                    doc_id,
                    collection_id,
                }
                | ReplicationResult::MergedButBroadcastFailed {
                    cid,
                    doc_id,
                    collection_id,
                    ..
                } => {
                    event_bus.publish(events::Message::merge_complete(events::MergeCompleteData {
                        doc_id: doc_id.clone(),
                        cid: *cid,
                        collection_id: collection_id.clone(),
                        by_peer: local_peer.clone(),
                    }));
                    if !doc_id.is_empty() {
                        event_bus.publish(events::Message::se_artifact_received(
                            events::SEArtifactReceivedData {
                                doc_id: doc_id.clone(),
                            },
                        ));
                    }
                }
                ReplicationResult::Failed { cid, error } => {
                    tracing::error!(cid = %cid, error = %error, "block merge failed");
                }
                ReplicationResult::Skipped { cid, reason, .. } => {
                    tracing::debug!(cid = %cid, reason = %reason, "replication loop skipped block");
                }
                _ => {}
            },
        )
        .await;
    })
}

fn spawn_failure_recorder<S: storage::corekv::Store + 'static>(
    store: Arc<S>,
    mut failure_rx: tokio::sync::mpsc::UnboundedReceiver<PushFailure>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(failure) = failure_rx.recv().await {
            let peerstore = storage::stores::Peerstore::new(store.clone());
            let retry_info = storage::stores::RetryInfo::new_initial();
            let info_bytes = match retry_info.to_bytes() {
                Ok(bytes) => bytes,
                Err(error) => {
                    tracing::warn!(error = %error, "failed to serialize retry info");
                    continue;
                }
            };

            if let Err(error) = peerstore
                .record_push_failure(
                    &failure.peer_id.to_string(),
                    &failure.doc_id,
                    &failure.collection_id,
                    &info_bytes,
                )
                .await
            {
                tracing::warn!(error = %error, "failed to record push failure");
            }
        }
    })
}

fn spawn_libp2p_retry_loop<S: storage::corekv::Store + 'static>(
    store: Arc<S>,
    handle: p2p::P2PHostHandle,
    doc_pusher: Arc<dyn crate::DocPusher>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let peerstore = storage::stores::Peerstore::new(store.clone());
            let peers = match peerstore.get_all_retry_peers().await {
                Ok(peers) => peers,
                Err(_) => continue,
            };

            for (peer_id_str, info_bytes) in peers {
                let mut retry_info = match storage::stores::RetryInfo::from_bytes(&info_bytes) {
                    Ok(info) => info,
                    Err(error) => {
                        tracing::warn!(peer_id = %peer_id_str, error = %error, "invalid retry info");
                        continue;
                    }
                };
                if !retry_info.is_due() {
                    continue;
                }

                let peer_id: libp2p::PeerId = match peer_id_str.parse() {
                    Ok(peer_id) => peer_id,
                    Err(error) => {
                        tracing::warn!(peer_id = %peer_id_str, error = %error, "invalid peer ID");
                        continue;
                    }
                };

                let connected = handle.connected_peers().await.unwrap_or_default();
                if !connected.contains(&peer_id) {
                    continue;
                }

                let docs = match peerstore.get_retry_doc_ids(&peer_id_str).await {
                    Ok(docs) => docs,
                    Err(_) => continue,
                };
                if docs.is_empty() {
                    let _ = peerstore.clear_retry_peer(&peer_id_str).await;
                    continue;
                }

                let mut all_succeeded = true;
                for (doc_id, collection_id) in &docs {
                    match doc_pusher
                        .retry_doc(&handle, peer_id, doc_id, collection_id)
                        .await
                    {
                        Ok(()) => {
                            let _ = peerstore.remove_retry_doc(&peer_id_str, doc_id).await;
                        }
                        Err(error) => {
                            tracing::warn!(doc_id = %doc_id, peer_id = %peer_id, error = %error, "retry push failed");
                            all_succeeded = false;
                        }
                    }
                }

                if all_succeeded {
                    let _ = peerstore.clear_retry_peer(&peer_id_str).await;
                } else {
                    retry_info.bump();
                    if let Ok(bytes) = retry_info.to_bytes() {
                        let _ = peerstore.update_retry_info(&peer_id_str, &bytes).await;
                    }
                }
            }
        }
    })
}

#[cfg(feature = "iroh")]
fn spawn_iroh_retry_loop<S: storage::corekv::Store + 'static>(
    store: Arc<S>,
    transport: p2p::iroh::IrohTransport,
    doc_pusher: Arc<dyn crate::TransportDocPusher>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let peerstore = storage::stores::Peerstore::new(store.clone());
            let peers = match peerstore.get_all_retry_peers().await {
                Ok(peers) => peers,
                Err(_) => continue,
            };

            for (peer_id_str, info_bytes) in peers {
                let mut retry_info = match storage::stores::RetryInfo::from_bytes(&info_bytes) {
                    Ok(info) => info,
                    Err(error) => {
                        tracing::warn!(peer_id = %peer_id_str, error = %error, "invalid retry info");
                        continue;
                    }
                };
                if !retry_info.is_due() {
                    continue;
                }

                let peer_id = p2p::transport::PeerId::new(peer_id_str.clone());
                let connected = transport.connected_peers().await.unwrap_or_default();
                if !connected.contains(&peer_id) {
                    continue;
                }

                let docs = match peerstore.get_retry_doc_ids(&peer_id_str).await {
                    Ok(docs) => docs,
                    Err(_) => continue,
                };
                if docs.is_empty() {
                    let _ = peerstore.clear_retry_peer(&peer_id_str).await;
                    continue;
                }

                let mut all_succeeded = true;
                for (doc_id, collection_id) in &docs {
                    match doc_pusher.retry_doc(&peer_id, doc_id, collection_id).await {
                        Ok(()) => {
                            let _ = peerstore.remove_retry_doc(&peer_id_str, doc_id).await;
                        }
                        Err(error) => {
                            tracing::warn!(doc_id = %doc_id, peer_id = %peer_id, error = %error, "retry push failed");
                            all_succeeded = false;
                        }
                    }
                }

                if all_succeeded {
                    let _ = peerstore.clear_retry_peer(&peer_id_str).await;
                } else {
                    retry_info.bump();
                    if let Ok(bytes) = retry_info.to_bytes() {
                        let _ = peerstore.update_retry_info(&peer_id_str, &bytes).await;
                    }
                }
            }
        }
    })
}

async fn restore_libp2p_replicators<S: storage::corekv::Store + 'static>(
    handle: &p2p::P2PHostHandle,
    peerstore: &storage::stores::Peerstore<S>,
) {
    match peerstore.list_replicators().await {
        Ok(entries) => {
            for (peer_id_str, data) in entries {
                match p2p::ReplicatorInfo::from_bytes(&data) {
                    Ok(info) => {
                        if let Some(peer_id) = info.peer_id() {
                            if let Err(error) = handle
                                .create_replicator(peer_id, info.collections.clone())
                                .await
                            {
                                tracing::warn!(peer_id = %peer_id, error = %error, "failed to restore replicator");
                                continue;
                            }

                            for collection_id in &info.collections {
                                let topic = DefraTopic::collection(collection_id);
                                if let Err(error) = handle.subscribe(topic).await {
                                    tracing::warn!(collection_id = %collection_id, error = %error, "failed to restore collection topic");
                                }
                            }
                        }
                    }
                    Err(error) => {
                        tracing::warn!(peer_id = %peer_id_str, error = %error, "failed to decode replicator info");
                    }
                }
            }
        }
        Err(error) => tracing::warn!(error = %error, "failed to load replicators from storage"),
    }
}

async fn restore_libp2p_documents<S: storage::corekv::Store + 'static>(
    handle: &p2p::P2PHostHandle,
    peerstore: &storage::stores::Peerstore<S>,
) -> std::collections::HashSet<String> {
    let mut restored = std::collections::HashSet::new();
    if let Ok(doc_ids) = peerstore.load_documents().await {
        for doc_id in &doc_ids {
            let _ = handle.subscribe(DefraTopic::document(doc_id)).await;
            restored.insert(doc_id.clone());
        }
    }
    restored
}

#[cfg(feature = "iroh")]
async fn restore_iroh_replicators<S, B>(
    coordinator: &Arc<p2p::sync::IrohSyncCoordinator<B>>,
    peerstore: &storage::stores::Peerstore<S>,
) where
    S: storage::corekv::Store + 'static,
    B: blockstore::Blockstore + 'static,
{
    match peerstore.list_replicators().await {
        Ok(entries) => {
            for (_peer_id_str, data) in entries {
                if let Ok(rep_info) = p2p::ReplicatorInfo::from_bytes(&data) {
                    let peer_id = p2p::transport::PeerId::new(rep_info.peer_id_str().to_string());
                    let _ = coordinator
                        .create_replicator(&peer_id, rep_info.collections.clone(), false)
                        .await;
                }
            }
        }
        Err(error) => tracing::warn!(error = %error, "failed to load replicators from storage"),
    }
}

#[cfg(feature = "iroh")]
async fn restore_iroh_documents<S: storage::corekv::Store + 'static>(
    transport: &p2p::iroh::IrohTransport,
    peerstore: &storage::stores::Peerstore<S>,
) -> std::collections::HashSet<String> {
    let mut restored = std::collections::HashSet::new();
    if let Ok(doc_ids) = peerstore.load_documents().await {
        for doc_id in &doc_ids {
            let _ = transport.subscribe(DefraTopic::document(doc_id)).await;
            restored.insert(doc_id.clone());
        }
    }
    restored
}

#[cfg(feature = "iroh")]
fn load_or_generate_iroh_secret_key(path: Option<&std::path::Path>) -> Result<iroh_net::SecretKey> {
    match path {
        Some(path) if path.exists() => {
            let bytes = std::fs::read(path)
                .with_context(|| format!("failed to read iroh secret key '{}'", path.display()))?;
            let array: [u8; 32] = bytes
                .try_into()
                .map_err(|_| anyhow!("iroh secret key file must contain exactly 32 bytes"))?;
            Ok(iroh_net::SecretKey::from_bytes(&array))
        }
        Some(path) => {
            let key = iroh_net::SecretKey::generate(&mut rand::rng());
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).with_context(|| {
                    format!("failed to create iroh key directory '{}'", parent.display())
                })?;
            }
            std::fs::write(path, key.to_bytes())
                .with_context(|| format!("failed to write iroh secret key '{}'", path.display()))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
                    .with_context(|| {
                        format!("failed to set permissions on '{}'", path.display())
                    })?;
            }
            Ok(key)
        }
        None => Ok(iroh_net::SecretKey::generate(&mut rand::rng())),
    }
}
