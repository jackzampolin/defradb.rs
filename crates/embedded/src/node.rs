use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
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
#[cfg(feature = "iroh")]
use p2p::P2PTransport;
use tokio::sync::Notify;

pub(crate) type EmbeddedBlockstore<S> = blockstore::DefraBlockstore<S>;
pub(crate) type EmbeddedMergeHandler<S> = db_merge::AcpMergeHandler<S, EmbeddedBlockstore<S>>;
type EmbeddedTxnRegistry<S> = db::DbTransactionRegistry<S>;
pub(crate) type WireDocumentAcpCallback = Box<dyn FnOnce(Arc<dyn acp::DocumentACP>)>;
pub(crate) type WireKmsCallback = Box<dyn FnOnce(Arc<dyn kms::KmsService>) + Send>;

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
    pub query_limits: query::QueryLimits,
    pub p2p: Option<Arc<ManagedP2PSystem>>,
    /// Idempotency guard for [`EmbeddedNode::shutdown`]. Set to `true`
    /// by the first caller of `shutdown()`.
    shutdown_started: AtomicBool,
    /// Marks that shutdown work has finished.
    shutdown_finished: AtomicBool,
    /// Wakes concurrent shutdown callers once teardown is complete.
    shutdown_notify: Notify,
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

        let _id_guard =
            defra_core::current_identity::scoped_current_identity(self.node_identity_did.clone());
        for collection in collections {
            self.database
                .create_collection(collection)
                .await
                .map_err(|error| anyhow!("create collection error: {error}"))?;
        }

        Ok(())
    }

    /// Add an equality encrypted (searchable-encryption) index on `field_name`
    /// of `collection`. Embedded-native equivalent of the CLI
    /// `encrypted_index_add` / HTTP `EncryptedIndexOperations::add_encrypted_index`,
    /// so embedded nodes can write SE artifacts and act as SE query owners (#976).
    pub async fn add_encrypted_index(&self, collection: &str, field_name: &str) -> Result<()> {
        use storage::corekv::Key;

        let col = self
            .database
            .get_collection(collection)
            .map_err(|error| anyhow!("get collection error: {error}"))?
            .ok_or_else(|| anyhow!("collection '{collection}' not found"))?;
        let schema = col.schema();

        if !schema.fields.iter().any(|f| f.name == field_name) {
            return Err(anyhow!(
                "encrypted index on non-existent field: {field_name}"
            ));
        }
        if schema
            .encrypted_indexes
            .iter()
            .any(|idx| idx.field_name == field_name)
        {
            return Err(anyhow!(
                "encrypted index already exists on field: {field_name}"
            ));
        }

        let txn = self
            .database
            .new_txn(false)
            .await
            .map_err(|error| anyhow!("failed to create transaction: {error}"))?;
        {
            let mut updated_schema = schema.clone();
            updated_schema
                .encrypted_indexes
                .push(schema::EncryptedIndexDescription::new(field_name));

            let collection_key =
                storage::keys::systemstore::CollectionKey::new(&updated_schema.version_id);
            let schema_data = serde_json::to_vec(&updated_schema)
                .map_err(|error| anyhow!("failed to serialize schema: {error}"))?;
            let systemstore = txn
                .systemstore()
                .map_err(|error| anyhow!("failed to get systemstore: {error}"))?;
            systemstore
                .set(&collection_key.bytes(), &schema_data)
                .await
                .map_err(|error| anyhow!("failed to save schema: {error}"))?;
            let name_key = storage::keys::systemstore::CollectionNameKey::new(collection);
            systemstore
                .set(&name_key.bytes(), updated_schema.version_id.as_bytes())
                .await
                .map_err(|error| anyhow!("failed to save name mapping: {error}"))?;
        }
        txn.commit()
            .await
            .map_err(|error| anyhow!("failed to commit: {error}"))?;
        self.database
            .reload_cache()
            .await
            .map_err(|error| anyhow!("failed to reload cache: {error}"))?;
        Ok(())
    }

    /// Shut the node down cleanly.
    ///
    /// Order:
    /// 1. **P2P transport** — closes the iroh endpoint or libp2p host
    ///    gracefully and aborts the replication/merge background tasks.
    ///    Addresses the `Endpoint dropped without calling Endpoint::close`
    ///    iroh warning downstream embedders have been hitting.
    /// 2. **Database** — close-guards future transactions (new `begin_txn`
    ///    calls will return `Error::DatabaseClosed`) and closes the
    ///    underlying store.
    ///
    /// The background downsample task is aborted by the `Drop` impl on
    /// `BackgroundTasks` when the node itself is dropped, so no explicit
    /// step is needed here.
    ///
    /// This method is **idempotent** — the first caller performs the
    /// work, concurrent callers wait for that teardown to finish and
    /// then return. Safe to call from multiple tasks concurrently.
    ///
    /// Embedded callers should `await` this before dropping the
    /// [`EmbeddedNode`] to ensure clean teardown:
    ///
    /// ```ignore
    /// node.shutdown().await;
    /// drop(node);
    /// ```
    pub async fn shutdown(&self) {
        // Idempotency guard. The first task performs shutdown. Any
        // concurrent caller waits for shutdown to finish before
        // returning so `shutdown().await` consistently means teardown
        // has completed.
        if self.shutdown_started.swap(true, Ordering::SeqCst) {
            self.wait_for_shutdown().await;
            return;
        }

        tracing::info!("embedded node shutdown: begin");

        // 1. P2P first — stops inbound/outbound traffic and awaits the
        //    transport's graceful close path (iroh Endpoint::close /
        //    libp2p Host::shutdown).
        if let Some(p2p) = &self.p2p {
            tracing::info!("embedded node shutdown: stopping p2p");
            p2p.shutdown().await;
        }

        // 2. Database — sets the is_closed flag and closes the store.
        //    Log but don't propagate errors: shutdown should complete
        //    even if the store close returns an I/O error, so embedders
        //    can't be blocked from exiting by a closing store.
        tracing::info!("embedded node shutdown: closing database");
        if let Err(error) = self.database.close().await {
            tracing::warn!(error = %error, "EmbeddedNode::shutdown: database close returned an error");
        }

        self.shutdown_finished.store(true, Ordering::SeqCst);
        self.shutdown_notify.notify_waiters();
        tracing::info!("embedded node shutdown: complete");
    }

    /// Returns true if [`EmbeddedNode::shutdown`] has been initiated.
    ///
    /// Does not guarantee that shutdown has finished — just that it has
    /// started. Useful for embedders that want to short-circuit pending
    /// operations when teardown is in progress.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown_started.load(Ordering::SeqCst)
    }

    async fn wait_for_shutdown(&self) {
        loop {
            let notified = self.shutdown_notify.notified();
            if self.shutdown_finished.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }
}

/// Builder for memory/redb embedded nodes.
#[derive(Default)]
pub struct NodeBuilder {
    data_path: Option<PathBuf>,
    config: EmbeddedNodeConfig,
    at_rest_encryption_key: Option<[u8; 32]>,
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

    pub fn with_query_limits(mut self, limits: query::QueryLimits) -> Self {
        self.config.query_limits = limits;
        self
    }

    /// Enable transparent at-rest value encryption for the storage backend,
    /// keyed by the given 32-byte AES-256 key. Opt-in; off by default.
    pub fn with_at_rest_encryption_key(mut self, key: [u8; 32]) -> Self {
        self.at_rest_encryption_key = Some(key);
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

            (EmbeddedStore::Redb(redb), Persistence::Persistent)
        } else {
            tracing::info!(
                storage_backend = "memory",
                "embedded node starting (ephemeral, no data_path)"
            );
            (
                EmbeddedStore::Memory(storage::MemoryStore::new()),
                Persistence::Memory,
            )
        };

        let store = match self.at_rest_encryption_key.take() {
            Some(key) => {
                tracing::info!("at-rest encryption enabled (value-only, AES-256-GCM)");
                Arc::new(store.encrypted(key))
            }
            None => Arc::new(store),
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
        coordinator: p2p::sync::SyncShutdownHandle,
        aborts: Vec<tokio::task::AbortHandle>,
    },
    #[cfg(feature = "iroh")]
    Iroh {
        transport: p2p::iroh::IrohTransport,
        coordinator: p2p::sync::SyncShutdownHandle,
        aborts: Vec<tokio::task::AbortHandle>,
    },
}

impl ShutdownHandle {
    pub(crate) fn libp2p(
        handle: p2p::P2PHostHandle,
        coordinator: p2p::sync::SyncShutdownHandle,
        aborts: Vec<tokio::task::AbortHandle>,
    ) -> Self {
        Self {
            inner: ShutdownKind::Libp2p {
                handle: Box::new(handle),
                coordinator,
                aborts,
            },
        }
    }

    #[cfg(feature = "iroh")]
    pub(crate) fn iroh(
        transport: p2p::iroh::IrohTransport,
        coordinator: p2p::sync::SyncShutdownHandle,
        aborts: Vec<tokio::task::AbortHandle>,
    ) -> Self {
        Self {
            inner: ShutdownKind::Iroh {
                transport,
                coordinator,
                aborts,
            },
        }
    }

    pub async fn shutdown(&self) {
        match &self.inner {
            ShutdownKind::Libp2p {
                handle,
                coordinator,
                aborts,
            } => {
                coordinator.shutdown().await;
                for abort in aborts {
                    abort.abort();
                }
                let _ = handle.shutdown().await;
            }
            #[cfg(feature = "iroh")]
            ShutdownKind::Iroh {
                transport,
                coordinator,
                aborts,
            } => {
                coordinator.shutdown().await;
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

    // Wire the NAC manager into the DB so DB-layer `check_node_access` calls go
    // live. First-call-wins via the DB's OnceLock setter. Covers both the
    // embedded node and the FFI node (which also builds via `build_with_store`).
    database.set_nac_manager(nac_manager.clone());

    if let Some(ref mut setup) = p2p_setup {
        setup.merge_handler.set_document_acp(document_acp.clone());
        setup
            .merge_handler
            .set_strict_replicated_doc_access(sourcehub_acp.is_some());
        if let Some(wire_document_acp) = setup.wire_document_acp.take() {
            wire_document_acp(document_acp.clone());
        }
    }

    // Build the KMS once document ACP + NAC manager exist (PR #4778 ordering:
    // the P2P transport was created earlier; the policy needs ACP/NAC which
    // initialize here).
    let kms: Arc<dyn kms::KmsService> = {
        // Blockstore-backed KeyStore (mirrors Go's internal/kms/enc_store.go):
        // the KMS serves DEKs for ANY encrypted write by reading/writing the
        // node's durable encstore→blockstore, not a RAM-only map. The DB owns
        // the blockstore Arc (set_kms_blockstore) so the adapter can hold a Weak
        // and avoid the lock-pinning cycle (#976) while sharing the block cache.
        let kms_blockstore = database.set_kms_blockstore(Arc::new(
            blockstore::DefraBlockstore::new(store.clone(), true),
        ));
        let enc_block_store: Arc<dyn kms::EncBlockStore> =
            Arc::new(db::DbEncBlockStore::new(database.clone(), kms_blockstore));
        let store: Arc<dyn kms::KeyStore> = Arc::new(kms::BlockstoreKeyStore::new(enc_block_store));
        let doc_lookup: Arc<dyn kms::DocCollectionLookup> =
            Arc::new(db::DbDocCollectionLookup::new(database.clone()));
        let policy = Arc::new(kms::NacDacPolicy::new(document_acp.clone(), doc_lookup));
        policy.set_node_acp(Arc::new(db::DbNodeAcpRead::new(nac_manager.clone())));

        // Node identity for the wire `identity` fallback on gossip-initiated
        // fetches. Anonymous nodes use a stable placeholder DID.
        let node_did = database.node_did().unwrap_or_else(|| {
            identity::Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK")
                .expect("static anonymous DID parses")
        });

        let transports: Vec<Arc<dyn kms::KeyTransport>> = match p2p_setup.as_ref() {
            Some(setup) => vec![setup.kms_transport.clone()],
            None => vec![],
        };

        Arc::new(kms::DefraKms::new(
            store,
            transports,
            policy as Arc<dyn kms::AccessPolicy>,
            node_did,
        ))
    };

    // Bind this node's transport peer id into the KMS so served ECIES
    // replies carry the correct AAD peer id (Go's `makeAssociatedData`).
    if let Some(ref setup) = p2p_setup {
        kms.set_local_peer_id(setup.local_peer_id.clone());
    }

    // Wire the KMS into the P2P transport (serve handler) + merge handler.
    if let Some(ref mut setup) = p2p_setup {
        // Install the serve handler. Use a Weak ref to break the
        // transport↔kms Arc cycle (transport holds handler → kms →
        // transports → transport).
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
                    Some(kms) => kms.serve_request(from, req).await,
                    None => Err(kms::Error::Internal("kms dropped".into())),
                }
            }
        }
        setup
            .kms_transport
            .install_handler(Arc::new(KmsServeHandler {
                kms: Arc::downgrade(&kms),
            }));

        if let Some(wire_kms) = setup.wire_kms.take() {
            wire_kms(kms.clone());
        }
    }

    // Wire the KMS into the write path (DB-held, read by doc_mutator).
    database.set_kms(kms.clone());

    let fetcher = db::LensedAutoCommitFetcher::new(database.clone());
    let collection_provider: Arc<dyn query::CollectionProvider> =
        db::DbCollectionProvider::new_arc(database.clone());
    let txn_broadcaster = p2p_setup
        .as_ref()
        .map(|setup| setup.txn_broadcaster.clone());
    let txn_registry = Arc::new(match txn_broadcaster {
        Some(b) => db::DbTransactionRegistry::with_broadcaster(database.clone(), b),
        None => db::DbTransactionRegistry::new(database.clone()),
    });

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
    .with_lens_store(database.lens_store().clone())
    .with_query_limits(config.query_limits);

    // Wire the SE remote query transport so this embedded node can act as an SE
    // query OWNER, fanning encrypted_<Collection> queries to replicators (#976).
    if let Some(se_transport) = p2p_setup
        .as_ref()
        .and_then(|setup| setup.se_transport.clone())
    {
        query_runner = query_runner.with_se_transport(se_transport);
    }

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
        query_limits: config.query_limits,
        p2p: p2p_setup.map(|setup| setup.system),
        shutdown_started: AtomicBool::new(false),
        shutdown_finished: AtomicBool::new(false),
        shutdown_notify: Notify::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    use async_trait::async_trait;

    #[derive(Clone, Default)]
    struct SlowCloseStore {
        inner: storage::MemoryStore,
        close_calls: Arc<AtomicUsize>,
        close_started: Arc<Notify>,
        close_finished: Arc<AtomicBool>,
        allow_close: Arc<Notify>,
    }

    impl SlowCloseStore {
        async fn wait_until_close_started(&self) {
            loop {
                let notified = self.close_started.notified();
                if self.close_calls.load(Ordering::SeqCst) > 0 {
                    return;
                }
                notified.await;
            }
        }
    }

    impl storage::corekv::private::Sealed for SlowCloseStore {}

    #[async_trait]
    impl storage::Store for SlowCloseStore {
        async fn new_txn(&self, readonly: bool) -> storage::Result<Box<dyn storage::Txn>> {
            self.inner.new_txn(readonly).await
        }

        async fn close(&self) -> storage::Result<()> {
            self.close_calls.fetch_add(1, Ordering::SeqCst);
            self.close_started.notify_waiters();
            self.allow_close.notified().await;
            let result = self.inner.close().await;
            self.close_finished.store(true, Ordering::SeqCst);
            result
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_shutdown_callers_wait_for_teardown_completion() -> Result<()> {
        let store = Arc::new(SlowCloseStore::default());
        let node = Arc::new(build_with_store(store.clone(), EmbeddedNodeConfig::default()).await?);

        let first = {
            let node = node.clone();
            tokio::spawn(async move {
                node.shutdown().await;
            })
        };

        store.wait_until_close_started().await;

        let second = {
            let node = node.clone();
            let store = store.clone();
            tokio::spawn(async move {
                node.shutdown().await;
                assert!(
                    store.close_finished.load(Ordering::SeqCst),
                    "shutdown() returned before store close completed"
                );
            })
        };

        tokio::task::yield_now().await;
        assert!(
            !second.is_finished(),
            "concurrent shutdown caller returned before teardown completed"
        );

        store.allow_close.notify_waiters();

        first.await.expect("first shutdown task should not panic");
        second.await.expect("second shutdown task should not panic");

        assert_eq!(
            store.close_calls.load(Ordering::SeqCst),
            1,
            "shutdown should only close the store once"
        );
        assert!(store.close_finished.load(Ordering::SeqCst));

        Ok(())
    }
}
