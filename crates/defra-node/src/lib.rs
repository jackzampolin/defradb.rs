//! Reusable embedded DefraDB node builder.
//!
//! Wraps defradb.rs library crates behind a clean builder API so that
//! downstream binaries can embed a DefraDB instance without duplicating
//! wiring code.
//!
//! P2P uses IROH (QUIC-native) transport for peer-to-peer replication.

pub mod version;

use std::path::PathBuf;
use std::sync::Arc;

#[cfg(feature = "p2p")]
use p2p::P2PTransport;

pub use events::EventName;
pub use query::{QueryExecutor, QueryRequest, QueryResponse};

#[cfg(all(feature = "http", feature = "p2p"))]
struct HttpP2PAdapter {
    inner: Arc<dyn P2POps>,
}

#[cfg(all(feature = "http", feature = "p2p"))]
impl HttpP2PAdapter {
    fn unsupported() -> String {
        "not supported by embedded defra-node HTTP adapter".to_string()
    }
}

#[cfg(all(feature = "http", feature = "p2p"))]
#[async_trait::async_trait]
impl defra_http::P2POperations for HttpP2PAdapter {
    async fn local_peer_id(&self) -> Result<String, String> {
        Ok(self.inner.local_peer_id().await)
    }

    async fn listen_addresses(&self) -> Result<Vec<String>, String> {
        Ok(self.inner.listen_addresses().await)
    }

    async fn connected_peers(&self) -> Result<Vec<String>, String> {
        self.inner
            .connected_peers()
            .await
            .map_err(|e| e.to_string())
    }

    async fn connect_peer(&self, addr: &str) -> Result<(), String> {
        self.inner
            .connect_peer(addr)
            .await
            .map_err(|e| e.to_string())
    }

    async fn get_replicators(&self) -> Result<Vec<defra_http::router::ReplicatorInfo>, String> {
        Err(Self::unsupported())
    }

    async fn add_replicator(
        &self,
        collections: Vec<String>,
        addr: Option<&str>,
        _explicit_replay_capabilities: Vec<defra_http::router::ExplicitReplayCapabilityInput>,
        _expected_authorizer_did: Option<&str>,
    ) -> Result<(), String> {
        let addr = addr.ok_or_else(|| "replicator address is required".to_string())?;
        self.inner
            .set_replicator(addr, collections)
            .await
            .map_err(|e| e.to_string())
    }

    async fn remove_replicator(
        &self,
        _collections: Vec<String>,
        _addr: Option<&str>,
    ) -> Result<(), String> {
        Err(Self::unsupported())
    }

    async fn get_collections(&self) -> Result<Vec<String>, String> {
        Err(Self::unsupported())
    }

    async fn add_collections(&self, collections: Vec<String>) -> Result<(), String> {
        for collection in collections {
            self.inner
                .subscribe_collection(&collection)
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    async fn remove_collections(&self, _collections: Vec<String>) -> Result<(), String> {
        Err(Self::unsupported())
    }

    async fn get_documents(&self) -> Result<Vec<defra_http::router::P2pDocumentInfo>, String> {
        Err(Self::unsupported())
    }

    async fn add_documents(
        &self,
        _docs: Vec<defra_http::router::P2pDocumentRequest>,
    ) -> Result<(), String> {
        Err(Self::unsupported())
    }

    async fn remove_documents(
        &self,
        _docs: Vec<defra_http::router::P2pDocumentRequest>,
    ) -> Result<(), String> {
        Err(Self::unsupported())
    }

    async fn sync_documents(
        &self,
        _collection_name: &str,
        _doc_ids: Vec<String>,
    ) -> Result<(), String> {
        Err(Self::unsupported())
    }

    async fn sync_branchable_collection(&self, _collection_id: &str) -> Result<(), String> {
        Err(Self::unsupported())
    }

    async fn sync_collection_versions(&self, _version_ids: Vec<String>) -> Result<(), String> {
        Err(Self::unsupported())
    }
}

/// Configuration for the optional HTTP GraphQL server.
#[cfg(feature = "http")]
pub struct HttpConfig {
    pub address: std::net::SocketAddr,
}

#[cfg(feature = "http")]
impl HttpConfig {
    pub fn new(port: u16) -> Self {
        Self {
            address: std::net::SocketAddr::from(([127, 0, 0, 1], port)),
        }
    }

    pub fn with_addr(addr: impl Into<std::net::SocketAddr>) -> Self {
        Self {
            address: addr.into(),
        }
    }
}

/// Configuration for the optional P2P networking layer (IROH/QUIC).
#[cfg(feature = "p2p")]
pub struct P2PConfig {
    /// UDP port for QUIC listener.
    pub port: u16,
    /// Bind to a specific IP address. When set, IROH only listens on this
    /// interface — use the Tailscale IP to keep P2P within the mesh and
    /// prevent IROH from advertising unreachable LAN addresses across sites.
    /// None = 0.0.0.0 (all interfaces).
    pub bind_addr: Option<std::net::IpAddr>,
    /// Optional relay URL for NAT traversal. None = direct connections only.
    pub relay_url: Option<String>,
    /// Enable DNS-based peer discovery.
    pub discovery: bool,
    /// Path to persist secret key. None = ephemeral (new identity each restart).
    pub secret_key_path: Option<std::path::PathBuf>,
    /// Reload collection subscriptions persisted in the local store on startup.
    /// When false, only explicit subscribe calls in the current process take effect.
    pub load_persisted_collections: bool,
}

/// Type-erased P2P operations exposed on EmbeddedNode.
#[cfg(feature = "p2p")]
#[async_trait::async_trait]
pub trait P2POps: Send + Sync {
    async fn local_peer_id(&self) -> String;
    async fn listen_addresses(&self) -> Vec<String>;
    async fn connected_peers(&self) -> anyhow::Result<Vec<String>>;
    async fn connect_peer(&self, addr: &str) -> anyhow::Result<()>;
    async fn subscribe_collection(&self, name: &str) -> anyhow::Result<()>;
    /// Set up push replication to a peer for the given collections.
    /// The peer_addr format is `<node-id>@<ip>:<port>` or just `<node-id>`.
    async fn set_replicator(&self, peer_addr: &str, collections: Vec<String>)
        -> anyhow::Result<()>;
}

/// Type-erased schema operations so we can store DB<S> without leaking the Store generic.
#[async_trait::async_trait]
trait SchemaOps: Send + Sync {
    async fn add_schema(&self, sdl: &str) -> anyhow::Result<()>;
    async fn add_view(&self, source_query: &str, target_sdl: &str) -> anyhow::Result<()>;
}

/// Type-erased collection lookup for resolving names to CIDs.
/// Required by P2P: gossip topics use collection CIDs, not names.
#[cfg(feature = "p2p")]
trait CollectionLookup: Send + Sync {
    fn get_collection_id(&self, name: &str) -> Option<String>;
}

#[async_trait::async_trait]
impl<S: storage::corekv::Store + 'static> SchemaOps for db::DB<S> {
    async fn add_schema(&self, sdl: &str) -> anyhow::Result<()> {
        let collections =
            query::parse_sdl(sdl).map_err(|e| anyhow::anyhow!("SDL parse error: {}", e))?;

        db::definition_validation::validate_new_collections(&collections)
            .map_err(|e| anyhow::anyhow!("schema validation error: {}", e))?;

        for collection in collections {
            self.create_collection(collection)
                .await
                .map_err(|e| anyhow::anyhow!("create collection error: {}", e))?;
        }
        Ok(())
    }

    async fn add_view(&self, source_query: &str, target_sdl: &str) -> anyhow::Result<()> {
        // Parse the view SDL (unknown directives like @downsample are warned but ignored)
        let mut collections = query::parse_sdl(target_sdl)
            .map_err(|e| anyhow::anyhow!("view SDL parse error: {}", e))?;

        // Parse the source query into a QuerySource Fields array.
        // Expected format: "SourceType { field1 field2 field3 }"
        let source_query = source_query.trim();
        let (source_type, fields_block) = source_query.split_once('{').ok_or_else(|| {
            anyhow::anyhow!("source_query must be 'TypeName {{ field1 field2 ... }}'")
        })?;
        let source_type = source_type.trim();
        let fields_str = fields_block.trim_end_matches('}').trim();
        let field_names: Vec<&str> = fields_str.split_whitespace().collect();

        let fields_json: Vec<serde_json::Value> = field_names
            .iter()
            .map(|name| serde_json::json!({"Name": name}))
            .collect();

        let query_json = serde_json::json!({
            "Name": source_type,
            "Fields": fields_json,
        });

        let query_source = schema::QuerySource::new(query_json);

        // Mark each collection as a materialized view backed by the query source
        for collection in &mut collections {
            collection.query = Some(query_source.clone());
            collection.is_materialized = true;
        }

        for collection in collections {
            self.create_collection(collection)
                .await
                .map_err(|e| anyhow::anyhow!("create view collection error: {}", e))?;
        }
        Ok(())
    }
}

#[cfg(feature = "p2p")]
impl<S: storage::corekv::Store + 'static> CollectionLookup for db::DB<S> {
    fn get_collection_id(&self, name: &str) -> Option<String> {
        match self.get_collection(name) {
            Ok(Some(collection)) => Some(collection.collection_id().to_string()),
            Ok(None) => {
                tracing::debug!(collection_name = %name, "collection not found for P2P lookup");
                None
            }
            Err(e) => {
                tracing::warn!(collection_name = %name, error = %e, "error looking up collection for P2P");
                None
            }
        }
    }
}

/// Concrete P2P handle implementation (type-erased behind P2POps trait).
#[cfg(feature = "p2p")]
struct P2PHandleImpl<B: blockstore::Blockstore + Send + Sync + 'static> {
    transport: p2p::iroh::IrohTransport,
    coordinator: Arc<p2p::sync::SyncCoordinator<B, p2p::iroh::IrohTransport>>,
    collection_lookup: Arc<dyn CollectionLookup>,
}

#[cfg(feature = "p2p")]
#[async_trait::async_trait]
impl<B: blockstore::Blockstore + Send + Sync + 'static> P2POps for P2PHandleImpl<B> {
    async fn local_peer_id(&self) -> String {
        self.transport.local_peer_id().to_string()
    }

    async fn listen_addresses(&self) -> Vec<String> {
        self.transport
            .listen_addresses()
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|a| a.to_string())
            .collect()
    }

    async fn connected_peers(&self) -> anyhow::Result<Vec<String>> {
        self.transport
            .connected_peers()
            .await
            .map(|peers| peers.into_iter().map(|peer| peer.to_string()).collect())
            .map_err(|e| anyhow::anyhow!("connected peers failed: {}", e))
    }

    async fn connect_peer(&self, addr: &str) -> anyhow::Result<()> {
        // IROH peer address format:
        //   <node-id>                  — discovery-only
        //   <node-id>@<ip>:<port>      — explicit address
        let (peer_id_str, addrs) = if let Some((id, host)) = addr.split_once('@') {
            let peer_addr = p2p::transport::PeerAddr::new(host.to_string());
            (id, vec![peer_addr])
        } else {
            (addr, vec![])
        };
        let peer_id = p2p::transport::PeerId::new(peer_id_str.to_string());
        self.transport
            .dial(&peer_id, addrs)
            .await
            .map_err(|e| anyhow::anyhow!("dial failed: {}", e))
    }

    async fn subscribe_collection(&self, name: &str) -> anyhow::Result<()> {
        // Resolve collection name → CID (gossip topics use CIDs, not names)
        let collection_id = self
            .collection_lookup
            .get_collection_id(name)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "collection '{}' not found — add schema before subscribing to P2P",
                    name
                )
            })?;
        tracing::debug!(
            collection_name = %name,
            collection_id = %collection_id,
            "resolved collection name to CID for P2P subscription"
        );
        self.coordinator
            .subscribe_collection(&collection_id)
            .await
            .map_err(|e| anyhow::anyhow!("subscribe collection failed: {}", e))?;
        Ok(())
    }

    async fn set_replicator(
        &self,
        peer_addr: &str,
        collections: Vec<String>,
    ) -> anyhow::Result<()> {
        if peer_addr.contains('@') {
            self.connect_peer(peer_addr).await?;
        }

        let peer_id_str = if let Some((id, _host)) = peer_addr.split_once('@') {
            id
        } else {
            peer_addr
        };
        let peer_id = p2p::transport::PeerId::new(peer_id_str.to_string());

        // Resolve collection names → CIDs
        let mut collection_cids = Vec::with_capacity(collections.len());
        for name in &collections {
            let cid = self
                .collection_lookup
                .get_collection_id(name)
                .ok_or_else(|| {
                    anyhow::anyhow!("collection '{}' not found for replicator setup", name)
                })?;
            tracing::debug!(
                collection_name = %name,
                collection_id = %cid,
                "resolved collection name to CID for replicator"
            );
            collection_cids.push(cid);
        }

        self.coordinator
            .create_replicator(&peer_id, collection_cids, true)
            .await
            .map_err(|e| anyhow::anyhow!("set replicator failed: {}", e))?;

        tracing::info!(
            peer_id = %peer_id,
            collections = ?collections,
            "configured live replicator; skipping eager backfill"
        );
        Ok(())
    }
}

/// An embedded DefraDB node with query execution and event subscription.
pub struct EmbeddedNode {
    runner: Arc<dyn QueryExecutor>,
    event_bus: Arc<dyn events::Bus>,
    schema_ops: Arc<dyn SchemaOps>,
    #[cfg(feature = "p2p")]
    p2p_ops: Option<Arc<dyn P2POps>>,
}

impl EmbeddedNode {
    /// Start building a new embedded node.
    pub fn builder() -> NodeBuilder {
        NodeBuilder::default()
    }

    /// Execute a GraphQL query or mutation.
    pub async fn execute(&self, query_str: &str) -> QueryResponse {
        self.runner.execute(QueryRequest::new(query_str)).await
    }

    /// Add a schema from a GraphQL SDL type definition.
    pub async fn add_schema(&self, sdl: &str) -> anyhow::Result<()> {
        self.schema_ops.add_schema(sdl).await
    }

    /// Create a materialized view from a source query and target SDL.
    ///
    /// `source_query` format: `"SourceType { field1 field2 ... }"`
    /// `target_sdl` is the SDL for the view collection (may include directives
    /// like `@downsample` that are forward-declared for future defradb.rs support).
    pub async fn add_view(&self, source_query: &str, target_sdl: &str) -> anyhow::Result<()> {
        self.schema_ops.add_view(source_query, target_sdl).await
    }

    /// Subscribe to DefraDB events.
    pub fn subscribe(&self, event_names: &[EventName]) -> events::Subscription {
        self.event_bus.subscribe(event_names)
    }

    /// Access the underlying query executor for advanced use.
    pub fn runner(&self) -> &Arc<dyn QueryExecutor> {
        &self.runner
    }

    /// Access the event bus directly.
    pub fn event_bus(&self) -> &Arc<dyn events::Bus> {
        &self.event_bus
    }

    /// Access P2P operations (if P2P is enabled and configured).
    #[cfg(feature = "p2p")]
    pub fn p2p(&self) -> Option<&dyn P2POps> {
        self.p2p_ops.as_deref()
    }

    /// Cloneable P2P operations handle for background tasks.
    #[cfg(feature = "p2p")]
    pub fn p2p_arc(&self) -> Option<Arc<dyn P2POps>> {
        self.p2p_ops.as_ref().map(Arc::clone)
    }
}

/// Builder for constructing an `EmbeddedNode`.
#[derive(Default)]
pub struct NodeBuilder {
    data_path: Option<PathBuf>,
    #[cfg(feature = "http")]
    http_config: Option<HttpConfig>,
    #[cfg(feature = "p2p")]
    p2p_config: Option<P2PConfig>,
}

impl NodeBuilder {
    /// Set the data directory for persistent storage (redb backend).
    /// If not set, uses in-memory storage.
    pub fn data_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.data_path = Some(path.into());
        self
    }

    /// Enable the HTTP GraphQL server.
    #[cfg(feature = "http")]
    pub fn with_http(mut self, config: HttpConfig) -> Self {
        self.http_config = Some(config);
        self
    }

    /// Enable P2P networking for replication.
    #[cfg(feature = "p2p")]
    pub fn with_p2p(mut self, config: P2PConfig) -> Self {
        self.p2p_config = Some(config);
        self
    }

    /// Build and start the embedded DefraDB node.
    pub async fn build(self) -> anyhow::Result<EmbeddedNode> {
        // 1. Event bus
        let event_bus: Arc<dyn events::Bus> = Arc::new(events::ChannelBus::default());

        // 2. Extract configs before moving self
        #[cfg(feature = "http")]
        let http_config = self.http_config;
        #[cfg(feature = "p2p")]
        let p2p_config = self.p2p_config;

        // 3. Storage backend + database
        let node = if let Some(path) = self.data_path {
            std::fs::create_dir_all(&path)?;

            let store = Arc::new(
                storage::RedbStore::open(
                    path.to_str()
                        .ok_or_else(|| anyhow::anyhow!("data_path contains non-UTF8 characters"))?,
                )
                .map_err(|e| anyhow::anyhow!("failed to open redb store: {}", e))?,
            );

            let acp_store: Arc<dyn acp::AcpStore> =
                Arc::new(acp::PersistentAcpStore::from_store(store.clone()));

            Self::build_with_store(
                store,
                acp_store,
                event_bus,
                #[cfg(feature = "p2p")]
                p2p_config,
            )
            .await?
        } else {
            let store = Arc::new(storage::MemoryStore::new());
            let acp_store: Arc<dyn acp::AcpStore> = Arc::new(acp::MemoryAcpStore::new());

            Self::build_with_store(
                store,
                acp_store,
                event_bus,
                #[cfg(feature = "p2p")]
                p2p_config,
            )
            .await?
        };

        // 4. Spawn HTTP server if configured
        #[cfg(feature = "http")]
        if let Some(http_cfg) = http_config {
            let server_config = defra_http::ServerConfig {
                address: http_cfg.address,
                ..Default::default()
            };
            let mut server =
                defra_http::Server::from_arc_with_config(node.runner.clone(), server_config)
                    .with_event_bus_arc(node.event_bus.clone());

            #[cfg(feature = "p2p")]
            if let Some(p2p) = node.p2p_ops.as_ref() {
                server = server.with_p2p(HttpP2PAdapter {
                    inner: Arc::clone(p2p),
                });
            }

            let addr = http_cfg.address;
            tokio::spawn(async move {
                if let Err(e) = server.run().await {
                    tracing::error!(error = %e, "HTTP server exited with error");
                }
            });
            tracing::info!(address = %addr, "HTTP server started");
        }

        Ok(node)
    }

    async fn build_with_store<S: storage::corekv::Store + 'static>(
        store: Arc<S>,
        acp_store: Arc<dyn acp::AcpStore>,
        event_bus: Arc<dyn events::Bus>,
        #[cfg(feature = "p2p")] p2p_config: Option<P2PConfig>,
    ) -> anyhow::Result<EmbeddedNode> {
        // Open database
        let mut database = db::DB::open_from_arc(store.clone())
            .await
            .map_err(|e| anyhow::anyhow!("failed to open database: {}", e))?;

        // Wire event bus so mutations publish events
        database.set_event_bus(event_bus.clone());
        let database = Arc::new(database);

        // P2P setup (affects mutator choice)
        #[cfg(feature = "p2p")]
        let p2p_result = if let Some(p2p_cfg) = p2p_config {
            Some(Self::setup_p2p(store.clone(), database.clone(), &p2p_cfg).await?)
        } else {
            None
        };

        // Choose mutator: BroadcastMutator if P2P, AutoCommitMutator otherwise
        #[cfg(feature = "p2p")]
        let mutator: Arc<dyn query::DocMutator> = if let Some(ref p2p) = p2p_result {
            p2p.mutator.clone()
        } else {
            Arc::new(db::AutoCommitMutator::new(database.clone()))
        };
        #[cfg(not(feature = "p2p"))]
        let mutator: Arc<dyn query::DocMutator> =
            Arc::new(db::AutoCommitMutator::new(database.clone()));

        // Query runner components
        let fetcher = db::LensedAutoCommitFetcher::new(database.clone());
        let provider: Arc<dyn query::CollectionProvider> =
            db::DbCollectionProvider::new_arc(database.clone());
        let registry = Arc::new(db::DbTransactionRegistry::new(database.clone()));
        let document_acp: Arc<dyn acp::DocumentACP> =
            Arc::new(acp::LocalDocumentACP::new(acp_store));

        // Assemble query runner
        let query_runner =
            query::QueryRunner::with_arc_registry_and_provider(fetcher, provider, registry)
                .with_mutator(mutator)
                .with_acp(document_acp)
                .with_lens_store(database.lens_store().clone());

        let runner: Arc<dyn QueryExecutor> = Arc::new(query_runner);
        let schema_ops: Arc<dyn SchemaOps> = database;

        Ok(EmbeddedNode {
            runner,
            event_bus,
            schema_ops,
            #[cfg(feature = "p2p")]
            p2p_ops: p2p_result.map(|r| r.ops),
        })
    }

    #[cfg(feature = "p2p")]
    async fn setup_p2p<S: storage::corekv::Store + 'static>(
        store: Arc<S>,
        database: Arc<db::DB<S>>,
        config: &P2PConfig,
    ) -> anyhow::Result<P2PSetupResult> {
        // 1. Load or generate secret key for stable node identity
        let secret_key = load_or_generate_secret_key(config.secret_key_path.as_deref())?;

        // 2. Configure and spawn IROH endpoint with pinned port + optional bind address
        let iroh_config = p2p::iroh::IrohEndpointConfig {
            secret_key: secret_key.clone(),
            relay_url: config.relay_url.clone(),
            discovery: config.discovery,
            bind_port: Some(config.port),
            bind_addr: config.bind_addr,
        };
        let (command_tx, iroh_events, _task_handle) = p2p::iroh::spawn_endpoint(iroh_config)
            .await
            .map_err(|e| anyhow::anyhow!("IROH endpoint spawn failed: {}", e))?;

        // 3. Create IROH transport facade
        let transport = p2p::iroh::IrohTransport::new(command_tx, secret_key);

        // 5. Blockstore for sync coordinator + merge handler
        let sync_blockstore = Arc::new(blockstore::DefraBlockstore::new(store.clone(), false));

        // 6. Collection store (persists subscriptions)
        let collection_store: Arc<dyn p2p::sync::P2PCollectionStorage> =
            Arc::new(p2p::sync::P2PCollectionStore::new(store.clone()));

        // 7. SyncCoordinator (transport-generic — same constructor, different type param)
        let (mut coordinator, sync_events) = p2p::sync::SyncCoordinator::with_collection_store(
            transport.clone(),
            sync_blockstore.clone(),
            p2p::sync::SyncConfig::default(),
            p2p::AccessMode::Open,
            collection_store,
        )
        .await
        .map_err(|e| anyhow::anyhow!("SyncCoordinator creation failed: {}", e))?;

        // Failure channel (required by replication loop)
        let (failure_tx, mut failure_rx) =
            tokio::sync::mpsc::unbounded_channel::<p2p::sync::PushFailure>();
        coordinator.set_failure_channel(failure_tx);
        tokio::spawn(async move {
            while let Some(failure) = failure_rx.recv().await {
                tracing::warn!(
                    peer_id = %failure.peer_id,
                    doc_id = %failure.doc_id,
                    collection_id = %failure.collection_id,
                    "P2P push to replicator failed"
                );
            }
        });

        let coordinator = Arc::new(coordinator);

        if config.load_persisted_collections {
            coordinator.load_p2p_collections().await.ok();
        } else {
            tracing::info!("skipping persisted P2P collection subscriptions");
        }

        // 8. Merge handler
        let merge_handler = Arc::new(db::DbMergeHandler::new(database.clone(), sync_blockstore));

        // 9. Replication loop (transport-generic)
        let coord_for_repl = coordinator.clone();
        tokio::spawn(async move {
            p2p::sync::ReplicationLoop::run(
                coord_for_repl,
                sync_events,
                merge_handler,
                p2p::sync::ReplicationConfig::default(),
            )
            .await;
        });

        // 10. IROH event handler (events are already TransportEvent — no conversion needed)
        let coord_for_events = coordinator.clone();
        tokio::spawn(async move {
            Self::run_event_handler(iroh_events, coord_for_events).await;
        });

        // 11. Collection lookup (resolves names → CIDs for gossip topics)
        let collection_lookup: Arc<dyn CollectionLookup> = database.clone();

        // 12. BroadcastMutator (replaces AutoCommitMutator)
        let mutator: Arc<dyn query::DocMutator> = Arc::new(db::BroadcastMutator::new(
            database.clone(),
            coordinator.clone(),
        ));

        let peer_id = transport.local_peer_id().to_string();
        tracing::info!(peer_id = %peer_id, "P2P started (IROH/QUIC)");
        let ops: Arc<dyn P2POps> = Arc::new(P2PHandleImpl {
            transport,
            coordinator,
            collection_lookup,
        });

        Ok(P2PSetupResult { ops, mutator })
    }

    #[cfg(feature = "p2p")]
    async fn run_event_handler<B: blockstore::Blockstore + Send + Sync + 'static>(
        mut events: tokio::sync::mpsc::Receiver<p2p::TransportEvent>,
        coordinator: Arc<p2p::sync::SyncCoordinator<B, p2p::iroh::IrohTransport>>,
    ) {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(32));
        while let Some(event) = events.recv().await {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let coord = coordinator.clone();
            tokio::spawn(async move {
                if let Err(e) = coord.handle_transport_event(event).await {
                    tracing::error!(error = %e, "P2P event handler error");
                }
                drop(permit);
            });
        }
    }
}

/// Load a secret key from disk, or generate and persist a new one.
/// If path is None, generates an ephemeral key (not persisted).
#[cfg(feature = "p2p")]
fn load_or_generate_secret_key(path: Option<&std::path::Path>) -> anyhow::Result<iroh::SecretKey> {
    match path {
        Some(p) if p.exists() => {
            let bytes = std::fs::read(p)?;
            let arr: [u8; 32] = bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("secret key file must be exactly 32 bytes"))?;
            Ok(iroh::SecretKey::from_bytes(&arr))
        }
        Some(p) => {
            let key = iroh::SecretKey::generate(&mut rand::rng());
            if let Some(parent) = p.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(p, key.to_bytes())?;
            // Restrict permissions to owner-only (Unix)
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o600))?;
            }
            Ok(key)
        }
        None => Ok(iroh::SecretKey::generate(&mut rand::rng())),
    }
}

/// Internal result from P2P setup, carrying the type-erased ops and mutator.
#[cfg(feature = "p2p")]
struct P2PSetupResult {
    ops: Arc<dyn P2POps>,
    mutator: Arc<dyn query::DocMutator>,
}
