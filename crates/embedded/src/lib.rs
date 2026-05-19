mod node;
mod node_acp;
mod node_identity;
mod node_p2p;
mod node_recovery;
mod node_tasks;

use std::sync::Arc;

use async_trait::async_trait;
pub use defra_p2p_adapter::{ReplicatorPushOptions, ReplicatorPushOptionsState};

pub use node::{build_with_store, EmbeddedNode, NodeBuilder};
pub use node_tasks::BackgroundTasks;

/// Storage persistence hints for ACP/NAC setup.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum Persistence {
    #[default]
    Memory,
    Persistent,
}

/// Supported runtime transports for embedded nodes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum TransportConfig {
    #[default]
    None,
    Libp2p(Libp2pConfig),
    #[cfg(feature = "iroh")]
    Iroh(IrohConfig),
}

/// Libp2p transport configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Libp2pConfig {
    pub listen_addr: String,
}

/// Iroh transport configuration.
#[cfg(feature = "iroh")]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IrohConfig {
    pub bind_addr: Option<std::net::IpAddr>,
    pub bind_port: Option<u16>,
    pub relay_mode: p2p::iroh::IrohRelayModeConfig,
    pub discovery: p2p::iroh::IrohDiscoveryConfig,
    pub secret_key_path: Option<std::path::PathBuf>,
}

/// Node signing configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum SigningConfig {
    #[default]
    Disabled,
    Enabled {
        key: Option<SigningKey>,
    },
    RegisteredIdentity {
        did: String,
    },
}

/// Explicit node signing key material.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SigningKey {
    Secp256k1(Vec<u8>),
    Secp256r1(Vec<u8>),
    Ed25519(Vec<u8>),
}

/// Document ACP configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum DocumentAcpConfig {
    #[default]
    Local,
    SourceHub(SourceHubConfig),
}

/// SourceHub document ACP configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceHubConfig {
    pub grpc_address: String,
    pub comet_rpc_address: String,
    pub chain_id: String,
    pub signer_key: Vec<u8>,
}

/// Node assembly configuration used by `build_with_store`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EmbeddedNodeConfig {
    pub persistence: Persistence,
    pub transport: TransportConfig,
    pub signing: SigningConfig,
    pub document_acp: DocumentAcpConfig,
    pub query_limits: query::QueryLimits,
    pub max_concurrent_dag_fetches: Option<usize>,
    pub max_concurrent_push_tasks: Option<usize>,
    pub rate_limit_burst: Option<u32>,
    pub rate_limit_rate: Option<f64>,
}

/// Runtime P2P transport kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TransportKind {
    Libp2p,
    #[cfg(feature = "iroh")]
    Iroh,
}

/// P2P runtime managed by an embedded node.
pub struct ManagedP2PSystem {
    kind: TransportKind,
    ops: Arc<dyn defra_http::P2POperations>,
    replicator_push_options: ReplicatorPushOptionsState,
    on_replicator_push_options:
        Option<Arc<dyn Fn(ReplicatorPushOptions) -> Result<(), String> + Send + Sync>>,
    shutdown: node::ShutdownHandle,
}

impl ManagedP2PSystem {
    pub fn new(
        kind: TransportKind,
        ops: Arc<dyn defra_http::P2POperations>,
        shutdown: node::ShutdownHandle,
    ) -> Self {
        Self::with_replicator_push_options(
            kind,
            ops,
            shutdown,
            ReplicatorPushOptionsState::default(),
        )
    }

    pub fn with_replicator_push_options(
        kind: TransportKind,
        ops: Arc<dyn defra_http::P2POperations>,
        shutdown: node::ShutdownHandle,
        replicator_push_options: ReplicatorPushOptionsState,
    ) -> Self {
        Self::with_replicator_push_options_callback(
            kind,
            ops,
            shutdown,
            replicator_push_options,
            None,
        )
    }

    pub fn with_replicator_push_options_callback(
        kind: TransportKind,
        ops: Arc<dyn defra_http::P2POperations>,
        shutdown: node::ShutdownHandle,
        replicator_push_options: ReplicatorPushOptionsState,
        on_replicator_push_options: Option<
            Arc<dyn Fn(ReplicatorPushOptions) -> Result<(), String> + Send + Sync>,
        >,
    ) -> Self {
        Self {
            kind,
            ops,
            replicator_push_options,
            on_replicator_push_options,
            shutdown,
        }
    }

    pub fn kind(&self) -> TransportKind {
        self.kind
    }

    pub fn ops(&self) -> &Arc<dyn defra_http::P2POperations> {
        &self.ops
    }

    pub fn set_replicator_push_options(
        &self,
        options: ReplicatorPushOptions,
    ) -> Result<(), String> {
        self.replicator_push_options.store(options.clone())?;
        if let Some(callback) = &self.on_replicator_push_options {
            callback(options)?;
        }
        Ok(())
    }

    pub fn replicator_push_options(&self) -> ReplicatorPushOptions {
        self.replicator_push_options.load()
    }

    pub async fn shutdown(&self) {
        self.shutdown.shutdown().await;
    }
}

/// Storage backends supported by the public `NodeBuilder`.
#[non_exhaustive]
pub enum EmbeddedStore {
    Memory(storage::MemoryStore),
    Redb(storage::RedbStore),
}

impl storage::corekv::private::Sealed for EmbeddedStore {}

#[async_trait]
impl storage::Store for EmbeddedStore {
    async fn new_txn(&self, readonly: bool) -> storage::Result<Box<dyn storage::Txn>> {
        match self {
            Self::Memory(store) => store.new_txn(readonly).await,
            Self::Redb(store) => store.new_txn(readonly).await,
        }
    }

    async fn close(&self) -> storage::Result<()> {
        match self {
            Self::Memory(store) => store.close().await,
            Self::Redb(store) => store.close().await,
        }
    }
}
