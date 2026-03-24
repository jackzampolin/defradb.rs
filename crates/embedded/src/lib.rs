mod libp2p_adapter;
mod node;
mod transport_doc_pusher;
mod transport_version_syncer;
mod version_syncer;

#[cfg(feature = "iroh")]
mod iroh_adapter;

use std::sync::Arc;

use async_trait::async_trait;

pub use libp2p_adapter::{CollectionLookup, DbDocPusher, DocPusher};
pub use node::{build_with_store, BackgroundTasks, EmbeddedNode, NodeBuilder};
pub use transport_doc_pusher::{DbTransportDocPusher, TransportDocPusher};
pub use transport_version_syncer::{DbTransportVersionSyncer, TransportVersionSyncer};
pub use version_syncer::DbVersionSyncer;

#[cfg(feature = "iroh")]
pub use iroh_adapter::IrohP2PAdapter;

pub use libp2p_adapter::{P2PAdapter, VersionSyncer};

/// Transport-agnostic P2P operations exposed by embedded nodes.
#[async_trait]
pub trait P2POperations: Send + Sync {
    async fn local_peer_id(&self) -> Result<String, String>;
    async fn listen_addresses(&self) -> Result<Vec<String>, String>;
    async fn connected_peers(&self) -> Result<Vec<String>, String>;
    async fn connect_peer(&self, addr: &str) -> Result<(), String>;
    async fn notify_network_change(&self) -> Result<(), String>;
    async fn get_replicators(&self) -> Result<Vec<ReplicatorInfo>, String>;
    async fn add_replicator(
        &self,
        collections: Vec<String>,
        addr: Option<&str>,
        push_options: ReplicatorPushOptions,
    ) -> Result<(), String>;
    async fn remove_replicator(
        &self,
        collections: Vec<String>,
        addr: Option<&str>,
    ) -> Result<(), String>;
    async fn retry_replicators(&self, push_options: ReplicatorPushOptions) -> Result<(), String>;
    async fn get_collections(&self) -> Result<Vec<String>, String>;
    async fn add_collections(&self, collections: Vec<String>) -> Result<(), String>;
    async fn remove_collections(&self, collections: Vec<String>) -> Result<(), String>;
    async fn get_documents(&self) -> Result<Vec<P2pDocumentInfo>, String>;
    async fn add_documents(&self, docs: Vec<P2pDocumentRequest>) -> Result<(), String>;
    async fn remove_documents(&self, docs: Vec<P2pDocumentRequest>) -> Result<(), String>;
    async fn sync_documents(
        &self,
        collection_name: &str,
        doc_ids: Vec<String>,
    ) -> Result<(), String>;
    async fn sync_branchable_collection(&self, collection_id: &str) -> Result<(), String>;
    async fn sync_collection_versions(&self, version_ids: Vec<String>) -> Result<(), String>;
}

/// Optional inputs used when pushing existing documents to replicators.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplicatorPushOptions {
    pub se_encryption_key: Option<Vec<u8>>,
    pub se_identity_pubkey: Option<Vec<u8>>,
}

/// Replicator metadata exposed by embedded P2P operations.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ReplicatorInfo {
    pub id: Option<String>,
    pub collections: Vec<String>,
    pub address: Option<String>,
}

/// Document subscription info exposed by embedded P2P operations.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct P2pDocumentInfo {
    #[serde(rename = "Collection")]
    pub collection: String,
    #[serde(rename = "DocID")]
    pub doc_id: String,
}

/// Request type for document subscription operations.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct P2pDocumentRequest {
    #[serde(rename = "Collection")]
    pub collection: String,
    #[serde(rename = "DocID")]
    pub doc_id: String,
}

/// Storage persistence hints for ACP/NAC setup.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Persistence {
    #[default]
    Memory,
    Persistent,
}

/// Supported runtime transports for embedded nodes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IrohConfig {
    pub bind_addr: Option<std::net::IpAddr>,
    pub bind_port: Option<u16>,
    pub relay_mode: p2p::iroh::IrohRelayModeConfig,
    pub discovery: p2p::iroh::IrohDiscoveryConfig,
    pub secret_key_path: Option<std::path::PathBuf>,
}

#[cfg(feature = "iroh")]
impl Default for IrohConfig {
    fn default() -> Self {
        Self {
            bind_addr: None,
            bind_port: None,
            relay_mode: p2p::iroh::IrohRelayModeConfig::default(),
            discovery: p2p::iroh::IrohDiscoveryConfig::default(),
            secret_key_path: None,
        }
    }
}

/// Node signing configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
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
pub enum SigningKey {
    Secp256k1(Vec<u8>),
    Secp256r1(Vec<u8>),
    Ed25519(Vec<u8>),
}

/// Document ACP configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
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
    pub encryption_key: Option<Vec<u8>>,
    pub max_concurrent_dag_fetches: Option<usize>,
    pub max_concurrent_push_tasks: Option<usize>,
    pub rate_limit_burst: Option<u32>,
    pub rate_limit_rate: Option<f64>,
}

/// Runtime P2P transport kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportKind {
    Libp2p,
    #[cfg(feature = "iroh")]
    Iroh,
}

/// P2P runtime managed by an embedded node.
pub struct ManagedP2PSystem {
    kind: TransportKind,
    ops: Arc<dyn P2POperations>,
    shutdown: node::ShutdownHandle,
}

impl ManagedP2PSystem {
    pub fn new(
        kind: TransportKind,
        ops: Arc<dyn P2POperations>,
        shutdown: node::ShutdownHandle,
    ) -> Self {
        Self {
            kind,
            ops,
            shutdown,
        }
    }

    pub fn kind(&self) -> TransportKind {
        self.kind
    }

    pub fn ops(&self) -> &Arc<dyn P2POperations> {
        &self.ops
    }

    pub async fn shutdown(&self) {
        self.shutdown.shutdown().await;
    }
}

/// Storage backends supported by the public `NodeBuilder`.
pub enum EmbeddedStore {
    Memory(storage::MemoryStore),
    Redb(storage::RedbStore),
}

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
