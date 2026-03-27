//! Node state management for FFI.
//!
//! This module manages the lifecycle of node instances and their handles.
//! Go code receives opaque usize handles that map to actual node state.

mod p2p;
mod policy_store;
mod registry;

use std::sync::Arc;

use async_trait::async_trait;
use storage::MemoryStore;
use zeroize::Zeroizing;

use blockstore::DefraBlockstore;

pub use p2p::P2PState;
pub use policy_store::PolicyStore;
pub use registry::{
    graphql_subscriptions, nodes, subscriptions, GraphQLSubscriptionRegistry,
    GraphQLSubscriptionsAccess, NodeRegistry, NodesAccess, SubscriptionRegistry,
    SubscriptionsAccess, GRAPHQL_SUBSCRIPTIONS, NODES, SUBSCRIPTIONS,
};

/// Storage backend enum for FFI nodes.
///
/// Wraps all backend implementations so that `DB<FfiStore>` works for
/// any backend without requiring separate type aliases or code paths.
#[non_exhaustive]
pub enum FfiStore {
    Memory(MemoryStore),
    Redb(storage::RedbStore),
    #[cfg(feature = "fjall")]
    Fjall(storage::FjallStore),
    #[cfg(feature = "rocksdb")]
    RocksDb(storage::RocksDbStore),
}

impl storage::corekv::private::Sealed for FfiStore {}

#[async_trait]
impl storage::Store for FfiStore {
    async fn new_txn(&self, readonly: bool) -> storage::Result<Box<dyn storage::Txn>> {
        match self {
            FfiStore::Memory(s) => s.new_txn(readonly).await,
            FfiStore::Redb(s) => s.new_txn(readonly).await,
            #[cfg(feature = "fjall")]
            FfiStore::Fjall(s) => s.new_txn(readonly).await,
            #[cfg(feature = "rocksdb")]
            FfiStore::RocksDb(s) => s.new_txn(readonly).await,
        }
    }

    async fn close(&self) -> storage::Result<()> {
        match self {
            FfiStore::Memory(s) => s.close().await,
            FfiStore::Redb(s) => s.close().await,
            #[cfg(feature = "fjall")]
            FfiStore::Fjall(s) => s.close().await,
            #[cfg(feature = "rocksdb")]
            FfiStore::RocksDb(s) => s.close().await,
        }
    }
}

/// Type alias for the database type used in FFI.
pub type FfiDatabase = db::DB<FfiStore>;

/// Type alias for the blockstore type used in FFI.
pub type FfiBlockstore = DefraBlockstore<FfiStore>;

/// Type alias for the merge handler used in FFI.
pub type FfiMergeHandler = db::DbMergeHandler<FfiStore, FfiBlockstore>;

/// Type alias for node handles (opaque to FFI callers).
pub type NodeHandle = usize;

/// Type alias for the NAC manager used in FFI (dynamic dispatch over store backend).
pub type FfiNacManager = dyn db::NacManagerApi;

/// Type alias for subscription handles (opaque to FFI callers).
pub type SubscriptionHandle = usize;

/// Type alias for the transaction registry type used in FFI.
pub type FfiTransactionRegistry = db::DbTransactionRegistry<FfiStore>;

/// State held for each FFI node.
pub struct NodeState {
    /// The database instance.
    pub database: Arc<FfiDatabase>,
    /// Background tasks owned by the embedded node (e.g. the downsample worker).
    pub background_tasks: Arc<embedded::BackgroundTasks>,
    /// The transaction registry for managing explicit transactions.
    pub txn_registry: Arc<FfiTransactionRegistry>,
    /// The query runner for executing GraphQL queries.
    pub query_runner: Arc<dyn query::QueryExecutor>,
    /// The NAC manager for node-level access control.
    pub nac_manager: Arc<FfiNacManager>,
    /// The document ACP for document-level access control.
    pub document_acp: Arc<dyn acp::DocumentACP>,
    /// The event bus for subscriptions.
    pub event_bus: Arc<dyn events::Bus>,
    /// The policy store for DAC policies.
    pub policy_store: Arc<PolicyStore>,
    /// P2P state (optional - not all nodes have P2P enabled).
    pub p2p: Option<Arc<P2PState>>,
    /// Node identity DID (set when signing is enabled).
    /// Used as fallback identity for signing blocks when no explicit identity is provided.
    pub node_identity_did: Option<String>,
    /// SourceHub ACP (optional - only set when using SourceHub for document ACP).
    /// Used by add_dac_policy to route policy creation through SourceHub transactions.
    pub sourcehub_acp: Option<Arc<sourcehub::SourceHubDocumentACP>>,
    /// Searchable encryption key (32-byte AES-256 key). Zeroized on drop.
    /// Set via `set_se_encryption_key` FFI when SE is enabled in test config.
    pub se_encryption_key: Option<Zeroizing<Vec<u8>>>,
}

/// State held for each FFI subscription.
pub struct SubscriptionState {
    /// The underlying events subscription.
    pub subscription: events::Subscription,
    /// The node handle this subscription belongs to.
    pub node_handle: NodeHandle,
    /// Optional collection name filter (None = all collections).
    pub collection_filter: Option<String>,
}

/// State held for each GraphQL subscription (used by poll_graphql_subscription).
///
/// Subscription queries are re-executed at **event time** (not poll time) to ensure
/// the DB state matches the event. A background tokio task processes events as they
/// arrive, executes the subscription query scoped to the changed document, and
/// buffers the full GraphQL JSON results for polling.
pub struct GraphQLSubscriptionState {
    /// Receiver for fully-processed GraphQL result JSON strings.
    pub result_receiver: tokio::sync::mpsc::Receiver<String>,
    /// The node handle this subscription belongs to.
    pub node_handle: NodeHandle,
    /// The event bus subscription ID (for cleanup/unsubscribe).
    pub event_sub_id: u64,
    /// Abort handle for the background event processing task.
    pub task_abort: tokio::task::AbortHandle,
}
