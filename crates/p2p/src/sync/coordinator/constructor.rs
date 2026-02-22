//! Constructor methods for the sync coordinator.

use std::sync::Arc;

use blockstore::Blockstore;
use tokio::sync::mpsc;

use super::{SyncCoordinator, MAX_CONCURRENT_DAG_FETCHES};
use crate::bitswap::{AccessMode, ReplicatorRegistry};
use crate::error::Result;
use crate::host::P2PHostHandle;
use crate::sync::broadcaster::Broadcaster;
use crate::sync::collection_store::{NoOpCollectionStorage, P2PCollectionStorage};
use crate::sync::head_provider::{DocumentHeadProvider, NoOpHeadProvider};
use crate::sync::manager::{SyncConfig, SyncEvent, SyncManager};
use crate::sync::peer_state::PeerStateTracker;
use crate::sync::rate_limiter::PeerRateLimiter;

impl<B: Blockstore + 'static> SyncCoordinator<B> {
    /// Create a new sync coordinator with default Open access mode.
    ///
    /// Returns the coordinator and a receiver for sync events.
    ///
    /// This constructor creates the coordinator without access control
    /// (AccessMode::Open) and no persistent storage for collections.
    /// Use `with_collection_store` for production deployments with persistence.
    pub async fn new(
        host: P2PHostHandle,
        blockstore: Arc<B>,
        config: SyncConfig,
    ) -> Result<(Self, mpsc::Receiver<SyncEvent>)> {
        Self::with_access_control(
            host,
            blockstore,
            config,
            AccessMode::Open,
            Arc::new(ReplicatorRegistry::new()),
            Arc::new(NoOpCollectionStorage),
        )
        .await
    }

    /// Create a new sync coordinator with a collection store for persistence.
    ///
    /// Returns the coordinator and a receiver for sync events.
    ///
    /// # Arguments
    ///
    /// * `host` - Handle to the P2P host
    /// * `blockstore` - Shared blockstore for storing blocks
    /// * `config` - Sync configuration
    /// * `collection_store` - Persistent storage for P2P collection subscriptions
    ///
    /// This constructor enables persistent storage for P2P collection subscriptions.
    /// Collections will be saved to storage when subscribed and loaded on startup.
    pub async fn with_collection_store(
        host: P2PHostHandle,
        blockstore: Arc<B>,
        config: SyncConfig,
        access_mode: AccessMode,
        collection_store: Arc<dyn P2PCollectionStorage>,
    ) -> Result<(Self, mpsc::Receiver<SyncEvent>)> {
        Self::with_access_control(
            host,
            blockstore,
            config,
            access_mode,
            Arc::new(ReplicatorRegistry::new()),
            collection_store,
        )
        .await
    }

    /// Create a new sync coordinator with access control.
    ///
    /// Returns the coordinator and a receiver for sync events.
    ///
    /// # Arguments
    ///
    /// * `host` - Handle to the P2P host
    /// * `blockstore` - Shared blockstore for storing blocks
    /// * `config` - Sync configuration
    /// * `access_mode` - Access control mode (Open or Controlled)
    /// * `replicators` - Registry of authorized replicator peers
    /// * `collection_store` - Persistent storage for P2P collection subscriptions
    ///
    /// When `access_mode` is `AccessMode::Controlled`, incoming PushLog requests
    /// and GossipSub messages are checked against the replicator registry. Only
    /// peers registered as replicators for the collection can sync documents.
    pub async fn with_access_control(
        host: P2PHostHandle,
        blockstore: Arc<B>,
        config: SyncConfig,
        access_mode: AccessMode,
        replicators: Arc<ReplicatorRegistry>,
        collection_store: Arc<dyn P2PCollectionStorage>,
    ) -> Result<(Self, mpsc::Receiver<SyncEvent>)> {
        Self::with_head_provider(
            host,
            blockstore,
            config,
            access_mode,
            replicators,
            collection_store,
            Arc::new(NoOpHeadProvider),
        )
        .await
    }

    /// Create a new sync coordinator with a document head provider for DocSync.
    ///
    /// Returns the coordinator and a receiver for sync events.
    ///
    /// # Arguments
    ///
    /// * `host` - Handle to the P2P host
    /// * `blockstore` - Shared blockstore for storing blocks
    /// * `config` - Sync configuration
    /// * `access_mode` - Access control mode (Open or Controlled)
    /// * `replicators` - Registry of authorized replicator peers
    /// * `collection_store` - Persistent storage for P2P collection subscriptions
    /// * `head_provider` - Provider for document head CIDs (for DocSync responses)
    pub async fn with_head_provider(
        host: P2PHostHandle,
        blockstore: Arc<B>,
        config: SyncConfig,
        access_mode: AccessMode,
        replicators: Arc<ReplicatorRegistry>,
        collection_store: Arc<dyn P2PCollectionStorage>,
        head_provider: Arc<dyn DocumentHeadProvider>,
    ) -> Result<(Self, mpsc::Receiver<SyncEvent>)> {
        let local_peer_id = host.local_peer_id().await?.to_string();
        let broadcaster = Broadcaster::new(host.clone());
        let peer_state = Arc::new(PeerStateTracker::new());
        let (manager, events) = SyncManager::new(blockstore, peer_state.clone(), config);

        Ok((
            Self {
                host,
                broadcaster,
                manager,
                peer_state,
                local_peer_id,
                access_mode,
                replicators,
                subscribed_collections: Arc::new(tokio::sync::RwLock::new(
                    std::collections::HashSet::new(),
                )),
                collection_store,
                head_provider,
                failure_tx: None,
                dag_fetch_semaphore: Arc::new(tokio::sync::Semaphore::new(
                    MAX_CONCURRENT_DAG_FETCHES,
                )),
                rate_limiter: Arc::new(PeerRateLimiter::default()),
            },
            events,
        ))
    }

    /// Set the failure channel for reporting push failures to the FFI layer.
    pub fn set_failure_channel(
        &mut self,
        tx: tokio::sync::mpsc::UnboundedSender<super::PushFailure>,
    ) {
        self.failure_tx = Some(tx);
    }
}
