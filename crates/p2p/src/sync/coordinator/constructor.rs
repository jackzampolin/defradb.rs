//! Constructor methods for the sync coordinator.

use std::sync::Arc;

use blockstore::Blockstore;
use tokio::sync::mpsc;

use super::SyncCoordinator;
use crate::bitswap::{AccessMode, ReplicatorRegistry};
use crate::error::Result;
use crate::sync::broadcaster::Broadcaster;
use crate::sync::collection_store::{NoOpCollectionStorage, P2PCollectionStorage};
use crate::sync::head_provider::{DocumentHeadProvider, NoOpHeadProvider};
use crate::sync::manager::{SyncConfig, SyncEvent, SyncManager};
use crate::sync::peer_state::PeerStateTracker;
use crate::sync::rate_limiter::PeerRateLimiter;
use crate::transport::P2PTransport;

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    /// Create a new sync coordinator with default Open access mode.
    ///
    /// Returns the coordinator and a receiver for sync events.
    pub async fn new(
        transport: T,
        blockstore: Arc<B>,
        config: SyncConfig,
    ) -> Result<(Self, mpsc::Receiver<SyncEvent>)> {
        Self::with_access_control(
            transport,
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
    pub async fn with_collection_store(
        transport: T,
        blockstore: Arc<B>,
        config: SyncConfig,
        access_mode: AccessMode,
        collection_store: Arc<dyn P2PCollectionStorage>,
    ) -> Result<(Self, mpsc::Receiver<SyncEvent>)> {
        Self::with_access_control(
            transport,
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
    pub async fn with_access_control(
        transport: T,
        blockstore: Arc<B>,
        config: SyncConfig,
        access_mode: AccessMode,
        replicators: Arc<ReplicatorRegistry>,
        collection_store: Arc<dyn P2PCollectionStorage>,
    ) -> Result<(Self, mpsc::Receiver<SyncEvent>)> {
        Self::with_head_provider(
            transport,
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
    pub async fn with_head_provider(
        transport: T,
        blockstore: Arc<B>,
        config: SyncConfig,
        access_mode: AccessMode,
        replicators: Arc<ReplicatorRegistry>,
        collection_store: Arc<dyn P2PCollectionStorage>,
        head_provider: Arc<dyn DocumentHeadProvider>,
    ) -> Result<(Self, mpsc::Receiver<SyncEvent>)> {
        let local_peer_id = transport.local_peer_id().to_string();
        let broadcaster = Broadcaster::new(transport.clone());
        let peer_state = Arc::new(PeerStateTracker::new());
        let max_dag_fetches = config.max_concurrent_dag_fetches.max(1);
        let max_push_tasks = config.max_concurrent_push_tasks.max(1);
        let (manager, events) = SyncManager::new(blockstore, peer_state.clone(), config);

        Ok((
            Self {
                transport,
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
                dag_fetch_semaphore: Arc::new(tokio::sync::Semaphore::new(max_dag_fetches)),
                push_semaphore: Arc::new(tokio::sync::Semaphore::new(max_push_tasks)),
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
