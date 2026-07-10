//! Constructor methods for the sync coordinator.

use std::sync::Arc;
use std::time::Duration;

use blockstore::Blockstore;
use tokio::sync::mpsc;

#[cfg(feature = "libp2p-transport")]
use super::authorizer::AccessAuthorizer;
use super::authorizer::RuntimeAuthorizer;
use super::{
    DagFetchLimiter, SyncAccessState, SyncCoordinator, SyncRuntime, SyncSubscriptionState,
};
use crate::bitswap::{
    AccessMode, BlockClassifier, DefaultBlockClassifier, LateBoundServeAcp, ReplicatorRegistry,
};
use crate::error::Result;
use crate::replicator::{EqOnlyFilterMatcher, ReplicationFilterMatcher};
use crate::sync::broadcaster::Broadcaster;
use crate::sync::collection_store::{NoOpCollectionStorage, P2PCollectionStorage};
use crate::sync::head_provider::{DocumentHeadProvider, NoOpHeadProvider};
use crate::sync::manager::{
    SyncConfig, SyncEvent, SyncManager, DEFAULT_MAX_DOC_SYNC_REQUEST_DOC_IDS,
    DEFAULT_PUSH_SEND_TIMEOUT,
};
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
            Arc::new(EqOnlyFilterMatcher),
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
            Arc::new(EqOnlyFilterMatcher),
        )
        .await
    }

    /// Create a new sync coordinator with access control.
    ///
    /// Returns the coordinator and a receiver for sync events.
    #[allow(clippy::too_many_arguments)]
    pub async fn with_access_control(
        transport: T,
        blockstore: Arc<B>,
        config: SyncConfig,
        access_mode: AccessMode,
        replicators: Arc<ReplicatorRegistry>,
        collection_store: Arc<dyn P2PCollectionStorage>,
        filter_matcher: Arc<dyn ReplicationFilterMatcher>,
    ) -> Result<(Self, mpsc::Receiver<SyncEvent>)> {
        Self::with_access_control_and_serve_gate(
            transport,
            blockstore,
            config,
            access_mode,
            replicators,
            collection_store,
            filter_matcher,
            Arc::new(DefaultBlockClassifier),
            Arc::new(LateBoundServeAcp::new()),
        )
        .await
    }

    /// Create a new sync coordinator with access control and explicit serve gate.
    #[allow(clippy::too_many_arguments)]
    pub async fn with_access_control_and_serve_gate(
        transport: T,
        blockstore: Arc<B>,
        config: SyncConfig,
        access_mode: AccessMode,
        replicators: Arc<ReplicatorRegistry>,
        collection_store: Arc<dyn P2PCollectionStorage>,
        filter_matcher: Arc<dyn ReplicationFilterMatcher>,
        classifier: Arc<dyn BlockClassifier>,
        serve_acp: Arc<LateBoundServeAcp>,
    ) -> Result<(Self, mpsc::Receiver<SyncEvent>)> {
        Self::with_head_provider_and_serve_gate(
            transport,
            blockstore,
            config,
            access_mode,
            replicators,
            collection_store,
            Arc::new(NoOpHeadProvider),
            filter_matcher,
            classifier,
            serve_acp,
        )
        .await
    }

    /// Create a new sync coordinator with a document head provider for DocSync.
    ///
    /// Returns the coordinator and a receiver for sync events.
    #[allow(clippy::too_many_arguments)]
    pub async fn with_head_provider(
        transport: T,
        blockstore: Arc<B>,
        config: SyncConfig,
        access_mode: AccessMode,
        replicators: Arc<ReplicatorRegistry>,
        collection_store: Arc<dyn P2PCollectionStorage>,
        head_provider: Arc<dyn DocumentHeadProvider>,
        filter_matcher: Arc<dyn ReplicationFilterMatcher>,
    ) -> Result<(Self, mpsc::Receiver<SyncEvent>)> {
        Self::with_head_provider_and_serve_gate(
            transport,
            blockstore,
            config,
            access_mode,
            replicators,
            collection_store,
            head_provider,
            filter_matcher,
            Arc::new(DefaultBlockClassifier),
            Arc::new(LateBoundServeAcp::new()),
        )
        .await
    }

    /// Create a new sync coordinator with a document head provider and explicit serve gate.
    #[allow(clippy::too_many_arguments)]
    pub async fn with_head_provider_and_serve_gate(
        transport: T,
        blockstore: Arc<B>,
        config: SyncConfig,
        access_mode: AccessMode,
        replicators: Arc<ReplicatorRegistry>,
        collection_store: Arc<dyn P2PCollectionStorage>,
        head_provider: Arc<dyn DocumentHeadProvider>,
        filter_matcher: Arc<dyn ReplicationFilterMatcher>,
        classifier: Arc<dyn BlockClassifier>,
        serve_acp: Arc<LateBoundServeAcp>,
    ) -> Result<(Self, mpsc::Receiver<SyncEvent>)> {
        let local_peer_id = transport.local_peer_id().to_string();
        let broadcaster = Broadcaster::new(transport.clone());
        let peer_state = Arc::new(PeerStateTracker::new());
        let max_dag_fetches = config.max_concurrent_dag_fetches.max(1);
        let max_push_tasks = config.max_concurrent_push_tasks.max(1);
        let push_backlog = crate::sync::push_backlog::PushBacklog::new(
            config.push_queue_capacity,
            config.push_queue_byte_capacity,
            config.max_active_pushes_per_peer,
            max_push_tasks,
        );
        let selective_car_access =
            Arc::new(super::selective_car_access::SelectiveCarAccess::default());
        let rate_limit_burst = config.rate_limit_burst;
        let rate_limit_rate = config.rate_limit_rate;
        let rate_limit_backoff = config.rate_limit_backoff.clone();
        let max_doc_sync_request_doc_ids =
            resolve_max_doc_sync_request_doc_ids(config.max_doc_sync_request_doc_ids);
        let push_send_timeout = if config.push_send_timeout.is_zero() {
            DEFAULT_PUSH_SEND_TIMEOUT
        } else {
            config.push_send_timeout.max(Duration::from_millis(1))
        };
        let subscribed_collections =
            Arc::new(tokio::sync::RwLock::new(std::collections::HashSet::new()));
        let (manager, events) =
            SyncManager::new(Arc::clone(&blockstore), peer_state.clone(), config);

        let authorizer = Arc::new(RuntimeAuthorizer::new(
            transport.clone(),
            Arc::clone(&peer_state),
            Arc::clone(&replicators),
            access_mode,
        ));
        #[cfg(feature = "libp2p-transport")]
        let pubsub_services = super::pubsub_services::PubsubServices::try_new(
            &local_peer_id,
            Arc::clone(&head_provider),
            Arc::clone(&authorizer) as Arc<dyn AccessAuthorizer>,
        );

        let failure_tx: Arc<parking_lot::Mutex<Option<mpsc::Sender<super::PushFailure>>>> =
            Arc::new(parking_lot::Mutex::new(None));
        let shutdown = super::SyncShutdownHandle::new();
        super::push_worker::spawn_push_workers(
            Arc::new(super::push_worker::PushWorkerContext {
                transport: transport.clone(),
                blockstore,
                backlog: Arc::clone(&push_backlog),
                selective_car_access: Arc::clone(&selective_car_access),
                failure_tx: Arc::clone(&failure_tx),
                send_timeout: push_send_timeout,
            }),
            &shutdown,
        );

        Ok((
            Self {
                runtime: SyncRuntime {
                    transport,
                    broadcaster,
                    failure_tx,
                    dag_fetch_limiter: DagFetchLimiter::new(max_dag_fetches),
                    push_backlog,
                    selective_car_access,
                    rate_limiter: Arc::new(PeerRateLimiter::with_backoff_steps(
                        rate_limit_burst,
                        rate_limit_rate,
                        rate_limit_backoff,
                    )),
                    request_rate_limiter: Arc::new(PeerRateLimiter::new_request_paced(
                        rate_limit_burst,
                        rate_limit_rate,
                    )),
                    max_doc_sync_request_doc_ids,
                    shutdown,
                    filter_matcher,
                },
                manager,
                access: SyncAccessState {
                    peer_state,
                    local_peer_id,
                    access_mode,
                    replicators,
                },
                subscriptions: SyncSubscriptionState {
                    subscribed_collections,
                    collection_store,
                    head_provider,
                },
                authorizer,
                classifier,
                serve_acp,
                document_acp: std::sync::OnceLock::new(),
                #[cfg(feature = "libp2p-transport")]
                kms_transport: std::sync::OnceLock::new(),
                #[cfg(feature = "libp2p-transport")]
                pubsub_services,
            },
            events,
        ))
    }

    /// Set the failure channel for reporting push failures to the FFI layer.
    pub fn set_failure_channel(&mut self, tx: tokio::sync::mpsc::Sender<super::PushFailure>) {
        *self.runtime.failure_tx.lock() = Some(tx);
    }
}

/// Resolve the effective DocSync request doc-ID limit from a configured value.
///
/// A configured `0` means "use the default" across every entry point
/// (CLI/config, FFI, embedded, mobile), matching the documented default
/// behavior. Any other value is used as-is.
fn resolve_max_doc_sync_request_doc_ids(configured: usize) -> usize {
    if configured == 0 {
        tracing::warn!(
            configured = 0,
            effective = DEFAULT_MAX_DOC_SYNC_REQUEST_DOC_IDS,
            "max_doc_sync_request_doc_ids of 0 is invalid, using default"
        );
        return DEFAULT_MAX_DOC_SYNC_REQUEST_DOC_IDS;
    }
    configured
}
