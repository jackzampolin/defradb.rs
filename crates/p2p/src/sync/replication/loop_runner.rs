//! Replication loop runner.
//!
//! The replication loop is the bridge between the P2P layer and the database.
//! It consumes SyncEvents, loads blocks from the blockstore, delegates merge
//! operations to the database layer, and marks blocks as merged.
//!
//! # Architecture
//!
//! ```text
//! SyncManager emits SyncEvent::BlockReceived
//!         ↓
//! ReplicationLoop receives event
//!         ↓
//! Load block from blockstore
//!         ↓
//! MergeHandler::handle_block() [database layer]
//!         ↓
//! SyncCoordinator::mark_as_merged()
//! ```

use std::sync::Arc;

use blockstore::Blockstore;
use tokio::sync::{mpsc, Semaphore};

use super::config::ReplicationConfig;
use super::handlers::{
    has_duplicate_merge_cids, is_mergeable_event, process_event_serialized,
    process_events_individually, process_merge_batch,
};
use super::result::ReplicationResult;
use crate::sync::coordinator::SyncCoordinator;
use crate::sync::manager::SyncEvent;
use crate::sync::merge::MergeHandler;
use crate::transport::P2PTransport;

/// Replication loop that processes sync events.
///
/// # Usage
///
/// ```ignore
/// // Create coordinator and get event receiver
/// let (coordinator, events) = SyncCoordinator::new(host, blockstore, config).await?;
///
/// // Create merge handler (database layer)
/// let handler = MyMergeHandler::new(db);
///
/// // Run the replication loop
/// let config = ReplicationConfig::default();
/// ReplicationLoop::run(coordinator, events, handler, config).await;
/// ```
pub struct ReplicationLoop;

impl ReplicationLoop {
    /// Run the replication loop with batch processing.
    ///
    /// This method runs until the event channel is closed or a fatal error occurs.
    /// It batches merge-eligible events together for efficient processing with
    /// shared transactions, reducing fsync overhead during P2P catch-up.
    pub async fn run<B, T, H>(
        coordinator: Arc<SyncCoordinator<B, T>>,
        mut events: mpsc::Receiver<SyncEvent>,
        handler: Arc<H>,
        config: ReplicationConfig,
    ) where
        B: Blockstore + 'static,
        T: P2PTransport,
        H: MergeHandler + 'static,
    {
        tracing::info!(batch_size = config.batch_size, "Starting replication loop");

        loop {
            let results =
                Self::process_next_batch(&coordinator, &mut events, handler.as_ref(), &config)
                    .await;

            let mut should_break = false;
            for result in &results {
                match result {
                    ReplicationResult::Merged {
                        cid,
                        doc_id,
                        collection_id,
                    } => {
                        tracing::info!(
                            cid = %cid,
                            doc_id = %doc_id,
                            collection_id = %collection_id,
                            "Block merged successfully"
                        );
                    }
                    ReplicationResult::MergedButBroadcastFailed {
                        cid,
                        doc_id,
                        broadcast_error,
                        ..
                    } => {
                        tracing::error!(
                            cid = %cid,
                            doc_id = %doc_id,
                            error = %broadcast_error,
                            "Block merged but re-broadcast failed"
                        );
                    }
                    ReplicationResult::Skipped { cid, reason, .. } => {
                        tracing::debug!(cid = %cid, reason = %reason, "Block skipped");
                    }
                    ReplicationResult::Failed { cid, error } => {
                        tracing::error!(cid = %cid, error = %error, "Block merge failed");
                        if !config.continue_on_error {
                            tracing::error!("Stopping replication loop due to error");
                            should_break = true;
                            break;
                        }
                    }
                    ReplicationResult::MergedButNotMarked { cid, error } => {
                        tracing::error!(
                            cid = %cid,
                            error = %error,
                            "Block merged but failed to mark - will be reprocessed on restart"
                        );
                    }
                    ReplicationResult::ChannelClosed => {
                        tracing::info!("Event channel closed, stopping replication loop");
                        should_break = true;
                        break;
                    }
                    ReplicationResult::DagFetchStarted { root_cid } => {
                        tracing::debug!(
                            cid = %root_cid,
                            "Poll-based DAG fetch started for missing blocks"
                        );
                    }
                    ReplicationResult::BitswapFetchStarted { root_cid, query_id } => {
                        tracing::debug!(
                            cid = %root_cid,
                            query_id = ?query_id,
                            "Bitswap fetch started for missing blocks"
                        );
                    }
                }
            }
            if should_break {
                break;
            }
        }

        tracing::info!("Replication loop stopped");
    }

    /// Run the replication loop with concurrent workers.
    ///
    /// Spawns up to `config.max_workers` concurrent tasks via a semaphore.
    /// Each result is passed to `on_result` for caller-specific handling
    /// (e.g., publishing events to an event bus).
    pub async fn run_parallel<B, T, H, F>(
        coordinator: Arc<SyncCoordinator<B, T>>,
        events: mpsc::Receiver<SyncEvent>,
        handler: Arc<H>,
        config: ReplicationConfig,
        on_result: F,
    ) where
        B: Blockstore + 'static,
        T: P2PTransport,
        H: MergeHandler + 'static,
        F: Fn(ReplicationResult) + Send + Sync + 'static,
    {
        tracing::info!(
            max_workers = config.max_workers,
            "Starting parallel replication loop"
        );

        let semaphore = Arc::new(Semaphore::new(config.max_workers));
        Self::run_parallel_with_semaphore(
            coordinator,
            events,
            handler,
            config,
            on_result,
            semaphore,
        )
        .await;
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) async fn run_parallel_with_semaphore<B, T, H, F>(
        coordinator: Arc<SyncCoordinator<B, T>>,
        mut events: mpsc::Receiver<SyncEvent>,
        handler: Arc<H>,
        config: ReplicationConfig,
        on_result: F,
        semaphore: Arc<Semaphore>,
    ) where
        B: Blockstore + 'static,
        T: P2PTransport,
        H: MergeHandler + 'static,
        F: Fn(ReplicationResult) + Send + Sync + 'static,
    {
        let on_result = Arc::new(on_result);
        let config = Arc::new(config);

        loop {
            let event = match events.recv().await {
                Some(e) => e,
                None => {
                    tracing::info!("Event channel closed, stopping parallel replication loop");
                    break;
                }
            };

            let permit = match semaphore.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => {
                    tracing::info!("Worker semaphore closed, stopping parallel replication loop");
                    break;
                }
            };
            let coord = coordinator.clone();
            let h = handler.clone();
            let c = config.clone();
            let cb = on_result.clone();

            tokio::spawn(async move {
                let result = process_event_serialized(&coord, event, h.as_ref(), &c).await;
                cb(result);
                drop(permit);
            });
        }

        tracing::info!("Parallel replication loop stopped");
    }

    /// Process the next sync event.
    ///
    /// This is public so that callers (e.g., FFI layer) can run a custom
    /// replication loop that injects additional behavior (like publishing
    /// MergeComplete events) after each successful merge.
    pub async fn process_next<B, T, H>(
        coordinator: &SyncCoordinator<B, T>,
        events: &mut mpsc::Receiver<SyncEvent>,
        handler: &H,
        config: &ReplicationConfig,
    ) -> ReplicationResult
    where
        B: Blockstore + 'static,
        T: P2PTransport,
        H: MergeHandler + ?Sized + 'static,
    {
        let event = match events.recv().await {
            Some(e) => e,
            None => return ReplicationResult::ChannelClosed,
        };

        process_event_serialized(coordinator, event, handler, config).await
    }

    /// Process the next batch of sync events.
    ///
    /// Waits for at least one event (blocking), then drains up to
    /// `config.batch_size - 1` more events using `try_recv()`.
    /// Merge-eligible events are batched together for efficient processing;
    /// other events (errors, already-merged, Bitswap) are processed individually.
    pub async fn process_next_batch<B, T, H>(
        coordinator: &SyncCoordinator<B, T>,
        events: &mut mpsc::Receiver<SyncEvent>,
        handler: &H,
        config: &ReplicationConfig,
    ) -> Vec<ReplicationResult>
    where
        B: Blockstore + 'static,
        T: P2PTransport,
        H: MergeHandler + ?Sized + 'static,
    {
        // Wait for first event (blocking)
        let first = match events.recv().await {
            Some(e) => e,
            None => return vec![ReplicationResult::ChannelClosed],
        };

        // Drain more events non-blocking
        let max_batch_size = config.batch_size.max(1);
        let mut batch = vec![first];
        while batch.len() < max_batch_size {
            match events.try_recv() {
                Ok(e) => batch.push(e),
                Err(_) => break,
            }
        }
        debug_assert!(batch.len() <= max_batch_size);

        // Separate merge-worthy events from immediate-action events
        let mut immediate = Vec::new();
        let mut merge_events = Vec::new();
        for event in batch {
            if is_mergeable_event(&event) {
                merge_events.push(event);
            } else {
                immediate.push(event);
            }
        }

        let mut results = Vec::new();

        // Process immediate events normally
        for event in immediate {
            results.push(process_event_serialized(coordinator, event, handler, config).await);
        }

        // Batch-process merge events
        if !merge_events.is_empty() {
            let batch_results = if has_duplicate_merge_cids(&merge_events) {
                tracing::debug!(
                    batch_size = merge_events.len(),
                    "Processing duplicate-CID merge batch individually"
                );
                process_events_individually(coordinator, merge_events, handler, config).await
            } else {
                tracing::debug!(batch_size = merge_events.len(), "Processing merge batch");
                process_merge_batch(coordinator, merge_events, handler, config).await
            };
            results.extend(batch_results);
        }

        results
    }

    /// Process all pending events without blocking.
    ///
    /// Useful for draining events during shutdown or testing.
    pub async fn drain<B, T, H>(
        coordinator: Arc<SyncCoordinator<B, T>>,
        events: &mut mpsc::Receiver<SyncEvent>,
        handler: Arc<H>,
        config: ReplicationConfig,
    ) -> Vec<ReplicationResult>
    where
        B: Blockstore + 'static,
        T: P2PTransport,
        H: MergeHandler + 'static,
    {
        let mut results = Vec::new();

        loop {
            match events.try_recv() {
                Ok(event) => {
                    let result =
                        process_event_serialized(&coordinator, event, handler.as_ref(), &config)
                            .await;
                    results.push(result);
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }

        results
    }
}
