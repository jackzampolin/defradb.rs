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
use tokio::sync::mpsc;

use super::config::ReplicationConfig;
use super::handlers::process_event;
use super::result::ReplicationResult;
use crate::sync::coordinator::SyncCoordinator;
use crate::sync::manager::SyncEvent;
use crate::sync::merge::MergeHandler;

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
    /// Run the replication loop.
    ///
    /// This method runs until the event channel is closed or a fatal error occurs.
    /// It processes SyncEvents, delegates merges to the handler, and marks blocks
    /// as merged.
    pub async fn run<B, H>(
        coordinator: Arc<SyncCoordinator<B>>,
        mut events: mpsc::Receiver<SyncEvent>,
        handler: Arc<H>,
        config: ReplicationConfig,
    ) where
        B: Blockstore + 'static,
        H: MergeHandler + 'static,
    {
        tracing::info!("Starting replication loop");

        loop {
            let result =
                Self::process_next(&coordinator, &mut events, handler.as_ref(), &config).await;

            match &result {
                ReplicationResult::Merged { cid, doc_id, .. } => {
                    tracing::info!(cid = %cid, doc_id = %doc_id, "Block merged successfully");
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
                        "Block merged but re-broadcast failed - other nodes may not receive this update"
                    );
                    // Continue processing - the local merge succeeded
                }
                ReplicationResult::Skipped { cid, reason } => {
                    tracing::debug!(cid = %cid, reason = %reason, "Block skipped");
                }
                ReplicationResult::Failed { cid, error } => {
                    tracing::error!(cid = %cid, error = %error, "Block merge failed");
                    if !config.continue_on_error {
                        tracing::error!("Stopping replication loop due to error");
                        break;
                    }
                }
                ReplicationResult::MergedButNotMarked { cid, error } => {
                    tracing::error!(
                        cid = %cid,
                        error = %error,
                        "Block merged but failed to mark - will be reprocessed on restart"
                    );
                    // Continue processing - the merge succeeded, just the bookkeeping failed
                }
                ReplicationResult::ChannelClosed => {
                    tracing::info!("Event channel closed, stopping replication loop");
                    break;
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

        tracing::info!("Replication loop stopped");
    }

    /// Process the next sync event.
    ///
    /// This is public so that callers (e.g., FFI layer) can run a custom
    /// replication loop that injects additional behavior (like publishing
    /// MergeComplete events) after each successful merge.
    pub async fn process_next<B, H>(
        coordinator: &SyncCoordinator<B>,
        events: &mut mpsc::Receiver<SyncEvent>,
        handler: &H,
        config: &ReplicationConfig,
    ) -> ReplicationResult
    where
        B: Blockstore + 'static,
        H: MergeHandler + ?Sized + 'static,
    {
        let event = match events.recv().await {
            Some(e) => e,
            None => return ReplicationResult::ChannelClosed,
        };

        process_event(coordinator, event, handler, config).await
    }

    /// Process all pending events without blocking.
    ///
    /// Useful for draining events during shutdown or testing.
    pub async fn drain<B, H>(
        coordinator: Arc<SyncCoordinator<B>>,
        events: &mut mpsc::Receiver<SyncEvent>,
        handler: Arc<H>,
        config: ReplicationConfig,
    ) -> Vec<ReplicationResult>
    where
        B: Blockstore + 'static,
        H: MergeHandler + 'static,
    {
        let mut results = Vec::new();

        loop {
            match events.try_recv() {
                Ok(event) => {
                    let result =
                        process_event(&coordinator, event, handler.as_ref(), &config).await;
                    results.push(result);
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }

        results
    }
}
