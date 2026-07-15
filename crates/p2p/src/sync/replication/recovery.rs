//! Recovery of unmerged blocks after restart.

use std::sync::Arc;

use blockstore::Blockstore;

use super::config::ReplicationConfig;
use super::handlers::handle_block_received;
use super::result::ReplicationResult;
use crate::sync::coordinator::SyncCoordinator;
use crate::sync::merge::{BlockMetadata, MergeHandler};
use crate::transport::P2PTransport;

/// Process unmerged blocks from startup recovery.
///
/// Call this at startup to process any blocks that were stored
/// but not yet merged (e.g., due to crash recovery).
///
/// # Recovery Mode
///
/// During recovery, `BlockMetadata::recovery()` is passed to the handler with
/// all metadata fields set to `None`. The `MergeHandler` implementation MUST:
/// 1. Extract doc_id, collection_id, and creator from the block data itself
/// 2. Return an error if extraction fails (do NOT silently use defaults)
///
/// This ensures data integrity is maintained even after crashes.
///
/// # Returns
///
/// * `Ok(results)` - All blocks recovered successfully (or skipped)
/// * `Err(RecoveryFailed)` - One or more blocks failed to recover
///
/// # Errors
///
/// Returns an error if:
/// * The unmerged block list cannot be retrieved
/// * One or more blocks failed to recover (returns `Error::RecoveryFailed`)
pub async fn recover_unmerged<B, T, H>(
    coordinator: Arc<SyncCoordinator<B, T>>,
    handler: Arc<H>,
) -> Result<Vec<ReplicationResult>, crate::error::Error>
where
    B: Blockstore + 'static,
    T: P2PTransport,
    H: MergeHandler + 'static,
{
    let config = ReplicationConfig {
        continue_on_error: true,
        rebroadcast_on_merge: false,
        batch_size: 50,
        max_workers: 32,
    };

    let unmerged = coordinator.get_unmerged().await?;
    let total = unmerged.len();

    if unmerged.is_empty() {
        tracing::info!("No unmerged blocks to recover");
        return Ok(Vec::new());
    }

    tracing::warn!(
        count = total,
        "Recovering unmerged blocks - metadata unavailable, handler must extract from block data"
    );

    let mut results = Vec::new();
    let mut success_count = 0;
    let mut failure_count = 0;

    for cid in unmerged {
        tracing::debug!(cid = %cid, "Recovering unmerged block in recovery mode");

        let result = handle_block_received(
            &coordinator,
            handler.as_ref(),
            &config,
            cid,
            BlockMetadata::recovery(),
        )
        .await;

        match &result {
            ReplicationResult::Merged { .. } | ReplicationResult::Skipped { .. } => {
                success_count += 1;
            }
            ReplicationResult::MergedButNotMarked { cid, error } => {
                // Merge succeeded but bookkeeping failed - count as success
                success_count += 1;
                tracing::warn!(
                    cid = %cid,
                    error = %error,
                    "Block merged during recovery but marking failed - will be reprocessed next startup"
                );
            }
            ReplicationResult::MergedButBroadcastFailed {
                cid,
                doc_id,
                broadcast_error,
                ..
            } => {
                // Merge succeeded - count as success (broadcast not expected during recovery)
                success_count += 1;
                tracing::debug!(
                    cid = %cid,
                    doc_id = %doc_id,
                    error = %broadcast_error,
                    "Block merged during recovery but broadcast failed (expected - recovery mode)"
                );
            }
            ReplicationResult::Quarantined {
                cid,
                doc_id,
                collection_id,
                reason,
            } => {
                // Quarantine resolves the recovery obligation just like a
                // merge or terminal skip: the block will not be retried
                // locally again, so it counts as handled, not failed.
                success_count += 1;
                tracing::warn!(
                    cid = %cid,
                    doc_id = %doc_id,
                    collection_id = %collection_id,
                    reason = %reason,
                    "Block quarantined during recovery: merge deterministically rejected"
                );
            }
            ReplicationResult::Failed { cid, error } => {
                failure_count += 1;
                tracing::error!(
                    cid = %cid,
                    error = %error,
                    "Failed to recover block - manual intervention may be required"
                );
            }
            ReplicationResult::DagFetchStarted { root_cid } => {
                tracing::warn!(
                    cid = %root_cid,
                    "Unexpected DagFetchStarted during recovery - block may have missing links"
                );
            }
            ReplicationResult::BitswapFetchStarted { root_cid, .. } => {
                // Unexpected during recovery - blocks should already be in blockstore
                tracing::warn!(
                    cid = %root_cid,
                    "Unexpected BitswapFetchStarted during recovery - block may have missing links"
                );
            }
            ReplicationResult::ChannelClosed => {
                tracing::error!(
                    "Channel closed during recovery - some blocks may not be recovered"
                );
                break; // Exit recovery loop early
            }
        }

        results.push(result);
    }

    tracing::info!(
        success = success_count,
        failed = failure_count,
        "Recovery complete"
    );

    // Return error if any blocks failed to recover
    if failure_count > 0 {
        return Err(crate::error::Error::RecoveryFailed {
            success: success_count,
            failed: failure_count,
            total,
        });
    }

    Ok(results)
}
