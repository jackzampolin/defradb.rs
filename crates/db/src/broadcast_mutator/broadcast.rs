//! Broadcast helper functions for fire-and-forget P2P propagation.

use blockstore::Blockstore;
use p2p::sync::{BroadcastResult, SyncCoordinator};
use p2p::transport::P2PTransport;
use query::mutator::BroadcastStatus;

use crate::block_builder::BlockResult;

pub(crate) const BROADCAST_MAX_RETRIES: u32 = 10;

pub(crate) fn broadcast_retry_delay_ms(
    err_str: &str,
    connected_peers: usize,
    attempt: u32,
) -> Option<u64> {
    if !err_str.contains("InsufficientPeers") {
        return None;
    }
    if connected_peers == 0 || attempt > BROADCAST_MAX_RETRIES {
        return None;
    }
    Some(100 * (1u64 << attempt.min(5)))
}

/// Log broadcast failures at error level for observability in fire-and-forget paths.
pub(crate) fn log_broadcast_failure(status: &BroadcastStatus) {
    if let BroadcastStatus::Failed(err) = status {
        tracing::error!(
            error = %err,
            "Fire-and-forget broadcast failed — document committed locally but NOT replicated"
        );
    }
}

/// Broadcast via GossipSub with retry, optionally overriding the creator DID.
pub(crate) async fn broadcast_with_retry_with_creator<B: Blockstore + 'static, T: P2PTransport>(
    sync: &SyncCoordinator<B, T>,
    block_result: &BlockResult,
    collection_id: &str,
    collection_name: &str,
    creator_override: Option<&str>,
) -> BroadcastStatus {
    let mut attempt = 0u32;

    loop {
        attempt += 1;
        match sync
            .broadcast_local_update_with_creator(
                &block_result.cid,
                &block_result.block,
                &block_result.doc_id,
                collection_id,
                creator_override,
            )
            .await
        {
            Ok(BroadcastResult::Success) => {
                tracing::debug!(
                    doc_id = %block_result.doc_id,
                    cid = %block_result.cid,
                    collection = %collection_name,
                    attempts = attempt,
                    "Broadcast document to P2P network"
                );
                return BroadcastStatus::Success;
            }
            Ok(BroadcastResult::PartialDocumentOnly { collection_error }) => {
                tracing::warn!(
                    doc_id = %block_result.doc_id,
                    collection = %collection_name,
                    error = %collection_error,
                    "Partial broadcast: document topic succeeded, collection topic failed"
                );
                return BroadcastStatus::Failed(format!(
                    "Partial: collection topic failed: {}",
                    collection_error
                ));
            }
            Ok(BroadcastResult::PartialCollectionOnly { document_error }) => {
                tracing::warn!(
                    doc_id = %block_result.doc_id,
                    collection = %collection_name,
                    error = %document_error,
                    "Partial broadcast: collection topic succeeded, document topic failed"
                );
                return BroadcastStatus::Failed(format!(
                    "Partial: document topic failed: {}",
                    document_error
                ));
            }
            Ok(other) => {
                tracing::warn!(
                    doc_id = %block_result.doc_id,
                    collection = %collection_name,
                    result = ?other,
                    "Unexpected broadcast result from P2P network"
                );
                return BroadcastStatus::Failed(format!(
                    "Unexpected broadcast result: {:?}",
                    other
                ));
            }
            Err(e) => {
                let err_str = e.to_string();
                let connected_peers = sync.peer_state().stats().connected_peers();
                if let Some(delay_ms) = broadcast_retry_delay_ms(&err_str, connected_peers, attempt)
                {
                    tracing::trace!(
                        doc_id = %block_result.doc_id,
                        attempt = attempt,
                        connected_peers = connected_peers,
                        delay_ms = delay_ms,
                        "Retrying broadcast after InsufficientPeers"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    continue;
                }
                if err_str.contains("InsufficientPeers") && connected_peers == 0 {
                    tracing::debug!(
                        doc_id = %block_result.doc_id,
                        collection = %collection_name,
                        attempts = attempt,
                        "Skipping GossipSub retries because no P2P peers are connected"
                    );
                }
                tracing::warn!(
                    doc_id = %block_result.doc_id,
                    collection = %collection_name,
                    error = %e,
                    attempts = attempt,
                    "Failed to broadcast document to P2P network - local mutation succeeded"
                );
                return BroadcastStatus::Failed(e.to_string());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::broadcast_retry_delay_ms;

    #[test]
    fn insufficient_peers_without_connections_does_not_retry() {
        let delay = broadcast_retry_delay_ms("gossipsub publish error: InsufficientPeers", 0, 1);
        assert_eq!(delay, None);
    }

    #[test]
    fn insufficient_peers_with_connections_retries_with_backoff() {
        let delay = broadcast_retry_delay_ms("gossipsub publish error: InsufficientPeers", 2, 3);
        assert_eq!(delay, Some(800));
    }

    #[test]
    fn non_retryable_broadcast_errors_fail_fast() {
        let delay = broadcast_retry_delay_ms("gossipsub publish error: MessageTooLarge", 2, 1);
        assert_eq!(delay, None);
    }
}
