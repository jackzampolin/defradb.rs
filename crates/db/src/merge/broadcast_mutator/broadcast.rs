//! Broadcast helper functions for asynchronous P2P propagation.

use blockstore::Blockstore;
use p2p::sync::{BroadcastResult, SyncCoordinator};
use p2p::transport::P2PTransport;
use query::mutator::BroadcastStatus;

use crate::block::builder::BlockResult;

pub(crate) const BROADCAST_MAX_RETRIES: u32 = 10;
const BROADCAST_MAX_RETRY_DELAY_MS: u64 = 10_000;

fn is_expected_fire_and_forget_failure(err: &str) -> bool {
    let lower = err.to_ascii_lowercase();
    lower.contains("insufficientpeers")
        || lower.contains("channel send error")
        || lower.contains("channel receive error")
        || p2p::error::Error::is_connection_loss_reason(&lower)
}

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
    let base_delay_ms = 100 * (1u64 << attempt.min(5));
    let peer_multiplier = match connected_peers {
        1 => 4,
        2 => 2,
        _ => 1,
    };
    Some(
        base_delay_ms
            .saturating_mul(peer_multiplier)
            .min(BROADCAST_MAX_RETRY_DELAY_MS),
    )
}

async fn connected_peer_count<B: Blockstore + 'static, T: P2PTransport>(
    sync: &SyncCoordinator<B, T>,
) -> usize {
    sync.transport()
        .connected_peers()
        .await
        .map(|peers| peers.len())
        .unwrap_or_else(|_| sync.peer_state().stats().connected_peers())
}

/// Log broadcast failures for asynchronous propagation paths.
pub(crate) fn log_broadcast_failure(status: &BroadcastStatus) {
    if let BroadcastStatus::Failed(err) = status {
        if is_expected_fire_and_forget_failure(err) {
            tracing::warn!(
                error = %err,
                "Fire-and-forget broadcast could not reach peers; document committed locally but was not replicated"
            );
        } else {
            tracing::error!(
                error = %err,
                "Fire-and-forget broadcast failed — document committed locally but NOT replicated"
            );
        }
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
                let connected_peers = connected_peer_count(sync).await;
                if let Some(delay_ms) =
                    broadcast_retry_delay_ms(&collection_error, connected_peers, attempt)
                {
                    tracing::trace!(
                        doc_id = %block_result.doc_id,
                        attempt = attempt,
                        connected_peers = connected_peers,
                        delay_ms = delay_ms,
                        "Retrying broadcast after collection-topic InsufficientPeers"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    continue;
                }
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
                // Per-doc topics only have subscribers when a client explicitly
                // calls subscribe_document. For the common batch-create path
                // nobody is watching the doc-topic, and gossipsub returns
                // InsufficientPeers. Go's PublishToTopicAsync silently skips
                // when the topic has no local subscription, so for parity we
                // treat this as success: the collection-topic delivered to
                // subscribed replicators, which is the meaningful path.
                if document_error.contains("InsufficientPeers") {
                    tracing::debug!(
                        doc_id = %block_result.doc_id,
                        cid = %block_result.cid,
                        collection = %collection_name,
                        attempts = attempt,
                        "Doc-topic has no subscribers; broadcast delivered via collection topic"
                    );
                    return BroadcastStatus::Success;
                }
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
                let connected_peers = connected_peer_count(sync).await;
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
    fn insufficient_peers_with_two_connections_uses_peer_aware_backoff() {
        let delay = broadcast_retry_delay_ms("gossipsub publish error: InsufficientPeers", 2, 3);
        assert_eq!(delay, Some(1600));
    }

    #[test]
    fn insufficient_peers_with_one_connection_waits_longer_than_many_peers() {
        let sparse_delay =
            broadcast_retry_delay_ms("gossipsub publish error: InsufficientPeers", 1, 3).unwrap();
        let many_peer_delay =
            broadcast_retry_delay_ms("gossipsub publish error: InsufficientPeers", 8, 3).unwrap();

        assert!(sparse_delay > many_peer_delay);
        assert_eq!(many_peer_delay, 800);
    }

    #[test]
    fn insufficient_peers_retry_delay_is_capped() {
        let delay = broadcast_retry_delay_ms("gossipsub publish error: InsufficientPeers", 1, 10);
        assert_eq!(delay, Some(10_000));
    }

    #[test]
    fn non_retryable_broadcast_errors_fail_fast() {
        let delay = broadcast_retry_delay_ms("gossipsub publish error: MessageTooLarge", 2, 1);
        assert_eq!(delay, None);
    }
}
