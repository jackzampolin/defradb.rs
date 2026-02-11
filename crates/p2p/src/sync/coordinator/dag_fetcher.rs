//! Poll-based DAG fetcher for DocSync and BranchableSync.
//!
//! Uses bitswap_sync + blockstore polling instead of the event-driven
//! pending DAG + BitswapComplete + retry mechanism. The poll-based approach
//! is more reliable for multi-level DAG fetching.

use std::sync::Arc;
use std::time::Duration;

use blockstore::Blockstore;
use cid::Cid;
use libp2p::PeerId;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::host::P2PHostHandle;
use crate::sync::manager::links::find_all_missing_links;
use crate::sync::manager::SyncEvent;

/// Fetch an entire DAG rooted at `root_cid` using poll-based Bitswap.
///
/// Walks the DAG level by level, fetching missing blocks via Bitswap
/// and polling the blockstore until they appear. Emits DagReady when complete.
#[allow(clippy::too_many_arguments)]
pub async fn poll_fetch_dag<B: Blockstore + 'static>(
    host: P2PHostHandle,
    blockstore: Arc<B>,
    event_tx: mpsc::Sender<SyncEvent>,
    root_cid: Cid,
    doc_id: String,
    collection_id: String,
    schema_version_id: String,
    source_peer: PeerId,
) {
    debug!(
        root_cid = %root_cid,
        doc_id = %doc_id,
        source_peer = %source_peer,
        "Starting poll-based DAG fetch"
    );

    // Fetch root block
    if !poll_fetch_block(&root_cid, &host, &blockstore, source_peer).await {
        warn!(root_cid = %root_cid, "Failed to fetch root block");
        return;
    }

    // Walk DAG, fetching missing blocks level by level
    for iteration in 0..20 {
        let root_data = match blockstore.get(&root_cid).await {
            Ok(Some(data)) => data,
            _ => {
                warn!(root_cid = %root_cid, "Root block disappeared from blockstore");
                return;
            }
        };

        let missing = match find_all_missing_links(blockstore.as_ref(), &root_data).await {
            Ok(m) => m,
            Err(e) => {
                warn!(root_cid = %root_cid, error = %e, "find_all_missing_links failed");
                return;
            }
        };

        if missing.is_empty() {
            break;
        }

        debug!(
            root_cid = %root_cid,
            iteration = iteration,
            missing_count = missing.len(),
            "Fetching missing DAG blocks"
        );

        for cid in &missing {
            if !poll_fetch_block(cid, &host, &blockstore, source_peer).await {
                warn!(
                    cid = %cid,
                    root_cid = %root_cid,
                    "Timeout fetching child block (30s)"
                );
            }
        }
    }

    // Verify DAG is complete
    let root_data = match blockstore.get(&root_cid).await {
        Ok(Some(data)) => data,
        _ => return,
    };
    let remaining = find_all_missing_links(blockstore.as_ref(), &root_data)
        .await
        .unwrap_or_default();

    if remaining.is_empty() {
        info!(root_cid = %root_cid, doc_id = %doc_id, "DAG fetch complete");
        let _ = event_tx
            .send(SyncEvent::DagReady {
                root_cid,
                doc_id,
                collection_id,
                schema_version_id,
            })
            .await;
    } else {
        warn!(
            root_cid = %root_cid,
            doc_id = %doc_id,
            remaining_count = remaining.len(),
            "DAG fetch incomplete"
        );
    }
}

/// Fetch a single block via Bitswap + blockstore polling.
async fn poll_fetch_block<B: Blockstore>(
    cid: &Cid,
    host: &P2PHostHandle,
    blockstore: &Arc<B>,
    source_peer: PeerId,
) -> bool {
    // Already have it?
    if let Ok(true) = blockstore.has(cid).await {
        return true;
    }

    // Start Bitswap fetch
    if let Err(e) = host.bitswap_sync(*cid, vec![source_peer], vec![*cid]).await {
        warn!(cid = %cid, error = %e, "bitswap_sync failed");
        return false;
    }

    // Poll blockstore for up to 30 seconds
    let timeout = Duration::from_secs(30);
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if let Ok(true) = blockstore.has(cid).await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    false
}
