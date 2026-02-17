//! Poll-based DAG fetcher for DocSync and BranchableSync.
//!
//! Tries CAR fetch first (single round-trip for entire DAG), then falls back
//! to bitswap_sync + blockstore polling for any remaining blocks.

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

/// Fetch an entire DAG rooted at `root_cid`.
///
/// Strategy: try CAR fetch first (one round-trip), then Bitswap for any missing blocks.
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
        "Starting DAG fetch (CAR-first, Bitswap fallback)"
    );

    // Try CAR fetch first — single round-trip for the entire DAG
    if try_car_fetch(&host, &blockstore, &root_cid, source_peer).await {
        // Verify completeness
        if let Ok(Some(root_data)) = blockstore.get(&root_cid).await {
            let missing = find_all_missing_links(blockstore.as_ref(), &root_data)
                .await
                .unwrap_or_default();
            if missing.is_empty() {
                info!(root_cid = %root_cid, doc_id = %doc_id, "DAG fetch complete via CAR");
                let _ = event_tx
                    .send(SyncEvent::DagReady {
                        root_cid,
                        doc_id,
                        collection_id,
                        schema_version_id,
                    })
                    .await;
                return;
            }
            debug!(
                root_cid = %root_cid,
                missing_count = missing.len(),
                "CAR fetch was partial, falling through to Bitswap"
            );
        }
    }

    // Bitswap fallback: fetch root block
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
            "Fetching missing DAG blocks via Bitswap"
        );

        let fetches: Vec<_> = missing
            .iter()
            .map(|cid| poll_fetch_block(cid, &host, &blockstore, source_peer))
            .collect();
        let results = futures::future::join_all(fetches).await;
        for (cid, success) in missing.iter().zip(results.iter()) {
            if !*success {
                warn!(cid = %cid, root_cid = %root_cid, "Timeout fetching child block (30s)");
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

/// Try to fetch an entire DAG via a single CAR request.
///
/// Sends a CAR request to the source peer, waits for the response
/// (which arrives via the event pipeline and is stored by the coordinator),
/// then checks if the root block appeared in the blockstore.
async fn try_car_fetch<B: Blockstore>(
    host: &P2PHostHandle,
    blockstore: &Arc<B>,
    root_cid: &Cid,
    source_peer: PeerId,
) -> bool {
    if let Err(e) = host.send_car_request(source_peer, *root_cid).await {
        debug!(root_cid = %root_cid, error = %e, "CAR request failed, will use Bitswap");
        return false;
    }

    // Poll blockstore for the root block (CAR response is stored by coordinator)
    let timeout = Duration::from_secs(10);
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if let Ok(true) = blockstore.has(root_cid).await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    debug!(root_cid = %root_cid, "CAR fetch timed out (10s), falling back to Bitswap");
    false
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
