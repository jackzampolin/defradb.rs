//! Bitswap sync and cancel command handling.

use cid::Cid;
use iroh_bitswap::Store;
use libp2p::PeerId;
use tracing::{debug, info};

use crate::error::Result;
use crate::host::event::HostEvent;
use crate::QueryId;

use super::super::p2p_host::P2PHost;

impl<S: Store> P2PHost<S> {
    /// Handle a Bitswap sync command.
    pub(super) async fn handle_bitswap_sync(
        &mut self,
        cid: Cid,
        providers: Vec<PeerId>,
        missing: Vec<Cid>,
        response: tokio::sync::oneshot::Sender<Result<QueryId>>,
    ) {
        // Generate a query ID for tracking
        static QUERY_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let query_id = QueryId(QUERY_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed));

        info!(
            cid = %cid,
            providers = ?providers,
            missing_count = missing.len(),
            query_id = query_id.0,
            "Starting Bitswap block fetch via Client API"
        );
        eprintln!(
            "[BITSWAP-SYNC] Starting fetch cid={} providers={:?} missing={}",
            cid,
            providers,
            missing.len()
        );

        // Clone the client for use in the spawned task
        let client = self.swarm.behaviour().bitswap.client().clone();
        let event_tx = self.event_tx.clone();
        let missing_cids: Vec<Cid> = missing;
        let providers_list = providers;

        // Spawn async task to fetch blocks (with cancellation support)
        let task_handle = tokio::spawn(async move {
            // Create a session and add providers for each CID
            let session = client.new_session().await;

            // Add each provider for each missing CID
            for cid in &missing_cids {
                for provider in &providers_list {
                    session.add_provider(cid, *provider).await;
                }
            }

            eprintln!(
                "[BITSWAP-SYNC] Session created, providers added, calling get_blocks for {} CIDs",
                missing_cids.len()
            );
            match session.get_blocks(&missing_cids).await {
                Ok(receiver) => {
                    eprintln!("[BITSWAP-SYNC] get_blocks returned receiver, waiting for blocks...");
                    // Use into_parts() to get the underlying channel
                    // BlockReceiver only implements Deref (not DerefMut), so we can't call recv() through it
                    let (chan, _guard) = receiver.into_parts();
                    let mut fetched = 0;

                    // Timeout per block: 10 seconds. If no block arrives within this
                    // window, we give up and report partial success so the retry
                    // mechanism can try again with fresh provider info.
                    let per_block_timeout = std::time::Duration::from_secs(10);

                    loop {
                        match tokio::time::timeout(per_block_timeout, chan.recv()).await {
                            Ok(Ok(block)) => {
                                fetched += 1;
                                let block_cid = *block.cid();
                                let block_data = block.data().to_vec();

                                // Send block to coordinator for storage
                                if let Err(e) = event_tx
                                    .send(HostEvent::BitswapBlockReceived {
                                        query_id,
                                        cid: block_cid,
                                        data: block_data,
                                    })
                                    .await
                                {
                                    tracing::warn!(
                                        query_id = query_id.0,
                                        error = %e,
                                        "Failed to send BitswapBlockReceived event"
                                    );
                                }

                                if fetched == missing_cids.len() {
                                    break;
                                }
                            }
                            Ok(Err(_)) => {
                                // Channel closed — no more blocks coming
                                eprintln!("[BITSWAP-SYNC] Channel closed, no more blocks");
                                break;
                            }
                            Err(_) => {
                                // Timeout — no block arrived within the window
                                eprintln!(
                                    "[BITSWAP-SYNC] TIMEOUT waiting for block ({} fetched of {})",
                                    fetched,
                                    missing_cids.len()
                                );
                                break;
                            }
                        }
                    }

                    let success = fetched == missing_cids.len();
                    info!(
                        query_id = query_id.0,
                        fetched = fetched,
                        total = missing_cids.len(),
                        success = success,
                        "Bitswap fetch complete"
                    );

                    // Notify completion
                    let _ = event_tx
                        .send(HostEvent::BitswapComplete {
                            query_id,
                            success,
                            error: if success {
                                None
                            } else {
                                Some(format!(
                                    "Only fetched {} of {} blocks",
                                    fetched,
                                    missing_cids.len()
                                ))
                            },
                        })
                        .await;
                }
                Err(e) => {
                    tracing::error!(
                        query_id = query_id.0,
                        error = %e,
                        "Bitswap get_blocks failed"
                    );
                    let _ = event_tx
                        .send(HostEvent::BitswapComplete {
                            query_id,
                            success: false,
                            error: Some(e.to_string()),
                        })
                        .await;
                }
            }
        });

        // Store the abort handle for cancellation support
        self.bitswap_queries
            .insert(query_id, task_handle.abort_handle());

        if response.send(Ok(query_id)).is_err() {
            debug!(cid = %cid, "BitswapSync command response dropped - caller cancelled");
        }
    }

    pub(super) fn handle_bitswap_cancel(
        &mut self,
        query_id: QueryId,
        response: tokio::sync::oneshot::Sender<bool>,
    ) {
        let cancelled = if let Some(abort_handle) = self.bitswap_queries.remove(&query_id) {
            debug!(query_id = ?query_id, "Cancelling Bitswap query");
            abort_handle.abort();
            true
        } else {
            debug!(query_id = ?query_id, "Bitswap query not found for cancellation");
            false
        };
        if response.send(cancelled).is_err() {
            debug!(query_id = ?query_id, "BitswapCancel command response dropped - caller cancelled");
        }
    }
}
