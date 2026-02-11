//! Bitswap block and completion event handling.

use cid::Cid;

use blockstore::Blockstore;

use super::super::SyncCoordinator;
use crate::error::Result;

impl<B: Blockstore + 'static> SyncCoordinator<B> {
    pub(super) async fn handle_bitswap_block_received(
        &self,
        query_id: crate::QueryId,
        cid: Cid,
        data: Vec<u8>,
    ) -> Result<()> {
        tracing::info!(
            query_id = query_id.0,
            cid = %cid,
            data_len = data.len(),
            "Storing Bitswap block in blockstore"
        );

        match self.manager.store_bitswap_block(&cid, &data).await {
            Ok(true) => {
                tracing::debug!(
                    query_id = query_id.0,
                    cid = %cid,
                    "Bitswap block stored successfully"
                );
            }
            Ok(false) => {
                tracing::debug!(
                    query_id = query_id.0,
                    cid = %cid,
                    "Bitswap block was already in blockstore"
                );
            }
            Err(e) => {
                tracing::error!(
                    query_id = query_id.0,
                    cid = %cid,
                    error = %e,
                    "Failed to store Bitswap block"
                );
                return Err(e);
            }
        }
        Ok(())
    }

    pub(super) async fn handle_bitswap_complete(
        &self,
        query_id: crate::QueryId,
        success: bool,
        error: Option<String>,
    ) -> Result<()> {
        tracing::info!(
            query_id = query_id.0,
            success = success,
            error = ?error,
            "Bitswap fetch completed"
        );

        if !success {
            if let Some(ref err) = error {
                tracing::warn!(
                    query_id = query_id.0,
                    error = %err,
                    "Bitswap fetch failed, retrying pending DAGs"
                );
            }
        }

        // Retry ALL pending DAGs on both success AND failure.
        // On success: newly fetched blocks may complete other DAGs.
        // On failure: the timeout ensures we re-issue bitswap_sync with
        //   a fresh session so the retry loop doesn't stall.
        let pending_dags: Vec<Cid> = self.manager.pending_dag_cids();
        for root_cid in pending_dags {
            match self.manager.retry_pending_dag(&root_cid).await {
                Ok(true) => {
                    tracing::info!(
                        query_id = query_id.0,
                        root_cid = %root_cid,
                        "Pending DAG completed after Bitswap fetch"
                    );
                }
                Ok(false) => {
                    let missing = self.manager.pending_dag_missing(&root_cid);
                    if !missing.is_empty() {
                        // Build provider list: connected peers + the original source peer.
                        // The source peer is the one that sent the DocSync/Branchable reply
                        // and definitely has the blocks.
                        let mut providers: Vec<libp2p::PeerId> =
                            self.peer_state.connected_peers().into_iter().collect();
                        if let Some(source) = self.manager.pending_dag_source_peer(&root_cid) {
                            if !providers.contains(&source) {
                                providers.push(source);
                            }
                        }
                        if let Err(e) = self.host.bitswap_sync(root_cid, providers, missing).await {
                            tracing::warn!(
                                root_cid = %root_cid,
                                error = %e,
                                "Failed to start Bitswap fetch for child blocks"
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        query_id = query_id.0,
                        root_cid = %root_cid,
                        error = %e,
                        "Failed to retry pending DAG"
                    );
                }
            }
        }
        Ok(())
    }
}
