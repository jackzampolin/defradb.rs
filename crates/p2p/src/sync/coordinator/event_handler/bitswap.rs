//! Bitswap block and completion event handling.

use cid::Cid;

use blockstore::Blockstore;

use super::super::SyncCoordinator;
use crate::error::Result;
use crate::transport::{P2PTransport, PeerId};

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
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
                        let mut providers: Vec<PeerId> = self
                            .access
                            .peer_state
                            .connected_peers()
                            .into_iter()
                            .map(PeerId::new)
                            .collect();
                        if let Some(source) = self.manager.pending_dag_source_peer(&root_cid) {
                            let source_transport_id = PeerId::new(source);
                            if !providers.contains(&source_transport_id) {
                                providers.push(source_transport_id);
                            }
                        }
                        if let Err(e) = self
                            .runtime
                            .transport
                            .sync_blocks(root_cid, providers, missing)
                            .await
                        {
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
