//! BranchableSync request and reply handling.

use cid::Cid;

use blockstore::Blockstore;

use super::super::SyncCoordinator;
use crate::error::Result;
use crate::message::BranchableSyncReply;
use crate::signing::sign_with_transport;
use crate::transport::{P2PTransport, PeerId};

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    pub(super) async fn handle_branchable_sync_request(
        &self,
        peer_id: PeerId,
        request: crate::message::BranchableSyncRequest,
        token: Option<T::ResponseToken>,
    ) -> Result<()> {
        self.check_access_str(peer_id.as_str(), &request.collection_id)
            .await?;

        tracing::debug!(
            peer_id = %peer_id,
            collection_id = %request.collection_id,
            "Received BranchableSync request"
        );

        let heads = match self
            .head_provider
            .get_collection_heads(&request.collection_id)
            .await
        {
            Ok(heads) => {
                tracing::debug!(
                    collection_id = %request.collection_id,
                    head_count = heads.len(),
                    "Found collection heads for BranchableSync response"
                );
                heads.iter().map(|cid| cid.to_bytes()).collect()
            }
            Err(e) => {
                tracing::warn!(
                    collection_id = %request.collection_id,
                    error = %e,
                    "Failed to get collection heads"
                );
                Vec::new()
            }
        };

        let mut reply = BranchableSyncReply::success(
            &request.metadata.message_id,
            &request.collection_id,
            heads,
        );

        if let Err(e) = sign_with_transport(&self.transport, &mut reply) {
            tracing::error!(
                peer_id = %peer_id,
                error = %e,
                "Failed to sign BranchableSync response"
            );
            return Err(e);
        }

        let send_result = if let Some(token) = token {
            self.transport
                .send_branchable_sync_response_token(token, reply)
                .await
        } else {
            self.transport
                .send_branchable_sync_response(&peer_id, reply)
                .await
        };
        if let Err(e) = send_result {
            tracing::warn!(
                peer_id = %peer_id,
                error = %e,
                "Failed to send BranchableSync response"
            );
        }
        Ok(())
    }

    pub(super) async fn handle_branchable_sync_reply(
        &self,
        peer_id: PeerId,
        reply: crate::message::BranchableSyncReply,
    ) -> Result<()> {
        tracing::debug!(
            peer_id = %peer_id,
            collection_id = %reply.collection_id,
            head_count = reply.heads.len(),
            "Received BranchableSync reply"
        );

        if reply.heads.is_empty() {
            tracing::debug!(
                collection_id = %reply.collection_id,
                "Peer has no heads for collection"
            );
            return Ok(());
        }

        let mut cids_to_fetch: Vec<Cid> = Vec::new();
        for head_bytes in &reply.heads {
            match Cid::try_from(head_bytes.as_slice()) {
                Ok(cid) => {
                    tracing::trace!(cid = %cid, "Parsed collection head CID");
                    match self.manager.blockstore().has(&cid).await {
                        Ok(true) => {
                            tracing::debug!(cid = %cid, "Already have block");
                        }
                        Ok(false) => {
                            tracing::debug!(cid = %cid, "Need to fetch block");
                            cids_to_fetch.push(cid);
                        }
                        Err(e) => {
                            tracing::warn!(cid = %cid, error = %e, "Error checking block");
                            cids_to_fetch.push(cid);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to parse CID from BranchableSync reply");
                }
            }
        }

        if !cids_to_fetch.is_empty() {
            tracing::info!(
                collection_id = %reply.collection_id,
                cid_count = cids_to_fetch.len(),
                "Spawning poll-based DAG fetchers for collection blocks"
            );

            let transport = self.transport.clone();
            let blockstore = self.manager.blockstore().clone();
            let event_tx = self.manager.event_sender();
            let semaphore = self.dag_fetch_semaphore.clone();

            for root_cid in cids_to_fetch {
                let transport = transport.clone();
                let blockstore = blockstore.clone();
                let event_tx = event_tx.clone();
                let collection_id = reply.collection_id.clone();
                let semaphore = semaphore.clone();
                let source_peer = peer_id.clone();

                tokio::spawn(async move {
                    let _permit = semaphore.acquire_owned().await;
                    super::super::dag_fetcher::poll_fetch_dag(
                        transport,
                        blockstore,
                        event_tx,
                        root_cid,
                        String::new(),
                        collection_id,
                        String::new(),
                        source_peer,
                    )
                    .await;
                });
            }
        } else {
            tracing::debug!(
                collection_id = %reply.collection_id,
                "All blocks already local for collection"
            );
        }
        Ok(())
    }
}
