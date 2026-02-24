//! DocSync request and reply handling.

use cid::Cid;

use blockstore::Blockstore;

use super::super::SyncCoordinator;
use crate::error::{Error, Result};
use crate::message::{DocSyncItem, DocSyncReply, MAX_DOC_IDS};
use crate::signing::sign_with_transport;
use crate::transport::{P2PTransport, PeerId};

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    pub(super) async fn handle_doc_sync_request(
        &self,
        peer_id: PeerId,
        request: crate::message::DocSyncRequest,
    ) -> Result<()> {
        self.check_peer_is_replicator(&peer_id)?;

        if request.doc_ids.len() > MAX_DOC_IDS {
            tracing::warn!(
                peer_id = %peer_id,
                doc_ids_count = request.doc_ids.len(),
                max = MAX_DOC_IDS,
                "DocSyncRequest exceeds MAX_DOC_IDS limit, rejecting"
            );
            return Err(Error::InvalidConfig(format!(
                "DocSyncRequest contains {} doc IDs, exceeding the limit of {}",
                request.doc_ids.len(),
                MAX_DOC_IDS,
            )));
        }

        tracing::debug!(
            peer_id = %peer_id,
            doc_ids = ?request.doc_ids,
            message_id = %request.metadata.message_id,
            "Received DocSync request"
        );

        let mut results: Vec<DocSyncItem> = Vec::new();
        for doc_id in &request.doc_ids {
            tracing::trace!(doc_id = %doc_id, "Looking up heads for document");
            match self.head_provider.get_document_heads(doc_id).await {
                Ok(heads) => {
                    tracing::debug!(
                        doc_id = %doc_id,
                        head_count = heads.len(),
                        "Found document heads for DocSync response"
                    );
                    if !heads.is_empty() {
                        results.push(DocSyncItem {
                            doc_id: doc_id.clone(),
                            heads: heads.iter().map(|cid| cid.to_bytes()).collect(),
                        });
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        doc_id = %doc_id,
                        error = %e,
                        "Failed to get document heads for DocSync"
                    );
                }
            }
        }

        tracing::debug!(
            peer_id = %peer_id,
            result_count = results.len(),
            "Sending DocSync response"
        );

        let mut reply = DocSyncReply::success(&request.metadata.message_id, results);

        if let Err(e) = sign_with_transport(&self.transport, &mut reply) {
            tracing::error!(
                peer_id = %peer_id,
                error = %e,
                "Failed to sign DocSync response"
            );
            return Err(e);
        }

        if let Err(e) = self.transport.send_doc_sync_response(&peer_id, reply).await {
            tracing::warn!(
                peer_id = %peer_id,
                error = %e,
                "Failed to send DocSync response"
            );
        } else {
            tracing::debug!(
                peer_id = %peer_id,
                "Sent DocSync response"
            );
        }
        Ok(())
    }

    pub(super) async fn handle_doc_sync_reply(
        &self,
        peer_id: PeerId,
        reply: DocSyncReply,
    ) -> Result<()> {
        tracing::info!(
            peer_id = %peer_id,
            message_id = %reply.message_id,
            results_count = reply.results.len(),
            "Processing DocSync reply"
        );
        let mut cids_to_fetch: Vec<(Cid, String)> = Vec::new();
        for item in &reply.results {
            for head_bytes in &item.heads {
                match Cid::try_from(head_bytes.as_slice()) {
                    Ok(cid) => match self.manager.blockstore().has(&cid).await {
                        Ok(true) => {
                            tracing::debug!(
                                cid = %cid,
                                doc_id = %item.doc_id,
                                "Already have block, skipping fetch"
                            );
                        }
                        Ok(false) => {
                            tracing::debug!(
                                cid = %cid,
                                doc_id = %item.doc_id,
                                "Need to fetch block via Bitswap"
                            );
                            cids_to_fetch.push((cid, item.doc_id.clone()));
                        }
                        Err(e) => {
                            tracing::warn!(
                                cid = %cid,
                                error = %e,
                                "Failed to check if block exists"
                            );
                            cids_to_fetch.push((cid, item.doc_id.clone()));
                        }
                    },
                    Err(e) => {
                        tracing::warn!(
                            doc_id = %item.doc_id,
                            error = %e,
                            "Failed to parse CID from DocSync reply"
                        );
                    }
                }
            }
        }

        if !cids_to_fetch.is_empty() {
            tracing::info!(
                cid_count = cids_to_fetch.len(),
                "Spawning poll-based DAG fetchers for DocSync blocks"
            );

            let transport = self.transport.clone();
            let blockstore = self.manager.blockstore().clone();
            let event_tx = self.manager.event_sender();
            let semaphore = self.dag_fetch_semaphore.clone();

            for (root_cid, doc_id) in cids_to_fetch {
                tracing::debug!(
                    cid = %root_cid,
                    doc_id = %doc_id,
                    "Spawning poll-based DAG fetcher for DocSync"
                );

                let transport = transport.clone();
                let blockstore = blockstore.clone();
                let event_tx = event_tx.clone();
                let semaphore = semaphore.clone();
                let source_peer = peer_id.clone();

                tokio::spawn(async move {
                    let _permit = semaphore.acquire_owned().await;
                    super::super::dag_fetcher::poll_fetch_dag(
                        transport,
                        blockstore,
                        event_tx,
                        root_cid,
                        doc_id,
                        String::new(),
                        String::new(),
                        source_peer,
                    )
                    .await;
                });
            }
        } else {
            tracing::debug!("No blocks to fetch from DocSync reply (all local)");
        }
        Ok(())
    }
}
