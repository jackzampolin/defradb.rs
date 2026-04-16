//! DocSync request and reply handling.

use cid::Cid;

use blockstore::Blockstore;

use super::super::SyncCoordinator;
use crate::error::{Error, Result};
use crate::message::{DocSyncItem, DocSyncReply, MAX_DOC_IDS};
use crate::signing::sign_with_transport;
use crate::transport::{P2PTransport, PeerId};

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    async fn send_doc_sync_reply(
        &self,
        peer_id: &PeerId,
        token: Option<T::ResponseToken>,
        mut reply: DocSyncReply,
    ) -> Result<()> {
        sign_with_transport(&self.runtime.transport, &mut reply)?;

        let send_result = if let Some(token) = token {
            self.runtime
                .transport
                .send_doc_sync_response_token(token, reply)
                .await
        } else {
            self.runtime
                .transport
                .send_doc_sync_response(peer_id, reply)
                .await
        };

        if let Err(e) = send_result {
            tracing::warn!(
                peer_id = %peer_id,
                error = %e,
                "Failed to send DocSync response"
            );
        } else {
            tracing::debug!(peer_id = %peer_id, "Sent DocSync response");
        }

        Ok(())
    }

    pub(super) async fn handle_doc_sync_request(
        &self,
        peer_id: PeerId,
        request: crate::message::DocSyncRequest,
        token: Option<T::ResponseToken>,
    ) -> Result<()> {
        self.check_peer_is_replicator(&peer_id).await?;

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
            message_id = %request.message_id,
            "Received DocSync request"
        );

        let mut results: Vec<DocSyncItem> = Vec::new();
        for doc_id in &request.doc_ids {
            tracing::trace!(doc_id = %doc_id, "Looking up heads for document");
            match self
                .subscriptions
                .head_provider
                .get_document_heads(doc_id)
                .await
            {
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

        if let Err(e) = self
            .send_doc_sync_reply(
                &peer_id,
                token,
                DocSyncReply::success(&request.message_id, results),
            )
            .await
        {
            tracing::debug!(
                peer_id = %peer_id,
                error = %e,
                "Failed to sign DocSync response"
            );
            return Err(e);
        }

        Ok(())
    }

    pub(in crate::sync::coordinator) async fn handle_doc_sync_reply(
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
        let mut cids_to_remerge: Vec<(Cid, String)> = Vec::new();
        for item in &reply.results {
            for head_bytes in &item.heads {
                match Cid::try_from(head_bytes.as_slice()) {
                    Ok(cid) => match self.manager.blockstore().has(&cid).await {
                        Ok(true) => {
                            // Block exists locally — but it may not have been merged.
                            // Check merge status and re-trigger merge if needed,
                            // otherwise stranded docs remain invisible to queries.
                            match self.manager.blockstore().is_merged(&cid).await {
                                Ok(true) => {
                                    tracing::debug!(
                                        cid = %cid,
                                        doc_id = %item.doc_id,
                                        "Already have and merged block, skipping"
                                    );
                                }
                                _ => {
                                    tracing::info!(
                                        cid = %cid,
                                        doc_id = %item.doc_id,
                                        "Block exists but not merged, scheduling re-merge"
                                    );
                                    cids_to_remerge.push((cid, item.doc_id.clone()));
                                }
                            }
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

        // Re-merge locally-present but unmerged blocks. Check if the full DAG
        // is available; if so, emit BlockReceived directly. If the DAG is
        // incomplete, fall through to the DAG fetcher so missing blocks are
        // retrieved from the source peer.
        if !cids_to_remerge.is_empty() {
            let event_tx = self.manager.event_sender();
            for (cid, doc_id) in cids_to_remerge {
                match self.manager.blockstore().get(&cid).await {
                    Ok(Some(data)) => {
                        let missing = crate::sync::manager::links::find_all_missing_links(
                            self.manager.blockstore().as_ref(),
                            &data,
                        )
                        .await
                        .unwrap_or_default();

                        if missing.is_empty() {
                            tracing::info!(
                                cid = %cid,
                                doc_id = %doc_id,
                                "DAG complete locally, emitting BlockReceived for re-merge"
                            );
                            let _ = event_tx
                                .send(crate::sync::manager::SyncEvent::BlockReceived {
                                    cid,
                                    doc_id: doc_id.clone(),
                                    collection_id: String::new(),
                                    creator: String::new(),
                                    sender_peer: Some(peer_id.to_string()),
                                    is_explicit_replicator: false,
                                    explicit_replay_authorization: None,
                                    acp_actor_relationships: None,
                                })
                                .await;
                        } else {
                            tracing::info!(
                                cid = %cid,
                                doc_id = %doc_id,
                                missing_count = missing.len(),
                                "Unmerged block has incomplete DAG, adding to fetch list"
                            );
                            cids_to_fetch.push((cid, doc_id));
                        }
                    }
                    _ => {
                        cids_to_fetch.push((cid, doc_id));
                    }
                }
            }
        }

        if !cids_to_fetch.is_empty() {
            tracing::info!(
                cid_count = cids_to_fetch.len(),
                "Spawning poll-based DAG fetchers for DocSync blocks"
            );

            let transport = self.runtime.transport.clone();
            let blockstore = self.manager.blockstore().clone();
            let event_tx = self.manager.event_sender();
            let semaphore = self.runtime.dag_fetch_semaphore.clone();

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

                self.spawn_background_task("doc_sync_reply_fetch_dag", async move {
                    let Ok(_permit) = semaphore.acquire_owned().await else {
                        return;
                    };
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
