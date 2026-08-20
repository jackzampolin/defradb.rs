//! DocSync request and reply handling.

use cid::Cid;
use futures::future::join_all;

use blockstore::Blockstore;

use super::super::authorizer::AccessAuthorizer;
use super::super::dag_context::{block_context_from_data, DagFetchContext};
use super::super::SyncCoordinator;
use crate::error::{Error, Result};
use crate::message::{DocSyncItem, DocSyncReply};
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

    async fn filter_authorized_doc_heads(
        &self,
        peer_id: &PeerId,
        doc_id: &str,
        heads: Vec<Cid>,
    ) -> Vec<Cid> {
        if self.access.access_mode.is_open() {
            return heads;
        }

        let mut authorized = Vec::with_capacity(heads.len());
        for cid in heads {
            let block_data = match self.manager.blockstore().get(&cid).await {
                Ok(Some(data)) => data,
                Ok(None) => {
                    tracing::debug!(
                        peer_id = %peer_id,
                        doc_id = %doc_id,
                        cid = %cid,
                        "Skipping DocSync head with missing local block during access check"
                    );
                    continue;
                }
                Err(error) => {
                    tracing::debug!(
                        peer_id = %peer_id,
                        doc_id = %doc_id,
                        cid = %cid,
                        error = %error,
                        "Skipping DocSync head after blockstore access-check failure"
                    );
                    continue;
                }
            };

            let Some(collection_id) = block_context_from_data(&block_data).collection_id else {
                tracing::debug!(
                    peer_id = %peer_id,
                    doc_id = %doc_id,
                    cid = %cid,
                    "Skipping DocSync head without collection context in Controlled mode"
                );
                continue;
            };

            let is_collection_replicator = self
                .authorizer
                .peer_authorized_for_collection(peer_id.as_str(), &collection_id)
                .await;
            let is_collection_subscriber = self
                .access
                .peer_state
                .peer_subscribed_to_collection(peer_id.as_str(), &collection_id);
            let is_connected_peer = self.access.peer_state.is_connected(peer_id.as_str());

            if is_connected_peer || is_collection_replicator || is_collection_subscriber {
                authorized.push(cid);
            } else {
                tracing::debug!(
                    peer_id = %peer_id,
                    doc_id = %doc_id,
                    cid = %cid,
                    collection_id = %collection_id,
                    is_connected_peer,
                    is_collection_replicator,
                    is_collection_subscriber,
                    "Skipping DocSync head for unauthorized collection"
                );
            }
        }

        authorized
    }

    pub(super) async fn handle_doc_sync_request(
        &self,
        peer_id: PeerId,
        request: crate::message::DocSyncRequest,
        token: Option<T::ResponseToken>,
    ) -> Result<()> {
        self.check_peer_is_replicator(&peer_id).await?;

        let max_doc_ids = self.runtime.max_doc_sync_request_doc_ids;
        if request.doc_ids.len() > max_doc_ids {
            tracing::warn!(
                peer_id = %peer_id,
                doc_ids_count = request.doc_ids.len(),
                max = max_doc_ids,
                "DocSyncRequest exceeds configured doc ID limit, rejecting"
            );
            return Err(Error::InvalidConfig(format!(
                "DocSyncRequest contains {} doc IDs, exceeding the limit of {}",
                request.doc_ids.len(),
                max_doc_ids,
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
                    let heads = self
                        .filter_authorized_doc_heads(&peer_id, doc_id, heads)
                        .await;
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
        // Error replies (backpressure nacks, access denials) carry no results;
        // without this branch they would read as an empty successful sync.
        // DocSync is a pull — the peer discarded no state on rejection — so
        // surfacing the error is the correct terminal handling here: the next
        // sync trigger (subscription event, reconnect) re-requests. A
        // puller-side backoff ladder is #1088 follow-up scope.
        if let Some(err) = reply.err_message.as_deref() {
            if crate::error::is_rate_limited_message(err) {
                tracing::warn!(
                    peer_id = %peer_id,
                    message_id = %reply.message_id,
                    "Peer rate-limited our DocSync request; will re-request on the next sync trigger"
                );
            } else {
                tracing::warn!(
                    peer_id = %peer_id,
                    message_id = %reply.message_id,
                    error = %err,
                    "DocSync request rejected by peer"
                );
            }
            return Err(Error::Transport(format!(
                "DocSync request rejected by peer {peer_id}: {err}"
            )));
        }

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
                            let mut context = DagFetchContext::new(
                                doc_id.clone(),
                                String::new(),
                                String::new(),
                                peer_id.clone(),
                            )
                            .with_explicit_replicator_collections(
                                self.access.replicators.get_collections(peer_id.as_str()),
                            );
                            context.fill_missing_from_block(&data);
                            tracing::info!(
                                cid = %cid,
                                doc_id = %doc_id,
                                "DAG complete locally, emitting BlockReceived for re-merge"
                            );
                            if event_tx
                                .send(crate::sync::manager::SyncEvent::BlockReceived {
                                    cid,
                                    doc_id: context.doc_id,
                                    collection_id: context.collection_id,
                                    creator: context.creator,
                                    sender_peer: Some(context.source_peer.to_string()),
                                    is_explicit_replicator: context.is_explicit_replicator,
                                    explicit_replay_authorization: None,
                                })
                                .await
                                .is_err()
                            {
                                tracing::error!(
                                    cid = %cid,
                                    doc_id = %doc_id,
                                    "Failed to emit BlockReceived for locally complete DocSync DAG"
                                );
                                return Err(Error::ChannelSend);
                            }
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
            let limiter = self.runtime.dag_fetch_limiter.clone();
            let diagnostics = self.manager.diagnostics();
            let source_peer = peer_id.clone();
            let explicit_replicator_collections =
                self.access.replicators.get_collections(peer_id.as_str());

            self.spawn_background_task("doc_sync_reply_fetch_dags", async move {
                join_all(cids_to_fetch.into_iter().map(|(root_cid, doc_id)| {
                    let transport = transport.clone();
                    let blockstore = blockstore.clone();
                    let event_tx = event_tx.clone();
                    let source_peer = source_peer.clone();
                    let explicit_replicator_collections = explicit_replicator_collections.clone();
                    let limiter = limiter.clone();
                    let diagnostics = diagnostics.clone();

                    async move {
                        tracing::debug!(
                            cid = %root_cid,
                            doc_id = %doc_id,
                            "Fetching DocSync DAG"
                        );
                        let alternate_providers =
                            super::super::dag_fetcher::connected_alternate_providers(
                                &transport, &root_cid,
                            )
                            .await;
                        super::super::dag_fetcher::poll_fetch_dag(
                            transport,
                            blockstore,
                            event_tx,
                            root_cid,
                            DagFetchContext::new(doc_id, String::new(), String::new(), source_peer)
                                .with_alternate_providers(alternate_providers)
                                .with_explicit_replicator_collections(
                                    explicit_replicator_collections,
                                ),
                            limiter,
                            diagnostics,
                        )
                        .await;
                    }
                }))
                .await;
            });
        } else {
            tracing::debug!("No blocks to fetch from DocSync reply (all local)");
        }
        Ok(())
    }
}
