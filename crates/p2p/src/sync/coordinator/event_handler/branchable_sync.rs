//! BranchableSync request and reply handling.

use cid::Cid;

use blockstore::Blockstore;

use super::super::dag_context::DagFetchContext;
use super::super::SyncCoordinator;
use crate::error::Result;
use crate::message::BranchableSyncReply;
use crate::signing::sign_with_transport;
use crate::sync::manager::links::find_all_missing_links;
use crate::transport::{P2PTransport, PeerId};

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    async fn send_branchable_sync_reply(
        &self,
        peer_id: &PeerId,
        token: Option<T::ResponseToken>,
        mut reply: BranchableSyncReply,
    ) -> Result<()> {
        sign_with_transport(&self.runtime.transport, &mut reply)?;

        let send_result = if let Some(token) = token {
            self.runtime
                .transport
                .send_branchable_sync_response_token(token, reply)
                .await
        } else {
            self.runtime
                .transport
                .send_branchable_sync_response(peer_id, reply)
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
            .subscriptions
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

        if let Err(e) = self
            .send_branchable_sync_reply(
                &peer_id,
                token,
                BranchableSyncReply::success(&request.message_id, &request.collection_id, heads),
            )
            .await
        {
            tracing::warn!(
                peer_id = %peer_id,
                error = %e,
                "Failed to sign BranchableSync response"
            );
            return Err(e);
        }

        Ok(())
    }

    pub(in crate::sync::coordinator) async fn handle_branchable_sync_reply(
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

        // Error replies carry empty heads; without this branch a backpressure
        // nack or access denial would read as "peer has no heads". Like
        // DocSync, this is a pull — surfacing the error is the correct
        // terminal handling; the next sync trigger re-requests.
        if let Some(err) = reply.err_message.as_deref() {
            if crate::error::is_rate_limited_message(err) {
                tracing::warn!(
                    peer_id = %peer_id,
                    collection_id = %reply.collection_id,
                    "Peer rate-limited our BranchableSync request; will re-request on the next sync trigger"
                );
            } else {
                tracing::warn!(
                    peer_id = %peer_id,
                    collection_id = %reply.collection_id,
                    error = %err,
                    "BranchableSync request rejected by peer"
                );
            }
            return Err(crate::error::Error::Transport(format!(
                "BranchableSync request rejected by peer {peer_id}: {err}"
            )));
        }

        if reply.heads.is_empty() {
            tracing::debug!(
                collection_id = %reply.collection_id,
                "Peer has no heads for collection"
            );
            return Ok(());
        }

        let mut cids_to_fetch: Vec<Cid> = Vec::new();
        let mut cids_to_remerge: Vec<Cid> = Vec::new();
        for head_bytes in &reply.heads {
            match Cid::try_from(head_bytes.as_slice()) {
                Ok(cid) => {
                    tracing::trace!(cid = %cid, "Parsed collection head CID");
                    // Having the head block locally does NOT imply we have its
                    // ancestors. The head can land via gossip for a single
                    // commit while earlier collection commits (other docs)
                    // remain missing. Walk the local DAG and skip the fetch
                    // only when no descendants are missing.
                    let needs_fetch = match self.manager.blockstore().has(&cid).await {
                        Ok(true) => match self.manager.blockstore().get(&cid).await {
                            Ok(Some(data)) => {
                                match find_all_missing_links(
                                    self.manager.blockstore().as_ref(),
                                    &data,
                                )
                                .await
                                {
                                    Ok(missing) => {
                                        if missing.is_empty() {
                                            match self.manager.blockstore().is_merged(&cid).await {
                                                Ok(true) => false,
                                                Ok(false) => {
                                                    tracing::info!(
                                                        cid = %cid,
                                                        collection_id = %reply.collection_id,
                                                        "Collection head exists with complete DAG but is unmerged, scheduling re-merge"
                                                    );
                                                    cids_to_remerge.push(cid);
                                                    false
                                                }
                                                Err(e) => {
                                                    tracing::debug!(
                                                        cid = %cid,
                                                        error = %e,
                                                        "Failed to check merge status; falling back to fetch"
                                                    );
                                                    true
                                                }
                                            }
                                        } else {
                                            true
                                        }
                                    }
                                    Err(e) => {
                                        tracing::debug!(
                                            cid = %cid,
                                            error = %e,
                                            "Failed to walk DAG; falling back to fetch"
                                        );
                                        true
                                    }
                                }
                            }
                            _ => true,
                        },
                        Ok(false) => true,
                        Err(e) => {
                            tracing::warn!(cid = %cid, error = %e, "Error checking block");
                            true
                        }
                    };
                    if needs_fetch {
                        tracing::debug!(cid = %cid, "Need to fetch DAG");
                        cids_to_fetch.push(cid);
                    } else {
                        tracing::debug!(cid = %cid, "Already have block and full DAG");
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to parse CID from BranchableSync reply");
                }
            }
        }

        if !cids_to_remerge.is_empty() {
            let event_tx = self.manager.event_sender();
            for cid in cids_to_remerge {
                match self.manager.blockstore().get(&cid).await {
                    Ok(Some(data)) => {
                        let mut context = DagFetchContext::new(
                            String::new(),
                            reply.collection_id.clone(),
                            String::new(),
                            peer_id.clone(),
                        )
                        .with_explicit_replicator(
                            self.is_registered_replicator(peer_id.as_str(), &reply.collection_id),
                        );
                        context.fill_missing_from_block(&data);
                        tracing::info!(
                            cid = %cid,
                            collection_id = %context.collection_id,
                            "DAG complete locally, emitting BlockReceived for BranchableSync re-merge"
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
                                collection_id = %reply.collection_id,
                                "Failed to emit BlockReceived for locally complete BranchableSync DAG"
                            );
                            return Err(crate::error::Error::ChannelSend);
                        }
                    }
                    _ => cids_to_fetch.push(cid),
                }
            }
        }

        if !cids_to_fetch.is_empty() {
            tracing::info!(
                collection_id = %reply.collection_id,
                cid_count = cids_to_fetch.len(),
                "Spawning poll-based DAG fetchers for collection blocks"
            );

            let transport = self.runtime.transport.clone();
            let blockstore = self.manager.blockstore().clone();
            let event_tx = self.manager.event_sender();
            let limiter = self.runtime.dag_fetch_limiter.clone();

            for root_cid in cids_to_fetch {
                let transport = transport.clone();
                let blockstore = blockstore.clone();
                let event_tx = event_tx.clone();
                let collection_id = reply.collection_id.clone();
                let limiter = limiter.clone();
                let source_peer = peer_id.clone();
                let is_explicit_replicator =
                    self.is_registered_replicator(peer_id.as_str(), &collection_id);

                self.spawn_background_task("branchable_sync_reply_fetch_dag", async move {
                    let Some(_permits) = limiter.acquire(&source_peer).await else {
                        return;
                    };
                    let alternate_providers = transport.connected_peers().await.unwrap_or_default();
                    super::super::dag_fetcher::poll_fetch_dag(
                        transport,
                        blockstore,
                        event_tx,
                        root_cid,
                        DagFetchContext::new(
                            String::new(),
                            collection_id,
                            String::new(),
                            source_peer,
                        )
                        .with_alternate_providers(alternate_providers)
                        .with_explicit_replicator(is_explicit_replicator),
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
