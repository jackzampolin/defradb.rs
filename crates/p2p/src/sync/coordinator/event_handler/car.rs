//! CAR fetch event handling for the sync coordinator.

use blockstore::{verify_block_cid, Blockstore};
use cid::Cid;

use super::super::authorizer::AccessAuthorizer;
use crate::error::{Error, Result};
use crate::message::CarFetchRequest;
use crate::sync::car::{collect_dag_blocks, collect_exact_blocks, decode_car, encode_car};
use crate::sync::coordinator::dag_context::block_context_from_data;
use crate::sync::coordinator::SyncCoordinator;
use crate::transport::{P2PTransport, PeerId};

fn sample_cids(cids: &[Cid]) -> Vec<String> {
    cids.iter().take(4).map(ToString::to_string).collect()
}

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    async fn check_car_fetch_access(
        &self,
        peer_id: &PeerId,
        request: &CarFetchRequest,
    ) -> Result<()> {
        if self.access.access_mode.is_open() {
            return Ok(());
        }

        let mut checked_collection = false;
        for cid in request.response_roots() {
            let block_data = match self.manager.blockstore().get(&cid).await {
                Ok(Some(data)) => data,
                Ok(None) => continue,
                Err(error) => {
                    tracing::debug!(
                        cid = %cid,
                        peer_id = %peer_id,
                        error = %error,
                        "CAR handler: failed to read requested block for collection access check"
                    );
                    continue;
                }
            };

            let Some(collection_id) = block_context_from_data(&block_data).collection_id else {
                continue;
            };

            checked_collection = true;
            let is_collection_replicator = self
                .authorizer
                .peer_authorized_for_collection(peer_id.as_str(), &collection_id)
                .await;
            let is_collection_subscriber = self
                .access
                .peer_state
                .peer_subscribed_to_collection(peer_id.as_str(), &collection_id);

            if !is_collection_replicator && !is_collection_subscriber {
                tracing::warn!(
                    peer_id = %peer_id,
                    collection_id = %collection_id,
                    is_collection_replicator,
                    is_collection_subscriber,
                    "Access denied: peer cannot fetch CAR blocks for this collection"
                );
                return Err(Error::AccessDenied {
                    peer_id: peer_id.to_string(),
                    collection_id,
                });
            }
        }

        if checked_collection {
            Ok(())
        } else {
            self.check_peer_is_replicator(peer_id).await
        }
    }

    /// Handle an inbound CAR fetch request: collect the DAG and send CARv1 response.
    pub(crate) async fn handle_car_fetch_request(
        &self,
        peer_id: PeerId,
        request: CarFetchRequest,
        token: Option<T::ResponseToken>,
    ) -> Result<()> {
        let root_present = match self.manager.blockstore().has(&request.root_cid).await {
            Ok(present) => Some(present),
            Err(error) => {
                tracing::debug!(
                    root_cid = %request.root_cid,
                    peer_id = %peer_id,
                    error = %error,
                    "CAR handler: failed to check root presence before serving request"
                );
                None
            }
        };

        if let Err(error) = self.check_car_fetch_access(&peer_id, &request).await {
            tracing::warn!(
                root_cid = %request.root_cid,
                peer_id = %peer_id,
                recursive = request.recursive,
                requested_count = request.wanted_cids.len(),
                root_present = ?root_present,
                connected = self.access.peer_state.is_connected(peer_id.as_str()),
                registered_any = self.access.replicators.is_any_replicator(peer_id.as_str()),
                error = %error,
                "CAR handler rejected request"
            );
            return Err(error);
        }

        tracing::debug!(
            root_cid = %request.root_cid,
            peer_id = %peer_id,
            recursive = request.recursive,
            requested_count = request.wanted_cids.len(),
            has_token = token.is_some(),
            "CAR handler: collecting blocks"
        );
        let response_roots = request.response_roots();
        let collected = if request.recursive {
            collect_dag_blocks(self.manager.blockstore().as_ref(), &request.root_cid).await?
        } else {
            collect_exact_blocks(self.manager.blockstore().as_ref(), &request.wanted_cids).await?
        };
        let truncated = collected.truncated();
        let blocks = collected.blocks;

        if blocks.is_empty() {
            self.manager.diagnostics.record_car_no_blocks_served();
            // Normal race: peer asked for blocks we have not yet received
            // ourselves. Noisy at WARN during concurrent replication catch-up;
            // the requester retries until some peer serves the DAG.
            if request.recursive {
                tracing::debug!(
                    root_cid = %request.root_cid,
                    peer_id = %peer_id,
                    recursive = request.recursive,
                    requested_count = request.wanted_cids.len(),
                    root_present = ?root_present,
                    "CAR handler: no blocks found for request"
                );
            } else {
                tracing::warn!(
                    root_cid = %request.root_cid,
                    peer_id = %peer_id,
                    root_present = ?root_present,
                    requested_count = request.wanted_cids.len(),
                    requested_cids = ?sample_cids(&request.wanted_cids),
                    "CAR handler: no exact blocks found for selective request"
                );
            }
            // Send a header-only CAR so both transports (iroh and libp2p)
            // receive a well-formed response they can decode. Without this,
            // the libp2p response handler errors on empty bytes and the
            // requester-side car_empty_responses counter stays at zero
            // (issue #858 review feedback).
            let car_data = encode_car(&response_roots, &[])?;
            if let Some(token) = token {
                self.runtime
                    .transport
                    .send_car_response_token(token, car_data)
                    .await?;
            } else {
                self.runtime
                    .transport
                    .send_car_response(&peer_id, car_data)
                    .await?;
            }
            return Ok(());
        }

        let block_refs: Vec<(&Cid, &[u8])> = blocks.iter().map(|(c, d)| (c, d.as_ref())).collect();
        let car_data = encode_car(&response_roots, &block_refs)?;

        tracing::debug!(
            root_cid = %request.root_cid,
            peer_id = %peer_id,
            recursive = request.recursive,
            response_roots = response_roots.len(),
            blocks = blocks.len(),
            car_bytes = car_data.len(),
            truncated,
            "Sending CAR response"
        );
        if truncated {
            tracing::warn!(
                root_cid = %request.root_cid,
                peer_id = %peer_id,
                recursive = request.recursive,
                response_roots = response_roots.len(),
                blocks = blocks.len(),
                car_bytes = car_data.len(),
                "CAR response truncated by server-side limits"
            );
        }

        if let Some(token) = token {
            self.runtime
                .transport
                .send_car_response_token(token, car_data)
                .await?;
        } else {
            self.runtime
                .transport
                .send_car_response(&peer_id, car_data)
                .await?;
        }
        Ok(())
    }

    /// Handle an inbound CAR fetch response: decode and store blocks.
    pub(crate) async fn handle_car_fetch_response(
        &self,
        peer_id: PeerId,
        root_cid: Cid,
        car_data: Vec<u8>,
    ) -> Result<()> {
        // Raw-empty: transport received zero bytes (peer had nothing for
        // this root, e.g. the serving side's handle_car_fetch_request
        // returned without writing a body). Count and skip decode.
        if car_data.is_empty() {
            self.manager.diagnostics.record_car_empty_response();
            tracing::debug!(
                root_cid = %root_cid,
                peer_id = %peer_id,
                "Received empty CAR response (raw bytes)"
            );
            return Ok(());
        }

        let (_roots, blocks) = decode_car(&car_data)?;

        if blocks.is_empty() {
            self.manager.diagnostics.record_car_empty_response();
            // Peer replied with a parseable but block-less CAR (provider
            // had nothing for this root). Debug: transport layer surfaces
            // the final "no provider succeeded" outcome via BitswapComplete
            // (see issue #858).
            tracing::debug!(
                root_cid = %root_cid,
                peer_id = %peer_id,
                "Received empty CAR response (decoded 0 blocks)"
            );
            return Ok(());
        }

        // Verify all block CIDs before storing (finding 03-35).
        for (cid, data) in &blocks {
            if let Err(e) = verify_block_cid(cid, data) {
                let p2p_err = crate::error::blockstore_verify_to_p2p(e, cid);
                tracing::warn!(
                    root_cid = %root_cid,
                    block_cid = %cid,
                    peer_id = %peer_id,
                    error = %p2p_err,
                    "CAR block failed CID verification, rejecting entire response"
                );
                return Err(p2p_err);
            }
        }

        let block_refs: Vec<(&Cid, &[u8])> = blocks.iter().map(|(c, d)| (c, d.as_ref())).collect();
        self.manager
            .blockstore()
            .as_ref()
            .put_many(&block_refs)
            .await
            .map_err(|e| crate::error::Error::BlockstoreError(e.to_string()))?;

        tracing::debug!(
            root_cid = %root_cid,
            peer_id = %peer_id,
            blocks_stored = blocks.len(),
            car_bytes = car_data.len(),
            "Stored blocks from CAR response"
        );

        Ok(())
    }
}
