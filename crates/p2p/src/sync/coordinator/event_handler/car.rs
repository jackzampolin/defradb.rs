//! CAR fetch event handling for the sync coordinator.

use blockstore::{verify_block_cid, Blockstore};
use cid::Cid;

use crate::error::Result;
use crate::message::CarFetchRequest;
use crate::sync::car::{collect_dag_blocks, collect_exact_blocks, decode_car, encode_car};
use crate::sync::coordinator::SyncCoordinator;
use crate::transport::{P2PTransport, PeerId, ResponseToken};

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    /// Handle an inbound CAR fetch request: collect the DAG and send CARv1 response.
    pub(crate) async fn handle_car_fetch_request(
        &self,
        peer_id: PeerId,
        request: CarFetchRequest,
        token: Option<ResponseToken>,
    ) -> Result<()> {
        self.check_peer_is_replicator(&peer_id)?;

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
            tracing::warn!(
                root_cid = %request.root_cid,
                peer_id = %peer_id,
                recursive = request.recursive,
                requested_count = request.wanted_cids.len(),
                "CAR handler: no blocks found for request"
            );
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
        let (_roots, blocks) = decode_car(&car_data)?;

        if blocks.is_empty() {
            tracing::warn!(
                root_cid = %root_cid,
                peer_id = %peer_id,
                "Received empty CAR response"
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
