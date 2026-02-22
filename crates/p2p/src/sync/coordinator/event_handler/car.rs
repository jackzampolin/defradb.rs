//! CAR fetch event handling for the sync coordinator.

use blockstore::Blockstore;
use cid::Cid;
use libp2p::PeerId;

use crate::error::Result;
use crate::sync::car::{collect_dag_blocks, decode_car, encode_car};
use crate::sync::coordinator::SyncCoordinator;

impl<B: Blockstore + 'static> SyncCoordinator<B> {
    /// Handle an inbound CAR fetch request: collect the DAG and send CARv1 response.
    pub(crate) async fn handle_car_fetch_request(
        &self,
        peer_id: PeerId,
        root_cid: Cid,
    ) -> Result<()> {
        self.check_peer_is_replicator(&peer_id)?;

        let blocks = collect_dag_blocks(self.manager.blockstore().as_ref(), &root_cid).await?;

        if blocks.is_empty() {
            tracing::debug!(
                root_cid = %root_cid,
                peer_id = %peer_id,
                "CAR request for unknown DAG, ignoring"
            );
            return Ok(());
        }

        let block_refs: Vec<(&Cid, &[u8])> =
            blocks.iter().map(|(c, d)| (c, d.as_slice())).collect();
        let car_data = encode_car(&[root_cid], &block_refs)?;

        tracing::debug!(
            root_cid = %root_cid,
            peer_id = %peer_id,
            blocks = blocks.len(),
            car_bytes = car_data.len(),
            "Sending CAR response"
        );

        self.host.send_car_response(peer_id, car_data).await?;
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

        let block_refs: Vec<(&Cid, &[u8])> =
            blocks.iter().map(|(c, d)| (c, d.as_slice())).collect();
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
            "Stored blocks from CAR response"
        );

        Ok(())
    }
}
