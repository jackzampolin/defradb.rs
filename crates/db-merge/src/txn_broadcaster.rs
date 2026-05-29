//! `TxnBroadcaster` implementation backed by the `SyncCoordinator`.
//!
//! Used by `DbTransactionRegistry::with_broadcaster` so that committed
//! transactional writes get pushed to replicators and gossipsub topics —
//! mirroring what `BroadcastMutator` already does for the single-mutation
//! auto-commit path.

use std::sync::Arc;

use async_trait::async_trait;
use blockstore::Blockstore;
use db::event_emission::{TxnBroadcastEvent, TxnBroadcaster};
use db_blocks::BlockResult;
use p2p::sync::SyncCoordinator;
use p2p::transport::P2PTransport;

use crate::broadcast_mutator::broadcast::{
    broadcast_with_retry_with_creator, log_broadcast_failure,
};

/// `TxnBroadcaster` that fans committed transactional writes out to peers via
/// `SyncCoordinator::push_to_replicators` and gossipsub.
pub struct SyncTxnBroadcaster<B: Blockstore + Send + Sync + 'static, T: P2PTransport + 'static> {
    sync: Arc<SyncCoordinator<B, T>>,
}

impl<B: Blockstore + Send + Sync + 'static, T: P2PTransport + 'static> SyncTxnBroadcaster<B, T> {
    pub fn new(sync: Arc<SyncCoordinator<B, T>>) -> Self {
        Self { sync }
    }
}

#[async_trait]
impl<B, T> TxnBroadcaster for SyncTxnBroadcaster<B, T>
where
    B: Blockstore + Send + Sync + 'static,
    T: P2PTransport + 'static,
{
    async fn broadcast_update(&self, event: TxnBroadcastEvent) {
        let TxnBroadcastEvent {
            collection_name,
            collection_id,
            doc_id,
            doc_cid,
            doc_block,
            collection_block,
            creator_did,
        } = event;

        let sync = self.sync.clone();

        // Spawn detached so the tx-success callback returns promptly. Mirrors
        // `BroadcastMutator::create`'s pattern of broadcasting on a background
        // task after the local commit lands.
        tokio::spawn(async move {
            let creator_ref = creator_did.as_deref();

            sync.push_to_replicators_with_creator(
                &doc_cid,
                &doc_block,
                &doc_id,
                &collection_id,
                creator_ref,
            )
            .await;

            let doc_block_result = BlockResult {
                cid: doc_cid,
                block: doc_block,
                doc_id: doc_id.clone(),
                field_cids: vec![],
            };
            log_broadcast_failure(
                &broadcast_with_retry_with_creator(
                    &sync,
                    &doc_block_result,
                    &collection_id,
                    &collection_name,
                    creator_ref,
                )
                .await,
            );

            if let Some((col_cid, col_block)) = collection_block {
                sync.push_to_replicators_with_creator(
                    &col_cid,
                    &col_block,
                    &doc_id,
                    &collection_id,
                    creator_ref,
                )
                .await;

                let col_block_result = BlockResult {
                    cid: col_cid,
                    block: col_block,
                    doc_id: String::new(),
                    field_cids: vec![],
                };
                log_broadcast_failure(
                    &broadcast_with_retry_with_creator(
                        &sync,
                        &col_block_result,
                        &collection_id,
                        &collection_name,
                        creator_ref,
                    )
                    .await,
                );
            }
        });
    }
}
