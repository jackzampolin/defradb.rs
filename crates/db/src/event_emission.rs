//! Shared helper for registering tx-success callbacks that publish Update events.
//!
//! Both `DbDocMutator` (explicit-tx path) and `BatchMutator` (auto-commit path)
//! use this to register a callback at mutation time. The underlying tx machinery
//! fires success callbacks only on commit; discards skip them.

use async_trait::async_trait;
use cid::Cid;
use events::{Bus, Message, Update};
use std::sync::Arc;
use storage::corekv::Store;

use crate::error::Result;
use crate::txn::DbTxn;

/// Per-mutation broadcast payload, mirroring Go's sendUpdate call.
#[derive(Debug, Clone)]
pub struct TxnBroadcastEvent {
    pub collection_name: String,
    pub collection_id: String,
    pub doc_id: String,
    pub doc_cid: Cid,
    pub doc_block: Vec<u8>,
    pub document_json: Option<serde_json::Value>,
    pub collection_block: Option<(Cid, Vec<u8>)>,
    pub creator_did: Option<String>,
}

/// Hook for forwarding committed transactional writes to the P2P stack.
///
/// `DbDocMutator` (the explicit-tx mutator) calls this from the
/// `on_success_async` callback so that a successful tx commit triggers
/// the same push-to-replicators + gossipsub broadcast that the
/// single-mutation auto-commit path gets via `BroadcastMutator`.
///
/// Without a `TxnBroadcaster`, transactional writes commit locally
/// and publish to the local bus only — P2P peers never see them.
/// Mirrors Go's `db.sendUpdate` at `internal/db/p2p.go:23-25`.
#[async_trait]
pub trait TxnBroadcaster: Send + Sync {
    async fn broadcast_update(&self, event: TxnBroadcastEvent);
}

/// Register an `on_success_async` callback that publishes an Update event
/// with the document block bytes, and — when `collection_block` is `Some` —
/// a second collection-level Update with the collection block's own cid and bytes.
///
/// If `bus` is `None`, no callback is registered — there's no subscriber to notify.
///
/// Mirrors Go's `db.sendUpdate` callback registration at
/// `internal/db/collection.go:755` and the branchable collection event at
/// `internal/db/collection.go:789`, both of which publish the actual block bytes.
#[allow(clippy::too_many_arguments)]
pub(crate) fn register_update_event_callback<S: Store + 'static>(
    txn: &mut DbTxn<S>,
    bus: Option<&Arc<dyn Bus>>,
    broadcaster: Option<&Arc<dyn TxnBroadcaster>>,
    collection_name: String,
    collection_id: String,
    doc_id: String,
    doc_cid: Cid,
    doc_block: Vec<u8>,
    document_json: Option<serde_json::Value>,
    collection_block: Option<(Cid, Vec<u8>)>,
    creator_did: Option<String>,
) -> Result<()> {
    if bus.is_none() && broadcaster.is_none() {
        return Ok(());
    }
    let bus = bus.map(Arc::clone);
    let broadcaster = broadcaster.map(Arc::clone);
    txn.on_success_async(Box::new(move || {
        Box::pin(async move {
            if let Some(bus) = bus {
                let subject_doc_id = doc_id.clone();
                let update = Update::new(
                    doc_id.clone(),
                    doc_cid,
                    collection_id.clone(),
                    doc_block.clone(),
                    false,
                    false,
                );
                bus.publish(Message::update(update));

                if let Some((col_cid, ref col_block)) = collection_block {
                    let collection_update = Update::new_with_subject_doc_id(
                        String::new(),
                        subject_doc_id,
                        col_cid,
                        collection_id.clone(),
                        col_block.clone(),
                        false,
                        false,
                    );
                    bus.publish(Message::update(collection_update));
                }
            }

            if let Some(broadcaster) = broadcaster {
                let event = TxnBroadcastEvent {
                    collection_name,
                    collection_id,
                    doc_id,
                    doc_cid,
                    doc_block,
                    document_json,
                    collection_block,
                    creator_did,
                };
                broadcaster.broadcast_update(event).await;
            }
        })
    }))
}
