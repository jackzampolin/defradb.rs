//! Shared helper for registering tx-success callbacks that publish Update events.
//!
//! Both `DbDocMutator` (explicit-tx path) and `BatchMutator` (auto-commit path)
//! use this to register a callback at mutation time. The underlying tx machinery
//! fires success callbacks only on commit; discards skip them.

use cid::Cid;
use events::{Bus, Message, Update};
use std::sync::Arc;
use storage::corekv::Store;

use crate::error::Result;
use crate::txn::DbTxn;

/// Register an `on_success_async` callback that publishes an Update event
/// with the document block bytes, and — when `collection_block` is `Some` —
/// a second collection-level Update with the collection block's own cid and bytes.
///
/// If `bus` is `None`, no callback is registered — there's no subscriber to notify.
///
/// Mirrors Go's `db.sendUpdate` callback registration at
/// `internal/db/collection.go:755` and the branchable collection event at
/// `internal/db/collection.go:789`, both of which publish the actual block bytes.
pub(crate) fn register_update_event_callback<S: Store + 'static>(
    txn: &mut DbTxn<S>,
    bus: Option<&Arc<dyn Bus>>,
    collection_id: String,
    doc_id: String,
    doc_cid: Cid,
    doc_block: Vec<u8>,
    collection_block: Option<(Cid, Vec<u8>)>,
) -> Result<()> {
    let Some(bus) = bus else {
        return Ok(());
    };
    let bus = Arc::clone(bus);
    txn.on_success_async(Box::new(move || {
        Box::pin(async move {
            let subject_doc_id = doc_id.clone();
            let update = Update::new(
                doc_id,
                doc_cid,
                collection_id.clone(),
                doc_block,
                false,
                false,
            );
            bus.publish(Message::update(update));

            if let Some((col_cid, col_block)) = collection_block {
                let collection_update = Update::new_with_subject_doc_id(
                    String::new(),
                    subject_doc_id,
                    col_cid,
                    collection_id,
                    col_block,
                    false,
                    false,
                );
                bus.publish(Message::update(collection_update));
            }
        })
    }))
}
