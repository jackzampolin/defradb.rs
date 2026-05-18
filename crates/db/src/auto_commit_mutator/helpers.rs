use super::*;

use crate::collection::Collection;

pub(super) fn ensure_collection_is_active<S: Store>(
    db: &DB<S>,
    collection_name: &str,
    collection: &Collection,
) -> query::error::Result<()> {
    let is_active = db
        .find_collection_by_id(collection.collection_id())
        .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
        .is_some();

    if is_active {
        Ok(())
    } else {
        Err(query::error::QueryError::collection_not_found(
            collection_name,
        ))
    }
}

impl<S: Store + 'static> AutoCommitMutator<S> {
    /// Get collection from DB cache or return a not-found error.
    pub(super) fn get_collection_or_err(
        &self,
        collection_name: &str,
    ) -> query::error::Result<Collection> {
        self.db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))
    }

    /// Emit update events for subscriptions, carrying the actual block bytes
    /// so downstream consumers can traverse the DAG without an extra fetch.
    ///
    /// For branchable collections, emits a second event keyed by collection_id
    /// using the collection block's own cid/bytes (Go publishes the collection
    /// block separately at internal/db/collection.go:789).
    pub(super) fn emit_update_events(
        &self,
        collection: &Collection,
        doc_id_str: &str,
        doc_cid: Cid,
        doc_block: Vec<u8>,
        collection_block: Option<(Cid, Vec<u8>)>,
    ) {
        if let Some(bus) = self.db.event_bus() {
            let update = Update::new(
                doc_id_str.to_string(),
                doc_cid,
                collection.collection_id().to_string(),
                doc_block,
                false, // is_retry
                false, // is_relay (local mutation)
            );
            bus.publish(Message::update(update));

            if let Some((col_cid, col_block)) = collection_block {
                let col_update = Update::new_with_subject_doc_id(
                    String::new(), // empty doc_id → keyed by collection_id
                    doc_id_str.to_string(),
                    col_cid,
                    collection.collection_id().to_string(),
                    col_block,
                    false,
                    false,
                );
                bus.publish(Message::update(col_update));
            }
        }
    }
}
