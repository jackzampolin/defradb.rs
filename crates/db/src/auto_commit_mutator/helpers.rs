use super::*;

use crate::collection::Collection;

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

    /// Emit update events for subscriptions.
    ///
    /// For branchable collections, emits a second event for the collection-level DAG.
    pub(super) fn emit_update_events(&self, collection: &Collection, doc_id_str: &str, cid: Cid) {
        if let Some(bus) = self.db.event_bus() {
            let update = Update::new(
                doc_id_str.to_string(),
                cid,
                collection.collection_id().to_string(),
                vec![],
                false, // is_retry
                false, // is_relay (local mutation)
            );
            bus.publish(Message::update(update));

            if collection.schema().is_branchable {
                let col_update = Update::new_with_subject_doc_id(
                    String::new(), // empty doc_id → keyed by collection_id
                    doc_id_str.to_string(),
                    cid,
                    collection.collection_id().to_string(),
                    vec![],
                    false,
                    false,
                );
                bus.publish(Message::update(col_update));
            }
        }
    }
}
