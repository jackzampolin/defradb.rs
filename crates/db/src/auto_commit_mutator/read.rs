use super::*;

impl<S: Store + 'static> AutoCommitMutator<S> {
    pub(super) async fn exists_impl(
        &self,
        collection_name: &str,
        doc_id: &DocID,
    ) -> query::error::Result<bool> {
        let collection = self.get_collection_or_err(collection_name)?;

        // Create a read-only transaction (exists is read-only)
        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        // Scope the datastore + run the check inside an inner block, then
        // explicitly drop the datastore NamespaceView before calling
        // `txn.discard()`. BasicTxn::discard() does Arc::try_unwrap on the
        // inner SharedTxn; any outstanding NamespaceView clone causes the
        // unwrap to fail with `TxnStillInUse` and spam the logs. Explicit
        // drop here makes the Arc refcount unambiguously 1 at the discard
        // point regardless of async state-machine layout (#821).
        let result = {
            let datastore = txn.datastore().map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to get datastore for collection '{}': {}",
                    collection_name, e
                ))
            })?;

            let r = collection
                .exists_with_datastore(&datastore, doc_id)
                .await
                .map_err(|e| query::error::QueryError::execution(format!("exists error: {}", e)));

            drop(datastore);
            r
        };

        // Discard the read-only transaction
        if let Err(e) = txn.discard() {
            warn!(
                collection = %collection_name,
                error = %e,
                "Failed to discard read-only transaction after exists"
            );
        }

        result
    }

    pub(super) async fn get_for_update_impl(
        &self,
        collection_name: &str,
        doc_id: &DocID,
    ) -> query::error::Result<Option<Document>> {
        let collection = self.get_collection_or_err(collection_name)?;

        // Create a read-only transaction (get_for_update is read-only)
        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        // See exists_impl — explicit drop(datastore) before txn.discard()
        // to guarantee the SharedTxn Arc refcount is 1 at the discard
        // point. Without this, BasicTxn::discard() can fail with
        // TxnStillInUse under certain async runtime schedules and spam
        // "Failed to discard read-only transaction after get_for_update"
        // warnings (#821).
        let result = {
            let datastore = txn.datastore().map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to get datastore for collection '{}': {}",
                    collection_name, e
                ))
            })?;

            let r = collection
                .get_with_datastore(&datastore, doc_id)
                .await
                .map_err(|e| {
                    query::error::QueryError::execution(format!("get_for_update error: {}", e))
                });

            drop(datastore);
            r
        };

        // Discard the read-only transaction
        if let Err(e) = txn.discard() {
            warn!(
                collection = %collection_name,
                error = %e,
                "Failed to discard read-only transaction after get_for_update"
            );
        }

        result
    }
}
