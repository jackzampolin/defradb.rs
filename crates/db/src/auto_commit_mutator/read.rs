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

        // Get the datastore
        let datastore = txn.datastore().map_err(|e| {
            query::error::QueryError::execution(format!(
                "failed to get datastore for collection '{}': {}",
                collection_name, e
            ))
        })?;

        // Execute the check
        let result = collection
            .exists_with_datastore(&datastore, doc_id)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("exists error: {}", e)));

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

        // Get the datastore
        let datastore = txn.datastore().map_err(|e| {
            query::error::QueryError::execution(format!(
                "failed to get datastore for collection '{}': {}",
                collection_name, e
            ))
        })?;

        // Execute the fetch
        let result = collection
            .get_with_datastore(&datastore, doc_id)
            .await
            .map_err(|e| {
                query::error::QueryError::execution(format!("get_for_update error: {}", e))
            });

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
