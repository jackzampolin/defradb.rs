use super::helpers::{
    blockstore_for_txn, commit_txn, delete_result_from_commit, discard_txn, headstore_for_txn,
    map_delete_error, write_delete_commit_result,
};
use super::*;

impl<S: Store + 'static> AutoCommitMutator<S> {
    pub(super) async fn delete_impl(
        &self,
        collection_name: &str,
        doc_id: &DocID,
    ) -> query::error::Result<DeleteResult> {
        let collection = self.get_collection_or_err(collection_name)?;

        // Create a write transaction
        let txn = self.new_write_txn().await?;

        // Execute the mutation in a block to drop datastore before commit
        let result = {
            let datastore = self.datastore_for_collection(&txn, collection_name)?;

            // Create an IndexManager for index maintenance
            let index_manager = self.index_manager_for_collection(&collection, collection_name)?;

            // Use delete_with_indexes to maintain index consistency
            collection
                .delete_with_indexes(&datastore, doc_id, &index_manager)
                .await
                .map_err(map_delete_error)
        };

        match result {
            Ok(existed) => {
                // Build delete block (composite with status=2) in a scoped block
                let commit_result: Option<(Cid, Vec<u8>)> = {
                    let blockstore = blockstore_for_txn(&txn)?;
                    let headstore = headstore_for_txn(&txn)?;
                    // Get signing config from thread-local (set by FFI exec_request)
                    let sign_config = get_signing_config();

                    write_delete_commit_result(
                        collection_name,
                        "delete",
                        &collection,
                        &blockstore,
                        &headstore,
                        doc_id,
                        sign_config.as_ref(),
                    )
                    .await
                }; // blockstore and headstore dropped here

                // Commit the transaction (datastore reference is now dropped)
                commit_txn(txn, collection_name, "delete").await?;

                // Emit update event for subscriptions (deletes are also "updates")
                let cid = commit_result.as_ref().map(|(c, _)| *c).unwrap_or_default();
                self.emit_update_events(&collection, &doc_id.to_string(), cid);

                Ok(delete_result_from_commit(
                    doc_id.clone(),
                    existed,
                    commit_result,
                ))
            }
            Err(e) => {
                // Discard the transaction on error
                discard_txn(txn, collection_name, "delete");
                Err(e)
            }
        }
    }
}
