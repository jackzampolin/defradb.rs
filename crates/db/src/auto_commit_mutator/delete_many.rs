use super::helpers::{
    blockstore_for_txn, commit_txn, headstore_for_txn, map_delete_error, write_delete_commit_result,
};
use super::*;

impl<S: Store + 'static> AutoCommitMutator<S> {
    /// Delete multiple documents in a single transaction.
    ///
    /// Mirrors `create_many_impl`: one transaction for all deletes, one commit.
    /// This avoids creating N separate MVCC snapshots and N COW epochs when
    /// purging large batches (e.g. 686 documents caused a 20GB memory spike
    /// with per-document transactions).
    pub async fn delete_many_impl(
        &self,
        collection_name: &str,
        doc_ids: &[DocID],
    ) -> query::error::Result<Vec<DeleteResult>> {
        if doc_ids.is_empty() {
            return Ok(Vec::new());
        }

        if doc_ids.len() == 1 {
            return self
                .delete_impl(collection_name, &doc_ids[0])
                .await
                .map(|r| vec![r]);
        }

        let collection = self.get_collection_or_err(collection_name)?;
        let sign_config = get_signing_config();

        let index_manager = self.index_manager_for_collection(&collection, collection_name)?;

        // Single transaction for all deletes
        let txn = self.new_write_txn().await?;

        let mut results: Vec<(DocID, bool, Option<Cid>)> = Vec::with_capacity(doc_ids.len());

        for doc_id in doc_ids {
            // Delete from datastore + indexes
            let existed = {
                let datastore = self.datastore_for_collection(&txn, collection_name)?;

                match collection
                    .delete_with_indexes(&datastore, doc_id, &index_manager)
                    .await
                {
                    Ok(existed) => existed,
                    Err(e) => {
                        let mapped_error = map_delete_error(e);
                        warn!(
                            collection = %collection_name,
                            doc_id = %doc_id,
                            error = %mapped_error,
                            "Failed to delete document in batch"
                        );
                        results.push((doc_id.clone(), false, None));
                        continue;
                    }
                }
            };

            // Write delete block (composite with status=2)
            let commit_cid = {
                let blockstore = blockstore_for_txn(&txn)?;
                let headstore = headstore_for_txn(&txn)?;

                match write_delete_commit_result(
                    collection_name,
                    "delete",
                    &collection,
                    &blockstore,
                    &headstore,
                    doc_id,
                    sign_config.as_ref(),
                )
                .await
                {
                    Some((composite_cid, _)) => Some(composite_cid),
                    None => None,
                }
            };

            results.push((doc_id.clone(), existed, commit_cid));
        }

        // Single commit for entire batch
        commit_txn(txn, collection_name, "batch delete").await?;

        // Emit events after commit
        let mut delete_results = Vec::with_capacity(results.len());
        for (doc_id, existed, commit_cid) in results {
            let cid = commit_cid.unwrap_or_default();
            self.emit_update_events(&collection, &doc_id.to_string(), cid);
            delete_results.push(DeleteResult::new(doc_id, existed));
        }

        Ok(delete_results)
    }
}
