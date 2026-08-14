//! Vector index lookup for LensedAutoCommitFetcher.

use storage::corekv::Store;

use super::LensedAutoCommitFetcher;

impl<S: Store + 'static> LensedAutoCommitFetcher<S> {
    pub(super) async fn vector_search_impl(
        &self,
        collection_name: &str,
        index_id: u32,
        query_vector: &[f64],
        k: usize,
        effort: Option<usize>,
    ) -> query::error::Result<Vec<u64>> {
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        let datastore = txn.datastore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get datastore: {}", e))
        })?;

        let hits = crate::vector_search::search_vector_index(
            &collection,
            &datastore,
            index_id,
            query_vector,
            k,
            effort,
        )
        .await;

        let _ = txn.discard();
        hits
    }
}
