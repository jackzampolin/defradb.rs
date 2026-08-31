#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use db::{DbCollectionProvider, DbTransactionRegistry, LensedAutoCommitFetcher, DB};
    use query::runner::QueryRunner;
    use query::txn::TransactionRegistry;
    use query::{QueryExecutor, QueryRequest};
    use serde_json::json;
    use storage::RegolithStore;
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    #[allow(clippy::arc_with_non_send_sync)]
    async fn query_in_txn_sees_uncommitted_schema() {
        let store = RegolithStore::in_memory().unwrap();
        let db = Arc::new(DB::new(store).unwrap());
        let registry = Arc::new(DbTransactionRegistry::new(Arc::clone(&db)));
        let handle = registry.begin(false).await.unwrap();

        registry
            .add_schema_in_txn(handle.as_str(), "type TxnOnly { value: Int }")
            .await
            .unwrap();

        let runner = QueryRunner::with_arc_registry_and_provider(
            LensedAutoCommitFetcher::new(Arc::clone(&db)),
            DbCollectionProvider::new_arc(db),
            Arc::clone(&registry),
        );
        let response = runner
            .execute_in_txn(QueryRequest::new("{ TxnOnly { value } }"), &handle)
            .await;

        assert!(
            !response.has_errors(),
            "transaction query failed: {:?}",
            response.errors
        );
        assert_eq!(response.data, Some(json!({ "TxnOnly": [] })));

        registry.rollback(&handle).await.unwrap();
    }
}
