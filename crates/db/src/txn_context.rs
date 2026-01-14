//! Transaction context for query execution.

use query::runner::DocFetcher;
use query::txn::TransactionContext;
use std::sync::Arc;
use storage::corekv::Store;

use crate::doc_fetcher::DbDocFetcher;
use crate::txn::DbTxn;

/// Transaction context for query execution.
///
/// Implements `query::TransactionContext` to provide transaction-scoped
/// document fetching to the query executor.
pub struct DbTransactionContext<S: Store> {
    id: String,
    readonly: bool,
    fetcher: Arc<DbDocFetcher<S>>,
}

impl<S: Store> DbTransactionContext<S> {
    /// Create a new transaction context.
    pub(crate) fn new(id: String, readonly: bool, fetcher: Arc<DbDocFetcher<S>>) -> Self {
        Self {
            id,
            readonly,
            fetcher,
        }
    }
}

impl<S: Store + 'static> DbTransactionContext<S> {
    /// Take the underlying transaction (for commit/rollback).
    pub(crate) async fn take_txn(&self) -> Option<DbTxn<S>> {
        self.fetcher.take_txn().await
    }
}

impl<S: Store + 'static> TransactionContext for DbTransactionContext<S> {
    fn id(&self) -> &str {
        &self.id
    }

    fn is_readonly(&self) -> bool {
        self.readonly
    }

    fn doc_fetcher(&self) -> Arc<dyn DocFetcher> {
        self.fetcher.clone()
    }
}
