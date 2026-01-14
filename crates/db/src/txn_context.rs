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
    ///
    /// After calling this, `is_consumed()` will return `true` and all
    /// fetcher operations will return an error.
    pub(crate) async fn take_txn(&self) -> Option<DbTxn<S>> {
        self.fetcher.take_txn().await
    }

    /// Check if the transaction has been consumed (via commit/rollback).
    ///
    /// Returns `true` if `take_txn()` was called and the transaction is
    /// no longer available for queries.
    pub async fn is_consumed(&self) -> bool {
        self.fetcher.is_consumed().await
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
