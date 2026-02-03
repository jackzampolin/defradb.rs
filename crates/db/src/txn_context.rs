//! Transaction context for query execution.

use query::mutator::DocMutator;
use query::runner::DocFetcher;
use query::txn::TransactionContext;
use std::sync::Arc;
use std::time::Instant;
use storage::corekv::Store;

use crate::doc_mutator::DbDocMutator;
use crate::lensed_fetcher::LensedDocFetcher;
use crate::txn::DbTxn;

/// Transaction context for query execution.
///
/// Implements `query::TransactionContext` to provide transaction-scoped
/// document fetching to the query executor. Uses `LensedDocFetcher` to support
/// lens migrations within transactions.
pub struct DbTransactionContext<S: Store> {
    id: String,
    readonly: bool,
    fetcher: Arc<LensedDocFetcher<S>>,
    created_at: Instant,
}

impl<S: Store> DbTransactionContext<S> {
    /// Create a new transaction context.
    pub(crate) fn new(id: String, readonly: bool, fetcher: Arc<LensedDocFetcher<S>>) -> Self {
        Self {
            id,
            readonly,
            fetcher,
            created_at: Instant::now(),
        }
    }

    /// Get the instant when this transaction was created.
    pub fn created_at(&self) -> Instant {
        self.created_at
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

impl<S: Store + 'static> DbTransactionContext<S> {
    /// Get a document mutator for performing mutations within this transaction.
    ///
    /// The mutator shares the same underlying transaction as the fetcher, so all
    /// read and write operations are within the same transaction context.
    ///
    /// # Note
    ///
    /// Should only be called on non-readonly transactions. Attempting to mutate
    /// via the returned mutator on a readonly transaction will fail.
    pub fn doc_mutator(&self) -> Arc<dyn DocMutator> {
        Arc::new(DbDocMutator::from_shared_txn(self.fetcher.shared_txn()))
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

    fn doc_mutator(&self) -> Option<Arc<dyn DocMutator>> {
        if self.readonly {
            None
        } else {
            Some(self.doc_mutator())
        }
    }
}
