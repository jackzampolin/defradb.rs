//! Transaction context for query execution.

use query::fetcher::CollectionProvider;
use query::mutator::DocMutator;
use query::runner::DocFetcher;
use query::txn::{DeferredAcpMutations, TransactionContext};
use std::sync::Arc;
use std::time::Instant;
use storage::corekv::Store;

use crate::collection_provider::TxnCollectionProvider;
use crate::database::DB;
use crate::doc_mutator::DbDocMutator;
use crate::lensed_fetcher::LensedDocFetcher;
use crate::txn::DbTxn;

/// Transaction context for query execution.
///
/// Implements `query::TransactionContext` to provide transaction-scoped
/// document fetching to the query executor. Uses `LensedDocFetcher` to support
/// lens migrations within transactions.
pub struct DbTransactionContext<S: Store> {
    db: Arc<DB<S>>,
    id: String,
    readonly: bool,
    fetcher: Arc<LensedDocFetcher<S>>,
    deferred_acp_mutations: Arc<DeferredAcpMutations>,
    action_lock: Arc<async_lock::Mutex<()>>,
    created_at: Instant,
}

impl<S: Store> DbTransactionContext<S> {
    /// Create a new transaction context.
    pub(crate) fn new(
        db: Arc<DB<S>>,
        id: String,
        readonly: bool,
        fetcher: Arc<LensedDocFetcher<S>>,
        deferred_acp_mutations: Arc<DeferredAcpMutations>,
    ) -> Self {
        Self {
            db,
            id,
            readonly,
            fetcher,
            deferred_acp_mutations,
            action_lock: Arc::new(async_lock::Mutex::new(())),
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
        Arc::new(DbDocMutator::from_shared_txn(
            self.db.clone(),
            self.fetcher.shared_txn(),
        ))
    }

    /// Get the underlying fetcher's shared transaction.
    ///
    /// This is used by `DbTransactionRegistry::set_migration_in_txn` to perform
    /// migration configuration within the transaction context.
    pub(crate) fn fetcher_shared_txn(&self) -> Arc<async_lock::Mutex<Option<DbTxn<S>>>> {
        self.fetcher.shared_txn()
    }

    pub(crate) fn lens_store(&self) -> Arc<dyn lens::TransformStore> {
        self.fetcher.lens_store()
    }

    pub(crate) fn action_lock(&self) -> Arc<async_lock::Mutex<()>> {
        self.action_lock.clone()
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

    fn collection_provider(&self) -> Option<Arc<dyn CollectionProvider>> {
        Some(Arc::new(TxnCollectionProvider::new(
            self.db.clone(),
            self.fetcher.shared_txn(),
        )))
    }

    fn deferred_acp_mutations(&self) -> Option<Arc<DeferredAcpMutations>> {
        Some(self.deferred_acp_mutations.clone())
    }

    fn action_lock(&self) -> Option<Arc<async_lock::Mutex<()>>> {
        Some(self.action_lock.clone())
    }
}
