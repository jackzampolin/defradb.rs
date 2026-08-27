//! Transaction context for query execution.

use query::fetcher::CollectionProvider;
use query::mutator::DocMutator;
use query::runner::DocFetcher;
use query::txn::{DeferredAcpMutations, TransactionContext};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use storage::corekv::Store;
use web_time::Instant;

use crate::collection::provider::TxnCollectionProvider;
use crate::database::DB;
use crate::read::lensed::fetcher::LensedDocFetcher;
use crate::txn::DbTxn;
use crate::write::doc::DbDocMutator;
use crate::LensedAutoCommitFetcher;

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
    broadcaster: Option<Arc<dyn crate::event::emission::TxnBroadcaster>>,
    action_lock: Arc<async_lock::Mutex<()>>,
    created_at: Instant,
    last_request_seen: Mutex<Instant>,
}

impl<S: Store> DbTransactionContext<S> {
    /// Create a transaction context. Pass `Some(broadcaster)` to forward
    /// committed writes to P2P peers; pass `None` for the non-P2P case.
    pub(crate) fn new_with_broadcaster(
        db: Arc<DB<S>>,
        id: String,
        readonly: bool,
        fetcher: Arc<LensedDocFetcher<S>>,
        deferred_acp_mutations: Arc<DeferredAcpMutations>,
        broadcaster: Option<Arc<dyn crate::event::emission::TxnBroadcaster>>,
    ) -> Self {
        let now = Instant::now();
        Self {
            db,
            id,
            readonly,
            fetcher,
            deferred_acp_mutations,
            broadcaster,
            action_lock: Arc::new(async_lock::Mutex::new(())),
            created_at: now,
            last_request_seen: Mutex::new(now),
        }
    }

    /// Get the instant when this transaction was created.
    pub fn created_at(&self) -> Instant {
        self.created_at
    }

    /// Mark that a request used this transaction.
    pub(crate) fn touch(&self) {
        match self.last_request_seen.lock() {
            Ok(mut last_request_seen) => {
                *last_request_seen = Instant::now();
            }
            Err(poisoned) => {
                let mut last_request_seen = poisoned.into_inner();
                *last_request_seen = Instant::now();
            }
        }
    }

    /// Get the instant when this transaction last saw a request.
    pub fn last_request_seen(&self) -> Instant {
        match self.last_request_seen.lock() {
            Ok(last_request_seen) => *last_request_seen,
            Err(poisoned) => *poisoned.into_inner(),
        }
    }

    /// Get how long this transaction has been idle.
    pub fn idle_for(&self, now: Instant) -> Duration {
        now.duration_since(self.last_request_seen())
    }
}

impl<S: Store + 'static> DbTransactionContext<S> {
    /// Take the underlying transaction (for commit/rollback).
    ///
    /// After calling this, `is_consumed()` will return `true` and all
    /// fetcher operations will return an error.
    pub async fn take_txn(&self) -> Option<DbTxn<S>> {
        self.fetcher.take_txn().await
    }

    /// Check if the transaction has been consumed (via commit/rollback).
    ///
    /// Returns `true` if `take_txn()` was called and the transaction is
    /// no longer available for queries.
    pub async fn is_consumed(&self) -> bool {
        self.fetcher.is_consumed().await
    }

    /// Persist lazy migrations collected by a successful implicit read.
    ///
    /// The caller must close the read snapshot first. Each collection is then
    /// written through the same guarded, conflict-retrying path as direct
    /// auto-commit reads.
    pub(crate) async fn persist_pending_migrations(&self) -> query::error::Result<()> {
        let pending = self.fetcher.take_pending_write_backs().await;
        let mut by_collection = BTreeMap::new();
        for candidate in pending.documents {
            if pending.full_scans.contains_key(&candidate.collection_name) {
                continue;
            }
            by_collection
                .entry(candidate.collection_name)
                .or_insert_with(Vec::new)
                .push(candidate.write_back);
        }

        if by_collection.is_empty() && pending.full_scans.is_empty() {
            return Ok(());
        }

        let fetcher = LensedAutoCommitFetcher::new(self.db.clone());
        for (collection_name, write_backs) in by_collection {
            let Some(collection) = self.db.get_collection(&collection_name).map_err(|error| {
                query::error::QueryError::execution(format!(
                    "failed to load collection for deferred lens write-back: {}",
                    error
                ))
            })?
            else {
                continue;
            };
            fetcher
                .persist_migrated_documents(&collection, write_backs)
                .await?;
        }
        for (collection_name, include_deleted) in pending.full_scans {
            if include_deleted {
                fetcher.get_all_with_deleted(&collection_name, true).await?;
            } else {
                fetcher.get_all(&collection_name).await?;
            }
        }

        Ok(())
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
        Arc::new(DbDocMutator::from_shared_txn_with_broadcaster(
            self.db.clone(),
            self.fetcher.shared_txn(),
            self.broadcaster.clone(),
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

    pub(crate) async fn invalidate_migration_cache(&self) {
        self.fetcher.invalidate_migration_cache().await;
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
