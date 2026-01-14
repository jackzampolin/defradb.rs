//! Document fetcher for transaction-scoped queries.

use async_trait::async_trait;
use document::Document;
use query::runner::DocFetcher;
use std::collections::HashMap;
use std::sync::Arc;
use storage::corekv::Store;
use tokio::sync::Mutex as TokioMutex;
use tracing::debug;

use crate::collection::Collection;
use crate::txn::DbTxn;

/// Document fetcher that uses a database transaction.
///
/// This fetcher holds a reference to an active transaction and collection
/// definitions, allowing it to fetch documents within the transaction context.
pub struct DbDocFetcher<S: Store> {
    txn: Arc<TokioMutex<Option<DbTxn<S>>>>,
    collections: Arc<HashMap<String, Collection>>,
}

impl<S: Store> DbDocFetcher<S> {
    /// Create a new transaction-scoped document fetcher.
    pub(crate) fn new(txn: DbTxn<S>, collections: Arc<HashMap<String, Collection>>) -> Self {
        Self {
            txn: Arc::new(TokioMutex::new(Some(txn))),
            collections,
        }
    }

    /// Take the transaction out of the fetcher (for commit/rollback).
    pub(crate) async fn take_txn(&self) -> Option<DbTxn<S>> {
        self.txn.lock().await.take()
    }
}

#[async_trait]
impl<S: Store + 'static> DocFetcher for DbDocFetcher<S> {
    async fn get_all(&self, collection_name: &str) -> query::error::Result<Vec<Document>> {
        let collection = self
            .collections
            .get(collection_name)
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Extract the NamespaceView (owned) while holding the lock, then release
        // the lock before awaiting. NamespaceView is Send + Sync so this is safe.
        let datastore = {
            let txn_guard = self.txn.lock().await;
            let db_txn = txn_guard.as_ref().ok_or_else(|| {
                query::error::QueryError::execution("transaction already consumed")
            })?;
            db_txn
                .datastore()
                .map_err(|e| query::error::QueryError::execution(format!("txn error: {}", e)))?
        };

        collection
            .get_all_with_datastore(&datastore)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("storage error: {}", e)))
    }

    async fn get_by_ids(
        &self,
        collection_name: &str,
        doc_ids: &[String],
    ) -> query::error::Result<Vec<Document>> {
        let collection = self
            .collections
            .get(collection_name)
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Extract the NamespaceView (owned) while holding the lock, then release
        // the lock before awaiting. NamespaceView is Send + Sync so this is safe.
        let datastore = {
            let txn_guard = self.txn.lock().await;
            let db_txn = txn_guard.as_ref().ok_or_else(|| {
                query::error::QueryError::execution("transaction already consumed")
            })?;
            db_txn
                .datastore()
                .map_err(|e| query::error::QueryError::execution(format!("txn error: {}", e)))?
        };

        let mut docs = Vec::new();
        for id_str in doc_ids {
            let doc_id = document::DocID::from_string(id_str).map_err(|e| {
                query::error::QueryError::execution(format!("invalid doc ID '{}': {}", id_str, e))
            })?;

            match collection
                .get_with_datastore(&datastore, &doc_id)
                .await
                .map_err(|e| query::error::QueryError::execution(format!("storage error: {}", e)))?
            {
                Some(doc) => docs.push(doc),
                None => {
                    debug!(
                        collection = %collection_name,
                        doc_id = %id_str,
                        "Requested document not found in collection"
                    );
                }
            }
        }

        Ok(docs)
    }
}
