//! CollectionProvider implementations for the database.
//!
//! This module implements the `CollectionProvider` trait from the query crate,
//! enabling on-demand collection resolution from the database at query time.
//!
//! Two providers are available:
//! - `DbCollectionProvider`: reads from the process-wide collection cache
//! - `TxnCollectionProvider`: reads from a transaction's systemstore first,
//!   falling back to the process-wide cache for schemas not yet in the transaction

use async_lock::Mutex as AsyncMutex;
use async_trait::async_trait;
use query::error::{QueryError, Result as QueryResult};
use query::fetcher::CollectionProvider;
use schema::CollectionVersion;
use std::sync::Arc;
use storage::corekv::{Key, Store};

use crate::database::DB;
use crate::txn::DbTxn;

/// Wrapper around Arc<DB> that implements CollectionProvider.
///
/// This enables the QueryRunner to resolve collections from the database
/// at query time, ensuring newly added schemas are immediately available.
pub struct DbCollectionProvider<S: Store + 'static> {
    db: Arc<DB<S>>,
}

impl<S: Store + 'static> DbCollectionProvider<S> {
    /// Create a new database collection provider.
    pub fn new(db: Arc<DB<S>>) -> Self {
        Self { db }
    }

    /// Create a new provider wrapped in an Arc.
    pub fn new_arc(db: Arc<DB<S>>) -> Arc<Self> {
        Arc::new(Self::new(db))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static> CollectionProvider for DbCollectionProvider<S> {
    async fn get_collection(&self, name: &str) -> QueryResult<Option<Arc<CollectionVersion>>> {
        match self.db.get_collection(name) {
            Ok(Some(coll)) => Ok(Some(Arc::new(coll.schema().clone()))),
            Ok(None) => Ok(None),
            Err(e) => Err(QueryError::execution(e.to_string())),
        }
    }

    async fn list_collections(&self) -> QueryResult<Vec<String>> {
        self.db
            .list_collections()
            .map_err(|e| QueryError::execution(e.to_string()))
    }
}

/// Transaction-aware collection provider.
///
/// Reads from the transaction's systemstore first (which includes uncommitted
/// writes like newly added schemas), then falls back to the process-wide cache.
pub struct TxnCollectionProvider<S: Store + 'static> {
    db: Arc<DB<S>>,
    shared_txn: Arc<AsyncMutex<Option<DbTxn<S>>>>,
}

impl<S: Store + 'static> TxnCollectionProvider<S> {
    /// Create a new transaction-aware collection provider.
    pub fn new(db: Arc<DB<S>>, shared_txn: Arc<AsyncMutex<Option<DbTxn<S>>>>) -> Self {
        Self { db, shared_txn }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static> CollectionProvider for TxnCollectionProvider<S> {
    async fn get_collection(&self, name: &str) -> QueryResult<Option<Arc<CollectionVersion>>> {
        let txn_guard = self.shared_txn.lock().await;
        if let Some(txn) = txn_guard.as_ref() {
            let systemstore = txn
                .systemstore()
                .map_err(|e| QueryError::execution(e.to_string()))?;

            let name_key = storage::keys::systemstore::CollectionNameKey::new(name);
            let maybe_version_id = systemstore
                .get(&name_key.bytes())
                .await
                .map_err(|e| QueryError::execution(e.to_string()))?;

            if let Some(data) = maybe_version_id {
                let version_id = match String::from_utf8(data.clone()) {
                    Ok(vid) if !vid.starts_with('{') => vid,
                    _ => {
                        let schema: CollectionVersion = serde_json::from_slice(&data)
                            .map_err(|e| QueryError::execution(e.to_string()))?;
                        return Ok(Some(Arc::new(schema)));
                    }
                };

                let collection_key = storage::keys::systemstore::CollectionKey::new(&version_id);
                if let Some(data) = systemstore
                    .get(&collection_key.bytes())
                    .await
                    .map_err(|e| QueryError::execution(e.to_string()))?
                {
                    let schema: CollectionVersion = serde_json::from_slice(&data)
                        .map_err(|e| QueryError::execution(e.to_string()))?;
                    return Ok(Some(Arc::new(schema)));
                }
            }
        }
        drop(txn_guard);

        match self.db.get_collection(name) {
            Ok(Some(coll)) => Ok(Some(Arc::new(coll.schema().clone()))),
            Ok(None) => Ok(None),
            Err(e) => Err(QueryError::execution(e.to_string())),
        }
    }

    async fn list_collections(&self) -> QueryResult<Vec<String>> {
        let mut names: std::collections::HashSet<String> = self
            .db
            .list_collections()
            .map_err(|e| QueryError::execution(e.to_string()))?
            .into_iter()
            .collect();

        let txn_guard = self.shared_txn.lock().await;
        if let Some(txn) = txn_guard.as_ref() {
            let systemstore = txn
                .systemstore()
                .map_err(|e| QueryError::execution(e.to_string()))?;

            let prefix = storage::keys::systemstore::CollectionNameKey::name_prefix();
            let opts = storage::corekv::IterOptions::new().with_prefix(prefix.clone());
            let mut iter = systemstore
                .iterator(opts)
                .await
                .map_err(|e| QueryError::execution(e.to_string()))?;

            let prefix_str =
                String::from_utf8(prefix).map_err(|e| QueryError::execution(e.to_string()))?;

            while let Some(pair) = iter
                .next()
                .await
                .map_err(|e| QueryError::execution(e.to_string()))?
            {
                if let Ok(key_str) = String::from_utf8(pair.key.to_vec()) {
                    if let Some(name) = key_str.strip_prefix(&prefix_str) {
                        names.insert(name.to_string());
                    }
                }
            }

            iter.close()
                .await
                .map_err(|e| QueryError::execution(e.to_string()))?;
        }

        Ok(names.into_iter().collect())
    }
}
