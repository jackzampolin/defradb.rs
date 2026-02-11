//! Lensed document fetcher that applies schema migrations.
//!
//! This fetcher wraps an inner fetcher and applies lens transforms to documents
//! that are stored with older schema versions.
//!
//! # Migration Flow
//!
//! When a document is fetched:
//! 1. The fetcher loads the document with its stored schema version
//! 2. If the document's version differs from the target collection version
//!    and migrations are registered, the document is transformed
//! 3. Migrated values are cached in the datastore to avoid re-migration
//!
//! # Lazy Migration
//!
//! Documents are migrated on first read, not when schemas are updated.
//! This allows schema updates without rewriting all existing documents.
//! The migrated values and new version are cached in the datastore.

mod fetcher;
mod migration;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::Arc;

use async_lock::Mutex as TokioMutex;
use async_trait::async_trait;
use document::Document;
use lens::{TargetedHistoryLink, TransformStore};
use query::runner::{DocFetcher, FetchByIdsResult};
use storage::corekv::Store;

use crate::txn::DbTxn;

/// Document fetcher that applies lens migrations to documents.
///
/// When documents are fetched from older schema versions, they are
/// transformed to the current (target) schema version using registered
/// lens migrations.
pub struct LensedDocFetcher<S: Store> {
    txn: Arc<TokioMutex<Option<DbTxn<S>>>>,
    #[allow(dead_code)]
    lens_store: Arc<dyn TransformStore>,
    /// Cache of collection version histories keyed by collection name.
    #[allow(dead_code)]
    history_cache: async_lock::RwLock<HashMap<String, HashMap<String, TargetedHistoryLink>>>,
}

impl<S: Store> LensedDocFetcher<S> {
    /// Create a new lensed document fetcher.
    ///
    /// # Arguments
    ///
    /// * `txn` - The database transaction
    /// * `lens_store` - The lens transform store for applying migrations
    #[allow(dead_code)]
    pub(crate) fn new(txn: DbTxn<S>, lens_store: Arc<dyn TransformStore>) -> Self {
        Self {
            txn: Arc::new(TokioMutex::new(Some(txn))),
            lens_store,
            history_cache: async_lock::RwLock::new(HashMap::new()),
        }
    }

    /// Take the transaction out of the fetcher (for commit/rollback).
    #[allow(dead_code)]
    pub(crate) async fn take_txn(&self) -> Option<DbTxn<S>> {
        self.txn.lock().await.take()
    }

    /// Check if the transaction has been consumed.
    pub async fn is_consumed(&self) -> bool {
        self.txn.lock().await.is_none()
    }

    /// Get the shared transaction reference.
    #[allow(dead_code)]
    pub(crate) fn shared_txn(&self) -> Arc<TokioMutex<Option<DbTxn<S>>>> {
        self.txn.clone()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static> DocFetcher for LensedDocFetcher<S> {
    async fn get_all(&self, collection_name: &str) -> query::error::Result<Vec<Document>> {
        self.get_all_impl(collection_name).await
    }

    async fn get_all_with_deleted(
        &self,
        collection_name: &str,
        show_deleted: bool,
    ) -> query::error::Result<Vec<(Document, bool)>> {
        self.get_all_with_deleted_impl(collection_name, show_deleted)
            .await
    }

    async fn get_by_ids(
        &self,
        collection_name: &str,
        doc_ids: &[String],
    ) -> query::error::Result<FetchByIdsResult> {
        self.get_by_ids_impl(collection_name, doc_ids).await
    }

    async fn get_by_field_value(
        &self,
        collection_name: &str,
        field_name: &str,
        value: &str,
    ) -> query::error::Result<Vec<Document>> {
        self.get_by_field_value_impl(collection_name, field_name, value)
            .await
    }

    async fn get_view_cache_items(&self, collection_id: u32) -> query::error::Result<Vec<Vec<u8>>> {
        self.get_view_cache_items_impl(collection_id).await
    }
}
