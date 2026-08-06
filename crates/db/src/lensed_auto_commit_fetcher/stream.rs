//! Streaming variant of the lensed auto-commit read path.
//!
//! Like `lensed_fetcher::stream`, this applies migration per document as it
//! is pulled from storage (Go's read-through model) instead of after a full
//! collection read. Write-back stays batched as the eager path batches it
//! (`process_document_with_bounded_write_back` flushes every
//! `migration_write_back_batch_size` documents via its own guarded write
//! transaction), with the partial batch flushed when the stream ends - either
//! by exhaustion or by [`DocStream::close`], which a consumer that stops early
//! must call.

use std::collections::HashMap;

use async_trait::async_trait;
use document::Document;
use lens::TargetedHistoryLink;
use query::doc_stream::DocStream;
use storage::corekv::{IterOptions, Store};
use tracing::warn;

use crate::collection::Collection;
use crate::collection_stream::CollectionDocStream;
use crate::txn::DbTxn;

use super::migration::MigrationWriteBack;
use super::LensedAutoCommitFetcher;

/// Pulls documents from storage and applies the auto-commit fetcher's
/// per-document, batched-write-back migration to each one as it is yielded.
///
/// Owns its own read-only transaction (auto-commit fetchers open one per
/// operation): `release_read_txn` drops the inner stream first so its
/// `NamespaceView`s give up their `Arc<SharedTxn>` clones, then discards the
/// transaction - `BasicTxn::discard` requires sole ownership of that Arc.
///
/// `txn` is wrapped in a `std::sync::Mutex` purely so `DbTxn`'s non-`Sync`
/// callback storage doesn't stop this struct satisfying `DocStream`'s
/// `MaybeSendSync` bound; every access goes through `&mut self` via
/// `get_mut`, so it never actually locks.
struct LensedAutoCommitDocStream<S: Store + 'static> {
    inner: Option<Box<dyn DocStream>>,
    txn: std::sync::Mutex<Option<DbTxn<S>>>,
    fetcher: LensedAutoCommitFetcher<S>,
    collection: Collection,
    migration_generation: u64,
    has_migrations: bool,
    preloaded_history: Option<HashMap<String, TargetedHistoryLink>>,
    write_backs: Vec<MigrationWriteBack>,
}

impl<S: Store + 'static> LensedAutoCommitDocStream<S> {
    fn release_read_txn(&mut self) {
        self.inner = None;
        let slot = self
            .txn
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(txn) = slot.take() {
            if let Err(e) = txn.discard() {
                warn!(error = %e, "failed to discard lensed auto-commit stream transaction");
            }
        }
    }
}

impl<S: Store + 'static> Drop for LensedAutoCommitDocStream<S> {
    fn drop(&mut self) {
        self.release_read_txn();
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static> DocStream for LensedAutoCommitDocStream<S> {
    async fn next(&mut self) -> query::error::Result<Option<(Document, bool)>> {
        let pulled = match self.inner.as_mut() {
            Some(inner) => inner.next().await?,
            None => None,
        };

        let Some((doc, is_deleted)) = pulled else {
            self.close().await?;
            return Ok(None);
        };

        let processed = self
            .fetcher
            .process_document_with_bounded_write_back(
                doc,
                &self.collection,
                self.migration_generation,
                self.has_migrations,
                &self.preloaded_history,
                &mut self.write_backs,
            )
            .await?;
        Ok(Some((processed, is_deleted)))
    }

    /// Release the read transaction and flush whatever partial write-back
    /// batch remains, matching the eager path's final
    /// `persist_migrated_documents` call after its loop. Called both on
    /// exhaustion and by consumers that stop pulling early; taking the buffer
    /// first makes a second call a no-op.
    async fn close(&mut self) -> query::error::Result<()> {
        let write_backs = std::mem::take(&mut self.write_backs);
        self.release_read_txn();
        self.fetcher
            .persist_migrated_documents(&self.collection, write_backs)
            .await
    }
}

impl<S: Store + 'static> LensedAutoCommitFetcher<S> {
    pub(super) async fn stream_all_with_deleted_impl(
        &self,
        collection_name: &str,
        show_deleted: bool,
    ) -> query::error::Result<Box<dyn DocStream>> {
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        let (migration_generation, has_migrations, preloaded_history) =
            self.load_migration_context(&collection).await?;

        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        let datastore = txn.datastore().map_err(|e| {
            query::error::QueryError::execution(format!(
                "failed to get datastore for collection '{}': {}",
                collection_name, e
            ))
        })?;
        let systemstore = txn.systemstore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get systemstore: {}", e))
        })?;

        let prefix = collection.collection_key_prefix();
        let prefix_len = prefix.len();
        let opts = IterOptions::new().with_prefix(prefix);
        let iter = datastore
            .iterator(opts)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("storage error: {}", e)))?;

        let inner = CollectionDocStream::new(
            collection.clone(),
            datastore,
            systemstore,
            iter,
            prefix_len,
            show_deleted,
        );

        Ok(Box::new(LensedAutoCommitDocStream {
            inner: Some(Box::new(inner)),
            txn: std::sync::Mutex::new(Some(txn)),
            fetcher: self.clone_for_stream(),
            collection,
            migration_generation,
            has_migrations,
            preloaded_history,
            write_backs: Vec::with_capacity(self.db.options().migration_write_back_batch_size()),
        }))
    }
}
