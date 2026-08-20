//! Streaming variant of the lensed read path.
//!
//! Mirrors Go's `internal/lens/fetcher.go`: migration is a per-document,
//! read-through transform (`FetchNext` → `updateDataStore`), not a
//! whole-collection pass. `LensedDocStream` applies
//! [`LensedDocFetcher::process_document`] to each document as it is pulled
//! from storage, so a bounded consumer (e.g. a `LimitNode`) stops the
//! underlying scan instead of paying for the whole collection.

use async_trait::async_trait;
use document::Document;
use query::doc_stream::DocStream;
use storage::corekv::{IterOptions, Store};

use crate::collection::loader::get_collection_with_lazy_load;
use crate::collection::stream::CollectionDocStream;
use crate::collection::Collection;
use datastore::NamespaceView;

use super::LensedDocFetcher;

/// Pulls documents from storage and applies the lensed fetcher's per-document
/// migration to each one as it is yielded.
struct LensedDocStream<S: Store + 'static> {
    inner: Box<dyn DocStream>,
    fetcher: LensedDocFetcher<S>,
    collection: Collection,
    datastore: NamespaceView,
    has_migrations: bool,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static> DocStream for LensedDocStream<S> {
    async fn next(&mut self) -> query::error::Result<Option<(Document, bool)>> {
        let Some((doc, is_deleted)) = self.inner.next().await? else {
            return Ok(None);
        };
        let processed = self
            .fetcher
            .process_document(doc, &self.collection, &self.datastore, self.has_migrations)
            .await?;
        Ok(Some((processed, is_deleted)))
    }

    async fn close(&mut self) -> query::error::Result<()> {
        self.inner.close().await
    }
}

impl<S: Store + 'static> LensedDocFetcher<S> {
    pub(super) async fn stream_all_with_deleted_impl(
        &self,
        collection_name: &str,
        show_deleted: bool,
    ) -> query::error::Result<Box<dyn DocStream>> {
        let (collection, datastore, systemstore) =
            get_collection_with_lazy_load(&self.txn, collection_name).await?;

        // Full-history check (matching Go), same as the eager paths.
        let (_, has_migrations) = self.load_versions_and_check_migrations(&collection).await?;

        let prefix = collection.collection_key_prefix();
        let prefix_len = prefix.len();
        let opts = IterOptions::new().with_prefix(prefix);
        let iter = datastore
            .iterator(opts)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("storage error: {}", e)))?;

        let inner = CollectionDocStream::new(
            collection.clone(),
            datastore.clone(),
            systemstore,
            iter,
            prefix_len,
            show_deleted,
        );

        Ok(Box::new(LensedDocStream {
            inner: Box::new(inner),
            // Streamed documents are processed one at a time, so per-document
            // deferred write-back (`process_document` → `update_datastore` →
            // `defer_document_write_back`) is the right model here - never
            // `defer_full_scan_write_back`, which discards per-document
            // candidates on the assumption the whole collection was already
            // read (see fetcher.rs's eager paths, which call it up front).
            fetcher: self.stream_clone(),
            collection,
            datastore,
            has_migrations,
        }))
    }
}

impl<S: Store + 'static> LensedDocFetcher<S> {
    /// The seeking counterpart of
    /// [`stream_all_with_deleted_impl`](Self::stream_all_with_deleted_impl).
    /// Identical but for its source: point reads instead of a prefix scan, so
    /// the lens still sees one document at a time.
    pub(super) async fn stream_by_doc_short_ids_impl(
        &self,
        collection_name: &str,
        doc_short_ids: &[u64],
        show_deleted: bool,
    ) -> query::error::Result<Box<dyn DocStream>> {
        let (collection, datastore, systemstore) =
            get_collection_with_lazy_load(&self.txn, collection_name).await?;
        let (_, has_migrations) = self.load_versions_and_check_migrations(&collection).await?;

        Ok(Box::new(LensedDocStream {
            inner: Box::new(crate::collection::stream::ShortIdDocStream::new(
                collection.clone(),
                datastore.clone(),
                systemstore,
                doc_short_ids.to_vec(),
                show_deleted,
            )),
            fetcher: self.stream_clone(),
            collection,
            datastore,
            has_migrations,
        }))
    }
}
