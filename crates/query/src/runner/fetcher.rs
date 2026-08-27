//! FetcherWrapper for converting references to owned DocFetcher.

use async_trait::async_trait;
use bytes::Bytes;
use document::Document;
use std::marker::PhantomData;

use crate::doc_stream::DocStream;
use crate::error::{QueryError, Result};
use crate::fetcher::{CommitsQueryOptions, DocFetcher, FetchByIdsResult};
use crate::planner::index_selection::IndexScanParams;

/// Wrapper to convert a `&dyn DocFetcher` reference into an owned `DocFetcher`.
///
/// This allows passing a fetcher reference to the Planner, which requires
/// `Arc<dyn DocFetcher>`. The wrapper is only valid for the duration of the
/// query execution.
///
/// # Safety Invariants
///
/// 1. **Lifetime**: The original `&dyn DocFetcher` reference MUST outlive all uses
///    of this wrapper. The caller is responsible for ensuring this - currently
///    enforced by only creating and using the wrapper within `execute_nested_select_with_planner`.
///
/// 2. **Thread Safety**: The `Send + Sync` implementations are safe because
///    `DocFetcher: Send + Sync` (see fetcher.rs:65), meaning the underlying data
///    can be safely accessed from any thread. The wrapper merely holds a pointer
///    to data that is already thread-safe.
///
/// 3. **Fat Pointer Layout**: The transmute relies on the standard fat pointer layout
///    `(data_ptr, vtable)` for trait objects, which is stable in practice but not
///    formally guaranteed. Consider using `std::ptr::metadata` when it stabilizes
///    for a safer alternative.
pub(crate) struct FetcherWrapper {
    // Store data pointer and vtable separately to avoid lifetime issues with fat pointers
    data_ptr: *const (),
    vtable: *const (),
    // PhantomData to express the logical lifetime relationship, even though
    // we can't enforce it at compile time due to the pointer erasure
    _phantom: PhantomData<*const dyn DocFetcher>,
}

impl FetcherWrapper {
    /// Create a new FetcherWrapper from a borrowed DocFetcher reference.
    ///
    /// # Safety contract (enforced by caller, not compiler)
    ///
    /// The original `&dyn DocFetcher` MUST outlive this wrapper. Currently
    /// this is guaranteed because the wrapper is only created and consumed
    /// within a single `execute_nested_select_with_planner` call scope.
    pub(crate) fn new(fetcher: &dyn DocFetcher) -> Self {
        let ptr = fetcher as *const dyn DocFetcher;
        // SAFETY: Decompose the fat pointer (data_ptr, vtable_ptr) for storage.
        // This relies on the standard two-word fat pointer layout for trait objects,
        // which is stable in practice across all Rust targets but not yet formally
        // guaranteed by the language spec. When `std::ptr::metadata` stabilizes,
        // this should be replaced with the safe equivalent.
        let (data_ptr, vtable) =
            unsafe { std::mem::transmute::<*const dyn DocFetcher, (*const (), *const ())>(ptr) };
        Self {
            data_ptr,
            vtable,
            _phantom: PhantomData,
        }
    }

    fn get_fetcher(&self) -> &dyn DocFetcher {
        // SAFETY: Reconstruct the fat pointer from stored components.
        // Valid only while the original reference is alive (see safety contract on `new`).
        let ptr = unsafe {
            std::mem::transmute::<(*const (), *const ()), *const dyn DocFetcher>((
                self.data_ptr,
                self.vtable,
            ))
        };
        unsafe { &*ptr }
    }
}

// SAFETY: These implementations are safe because:
// 1. DocFetcher: Send + Sync (the underlying data is thread-safe)
// 2. The wrapper only holds a pointer to already-thread-safe data
// 3. The lifetime invariant (original ref outlives wrapper) is maintained by the caller
unsafe impl Send for FetcherWrapper {}
unsafe impl Sync for FetcherWrapper {}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl DocFetcher for FetcherWrapper {
    async fn get_all(&self, collection_name: &str) -> Result<Vec<Document>> {
        self.get_fetcher()
            .get_all(collection_name)
            .await
            .map_err(|e| {
                QueryError::execution(format!(
                    "fetcher error during planner execution for collection '{}': {}",
                    collection_name, e
                ))
            })
    }

    async fn vector_search(
        &self,
        collection_name: &str,
        index_id: u32,
        query_vector: &[f64],
        k: usize,
        effort: Option<usize>,
    ) -> Result<Vec<u64>> {
        self.get_fetcher()
            .vector_search(collection_name, index_id, query_vector, k, effort)
            .await
    }

    fn supports_vector_search(&self) -> bool {
        self.get_fetcher().supports_vector_search()
    }

    async fn stream_by_doc_short_ids(
        &self,
        collection_name: &str,
        doc_short_ids: &[u64],
        show_deleted: bool,
    ) -> Result<Box<dyn DocStream>> {
        self.get_fetcher()
            .stream_by_doc_short_ids(collection_name, doc_short_ids, show_deleted)
            .await
            .map_err(|e| {
                QueryError::execution(format!(
                    "fetcher error during planner execution for collection '{}' \
                     (get_by_doc_short_ids): {}",
                    collection_name, e
                ))
            })
    }

    async fn get_all_with_deleted(
        &self,
        collection_name: &str,
        show_deleted: bool,
    ) -> Result<Vec<(Document, bool)>> {
        self.get_fetcher()
            .get_all_with_deleted(collection_name, show_deleted)
            .await
            .map_err(|e| {
                QueryError::execution(format!(
                    "fetcher error during planner execution for collection '{}' (get_all_with_deleted): {}",
                    collection_name, e
                ))
            })
    }

    async fn stream_all_with_deleted(
        &self,
        collection_name: &str,
        show_deleted: bool,
    ) -> Result<Box<dyn DocStream>> {
        // Forwards to the wrapped fetcher's own streaming override, so a
        // bounded downstream consumer still short-circuits the underlying
        // scan through this wrapper instead of paying for the whole
        // collection.
        self.get_fetcher()
            .stream_all_with_deleted(collection_name, show_deleted)
            .await
            .map_err(|e| {
                QueryError::execution(format!(
                    "fetcher error during planner execution for collection '{}' (stream_all_with_deleted): {}",
                    collection_name, e
                ))
            })
    }

    async fn get_by_ids(
        &self,
        collection_name: &str,
        doc_ids: &[String],
    ) -> Result<FetchByIdsResult> {
        self.get_fetcher()
            .get_by_ids(collection_name, doc_ids)
            .await
            .map_err(|e| {
                QueryError::execution(format!(
                    "fetcher error during planner execution for collection '{}' (fetching {} doc IDs): {}",
                    collection_name,
                    doc_ids.len(),
                    e
                ))
            })
    }

    async fn get_by_field_value(
        &self,
        collection_name: &str,
        field_name: &str,
        value: &str,
    ) -> Result<Vec<Document>> {
        self.get_fetcher()
            .get_by_field_value(collection_name, field_name, value)
            .await
            .map_err(|e| {
                QueryError::execution(format!(
                    "fetcher error during planner execution for collection '{}' (field lookup {}='{}'): {}",
                    collection_name, field_name, value, e
                ))
            })
    }

    async fn get_by_index_scan(
        &self,
        collection_name: &str,
        params: &IndexScanParams,
    ) -> Result<crate::fetcher::IndexScanResult> {
        self.get_fetcher()
            .get_by_index_scan(collection_name, params)
            .await
            .map_err(|e| {
                QueryError::execution(format!(
                    "fetcher error during planner execution for collection '{}' (index scan on '{}'): {}",
                    collection_name, params.index_name, e
                ))
            })
    }

    fn supports_index_queries(&self) -> bool {
        self.get_fetcher().supports_index_queries()
    }

    async fn get_commits(&self, options: &CommitsQueryOptions) -> Result<Vec<Document>> {
        self.get_fetcher().get_commits(options).await
    }

    async fn get_document_at_cid(
        &self,
        cid: &str,
        expected_doc_id: Option<&str>,
        caller_identity: Option<&identity::Did>,
    ) -> Result<Document> {
        self.get_fetcher()
            .get_document_at_cid(cid, expected_doc_id, caller_identity)
            .await
    }

    async fn get_documents_at_cid(
        &self,
        collection_name: &str,
        cid: &str,
        expected_doc_id: Option<&str>,
        caller_identity: Option<&identity::Did>,
    ) -> Result<Vec<Document>> {
        self.get_fetcher()
            .get_documents_at_cid(collection_name, cid, expected_doc_id, caller_identity)
            .await
    }

    async fn search_fulltext_scored(
        &self,
        collection_name: &str,
        field_name: &str,
        query: &str,
    ) -> Result<std::collections::HashMap<String, f64>> {
        self.get_fetcher()
            .search_fulltext_scored(collection_name, field_name, query)
            .await
            .map_err(|e| {
                QueryError::execution(format!(
                    "fetcher error during fulltext search for collection '{}' field '{}': {}",
                    collection_name, field_name, e
                ))
            })
    }

    async fn get_view_cache_items(&self, collection_id: u32) -> Result<Vec<Bytes>> {
        self.get_fetcher()
            .get_view_cache_items(collection_id)
            .await
            .map_err(|e| {
                QueryError::execution(format!(
                    "fetcher error during view cache retrieval for collection {}: {}",
                    collection_id, e
                ))
            })
    }
}
