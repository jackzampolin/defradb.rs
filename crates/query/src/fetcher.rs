//! Document fetcher abstraction for query execution.
//!
//! This module provides the `DocFetcher` trait which abstracts storage access
//! for query execution, along with result types for handling partial fetches.

use async_trait::async_trait;
use bytes::Bytes;
use document::Document;
use identity::Did;
use storage::corekv::MaybeSendSync;

use crate::doc_stream::DocStream;
use crate::error::Result;
use crate::planner::index_selection::IndexScanParams;

/// Result of fetching documents by ID, including information about missing documents.
#[derive(Debug, Clone)]
pub struct FetchByIdsResult {
    docs: Vec<Document>,
    missing_ids: Vec<String>,
}

impl FetchByIdsResult {
    /// Create a new result with no missing IDs.
    pub fn all_found(docs: Vec<Document>) -> Self {
        Self {
            docs,
            missing_ids: Vec::new(),
        }
    }

    /// Create a new result with some missing IDs.
    pub fn partial(docs: Vec<Document>, missing_ids: Vec<String>) -> Self {
        Self { docs, missing_ids }
    }

    /// Check if all requested documents were found.
    pub fn is_complete(&self) -> bool {
        self.missing_ids.is_empty()
    }

    /// Get the number of documents found.
    pub fn found_count(&self) -> usize {
        self.docs.len()
    }

    /// Get the number of missing documents.
    pub fn missing_count(&self) -> usize {
        self.missing_ids.len()
    }

    /// Get the found documents.
    pub fn docs(&self) -> &[Document] {
        &self.docs
    }

    /// Take ownership of the found documents.
    pub fn into_docs(self) -> Vec<Document> {
        self.docs
    }

    /// Get the IDs that were not found.
    pub fn missing_ids(&self) -> &[String] {
        &self.missing_ids
    }
}

/// Result of an index scan, including raw fetch count and deduplicated document IDs.
///
/// For array-indexed fields, the same document can appear multiple times in the index
/// (once per array element). This struct tracks both:
/// - `raw_fetches`: Total index entries scanned (for explain metrics)
/// - `doc_ids`: Deduplicated document IDs (for actual document fetching)
#[derive(Debug, Clone)]
pub struct IndexScanResult {
    /// Deduplicated document IDs matching the index scan
    doc_ids: Vec<String>,
    /// Raw number of index entries fetched (before deduplication)
    raw_fetches: u64,
}

impl IndexScanResult {
    /// Create a new index scan result with raw count equal to doc_ids length.
    pub fn new(doc_ids: Vec<String>) -> Self {
        let raw_fetches = doc_ids.len() as u64;
        Self {
            doc_ids,
            raw_fetches,
        }
    }

    /// Create a new index scan result with explicit raw fetch count.
    ///
    /// Use this when deduplication was applied and raw_fetches != doc_ids.len()
    pub fn with_raw_count(doc_ids: Vec<String>, raw_fetches: u64) -> Self {
        Self {
            doc_ids,
            raw_fetches,
        }
    }

    /// Get the deduplicated document IDs.
    pub fn doc_ids(&self) -> &[String] {
        &self.doc_ids
    }

    /// Take ownership of the document IDs.
    pub fn into_doc_ids(self) -> Vec<String> {
        self.doc_ids
    }

    /// Get the raw number of index entries fetched (before deduplication).
    ///
    /// This is used for explain metrics (indexFetches count).
    pub fn raw_fetches(&self) -> u64 {
        self.raw_fetches
    }
}

/// Storage abstraction for fetching documents.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait DocFetcher: MaybeSendSync {
    /// Get all documents from a collection (excludes deleted documents).
    async fn get_all(&self, collection_name: &str) -> Result<Vec<Document>>;

    /// Get all documents from a collection with their deletion status.
    ///
    /// If `show_deleted` is true, returns all documents including deleted ones.
    /// If `show_deleted` is false, returns only non-deleted documents.
    /// Each document is paired with a boolean indicating if it's deleted.
    ///
    /// Default implementation calls `get_all` and returns all docs as non-deleted.
    async fn get_all_with_deleted(
        &self,
        collection_name: &str,
        _show_deleted: bool,
    ) -> Result<Vec<(Document, bool)>> {
        let docs = self.get_all(collection_name).await?;
        Ok(docs.into_iter().map(|d| (d, false)).collect())
    }

    /// Stream named documents by short id, without scanning the collection.
    ///
    /// A stream rather than a `Vec` for the same reason
    /// [`stream_all_with_deleted`](Self::stream_all_with_deleted) is one: the
    /// caller decides how many it pulls, and a consumer that stops early stops
    /// the reads. Nothing here holds more than one document.
    ///
    /// Required, not defaulted. A default is how a delegating wrapper silently
    /// loses the ability: `FetcherWrapper` forwards only what it overrides, so
    /// a default would have left the query runner unable to seek while every
    /// layer beneath it could.
    ///
    /// Absent ids are skipped, since a caller holding an id from an index may
    /// hold one whose document has since gone. Documents arrive in the order
    /// asked for.
    async fn stream_by_doc_short_ids(
        &self,
        collection_name: &str,
        doc_short_ids: &[u64],
        show_deleted: bool,
    ) -> Result<Box<dyn crate::doc_stream::DocStream>>;

    /// Stream documents from a collection with their deletion status.
    ///
    /// Unlike [`Self::get_all_with_deleted`], the returned stream yields one
    /// document per call, so a consumer that stops pulling stops the work.
    ///
    /// Required rather than defaulted: an eager default (fetch everything,
    /// wrap it in a [`crate::doc_stream::VecStream`]) is correct, so it passes
    /// every test while silently costing a full collection scan per limited
    /// query. Fetchers with no real streaming source should say so explicitly
    /// with `Ok(Box::new(VecStream::new(self.get_all_with_deleted(..).await?)))`.
    async fn stream_all_with_deleted(
        &self,
        collection_name: &str,
        show_deleted: bool,
    ) -> Result<Box<dyn DocStream>>;

    /// Get documents by their IDs.
    ///
    /// Returns both the found documents and the IDs that were not found.
    /// This allows callers to handle missing documents appropriately.
    async fn get_by_ids(
        &self,
        collection_name: &str,
        doc_ids: &[String],
    ) -> Result<FetchByIdsResult>;

    /// Get documents by a field value (for FK lookups).
    ///
    /// This method is optimized for type joins - it looks up documents where
    /// a specific field equals a given value. Implementations may use indexes
    /// for efficient lookups when available.
    ///
    /// # Arguments
    ///
    /// * `collection_name` - The collection to search
    /// * `field_name` - The field to match (e.g., "author_id" for FK lookups)
    /// * `value` - The value to match against
    ///
    /// # Returns
    ///
    /// All documents where the field equals the given value.
    async fn get_by_field_value(
        &self,
        collection_name: &str,
        field_name: &str,
        value: &str,
    ) -> Result<Vec<Document>>;

    /// Fetch commits from the _commits system collection.
    ///
    /// This method fetches commit history from the headstore and blockstore.
    /// Default implementation returns an error - implementations that support
    /// commits queries should override this.
    ///
    /// # Arguments
    ///
    /// * `options` - Query options (docID, cid, depth, fieldName filters)
    ///
    /// # Returns
    ///
    /// Commit documents with fields: cid, height, fieldName, docID, delta,
    /// collectionVersionId, links, heads, signature.
    async fn get_commits(&self, options: &CommitsQueryOptions) -> Result<Vec<Document>> {
        let _ = options;
        Err(crate::error::QueryError::execution(
            "_commits queries are not supported by this fetcher".to_string(),
        ))
    }

    /// Get documents using an index scan.
    ///
    /// This method uses a secondary index to efficiently fetch documents matching
    /// the index scan parameters. It returns deduplicated document IDs and the
    /// raw fetch count (for explain metrics).
    ///
    /// # Arguments
    ///
    /// * `collection_name` - The collection to query
    /// * `params` - Index scan parameters specifying the index and scan type
    ///
    /// # Returns
    ///
    /// An `IndexScanResult` containing:
    /// - Deduplicated document IDs matching the index scan
    /// - Raw fetch count (total index entries scanned, before deduplication)
    ///
    /// The caller can then use `get_by_ids` to fetch the full documents.
    ///
    /// When `params.cursor_seek` is `Some`, the implementation must resolve any
    /// `boundary_doc_id` into its storage-local key suffix, then position the
    /// iterator at `cursor_seek.seek_key`, honoring `inclusive` and `reversed`.
    /// This is used by cursor pagination to seek directly into an index without
    /// offset-scan.
    ///
    /// Default implementation returns an error - implementations that support
    /// index queries should override this.
    async fn get_by_index_scan(
        &self,
        collection_name: &str,
        params: &IndexScanParams,
    ) -> Result<IndexScanResult> {
        let _ = (collection_name, params);
        Err(crate::error::QueryError::execution(
            "Index-based queries are not supported by this fetcher".to_string(),
        ))
    }

    /// Check if this fetcher supports index-based queries.
    ///
    /// Returns true if `get_by_index_scan` is implemented and functional.
    fn supports_index_queries(&self) -> bool {
        false
    }

    /// Nearest documents to `query_vector` under a vector index, nearest first.
    ///
    /// Returns document short ids, which is what a scan can be narrowed by.
    /// `admit` restricts what may be *returned* without restricting what may be
    /// traversed, so a filtered query still gets a full `k` whenever `k`
    /// matching documents exist.
    ///
    /// Follows the same capability shape as
    /// [`get_by_index_scan`](Self::get_by_index_scan): defaulted to an error
    /// and gated by [`supports_vector_search`](Self::supports_vector_search),
    /// so a fetcher without storage access is never asked.
    async fn vector_search(
        &self,
        collection_name: &str,
        index_id: u32,
        query_vector: &[f64],
        k: usize,
        effort: Option<usize>,
    ) -> Result<Vec<u64>> {
        let _ = (collection_name, index_id, query_vector, k, effort);
        Err(crate::error::QueryError::execution(
            "Vector search is not supported by this fetcher".to_string(),
        ))
    }

    /// Whether [`vector_search`](Self::vector_search) can be used.
    fn supports_vector_search(&self) -> bool {
        false
    }

    /// Get a document at a specific historical version (CID-based time-travel query).
    ///
    /// This method reconstructs the document as it existed when the commit at `cid`
    /// was created by walking the merkle DAG backwards and replaying CRDT deltas.
    ///
    /// # Arguments
    ///
    /// * `cid` - The CID of the commit to reconstruct state at
    /// * `expected_doc_id` - Optional document ID to validate the CID belongs to
    /// * `caller_identity` - Verified identity requesting the historical data
    ///
    /// # Returns
    ///
    /// The document state at that commit, or an error if:
    /// - The CID is invalid format: `"invalid cid: {parse_error}"`
    /// - The CID doesn't exist or doesn't belong to the document:
    ///   `"cid either does not exist or belong to document"`
    ///
    /// Default implementation returns an error - implementations that support
    /// CID-based queries should override this.
    async fn get_document_at_cid(
        &self,
        cid: &str,
        expected_doc_id: Option<&str>,
        caller_identity: Option<&Did>,
    ) -> Result<Document> {
        let _ = (cid, expected_doc_id, caller_identity);
        Err(crate::error::QueryError::execution(
            "CID-based time-travel queries are not supported by this fetcher".to_string(),
        ))
    }

    /// Reconstruct documents at the specified CID, scoped to a collection.
    ///
    /// For document-level CIDs, returns a single document.
    /// For collection-level CIDs (branchable collections), returns all
    /// documents visible at that collection state.
    ///
    /// `collection_name` is the queried collection: documents belonging to
    /// another collection are excluded, so a foreign collection's commit CID
    /// yields an empty result (Go parity).
    async fn get_documents_at_cid(
        &self,
        collection_name: &str,
        cid: &str,
        expected_doc_id: Option<&str>,
        caller_identity: Option<&Did>,
    ) -> Result<Vec<Document>> {
        let _ = collection_name;
        // Default: delegate to single-document method
        let doc = self
            .get_document_at_cid(cid, expected_doc_id, caller_identity)
            .await?;
        Ok(vec![doc])
    }

    /// Get all cached view items for a materialized view.
    ///
    /// Returns serialized view items (JSON bytes) that can be deserialized
    /// using `unmarshal_view_item`.
    ///
    /// # Arguments
    ///
    /// * `collection_id` - The collection root ID of the materialized view
    ///
    /// # Returns
    ///
    /// A vector of serialized view items (each as JSON bytes).
    ///
    /// Default implementation returns an empty vector - implementations that support
    /// materialized views should override this.
    async fn get_view_cache_items(&self, collection_id: u32) -> Result<Vec<Bytes>> {
        let _ = collection_id;
        Ok(Vec::new())
    }

    /// Compute BM25 scores for documents matching a full-text search query.
    ///
    /// Uses the stored inverted index for the given field to compute scores
    /// without re-tokenizing document text.
    async fn search_fulltext_scored(
        &self,
        collection_name: &str,
        field_name: &str,
        query: &str,
    ) -> Result<std::collections::HashMap<String, f64>> {
        let _ = (collection_name, field_name, query);
        Ok(std::collections::HashMap::new())
    }
}

/// Options for _commits queries
#[derive(Debug, Clone, Default)]
pub struct CommitsQueryOptions {
    /// Filter by document ID
    pub doc_id: Option<String>,
    /// Filter by specific CID
    pub cid: Option<String>,
    /// Maximum depth to traverse (None = unlimited)
    pub depth: Option<u64>,
    /// Inclusive minimum commit height for indexed range scans
    pub height_start: Option<u64>,
    /// Exclusive maximum commit height for indexed range scans
    pub height_end: Option<u64>,
    /// Filter by field name
    pub field_name: Option<String>,
}

/// Provides collection schemas on-demand.
///
/// This trait abstracts collection resolution, allowing the QueryRunner to
pub use crate::collection_provider::{CollectionProvider, StaticCollectionProvider};
