//! ScanNode for scanning collection documents

use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::Arc;

use schema::CollectionVersion;

use tracing::debug;

use crate::doc_stream::DocStream;
use crate::fetcher::DocFetcher;
use crate::planner::vector_routing::VectorRoute;
use crate::planner::{Doc, ExecInfo, PlanNode};
use query_types::document::{document_to_plan_doc_with_status, DocumentMapping};
use query_types::error::Result;
use query_types::mapper::Filter;

/// ScanNode scans documents from a collection.
///
/// This is the primary data source node in query plans.
/// It reads documents from storage and yields them to parent nodes.
///
/// # Data Loading
///
/// ScanNode can obtain documents in two ways:
/// 1. Pre-loaded via `with_docs()` - for testing, or when an earlier stage
///    (a docID lookup, an index scan) already produced the document set
/// 2. Streamed from a `DocFetcher` - `init()` opens a stream and each
///    `next()` pulls a single document from it
///
/// The streaming path is what keeps a `LIMIT` cheap: nothing below reads
/// further once the parent stops pulling, so the cost is proportional to the
/// documents actually consumed rather than to collection size.
pub struct ScanNode {
    /// Collection schema
    collection: CollectionVersion,
    /// Document mapping for field positions
    document_mapping: DocumentMapping,
    /// Optional filter to apply during scan
    filter: Option<Filter>,
    /// Optional document IDs supplied separately from the scan filter
    doc_ids: Option<Vec<String>>,
    /// Documents this scan is restricted to, by short id.
    ///
    /// Unlike [`ScanNode::doc_ids`], which drops non-matching documents after
    /// reading them, this narrows the read itself: the fetcher seeks each one.
    /// A query that already knows which documents it wants, such as one
    /// narrowed by a vector index, costs what it asked for rather than the
    /// size of the collection.
    doc_short_ids: Option<Vec<u64>>,
    /// Whether the restriction came from a vector index search.
    vector_indexed: bool,
    /// The vector index this scan draws its candidates from, when routed.
    ///
    /// The search runs in `init()` rather than at plan time because it is
    /// async, and it lives here rather than in the caller so that every path
    /// that builds a plan gets it: a similarity query always goes through the
    /// planner, so a narrowing done anywhere else never runs.
    vector_route: Option<VectorRoute>,
    /// Candidate short ids already streamed, so widening never re-reads one.
    vector_seen: HashSet<u64>,
    /// Set once the index returned fewer candidates than asked for.
    vector_exhausted: bool,
    /// Documents this scan produced, which is what a page of `k` counts.
    emitted: usize,
    /// The `k` the last search asked for.
    vector_k: usize,
    /// Whether to show deleted documents
    show_deleted: bool,
    /// Current document
    current_doc: Doc,
    /// Iterator state (simulated for now)
    docs: Vec<Doc>,
    /// Current position in docs
    position: usize,
    /// Streaming source, used when no docs were pre-provided.
    stream: Option<Box<dyn DocStream>>,
    /// Whether the node has been initialized
    initialized: bool,
    /// Optional fetcher for loading documents on-demand
    fetcher: Option<Arc<dyn DocFetcher>>,
    /// Whether docs were explicitly provided (even if empty)
    docs_provided: bool,
    /// Execution statistics for explain execute mode
    exec_info: ExecInfo,
    /// Number of fields per document (for fieldFetches calculation)
    #[allow(dead_code)]
    fields_per_doc: usize,
}

impl ScanNode {
    /// Create a new scan node for a collection
    pub fn new(collection: CollectionVersion, document_mapping: DocumentMapping) -> Self {
        // Count storable fields from the collection schema (matches Go's field fetch counting).
        // Go counts KV pairs from storage, which corresponds to fields with a non-empty FieldID.
        // This excludes virtual relation objects (no FieldID) and system fields.
        let fields_per_doc = collection
            .fields
            .iter()
            .filter(|f| !f.id.is_empty())
            .count();
        Self {
            collection,
            document_mapping,
            filter: None,
            doc_ids: None,
            doc_short_ids: None,
            vector_indexed: false,
            vector_route: None,
            vector_seen: HashSet::new(),
            vector_exhausted: false,
            emitted: 0,
            vector_k: 0,
            show_deleted: false,
            current_doc: Doc::default(),
            docs: Vec::new(),
            position: 0,
            stream: None,
            initialized: false,
            fetcher: None,
            docs_provided: false,
            exec_info: ExecInfo::default(),
            fields_per_doc,
        }
    }

    /// Set the filter for this scan
    pub fn with_filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Set document IDs supplied separately from the scan filter.
    /// Restricts this scan to the given documents, read by seeking rather
    /// than by scanning and discarding.
    ///
    /// An empty list means no documents match, which is different from no
    /// restriction: the scan yields nothing rather than everything.
    ///
    /// Applies to the fetcher path only. Pre-loaded documents carry no short
    /// id, so a restriction cannot address them; `ScanSource` yields either
    /// documents or a fetcher, never both, so the two do not meet.
    pub fn with_doc_short_ids(mut self, doc_short_ids: Vec<u64>) -> Self {
        self.doc_short_ids = Some(doc_short_ids);
        self
    }

    /// Marks the restriction as coming from a vector index, which explain
    /// reports as one index fetch.
    pub fn as_vector_indexed(mut self) -> Self {
        self.vector_indexed = true;
        self
    }

    /// Draw this scan's candidates from a vector index.
    pub fn with_vector_route(mut self, route: VectorRoute) -> Self {
        self.vector_route = Some(route);
        self
    }

    /// Ask the index for the next, larger batch of candidates and stream the
    /// ones not already seen.
    ///
    /// A filter is applied to documents, not to the graph walk, so the nearest
    /// `k` overall can contain fewer than `k` matches. Asking for a wider `k`
    /// and continuing is what makes a filtered similarity query return a full
    /// page instead of whatever survived filtering the first `k`. Widening
    /// stops once the index reports fewer candidates than asked for, or offers
    /// nothing it has not already offered, so an unsatisfiable filter costs one
    /// pass over the collection rather than looping.
    async fn open_vector_stream(&mut self) -> Result<bool> {
        let (Some(route), Some(fetcher)) = (self.vector_route.clone(), self.fetcher.clone()) else {
            return Ok(false);
        };
        if self.vector_exhausted {
            return Ok(false);
        }

        let next_k = if self.vector_k == 0 {
            route.k
        } else {
            self.vector_k.saturating_mul(2)
        };

        let candidates = fetcher
            .vector_search(
                &self.collection.name,
                route.index_id,
                &route.query_vector,
                next_k,
                None,
            )
            .await?;

        self.vector_exhausted = candidates.len() < next_k;
        self.vector_k = next_k;

        let fresh: Vec<u64> = candidates
            .into_iter()
            .filter(|id| self.vector_seen.insert(*id))
            .collect();

        debug!(
            collection = %self.collection.name,
            index_id = route.index_id,
            k = next_k,
            fresh = fresh.len(),
            exhausted = self.vector_exhausted,
            "vector index candidates"
        );

        if fresh.is_empty() {
            // Nothing new at this width. Widening again would re-read the same
            // ids, so the index has no more to offer this query.
            self.vector_exhausted = true;
            return Ok(false);
        }

        // A stream flushes deferred per-document work on close, so the exhausted
        // one is closed before it is replaced rather than dropped: for the
        // auto-commit fetcher that flush releases the read transaction and
        // persists lens write-backs.
        if let Some(mut previous) = self.stream.take() {
            previous.close().await?;
        }
        self.stream = Some(
            fetcher
                .stream_by_doc_short_ids(&self.collection.name, &fresh, self.show_deleted)
                .await?,
        );

        // Only once a stream is actually open, so an explain never names an
        // index for a scan that fell through to reading the whole collection.
        self.vector_indexed = true;
        self.exec_info.indexes_fetched += 1;
        Ok(true)
    }

    /// The vector index serving this scan, by name.
    ///
    /// `indexFetches` alone cannot say which index was used, so a scan narrowed
    /// by a vector index names it. Its absence means the scan was not routed,
    /// which is the difference between "the index answered this" and "some
    /// index was read".
    fn vector_index_name(&self) -> Option<&str> {
        if !self.vector_indexed {
            return None;
        }
        let route = self.vector_route.as_ref()?;
        self.collection
            .indexes
            .iter()
            .find(|index| index.id == route.index_id)
            .map(|index| index.name.as_str())
    }

    /// Whether a short page is worth asking the index to widen for.
    ///
    /// Counted at this scan, which is sound only because the planner refuses to
    /// route a query whose rows can be rejected above it: an `OrderNode` blocks
    /// and consumes everything, so widening cannot be driven by the consumer
    /// still pulling.
    fn wants_more_candidates(&self) -> bool {
        self.vector_route
            .as_ref()
            .is_some_and(|route| !self.vector_exhausted && self.emitted < route.k)
    }

    pub fn with_doc_ids(mut self, doc_ids: Vec<String>) -> Self {
        // Only set if non-empty; empty means scan entire collection
        if !doc_ids.is_empty() {
            self.doc_ids = Some(doc_ids);
        }
        self
    }

    /// Set whether to include deleted documents
    pub fn with_show_deleted(mut self, show_deleted: bool) -> Self {
        self.show_deleted = show_deleted;
        self
    }

    /// Set documents directly (for testing or in-memory operations).
    ///
    /// Providing an empty vector is valid and represents an empty collection.
    pub fn with_docs(mut self, docs: Vec<Doc>) -> Self {
        self.docs = docs;
        self.docs_provided = true;
        self
    }

    /// Set a document fetcher for on-demand data loading.
    ///
    /// When set, the node will fetch documents from storage during `init()`
    /// if no documents were pre-loaded via `with_docs()`.
    pub fn with_fetcher(mut self, fetcher: Arc<dyn DocFetcher>) -> Self {
        self.fetcher = Some(fetcher);
        self
    }

    /// Get the collection
    pub fn collection(&self) -> &CollectionVersion {
        &self.collection
    }

    /// Get the collection name
    pub fn collection_name(&self) -> &str {
        &self.collection.name
    }

    /// Get the storage prefix for this collection.
    fn collection_prefix(&self) -> u32 {
        self.collection.resolved_root_id()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PlanNode for ScanNode {
    async fn init(&mut self) -> Result<()> {
        self.position = 0;
        // Reset execution stats
        self.exec_info = ExecInfo::default();

        // If docs weren't provided and we have a fetcher, open a stream over the
        // collection instead of materializing it - callers that stop pulling
        // (e.g. a satisfied LimitNode) stop the underlying fetch.
        self.vector_seen.clear();
        self.vector_exhausted = false;
        self.vector_k = 0;
        self.emitted = 0;

        if !self.docs_provided {
            if self.vector_route.is_some() && self.open_vector_stream().await? {
                self.initialized = true;
                return Ok(());
            }
            if let Some(ref fetcher) = self.fetcher {
                self.stream = Some(match self.doc_short_ids.as_deref() {
                    Some(ids) => {
                        fetcher
                            .stream_by_doc_short_ids(&self.collection.name, ids, self.show_deleted)
                            .await?
                    }
                    None => {
                        fetcher
                            .stream_all_with_deleted(&self.collection.name, self.show_deleted)
                            .await?
                    }
                });
            } else {
                // No docs provided and no fetcher - this is a programming error.
                // Either pre-load docs with with_docs() or attach a fetcher with with_fetcher().
                return Err(query_types::error::QueryError::internal(format!(
                    "ScanNode for collection '{}' has no documents and no fetcher - \
                     this indicates a bug in query planning or test setup",
                    self.collection.name
                )));
            }
        }

        self.initialized = true;
        debug!(
            collection = %self.collection.name,
            "ScanNode initialized"
        );
        Ok(())
    }

    async fn start(&mut self) -> Result<()> {
        Ok(())
    }

    async fn next(&mut self) -> Result<bool> {
        if !self.initialized {
            return Err(query_types::error::QueryError::execution(
                "ScanNode.next() called before init()",
            ));
        }

        // Track iteration (Go counts each call to next, including final false)
        self.exec_info.iterations += 1;

        // The stream branch already yields an owned `Doc` per pull, so it moves
        // straight into `current_doc` on a match. The Vec branch instead holds a
        // borrow through the skip checks and only clones the document that
        // actually passes them - cloning eagerly here would pay for every
        // examined document instead of only the returned ones.
        if self.stream.is_some() {
            loop {
                let pulled = match self.stream.as_mut() {
                    Some(stream) => stream.next().await?,
                    None => None,
                };
                let doc = match pulled {
                    Some((document, is_deleted)) => document_to_plan_doc_with_status(
                        &document,
                        &self.document_mapping,
                        is_deleted,
                    )?,
                    None => {
                        if self.wants_more_candidates() && self.open_vector_stream().await? {
                            continue;
                        }
                        return Ok(false);
                    }
                };

                // Track document fetch
                self.exec_info.docs_fetched += 1;
                // Track field fetches (actual stored fields in this document)
                self.exec_info.fields_fetched += doc.stored_field_count as u64;

                // Skip deleted docs if not showing deleted
                if !self.show_deleted && doc.is_deleted() {
                    continue;
                }

                if let Some(doc_ids) = &self.doc_ids {
                    let Some(doc_id) = doc.doc_id() else {
                        continue;
                    };
                    if !doc_ids.iter().any(|id| id == doc_id) {
                        continue;
                    }
                }

                // Apply filter if present
                if let Some(ref filter) = self.filter {
                    if !filter.matches(doc.fields(), &self.document_mapping)? {
                        continue;
                    }
                }

                self.emitted += 1;
                self.current_doc = doc;
                return Ok(true);
            }
        }

        loop {
            if self.position >= self.docs.len() {
                return Ok(false);
            }

            let doc = &self.docs[self.position];
            self.position += 1;

            // Track document fetch
            self.exec_info.docs_fetched += 1;
            // Track field fetches (actual stored fields in this document)
            self.exec_info.fields_fetched += doc.stored_field_count as u64;

            // Skip deleted docs if not showing deleted
            if !self.show_deleted && doc.is_deleted() {
                continue;
            }

            if let Some(doc_ids) = &self.doc_ids {
                let Some(doc_id) = doc.doc_id() else {
                    continue;
                };
                if !doc_ids.iter().any(|id| id == doc_id) {
                    continue;
                }
            }

            // Apply filter if present
            if let Some(ref filter) = self.filter {
                if !filter.matches(doc.fields(), &self.document_mapping)? {
                    continue;
                }
            }

            self.current_doc = doc.deep_clone();
            return Ok(true);
        }
    }

    fn value(&self) -> &Doc {
        &self.current_doc
    }

    async fn close(&mut self) -> Result<()> {
        self.docs.clear();
        // Close before dropping: a scan stopped early by a satisfied LimitNode
        // gets no other chance to flush work it deferred per document.
        if let Some(mut stream) = self.stream.take() {
            stream.close().await?;
        }
        self.initialized = false;
        Ok(())
    }

    fn source(&self) -> Option<&dyn PlanNode> {
        None // ScanNode is a leaf node
    }

    fn document_map(&self) -> &DocumentMapping {
        &self.document_mapping
    }

    fn kind(&self) -> &'static str {
        "scanNode"
    }

    fn explain_inner(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();

        // Go DefraDB format: always include filter (null if none or empty)
        // Only strip _docID conditions when doc_ids are provided as a query argument
        // (Go converts those to prefix scans). When _docID is a regular filter condition,
        // keep it in the filter output.
        if let Some(ref filter) = self.filter {
            if self.doc_ids.is_some() {
                // doc_ids provided → strip _docID (it's shown in prefixes)
                obj.insert("filter".to_string(), super::strip_docid_from_filter(filter));
            } else {
                obj.insert("filter".to_string(), filter.to_explain_json());
            }
        } else {
            obj.insert("filter".to_string(), serde_json::Value::Null);
        }

        // Go DefraDB uses "collectionName" and "collectionID"
        // Note: Go's explain uses VersionID (not CollectionID) for the collectionID field
        obj.insert(
            "collectionName".to_string(),
            serde_json::Value::String(self.collection.name.clone()),
        );
        obj.insert(
            "collectionID".to_string(),
            serde_json::Value::String(self.collection.version_id.clone()),
        );

        // Go keeps document IDs on the select node and hides the optimized
        // per-document storage prefixes from the public explain result.
        let prefixes = if self.doc_ids.is_some() {
            Vec::new()
        } else {
            vec![format!("/{}", self.collection_prefix())]
        };
        obj.insert("prefixes".to_string(), serde_json::json!(prefixes));

        if self.show_deleted {
            obj.insert("showDeleted".to_string(), serde_json::Value::Bool(true));
        }

        serde_json::Value::Object(obj)
    }

    fn exec_info(&self) -> ExecInfo {
        self.exec_info.clone()
    }

    fn explain_execute_inner(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();

        obj.insert(
            "iterations".to_string(),
            serde_json::json!(self.exec_info.iterations),
        );
        obj.insert(
            "docFetches".to_string(),
            serde_json::json!(self.exec_info.docs_fetched),
        );
        obj.insert(
            "fieldFetches".to_string(),
            serde_json::json!(self.exec_info.fields_fetched),
        );
        // A vector-index hit counts as one fetch. Unlike a scalar index, which
        // counts each entry read, this does not reflect the graph search's real
        // node reads; matching the reference, which tracks the same gap.
        let index_fetches = self.exec_info.indexes_fetched + u64::from(self.vector_indexed);
        obj.insert("indexFetches".to_string(), serde_json::json!(index_fetches));
        if let Some(name) = self.vector_index_name() {
            obj.insert("vectorIndex".to_string(), serde_json::json!(name));
        }

        serde_json::Value::Object(obj)
    }
}

#[cfg(test)]
mod tests {
    use schema::{CollectionVersion, FieldDescription, FieldKind};
    use serde_json::json;

    use super::ScanNode;
    use crate::planner::{Doc, PlanNode};
    use query_types::document::DocumentMapping;
    use query_types::mapper::Filter;

    fn make_collection() -> CollectionVersion {
        CollectionVersion::new(
            "users",
            "v1",
            "users-v1",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
            ],
        )
    }

    fn make_mapping() -> DocumentMapping {
        let mut mapping = DocumentMapping::new();
        mapping.add(0, "_docID");
        mapping.add(1, "name");
        mapping
    }

    #[test]
    fn explain_keeps_docid_filter_without_doc_id_prefix_scan() {
        let filter = Filter::from_conditions(serde_json::Map::from_iter([
            ("_docID".to_string(), json!({"_eq": "doc-1"})),
            ("name".to_string(), json!({"_eq": "Alice"})),
        ]));

        let explain = ScanNode::new(make_collection(), make_mapping())
            .with_filter(filter)
            .explain();

        assert_eq!(
            explain["scanNode"]["filter"],
            json!({
                "_docID": {"_eq": "doc-1"},
                "name": {"_eq": "Alice"},
            })
        );
    }

    #[test]
    fn explain_strips_docid_filter_when_doc_ids_are_supplied_separately() {
        let filter = Filter::from_conditions(serde_json::Map::from_iter([
            ("_docID".to_string(), json!({"_eq": "doc-1"})),
            ("name".to_string(), json!({"_eq": "Alice"})),
        ]));

        let explain = ScanNode::new(make_collection(), make_mapping())
            .with_filter(filter)
            .with_doc_ids(vec!["doc-1".to_string()])
            .explain();

        assert_eq!(
            explain["scanNode"]["filter"],
            json!({"name": {"_eq": "Alice"}})
        );
        assert_eq!(explain["scanNode"]["prefixes"], json!([]));
    }

    #[tokio::test]
    async fn supplied_doc_ids_filter_preloaded_deleted_documents() {
        let mut requested = Doc::new(2);
        requested.set_doc_id("doc-1");
        requested.mark_deleted();
        let mut unrelated = Doc::new(2);
        unrelated.set_doc_id("doc-2");
        unrelated.mark_deleted();

        let mut scan = ScanNode::new(make_collection(), make_mapping())
            .with_docs(vec![unrelated, requested])
            .with_doc_ids(vec!["doc-1".to_string()])
            .with_show_deleted(true);
        scan.init().await.unwrap();

        assert!(scan.next().await.unwrap());
        assert_eq!(scan.value().doc_id(), Some("doc-1"));
        assert!(!scan.next().await.unwrap());
    }
}

#[cfg(test)]
mod stream_tests {
    use super::*;
    use crate::fetcher::FetchByIdsResult;
    use document::Document;
    use schema::{FieldDescription, FieldKind};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn collection_fixture() -> CollectionVersion {
        CollectionVersion::new(
            "users",
            "v1",
            "users-v1",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
            ],
        )
    }

    fn mapping_fixture() -> DocumentMapping {
        let mut mapping = DocumentMapping::new();
        mapping.add(0, "_docID");
        mapping.add(1, "name");
        mapping
    }

    fn plan_docs_fixture(n: usize) -> Vec<Doc> {
        (0..n)
            .map(|i| {
                let mut doc = Doc::new(2);
                doc.set_doc_id(format!("doc-{i}"));
                doc
            })
            .collect()
    }

    /// A stream that records how many documents were pulled from it.
    struct CountingStream {
        pairs: std::vec::IntoIter<(Document, bool)>,
        pulled: Arc<AtomicUsize>,
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl DocStream for CountingStream {
        async fn next(&mut self) -> Result<Option<(Document, bool)>> {
            match self.pairs.next() {
                Some(pair) => {
                    self.pulled.fetch_add(1, Ordering::SeqCst);
                    Ok(Some(pair))
                }
                None => Ok(None),
            }
        }
    }

    /// A fetcher that records how many documents were pulled.
    struct CountingFetcher {
        docs: Vec<(Document, bool)>,
        pulled: Arc<AtomicUsize>,
    }

    impl CountingFetcher {
        fn with_docs(n: usize, pulled: Arc<AtomicUsize>) -> Self {
            let docs = (0..n).map(|_| (Document::new(), false)).collect();
            Self { docs, pulled }
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl DocFetcher for CountingFetcher {
        /// A mock has no storage, so a document's short id is its 1-based position
        /// in the collection, mirroring how the real allocator hands them out.
        async fn stream_by_doc_short_ids(
            &self,
            collection_name: &str,
            doc_short_ids: &[u64],
            show_deleted: bool,
        ) -> Result<Box<dyn crate::doc_stream::DocStream>> {
            let all = self
                .get_all_with_deleted(collection_name, show_deleted)
                .await?;
            let picked = doc_short_ids
                .iter()
                .filter_map(|id| all.get(id.checked_sub(1)? as usize).cloned())
                .collect();
            Ok(Box::new(crate::doc_stream::VecStream::new(picked)))
        }
        async fn get_all(&self, _collection_name: &str) -> Result<Vec<Document>> {
            Ok(self.docs.iter().map(|(doc, _)| doc.clone()).collect())
        }

        async fn get_by_ids(
            &self,
            _collection_name: &str,
            _doc_ids: &[String],
        ) -> Result<FetchByIdsResult> {
            Ok(FetchByIdsResult::all_found(Vec::new()))
        }

        async fn get_by_field_value(
            &self,
            _collection_name: &str,
            _field_name: &str,
            _value: &str,
        ) -> Result<Vec<Document>> {
            Ok(Vec::new())
        }

        async fn stream_all_with_deleted(
            &self,
            _collection_name: &str,
            _show_deleted: bool,
        ) -> Result<Box<dyn DocStream>> {
            Ok(Box::new(CountingStream {
                pairs: self.docs.clone().into_iter(),
                pulled: self.pulled.clone(),
            }))
        }
    }

    #[tokio::test]
    async fn scan_node_pulls_only_what_is_consumed() {
        let pulled = Arc::new(AtomicUsize::new(0));
        let fetcher = Arc::new(CountingFetcher::with_docs(1000, pulled.clone()));
        let mut node = ScanNode::new(collection_fixture(), mapping_fixture()).with_fetcher(fetcher);

        node.init().await.unwrap();
        assert_eq!(
            pulled.load(Ordering::SeqCst),
            0,
            "init() must not pull any document"
        );

        for _ in 0..10 {
            assert!(node.next().await.unwrap());
        }
        assert_eq!(pulled.load(Ordering::SeqCst), 10);
    }

    #[tokio::test]
    async fn scan_node_with_docs_path_still_works() {
        let mut node =
            ScanNode::new(collection_fixture(), mapping_fixture()).with_docs(plan_docs_fixture(3));

        node.init().await.unwrap();
        let mut seen = 0;
        while node.next().await.unwrap() {
            seen += 1;
        }
        assert_eq!(seen, 3);
    }

    #[tokio::test]
    async fn scan_node_without_docs_or_fetcher_still_errors() {
        let mut node = ScanNode::new(collection_fixture(), mapping_fixture());
        assert!(node.init().await.is_err());
    }
}
