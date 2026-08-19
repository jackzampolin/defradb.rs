//! Widening a vector-narrowed scan must not drop the stream it replaces.
//!
//! A `DocStream` flushes deferred per-document work on close: for the
//! auto-commit fetcher that flush releases the read transaction and persists
//! lens write-backs. Each doubling pass opens a new stream, so an unclosed one
//! leaks a transaction and silently discards those writes.

use async_trait::async_trait;
use query::doc_stream::DocStream;
use query::document::DocumentMapping;
use query::error::Result;
use query::fetcher::DocFetcher;
use query::plan::ScanNode;
use query::planner::vector_routing::VectorRoute;
use query::planner::PlanNode;
use schema::{CollectionVersion, FieldDescription, FieldKind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

const CORPUS: u64 = 64;

/// Records that it was closed, the way the real streams flush on close.
struct ClosingStream {
    documents: std::vec::IntoIter<(document::Document, bool)>,
    closes: Arc<AtomicUsize>,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl DocStream for ClosingStream {
    async fn next(&mut self) -> Result<Option<(document::Document, bool)>> {
        Ok(self.documents.next())
    }

    async fn close(&mut self) -> Result<()> {
        self.closes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct WideningFetcher {
    opened: AtomicUsize,
    closed: Arc<AtomicUsize>,
    /// How many of the corpus\'s documents the index actually holds.
    indexed: u64,
}

impl Default for WideningFetcher {
    fn default() -> Self {
        Self {
            opened: AtomicUsize::new(0),
            closed: Arc::new(AtomicUsize::new(0)),
            indexed: CORPUS,
        }
    }
}

impl WideningFetcher {
    fn document(short_id: u64) -> document::Document {
        let mut doc = document::Document::new();
        doc.set(
            "_docID",
            document::NormalValue::String(format!("bae-{short_id}")),
        );
        doc.set(
            "title",
            document::NormalValue::String(format!("doc-{short_id}")),
        );
        doc
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl DocFetcher for WideningFetcher {
    async fn get_all(&self, _collection: &str) -> Result<Vec<document::Document>> {
        unreachable!("this test always streams")
    }

    async fn get_by_ids(
        &self,
        _collection: &str,
        _doc_ids: &[String],
    ) -> Result<query::fetcher::FetchByIdsResult> {
        unreachable!("this test never fetches by document id")
    }

    async fn get_by_field_value(
        &self,
        _collection: &str,
        _field_name: &str,
        _value: &str,
    ) -> Result<Vec<document::Document>> {
        unreachable!("this test never fetches by field value")
    }

    /// A routed scan falls back to this once the index is exhausted and the
    /// page is still short, so it counts toward the open/close balance too.
    async fn stream_all_with_deleted(
        &self,
        _collection: &str,
        _show_deleted: bool,
    ) -> Result<Box<dyn DocStream>> {
        self.opened.fetch_add(1, Ordering::Relaxed);
        let documents: Vec<(document::Document, bool)> =
            (1..=CORPUS).map(|id| (Self::document(id), false)).collect();
        Ok(Box::new(ClosingStream {
            documents: documents.into_iter(),
            closes: self.closed.clone(),
        }))
    }

    async fn stream_by_doc_short_ids(
        &self,
        _collection: &str,
        doc_short_ids: &[u64],
        _show_deleted: bool,
    ) -> Result<Box<dyn DocStream>> {
        self.opened.fetch_add(1, Ordering::Relaxed);
        let documents: Vec<(document::Document, bool)> = doc_short_ids
            .iter()
            .map(|id| (Self::document(*id), false))
            .collect();
        Ok(Box::new(ClosingStream {
            documents: documents.into_iter(),
            closes: self.closed.clone(),
        }))
    }

    fn supports_vector_search(&self) -> bool {
        true
    }

    /// Nearest-first ids, capped at the corpus so the scan sees exhaustion.
    async fn vector_search(
        &self,
        _collection: &str,
        _index_id: u32,
        _query_vector: &[f64],
        k: usize,
        _effort: Option<usize>,
    ) -> Result<Vec<u64>> {
        Ok((1..=self.indexed.min(k as u64)).collect())
    }
}

fn collection() -> CollectionVersion {
    let mut version = CollectionVersion::new(
        "docs",
        "v1",
        "col-docs",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
        ],
    );
    version.indexes = vec![schema::IndexDescription::new("by_embedding")];
    version
}

fn mapping() -> DocumentMapping {
    let mut mapping = DocumentMapping::new();
    mapping.add(0, "_docID");
    mapping.add(1, "title");
    mapping
}

async fn drain(node: &mut ScanNode) -> usize {
    node.init().await.unwrap();
    node.start().await.unwrap();
    let mut rows = 0;
    while node.next().await.unwrap() {
        rows += 1;
    }
    node.close().await.unwrap();
    rows
}

/// A page that fills on the first batch opens one stream, so nothing is
/// replaced and the count is trivially balanced.
#[tokio::test]
async fn a_single_batch_closes_its_stream() {
    let fetcher = Arc::new(WideningFetcher::default());
    let mut node = ScanNode::new(collection(), mapping())
        .with_fetcher(fetcher.clone())
        .with_vector_route(VectorRoute {
            index_id: 0,
            query_vector: vec![1.0],
            k: 4,
        });

    assert_eq!(drain(&mut node).await, 4);
    assert_eq!(fetcher.opened.load(Ordering::Relaxed), 1);
    assert_eq!(fetcher.closed.load(Ordering::Relaxed), 1);
}

/// Every stream a widening pass replaces must be closed, not dropped.
#[tokio::test]
async fn every_replaced_stream_is_closed() {
    let fetcher = Arc::new(WideningFetcher::default());
    // A filter nothing matches forces widening all the way to exhaustion.
    let filter = query::mapper::Filter::from_conditions(
        [(
            "title".to_string(),
            serde_json::json!({"_eq": "no such document"}),
        )]
        .into_iter()
        .collect(),
    );
    let mut node = ScanNode::new(collection(), mapping())
        .with_fetcher(fetcher.clone())
        .with_filter(filter)
        .with_vector_route(VectorRoute {
            index_id: 0,
            query_vector: vec![1.0],
            k: 4,
        });

    assert_eq!(drain(&mut node).await, 0, "the filter matches nothing");

    let opened = fetcher.opened.load(Ordering::Relaxed);
    assert!(
        opened > 1,
        "an unsatisfiable filter must widen more than once, opened {opened}"
    );
    assert_eq!(
        fetcher.closed.load(Ordering::Relaxed),
        opened,
        "every stream opened must also be closed"
    );
}

/// An index that cannot fill the page hands the rest of the scan to a full
/// read, rather than reporting a short page as the whole answer.
#[tokio::test]
async fn an_exhausted_index_falls_back_to_the_collection() {
    // The index holds a fraction of the collection, as it does when vectors are
    // null or the index was created over existing documents.
    let fetcher = Arc::new(WideningFetcher {
        indexed: 8,
        ..WideningFetcher::default()
    });
    let mut node = ScanNode::new(collection(), mapping())
        .with_fetcher(fetcher.clone())
        .with_vector_route(VectorRoute {
            index_id: 0,
            query_vector: vec![1.0],
            k: (CORPUS as usize) + 8,
        });

    let rows = drain(&mut node).await;

    assert_eq!(
        rows, CORPUS as usize,
        "the fallback must supply every document the index could not"
    );
    assert_eq!(
        fetcher.closed.load(Ordering::Relaxed),
        fetcher.opened.load(Ordering::Relaxed),
        "every stream opened, fallback included, must be closed"
    );
}
