//! Restricting a scan to known documents by short id.

use async_trait::async_trait;
use query_plan::doc_stream::{DocStream, VecStream};
use query_plan::fetcher::DocFetcher;
use query_plan::plan::ScanNode;
use query_plan::planner::PlanNode;
use query_types::document::DocumentMapping;
use query_types::error::Result;
use schema::{CollectionVersion, FieldDescription, FieldKind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Counts what the scan actually asked for, so a claim of narrowing can be
/// checked rather than assumed.
#[derive(Default)]
struct RecordingFetcher {
    scans: AtomicUsize,
    seeks: AtomicUsize,
    sought: std::sync::Mutex<Vec<u64>>,
}

impl RecordingFetcher {
    /// A document per short id, titled after it, so a caller can tell which
    /// ones came back.
    fn document(short_id: u64) -> document::Document {
        let mut doc = document::Document::new();
        doc.set(
            "title",
            document::NormalValue::String(format!("doc-{short_id}")),
        );
        doc
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl DocFetcher for RecordingFetcher {
    async fn get_all(&self, _collection: &str) -> Result<Vec<document::Document>> {
        Ok((1..=10).map(Self::document).collect())
    }

    async fn get_by_ids(
        &self,
        _collection: &str,
        _doc_ids: &[String],
    ) -> Result<query_plan::fetcher::FetchByIdsResult> {
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

    async fn stream_all_with_deleted(
        &self,
        _collection: &str,
        _show_deleted: bool,
    ) -> Result<Box<dyn DocStream>> {
        self.scans.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(VecStream::new(
            (1..=10).map(|id| (Self::document(id), false)).collect(),
        )))
    }

    async fn stream_by_doc_short_ids(
        &self,
        _collection: &str,
        doc_short_ids: &[u64],
        _show_deleted: bool,
    ) -> Result<Box<dyn DocStream>> {
        self.seeks.fetch_add(1, Ordering::Relaxed);
        self.sought.lock().unwrap().extend_from_slice(doc_short_ids);
        Ok(Box::new(VecStream::new(
            doc_short_ids
                .iter()
                .filter(|id| (1u64..=10).contains(id))
                .map(|id| (Self::document(*id), false))
                .collect(),
        )))
    }
}

fn collection() -> CollectionVersion {
    CollectionVersion::new(
        "docs",
        "v1",
        "col-docs",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
        ],
    )
}

async fn titles(node: &mut ScanNode) -> Vec<String> {
    node.init().await.unwrap();
    node.start().await.unwrap();
    let mut out = Vec::new();
    while node.next().await.unwrap() {
        if let Some(title) = node.value().get(1).and_then(|v| v.as_str()) {
            out.push(title.to_string());
        }
    }
    out
}

fn mapping() -> DocumentMapping {
    let mut mapping = DocumentMapping::new();
    mapping.add(0, "_docID");
    mapping.add(1, "title");
    mapping
}

fn scan(fetcher: Arc<RecordingFetcher>) -> ScanNode {
    ScanNode::new(collection(), mapping()).with_fetcher(fetcher)
}

/// The restriction must narrow the read, not filter after it.
#[tokio::test]
async fn a_restricted_scan_seeks_instead_of_scanning() {
    let fetcher = Arc::new(RecordingFetcher::default());
    let mut node = scan(fetcher.clone()).with_doc_short_ids(vec![2, 5, 9]);

    assert_eq!(titles(&mut node).await, vec!["doc-2", "doc-5", "doc-9"]);
    assert_eq!(fetcher.seeks.load(Ordering::Relaxed), 1, "must have sought");
    assert_eq!(
        fetcher.scans.load(Ordering::Relaxed),
        0,
        "a restricted scan must not read the collection"
    );
    assert_eq!(*fetcher.sought.lock().unwrap(), vec![2, 5, 9]);
}

#[tokio::test]
async fn an_unrestricted_scan_still_scans() {
    let fetcher = Arc::new(RecordingFetcher::default());
    let mut node = scan(fetcher.clone());

    assert_eq!(titles(&mut node).await.len(), 10);
    assert_eq!(fetcher.scans.load(Ordering::Relaxed), 1);
    assert_eq!(fetcher.seeks.load(Ordering::Relaxed), 0);
}

/// An empty restriction means nothing matched, which is not the same as no
/// restriction. Falling back to a full scan here would turn an empty vector
/// search result into every document in the collection.
#[tokio::test]
async fn an_empty_restriction_yields_nothing() {
    let fetcher = Arc::new(RecordingFetcher::default());
    let mut node = scan(fetcher.clone()).with_doc_short_ids(Vec::new());

    assert!(titles(&mut node).await.is_empty());
    assert_eq!(fetcher.scans.load(Ordering::Relaxed), 0);
}

/// A short id whose document is gone is skipped, not reported: an id from an
/// index may be stale.
#[tokio::test]
async fn absent_ids_are_skipped() {
    let fetcher = Arc::new(RecordingFetcher::default());
    let mut node = scan(fetcher.clone()).with_doc_short_ids(vec![3, 999, 7]);

    assert_eq!(titles(&mut node).await, vec!["doc-3", "doc-7"]);
}

/// The order asked for is the order returned, because the caller asked in
/// nearest-first order and nothing below re-sorts.
#[tokio::test]
async fn the_requested_order_is_preserved() {
    let fetcher = Arc::new(RecordingFetcher::default());
    let mut node = scan(fetcher.clone()).with_doc_short_ids(vec![8, 1, 4]);

    assert_eq!(titles(&mut node).await, vec!["doc-8", "doc-1", "doc-4"]);
}

/// A pre-loaded document carries no short id, so a restriction over it could
/// only be ignored. That must fail loudly rather than quietly read everything.
#[test]
#[should_panic(expected = "short-id restriction")]
fn a_restriction_over_preloaded_documents_is_refused() {
    let _ = ScanNode::new(collection(), mapping())
        .with_docs(Vec::new())
        .with_doc_short_ids(vec![1]);
}
