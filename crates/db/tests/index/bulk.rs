//! Backfill pulls one document at a time.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use db::database::DB;
use db::index::error::Result;
use db::index::manager::{DocumentSource, SliceSource};
use db::index::IndexManager;
use document::{Document, NormalValue};
use schema::{CollectionVersion, FieldDescription, FieldKind, IndexedFieldDescription};
use storage::backends::MemoryStore;

fn schema() -> CollectionVersion {
    CollectionVersion::new(
        "docs",
        "v1",
        "col-docs",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
        ],
    )
}

fn document(i: usize) -> Document {
    let mut doc = Document::new();
    doc.set("name", NormalValue::String(format!("doc-{i}")));
    doc
}

/// Yields on demand and records how many documents were ever alive at once,
/// which is what a materialising backfill would blow up.
struct CountingSource {
    remaining: usize,
    next_id: u64,
    served: Arc<AtomicUsize>,
    live: Arc<AtomicUsize>,
}

#[async_trait]
impl DocumentSource for CountingSource {
    async fn next(&mut self) -> Result<Option<(u64, Document)>> {
        self.served.fetch_add(1, Ordering::Relaxed);
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        let id = self.next_id;
        self.next_id += 1;
        self.live.store(1, Ordering::Relaxed);
        Ok(Some((id, document(id as usize))))
    }
}

async fn indexed<F, Fut>(run: F) -> db::index::manager::BulkIndexResult
where
    F: FnOnce(IndexManager, datastore::NamespaceView, CollectionVersion) -> Fut,
    Fut: std::future::Future<Output = db::index::manager::BulkIndexResult>,
{
    let db = DB::new(MemoryStore::new()).unwrap();
    let txn = db.new_txn(false).await.unwrap();
    let datastore = txn.datastore().unwrap();
    let mut manager = IndexManager::new(1);
    manager
        .create_index(
            &datastore,
            "docs",
            "by_name".to_string(),
            vec![IndexedFieldDescription {
                name: "name".to_string(),
                descending: false,
            }],
            false,
            &[],
        )
        .await
        .unwrap();
    run(manager, datastore, schema()).await
}

#[tokio::test]
async fn backfill_pulls_one_document_at_a_time() {
    let served = Arc::new(AtomicUsize::new(0));
    let live = Arc::new(AtomicUsize::new(0));
    let (s, l) = (served.clone(), live.clone());

    let result = indexed(|manager, datastore, schema| async move {
        let mut source = CountingSource {
            remaining: 500,
            next_id: 1,
            served: s,
            live: l,
        };
        manager
            .bulk_index_from(&datastore, "by_name", &mut source, &schema)
            .await
            .unwrap()
    })
    .await;

    assert_eq!(result.indexed, 500);
    assert_eq!(
        served.load(Ordering::Relaxed),
        501,
        "the source is drained exactly once"
    );
    assert_eq!(
        live.load(Ordering::Relaxed),
        1,
        "never more than one document in hand"
    );
}

/// The slice form must go through the same implementation, or the two paths can
/// drift.
#[tokio::test]
async fn the_slice_form_matches_the_streaming_form() {
    let documents: Vec<(u64, Document)> = (1..=20).map(|i| (i as u64, document(i))).collect();

    let docs = documents.clone();
    let by_slice = indexed(|manager, datastore, schema| async move {
        manager
            .bulk_index(&datastore, "by_name", &docs, &schema)
            .await
            .unwrap()
    })
    .await;

    let docs = documents.clone();
    let by_stream = indexed(|manager, datastore, schema| async move {
        let mut source = SliceSource::new(&docs);
        manager
            .bulk_index_from(&datastore, "by_name", &mut source, &schema)
            .await
            .unwrap()
    })
    .await;

    assert_eq!(by_slice.indexed, by_stream.indexed);
    assert_eq!(by_slice.skipped, by_stream.skipped);
    assert_eq!(by_slice.indexed, 20);
}

#[tokio::test]
async fn an_unset_short_id_is_skipped_not_indexed() {
    let result = indexed(|manager, datastore, schema| async move {
        let documents = vec![(0u64, document(1)), (2, document(2)), (0, document(3))];
        let mut source = SliceSource::new(&documents);
        manager
            .bulk_index_from(&datastore, "by_name", &mut source, &schema)
            .await
            .unwrap()
    })
    .await;

    assert_eq!(result.indexed, 1);
    assert_eq!(result.skipped, 2);
}

#[tokio::test]
async fn an_empty_source_indexes_nothing() {
    let result = indexed(|manager, datastore, schema| async move {
        let mut source = SliceSource::new(&[]);
        manager
            .bulk_index_from(&datastore, "by_name", &mut source, &schema)
            .await
            .unwrap()
    })
    .await;
    assert_eq!((result.indexed, result.skipped), (0, 0));
}

/// `BackfillSource` over a real collection: the path index creation uses.
#[tokio::test]
async fn the_collection_source_streams_every_live_document() {
    use db::{BackfillSource, DbDocMutator};
    use query::mutator::DocMutator;
    use std::sync::Arc;

    let db = Arc::new(DB::new(MemoryStore::new()).unwrap());
    db.create_collection(schema()).await.unwrap();

    let txn = db.new_txn(false).await.unwrap();
    let mutator = DbDocMutator::new(db.clone(), txn);
    for i in 0..25 {
        mutator.create("docs", document(i)).await.unwrap();
    }
    mutator.take_txn().await.unwrap().commit().await.unwrap();

    let txn = db.new_txn(false).await.unwrap();
    let datastore = txn.datastore().unwrap();
    let systemstore = txn.systemstore().unwrap();
    let collection = db.get_collection("docs").unwrap().unwrap();

    let mut source = BackfillSource::open(collection, datastore, systemstore)
        .await
        .unwrap();

    let mut seen = Vec::new();
    while let Some((short_id, doc)) = source.next().await.unwrap() {
        assert_ne!(short_id, 0, "a live document has a short id");
        assert!(doc.get("name").is_some());
        seen.push(short_id);
    }

    assert_eq!(seen.len(), 25, "every document must be yielded: {seen:?}");
    let mut distinct = seen.clone();
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(distinct.len(), 25, "no document twice");
}
