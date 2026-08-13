//! `DbDocFetcher::vector_search` resolving a collection by name, which
//! `vector_query_routing.rs` bypasses with a hardcoded short id.

use std::sync::Arc;

use db::database::DB;
use db::{DbDocFetcher, DbDocMutator};
use document::{Document, NormalValue};
use query::mutator::DocMutator;
use query::runner::DocFetcher;
use schema::{
    CollectionVersion, DistanceMetric, FieldDescription, FieldKind, HnswParams, IndexDescription,
    IndexedFieldDescription, VectorAlgorithm, VectorIndexDescription,
};
use storage::backends::MemoryStore;

const COLLECTION: &str = "docs";
const DIMENSIONS: u32 = 4;
const DOCUMENTS: usize = 40;

fn schema() -> CollectionVersion {
    let mut version = CollectionVersion::new(
        COLLECTION,
        "v1",
        "col-docs",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
            FieldDescription::new("3", "embedding", FieldKind::float64_array()),
        ],
    );
    version.indexes = vec![IndexDescription {
        name: "by_embedding".to_string(),
        id: 0,
        fields: vec![IndexedFieldDescription {
            name: "embedding".to_string(),
            descending: false,
        }],
        unique: false,
        kind: None,
        auto_generated: false,
    }
    .as_vector(VectorIndexDescription {
        algorithm: VectorAlgorithm::Hnsw,
        metric: DistanceMetric::Cosine,
        dimensions: DIMENSIONS,
        hnsw: Some(HnswParams::default()),
        ivfpq: None,
        ssg: None,
    })];
    version
}

fn vector_at(i: usize) -> Vec<f64> {
    let angle = i as f64 * 0.37;
    vec![angle.sin(), angle.cos(), (angle * 0.5).sin(), 0.25]
}

async fn populated() -> (Arc<DB<MemoryStore>>, u32) {
    let db = Arc::new(DB::new(MemoryStore::new()).expect("a database"));
    db.create_collection(schema())
        .await
        .expect("the collection must register");

    let index_id = db
        .get_collection(COLLECTION)
        .expect("the collection is readable")
        .expect("the collection exists")
        .get_indexes()
        .iter()
        .find(|index| index.is_vector())
        .expect("the vector index must survive registration")
        .id;

    let txn = db.new_txn(false).await.unwrap();
    let mutator = DbDocMutator::new(db.clone(), txn);
    for i in 0..DOCUMENTS {
        let mut doc = Document::new();
        doc.set("title", NormalValue::String(format!("doc-{i}")));
        doc.set("embedding", NormalValue::Float64Array(vector_at(i)));
        mutator
            .create(COLLECTION, doc)
            .await
            .expect("the document must be created");
    }
    mutator
        .take_txn()
        .await
        .expect("the mutator still holds its transaction")
        .commit()
        .await
        .unwrap();

    (db, index_id)
}

async fn search(db: &Arc<DB<MemoryStore>>, name: &str, index_id: u32, k: usize) -> Vec<u64> {
    let txn = db.new_txn(false).await.unwrap();
    let fetcher = DbDocFetcher::new(txn);
    fetcher
        .vector_search(name, index_id, &vector_at(3), k, None)
        .await
        .expect("the search must resolve the collection")
}

#[tokio::test]
async fn the_fetcher_resolves_the_collection_and_searches_its_index() {
    let (db, index_id) = populated().await;
    let hits = search(&db, COLLECTION, index_id, 5).await;

    assert_eq!(hits.len(), 5, "k documents must come back");
    let mut distinct = hits.clone();
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(distinct.len(), hits.len(), "no document twice: {hits:?}");
}

#[tokio::test]
async fn the_nearest_hit_is_the_document_the_query_came_from() {
    let (db, index_id) = populated().await;
    let hits = search(&db, COLLECTION, index_id, 3).await;
    let nearest = *hits.first().expect("at least one hit");

    let txn = db.new_txn(true).await.unwrap();
    let fetcher = DbDocFetcher::new(txn);
    let mut stream = fetcher
        .stream_by_doc_short_ids(COLLECTION, &[nearest], false)
        .await
        .expect("the short id must resolve to a document");
    let (document, _) = stream
        .next()
        .await
        .expect("the stream must read")
        .expect("the nearest hit must be a live document");

    assert_eq!(
        document.get("title"),
        Some(&NormalValue::String("doc-3".to_string())),
        "the nearest neighbour of doc-3's own vector must be doc-3"
    );
}

#[tokio::test]
async fn an_unknown_index_id_is_an_error() {
    let (db, index_id) = populated().await;
    let txn = db.new_txn(true).await.unwrap();
    let fetcher = DbDocFetcher::new(txn);

    let err = fetcher
        .vector_search(COLLECTION, index_id + 99, &vector_at(0), 5, None)
        .await
        .expect_err("an unknown index id must fail");
    assert!(
        err.to_string().contains("vector index"),
        "the error must say what was missing, got: {err}"
    );
}

#[tokio::test]
async fn an_unknown_collection_is_an_error() {
    let (db, index_id) = populated().await;
    let txn = db.new_txn(true).await.unwrap();
    let fetcher = DbDocFetcher::new(txn);

    assert!(fetcher
        .vector_search("nope", index_id, &vector_at(0), 5, None)
        .await
        .is_err());
}
