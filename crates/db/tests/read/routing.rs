//! A similarity query answered by its vector index must return exactly what an
//! exhaustive scan returns.
//!
//! Exercised through `IndexManager` and a real transaction, down to the stored
//! graph, which isolates the ranking from the collection lookup around it.
//! `read/lookup.rs` covers that lookup, over a registered collection.

use crate::common::schema::docs_schema;
use crate::common::schema::vector_kind;
use crate::common::schema::COLLECTION_SHORT_ID;
use db::database::DB;
use db::index::IndexManager;
use defra_core::vector::Metric;
use document::Document;
use document::NormalValue;
use schema::IndexedFieldDescription;
use storage::RegolithStore;

/// Directions spread around a circle, so nearest-neighbour order is
/// unambiguous for any query.
fn corpus(count: usize) -> Vec<(u64, Document, Vec<f64>)> {
    (0..count)
        .map(|i| {
            let angle = i as f64 * 0.37;
            let vector = vec![angle.sin(), angle.cos(), (angle * 0.5).sin(), 0.25];
            let mut doc = Document::new();
            doc.set("title", NormalValue::String(format!("doc-{i}")));
            doc.set("embedding", NormalValue::Float64Array(vector.clone()));
            (i as u64 + 1, doc, vector)
        })
        .collect()
}

/// Cosine distance, nearest first, over the whole corpus.
fn exhaustive(corpus: &[(u64, Document, Vec<f64>)], query: &[f64], k: usize) -> Vec<u64> {
    let mut scored: Vec<(u64, f64)> = corpus
        .iter()
        .map(|(id, _, vector)| (*id, Metric::Cosine.distance(query, vector)))
        .collect();
    scored.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    scored.into_iter().take(k).map(|(id, _)| id).collect()
}

/// Builds the index, backfills it, and returns the fetcher's answer for a
/// query alongside the exhaustive one.
async fn routed_and_exact(documents: usize, query: &[f64], k: usize) -> (Vec<u64>, Vec<u64>) {
    let db = DB::new(RegolithStore::in_memory().unwrap()).unwrap();
    let schema = docs_schema();
    let corpus = corpus(documents);
    let mut manager = IndexManager::new(COLLECTION_SHORT_ID);

    let txn = db.new_txn(false).await.unwrap();
    let index_id;
    {
        let datastore = txn.datastore().unwrap();
        let desc = manager
            .create_index_of_kind(
                &datastore,
                "docs",
                "by_embedding".to_string(),
                vec![IndexedFieldDescription {
                    name: "embedding".to_string(),
                    descending: false,
                }],
                vector_kind(),
                &[],
            )
            .await
            .unwrap();
        index_id = desc.id;

        let docs: Vec<(u64, Document)> = corpus
            .iter()
            .map(|(id, doc, _)| (*id, doc.clone()))
            .collect();
        manager
            .bulk_index(&datastore, "by_embedding", &docs, &schema)
            .await
            .unwrap();
    }
    txn.commit().await.unwrap();

    let txn = db.new_txn(false).await.unwrap();
    let datastore = txn.datastore().unwrap();
    let index = db::index::vector::index::VectorIndex::try_new(
        COLLECTION_SHORT_ID,
        manager
            .get_index("by_embedding")
            .unwrap()
            .description()
            .clone(),
    )
    .unwrap();
    let _ = index_id;

    let mut view = datastore.clone();
    let routed = index
        .search(&mut view, query, k, None)
        .await
        .unwrap()
        .into_iter()
        .map(|hit| hit.id.0)
        .collect();

    (routed, exhaustive(&corpus, query, k))
}

/// The gate: routing must return the same documents, in the same order, as an
/// exhaustive scan on a corpus where the index is exact enough.
#[tokio::test]
async fn a_routed_query_matches_an_exhaustive_scan() {
    for query in [
        vec![1.0, 0.0, 0.0, 0.25],
        vec![0.0, 1.0, 0.0, 0.25],
        vec![-0.5, 0.5, 0.5, 0.25],
    ] {
        let (routed, exact) = routed_and_exact(200, &query, 10).await;
        assert_eq!(routed, exact, "query {query:?} routed differently");
    }
}

/// Asking for more than the corpus holds returns the corpus, not an error.
#[tokio::test]
async fn asking_for_more_than_exists_returns_what_exists() {
    let (routed, exact) = routed_and_exact(15, &[1.0, 0.0, 0.0, 0.25], 50).await;
    assert_eq!(routed.len(), 15);
    assert_eq!(routed, exact);
}

/// An index that was never populated answers empty rather than failing.
#[tokio::test]
async fn an_empty_index_answers_empty() {
    let (routed, _) = routed_and_exact(0, &[1.0, 0.0, 0.0, 0.25], 10).await;
    assert!(routed.is_empty());
}
