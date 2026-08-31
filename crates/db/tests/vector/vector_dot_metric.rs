//! The `DOT` metric. A wire divergence from Go, so it has to earn that by
//! being magnitude-sensitive rather than an alias for cosine.

use db::index::vector::index::VectorIndex;
use db::index::vector::kv_store::KvNodeStore;
use db::index::vector::store::NodeId;
use db::index::vector::store::VectorNodeStore;
use document::NormalValue;
use schema::DistanceMetric;
use schema::HnswParams;
use schema::IndexDescription;
use schema::IndexedFieldDescription;
use schema::VectorAlgorithm;
use schema::VectorIndexDescription;
use storage::corekv::Store;
use storage::corekv::Txn;
use storage::index::CollectionIndex;
use storage::RegolithStore;

const COLLECTION: u32 = 11;
const INDEX_ID: u32 = 4;

fn index(metric: DistanceMetric) -> VectorIndex {
    let desc = IndexDescription {
        name: "by_embedding".to_string(),
        id: INDEX_ID,
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
        metric,
        dimensions: 2,
        hnsw: Some(HnswParams::default()),
        ivfpq: None,
        ssg: None,
    });
    VectorIndex::try_new(COLLECTION, desc).expect("a valid vector description")
}

async fn txn(store: &RegolithStore) -> Box<dyn Txn> {
    store.new_txn(false).await.unwrap()
}

async fn stored(metric: DistanceMetric, vector: Vec<f64>) -> Option<Vec<f32>> {
    let store = RegolithStore::in_memory().unwrap();
    let index = index(metric);
    let mut write = txn(&store).await;
    index
        .save(&mut write, 1, &[NormalValue::Float64Array(vector)])
        .await
        .unwrap();
    write.commit().await.unwrap();

    let mut read = txn(&store).await;
    let kv = KvNodeStore::new(&mut read, COLLECTION, INDEX_ID, 0);
    kv.get_node(NodeId(1))
        .await
        .unwrap()
        .map(|node| node.vector)
}

/// Documents 1 and 2 are collinear, 2 four times longer; 3 points elsewhere.
async fn ranked(metric: DistanceMetric, query: &[f64]) -> Vec<u64> {
    let store = RegolithStore::in_memory().unwrap();
    let index = index(metric);

    let mut write = txn(&store).await;
    for (id, vector) in [
        (1u64, vec![1.0, 0.0]),
        (2, vec![4.0, 0.0]),
        (3, vec![0.0, 1.0]),
    ] {
        index
            .save(&mut write, id, &[NormalValue::Float64Array(vector)])
            .await
            .unwrap();
    }
    write.commit().await.unwrap();

    let mut read = txn(&store).await;
    index
        .search(&mut read, query, 3, None)
        .await
        .unwrap()
        .into_iter()
        .map(|hit| hit.id.0)
        .collect()
}

#[tokio::test]
async fn cosine_ignores_magnitude() {
    let hits = ranked(DistanceMetric::Cosine, &[1.0, 0.0]).await;
    assert_eq!(hits.len(), 3);
    assert!(
        hits[0..2].contains(&1) && hits[0..2].contains(&2),
        "both collinear documents must outrank the orthogonal one: {hits:?}"
    );
    assert_eq!(hits[2], 3, "the orthogonal document is last: {hits:?}");
}

#[tokio::test]
async fn dot_prefers_the_longer_vector() {
    let hits = ranked(DistanceMetric::Dot, &[1.0, 0.0]).await;
    assert_eq!(hits[0], 2, "the longer collinear vector first: {hits:?}");
    assert_eq!(hits[1], 1, "then the shorter one: {hits:?}");
}

#[tokio::test]
async fn dot_stores_vectors_unnormalized() {
    let dot = stored(DistanceMetric::Dot, vec![3.0, 4.0]).await.unwrap();
    assert!((dot[0] - 3.0).abs() < 1e-6, "dot stored {dot:?}");

    let cosine = stored(DistanceMetric::Cosine, vec![3.0, 4.0])
        .await
        .unwrap();
    assert!((cosine[0] - 0.6).abs() < 1e-6, "cosine stored {cosine:?}");
}

#[tokio::test]
async fn dot_indexes_a_zero_vector_and_cosine_does_not() {
    assert!(stored(DistanceMetric::Dot, vec![0.0, 0.0]).await.is_some());
    assert!(stored(DistanceMetric::Cosine, vec![0.0, 0.0])
        .await
        .is_none());
}
