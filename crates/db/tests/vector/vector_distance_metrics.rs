//! The three distance metrics, which are Go's set as of
//! sourcenetwork/defradb#5169.
//!
//! Each has to earn its place by ordering a corpus differently from the other
//! two, so the shared fixture is built to separate them: documents 1 and 2 are
//! collinear with 2 four times longer, and 3 points elsewhere. Against the
//! query `[1, 0]` cosine cannot tell 1 from 2, dot puts 2 first, and euclidean
//! puts 1 first and 2 last. A metric that quietly aliased another would fail
//! here rather than on a corpus where the difference does not show.

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

/// `[1, 0]` is nearest 1 exactly, `sqrt(2)` from 3, and 3 from 2, so the
/// magnitude that dot rewards is the one euclidean penalises. The two
/// magnitude-sensitive metrics therefore produce opposite orders, which is the
/// only way to show neither is an alias for the other.
#[tokio::test]
async fn euclidean_ranks_by_straight_line_distance() {
    let hits = ranked(DistanceMetric::Euclidean, &[1.0, 0.0]).await;
    assert_eq!(
        hits,
        vec![1, 3, 2],
        "nearest, then the orthogonal unit vector, then the distant collinear one: {hits:?}"
    );
}

/// Normalizing is cosine's alone: it is what makes magnitude unreadable, so a
/// metric that reads magnitude must not have it applied. Storing `[3, 4]` shows
/// it directly, since cosine scales it to `[0.6, 0.8]`.
#[tokio::test]
async fn only_cosine_normalizes_what_it_stores() {
    let cosine = stored(DistanceMetric::Cosine, vec![3.0, 4.0])
        .await
        .unwrap();
    assert!((cosine[0] - 0.6).abs() < 1e-6, "cosine stored {cosine:?}");

    for metric in [DistanceMetric::Euclidean, DistanceMetric::Dot] {
        let kept = stored(metric, vec![3.0, 4.0]).await.unwrap();
        assert!(
            (kept[0] - 3.0).abs() < 1e-6,
            "{metric:?} stored {kept:?}, which is normalized"
        );
    }
}

/// A zero vector has no direction, so cosine cannot rank it and the index
/// refuses to store one. The other two rank it fine, so they must.
#[tokio::test]
async fn a_zero_vector_is_indexed_by_every_metric_but_cosine() {
    assert!(stored(DistanceMetric::Cosine, vec![0.0, 0.0])
        .await
        .is_none());

    for metric in [DistanceMetric::Euclidean, DistanceMetric::Dot] {
        assert!(
            stored(metric, vec![0.0, 0.0]).await.is_some(),
            "{metric:?} must index a zero vector"
        );
    }
}

/// Every metric in the enum has to be constructible as an index, or an
/// algorithm that claims to support it would still fail at description time.
#[tokio::test]
async fn every_metric_builds_a_searchable_index() {
    for metric in DistanceMetric::ALL {
        let hits = ranked(*metric, &[1.0, 0.0]).await;
        assert_eq!(hits.len(), 3, "{metric:?} returned {hits:?}");
    }
}
