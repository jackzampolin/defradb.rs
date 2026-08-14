//! `FLAT` as a user-selectable algorithm, dispatched through `VectorIndex`.

use db_index::vector::engine::ann::EngineKind;
use db_index::vector::index::VectorIndex;
use db_index::vector::kv_store::KvNodeStore;
use db_index::vector::store::{NodeId, VectorNodeStore};
use document::NormalValue;
use schema::{
    DistanceMetric, HnswParams, IndexDescription, IndexedFieldDescription, VectorAlgorithm,
    VectorIndexDescription,
};
use storage::backends::MemoryStore;
use storage::corekv::{Store, Txn};
use storage::index::CollectionIndex;

mod common;

const COLLECTION: u32 = 21;
const INDEX_ID: u32 = 6;
const DIMENSIONS: u32 = 8;

fn description(algorithm: VectorAlgorithm, hnsw: Option<HnswParams>) -> IndexDescription {
    IndexDescription {
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
        algorithm,
        metric: DistanceMetric::Cosine,
        dimensions: DIMENSIONS,
        hnsw,
        ivfpq: None,
        ssg: None,
    })
}

fn index(algorithm: VectorAlgorithm) -> VectorIndex {
    VectorIndex::try_new(
        COLLECTION,
        description(algorithm, Some(HnswParams::default())),
    )
    .expect("a valid vector description")
}

async fn txn(store: &MemoryStore) -> Box<dyn Txn> {
    store.new_txn(false).await.unwrap()
}

async fn populated(algorithm: VectorAlgorithm, vectors: &[Vec<f32>]) -> (MemoryStore, VectorIndex) {
    let store = MemoryStore::new();
    let index = index(algorithm);
    let mut write = txn(&store).await;
    for (i, vector) in vectors.iter().enumerate() {
        let wide: Vec<f64> = vector.iter().map(|x| *x as f64).collect();
        index
            .save(&mut write, i as u64 + 1, &[NormalValue::Float64Array(wide)])
            .await
            .unwrap();
    }
    write.commit().await.unwrap();
    (store, index)
}

async fn ranked(
    algorithm: VectorAlgorithm,
    vectors: &[Vec<f32>],
    query: &[f64],
    k: usize,
) -> Vec<u64> {
    let (store, index) = populated(algorithm, vectors).await;
    let mut read = txn(&store).await;
    index
        .search(&mut read, query, k, None)
        .await
        .unwrap()
        .into_iter()
        .map(|hit| hit.id.0)
        .collect()
}

#[tokio::test]
async fn the_flat_algorithm_is_selectable_and_dispatched() {
    let store = MemoryStore::new();
    let index = index(VectorAlgorithm::Flat);
    let mut write = txn(&store).await;
    index
        .save(
            &mut write,
            1,
            &[NormalValue::Float64Array(vec![1.0; DIMENSIONS as usize])],
        )
        .await
        .unwrap();
    write.commit().await.unwrap();

    let mut read = txn(&store).await;
    let kv = KvNodeStore::new(&mut read, COLLECTION, INDEX_ID, 0);
    let node = kv.get_node(NodeId(1)).await.unwrap().unwrap();
    assert!(
        node.layers.is_empty(),
        "flat stores no graph layers, got {:?}",
        node.layers
    );
}

/// Flat is exact, so it must agree with a brute-force ranking on every query.
#[tokio::test]
async fn flat_returns_the_exact_ranking() {
    let mut corpus = common::Corpus::new(0xF1A7);
    let vectors = corpus.vectors(200, DIMENSIONS as usize);
    let query: Vec<f64> = vectors[11].iter().map(|x| *x as f64).collect();

    let got = ranked(VectorAlgorithm::Flat, &vectors, &query, 10).await;
    let query_narrow: Vec<f32> = query.iter().map(|x| *x as f32).collect();
    let want: Vec<u64> = common::scored(&vectors, &query_narrow, 10)
        .into_iter()
        .map(|id| id.0 + 1)
        .collect();

    assert_eq!(got, want, "flat must be exact");
}

/// The two algorithms answer the same question, so on a corpus small enough for
/// the graph to be exact they must return the same documents.
#[tokio::test]
async fn flat_and_hnsw_agree_on_a_small_corpus() {
    let mut corpus = common::Corpus::new(0xA6B3);
    let vectors = corpus.vectors(60, DIMENSIONS as usize);
    let query: Vec<f64> = vectors[3].iter().map(|x| *x as f64).collect();

    let flat = ranked(VectorAlgorithm::Flat, &vectors, &query, 5).await;
    let hnsw = ranked(VectorAlgorithm::Hnsw, &vectors, &query, 5).await;
    assert_eq!(flat, hnsw);
}

#[tokio::test]
async fn flat_honours_deletes() {
    let mut corpus = common::Corpus::new(0x0DE1);
    let vectors = corpus.vectors(20, DIMENSIONS as usize);
    let query: Vec<f64> = vectors[0].iter().map(|x| *x as f64).collect();

    let (store, index) = populated(VectorAlgorithm::Flat, &vectors).await;
    let mut write = txn(&store).await;
    index.delete(&mut write, 1, &[]).await.unwrap();
    write.commit().await.unwrap();

    let mut read = txn(&store).await;
    let hits: Vec<u64> = index
        .search(&mut read, &query, 5, None)
        .await
        .unwrap()
        .into_iter()
        .map(|hit| hit.id.0)
        .collect();
    assert!(
        !hits.contains(&1),
        "a deleted document must not rank: {hits:?}"
    );
}

/// Flat has no build parameters, so an out-of-range HNSW block must not stop it
/// being created; the same block must still be refused for HNSW.
#[tokio::test]
async fn flat_ignores_hnsw_parameters_that_hnsw_refuses() {
    let bad = HnswParams {
        m: 16,
        ef_construction: 1_000_000,
        ef_search: 64,
    };
    assert!(
        VectorIndex::try_new(COLLECTION, description(VectorAlgorithm::Hnsw, Some(bad))).is_err()
    );
    assert!(
        VectorIndex::try_new(COLLECTION, description(VectorAlgorithm::Flat, Some(bad))).is_ok()
    );
    assert!(VectorIndex::try_new(COLLECTION, description(VectorAlgorithm::Flat, None)).is_ok());
}

#[test]
fn the_engine_kinds_are_distinct() {
    assert_ne!(EngineKind::Flat, EngineKind::Hnsw);
}

/// Go defines only `HNSW`, so selecting flat is a wire divergence and has to be
/// visible as one.
#[test]
fn only_hnsw_is_go_compatible() {
    assert!(VectorAlgorithm::Hnsw.is_go_compatible());
    assert!(!VectorAlgorithm::Flat.is_go_compatible());
}

/// IVF-PQ dispatched through `VectorIndex`, the way a collection reaches it.
#[tokio::test]
async fn the_ivfpq_algorithm_is_selectable() {
    let mut desc = description(VectorAlgorithm::IvfPq, None);
    desc = desc.as_vector(schema::VectorIndexDescription {
        algorithm: VectorAlgorithm::IvfPq,
        metric: DistanceMetric::Cosine,
        dimensions: DIMENSIONS,
        hnsw: None,
        ivfpq: Some(schema::IvfPqParams {
            nlist: 4,
            nprobe: 4,
            m: 4,
            ..schema::IvfPqParams::default()
        }),
        ssg: None,
    });
    let index = VectorIndex::try_new(COLLECTION, desc).expect("a valid IVF-PQ description");

    let store = MemoryStore::new();
    let mut corpus = common::Corpus::new(0x1F);
    let vectors = corpus.vectors(60, DIMENSIONS as usize);

    let mut write = txn(&store).await;
    for (i, vector) in vectors.iter().enumerate() {
        let wide: Vec<f64> = vector.iter().map(|x| *x as f64).collect();
        index
            .save(&mut write, i as u64 + 1, &[NormalValue::Float64Array(wide)])
            .await
            .unwrap();
    }
    write.commit().await.unwrap();

    // Untrained, so this is an exact scan and must match brute force exactly.
    let query: Vec<f64> = vectors[3].iter().map(|x| *x as f64).collect();
    let mut read = txn(&store).await;
    let got: Vec<u64> = index
        .search(&mut read, &query, 5, None)
        .await
        .unwrap()
        .into_iter()
        .map(|hit| hit.id.0)
        .collect();
    let want: Vec<u64> = common::scored(&vectors, &vectors[3], 5)
        .into_iter()
        .map(|id| id.0 + 1)
        .collect();
    assert_eq!(got, want);
}

/// A metric IVF-PQ cannot rank must be refused where the index is built, not
/// discovered at query time.
#[test]
fn an_ivfpq_index_with_an_unrankable_metric_is_refused() {
    let desc =
        description(VectorAlgorithm::IvfPq, None).as_vector(schema::VectorIndexDescription {
            algorithm: VectorAlgorithm::IvfPq,
            metric: DistanceMetric::Dot,
            dimensions: DIMENSIONS,
            hnsw: None,
            ivfpq: Some(schema::IvfPqParams::default()),
            ssg: None,
        });
    let err = VectorIndex::try_new(COLLECTION, desc).unwrap_err();
    assert!(
        err.to_string().contains("squared distance"),
        "the error must say why, got: {err}"
    );
}

/// SSG dispatched through `VectorIndex`, the way a collection reaches it.
#[tokio::test]
async fn the_ssg_algorithm_is_selectable() {
    let desc = description(VectorAlgorithm::Ssg, None).as_vector(schema::VectorIndexDescription {
        algorithm: VectorAlgorithm::Ssg,
        metric: DistanceMetric::Cosine,
        dimensions: DIMENSIONS,
        hnsw: None,
        ivfpq: None,
        ssg: Some(schema::SsgParams::default()),
    });
    let index = VectorIndex::try_new(COLLECTION, desc).expect("a valid SSG description");

    let store = MemoryStore::new();
    let mut corpus = common::Corpus::new(0x55);
    let vectors = corpus.clustered(120, DIMENSIONS as usize, 6, 0.2);

    let mut write = txn(&store).await;
    for (i, vector) in vectors.iter().enumerate() {
        let wide: Vec<f64> = vector.iter().map(|x| *x as f64).collect();
        index
            .save(&mut write, i as u64 + 1, &[NormalValue::Float64Array(wide)])
            .await
            .unwrap();
    }
    write.commit().await.unwrap();

    let query: Vec<f64> = vectors[4].iter().map(|x| *x as f64).collect();
    let mut read = txn(&store).await;
    let hits = index.search(&mut read, &query, 5, None).await.unwrap();
    assert_eq!(hits.len(), 5);
    assert_eq!(hits[0].id.0, 5, "a vector is nearest itself");
}

/// Out-of-range SSG parameters are refused where the index is built.
#[test]
fn out_of_range_ssg_parameters_are_refused() {
    let desc = description(VectorAlgorithm::Ssg, None).as_vector(schema::VectorIndexDescription {
        algorithm: VectorAlgorithm::Ssg,
        metric: DistanceMetric::Cosine,
        dimensions: DIMENSIONS,
        hnsw: None,
        ivfpq: None,
        ssg: Some(schema::SsgParams {
            angle: 200,
            ..schema::SsgParams::default()
        }),
    });
    let err = VectorIndex::try_new(COLLECTION, desc).unwrap_err();
    assert!(err.to_string().contains("angle"), "got: {err}");
}
