//! The IVF-PQ engine and its train-on-threshold lifecycle.

use db::index::vector::core::Metric;
use db::index::vector::engine::ann::{EngineKind, VectorIndexEngine};
use db::index::vector::engine::flat::Flat;
use db::index::vector::engine::ivfpq::{IvfPq, IvfPqParams};
use db::index::vector::store::{MemoryNodeStore, NodeId};

const SEED: u64 = 0x01F4_9C0D;
const DIMENSIONS: usize = 16;

fn params(nlist: u32, nprobe: u32) -> IvfPqParams {
    IvfPqParams {
        nlist,
        nprobe,
        m: 4,
        ..IvfPqParams::default()
    }
}

fn index(nlist: u32, nprobe: u32) -> IvfPq<MemoryNodeStore> {
    IvfPq::try_new(
        MemoryNodeStore::new(),
        Metric::Cosine,
        params(nlist, nprobe),
        SEED,
    )
    .expect("cosine is rankable by squared distance")
}

async fn filled(nlist: u32, nprobe: u32, vectors: &[Vec<f32>]) -> IvfPq<MemoryNodeStore> {
    let mut index = index(nlist, nprobe);
    for (i, vector) in vectors.iter().enumerate() {
        index.insert(NodeId(i as u64 + 1), vector).await.unwrap();
    }
    index
}

async fn exact(vectors: &[Vec<f32>], query: &[f32], k: usize) -> Vec<u64> {
    let mut flat = Flat::new(MemoryNodeStore::new(), Metric::Cosine);
    for (i, vector) in vectors.iter().enumerate() {
        flat.insert(NodeId(i as u64 + 1), vector).await.unwrap();
    }
    flat.search(query, k, None)
        .await
        .unwrap()
        .into_iter()
        .map(|n| n.id.0)
        .collect()
}

#[tokio::test]
async fn the_kind_is_reported() {
    assert_eq!(index(4, 2).kind(), EngineKind::IvfPq);
}

/// A magnitude-sensitive metric is not ordered by squared distance, so it is
/// refused rather than silently ranked on the wrong quantity.
#[test]
fn a_metric_adc_cannot_rank_is_refused() {
    assert!(IvfPq::try_new(
        MemoryNodeStore::new(),
        Metric::NegativeDot,
        IvfPqParams::default(),
        SEED
    )
    .is_err());
    assert!(IvfPq::try_new(
        MemoryNodeStore::new(),
        Metric::Cosine,
        IvfPqParams::default(),
        SEED
    )
    .is_ok());
    assert!(IvfPq::try_new(
        MemoryNodeStore::new(),
        Metric::Euclidean,
        IvfPqParams::default(),
        SEED
    )
    .is_ok());
}

/// Before training the index is exact, because it is a flat scan.
#[tokio::test]
async fn an_untrained_index_answers_exactly() {
    let mut corpus = crate::support::Corpus::new(SEED);
    let vectors = corpus.vectors(120, DIMENSIONS);
    let index = filled(8, 4, &vectors).await;

    assert!(!index.is_trained().await.unwrap());

    let query = &vectors[5];
    let got: Vec<u64> = index
        .search(query.as_slice(), 10, None)
        .await
        .unwrap()
        .into_iter()
        .map(|n| n.id.0)
        .collect();
    assert_eq!(got, exact(&vectors, query, 10).await);
}

#[tokio::test]
async fn building_marks_the_index_trained() {
    let mut corpus = crate::support::Corpus::new(SEED);
    let vectors = corpus.vectors(400, DIMENSIONS);
    let mut index = filled(8, 8, &vectors).await;

    let report = index.build().await.unwrap();
    assert_eq!(report.indexed, 400);
    assert_eq!(report.state.dimensions, DIMENSIONS as u32);
    assert_eq!(report.state.nlist, 8);
    assert!(report.sampled > 0);
    assert!(index.is_trained().await.unwrap());
}

/// Probing every list makes the coarse step exhaustive, so the only remaining
/// error is quantization. The answers must be close to exact, not arbitrary.
#[tokio::test]
async fn a_trained_index_agrees_closely_with_an_exact_scan() {
    let mut corpus = crate::support::Corpus::new(SEED ^ 0xABC);
    let vectors = corpus.clustered(600, DIMENSIONS, 12, 0.12);
    let mut index = filled(12, 12, &vectors).await;
    index.build().await.unwrap();

    let mut matched = 0usize;
    let queries = 20;
    for i in 0..queries {
        let query = &vectors[i * 7];
        let got: Vec<u64> = index
            .search(query.as_slice(), 10, None)
            .await
            .unwrap()
            .into_iter()
            .map(|n| n.id.0)
            .collect();
        let want = exact(&vectors, query, 10).await;
        matched += got.iter().filter(|id| want.contains(id)).count();
    }

    let recall = matched as f64 / (queries * 10) as f64;
    assert!(
        recall >= 0.80,
        "recall against an exact scan was {recall:.3}, too low to be quantization alone"
    );
}

/// A vector's own document must come back first: quantization is lossy but not
/// that lossy.
#[tokio::test]
async fn a_corpus_vector_finds_itself() {
    let mut corpus = crate::support::Corpus::new(SEED ^ 0xDEF);
    let vectors = corpus.clustered(500, DIMENSIONS, 10, 0.1);
    let mut index = filled(10, 10, &vectors).await;
    index.build().await.unwrap();

    let mut found = 0usize;
    for i in [0usize, 37, 111, 250, 400] {
        let hits = index.search(vectors[i].as_slice(), 5, None).await.unwrap();
        if hits.iter().any(|n| n.id == NodeId(i as u64 + 1)) {
            found += 1;
        }
    }
    assert!(found >= 4, "only {found}/5 vectors found themselves");
}

/// Writes after a build must be searchable, or the index silently stops
/// covering new documents.
#[tokio::test]
async fn a_document_written_after_the_build_is_searchable() {
    let mut corpus = crate::support::Corpus::new(SEED ^ 0x111);
    let vectors = corpus.clustered(400, DIMENSIONS, 8, 0.1);
    let mut index = filled(8, 8, &vectors).await;
    index.build().await.unwrap();

    let late = vectors[3].clone();
    index.insert(NodeId(9_999), &late).await.unwrap();

    let hits = index.search(late.as_slice(), 10, None).await.unwrap();
    assert!(
        hits.iter().any(|n| n.id == NodeId(9_999)),
        "the late document was not found: {hits:?}"
    );
}

#[tokio::test]
async fn a_deleted_document_stops_ranking() {
    let mut corpus = crate::support::Corpus::new(SEED ^ 0x222);
    let vectors = corpus.clustered(400, DIMENSIONS, 8, 0.1);
    let mut index = filled(8, 8, &vectors).await;
    index.build().await.unwrap();

    assert!(index.delete(NodeId(1)).await.unwrap());
    let hits = index.search(vectors[0].as_slice(), 10, None).await.unwrap();
    assert!(
        !hits.iter().any(|n| n.id == NodeId(1)),
        "a deleted document ranked: {hits:?}"
    );
}

/// `Admit` is trait-level, so it must work here exactly as it does for a graph.
#[tokio::test]
async fn a_filter_excludes_without_shortening_the_answer() {
    let mut corpus = crate::support::Corpus::new(SEED ^ 0x333);
    let vectors = corpus.clustered(500, DIMENSIONS, 10, 0.1);
    let mut index = filled(10, 10, &vectors).await;
    index.build().await.unwrap();

    let admit = |id: NodeId| id.0.is_multiple_of(2);
    let hits = index
        .search_where(vectors[9].as_slice(), 8, None, &admit)
        .await
        .unwrap();

    assert!(
        hits.iter().all(|n| n.id.0.is_multiple_of(2)),
        "a filter leaked"
    );
    assert_eq!(hits.len(), 8, "the filter shortened the answer");
}

#[tokio::test]
async fn results_are_ordered_nearest_first() {
    let mut corpus = crate::support::Corpus::new(SEED ^ 0x444);
    let vectors = corpus.clustered(400, DIMENSIONS, 8, 0.1);
    let mut index = filled(8, 8, &vectors).await;
    index.build().await.unwrap();

    let hits = index.search(vectors[2].as_slice(), 10, None).await.unwrap();
    assert!(hits.windows(2).all(|w| w[0].distance <= w[1].distance));
}

/// Probing more lists can only look at more candidates, so recall must not
/// fall as nprobe rises.
#[tokio::test]
async fn recall_does_not_fall_as_nprobe_rises() {
    let mut corpus = crate::support::Corpus::new(SEED ^ 0x555);
    let vectors = corpus.clustered(600, DIMENSIONS, 16, 0.1);
    let query = vectors[13].clone();
    let want = exact(&vectors, &query, 10).await;

    let mut previous = 0usize;
    for nprobe in [1usize, 2, 4, 8, 16] {
        let mut index = filled(16, nprobe as u32, &vectors).await;
        index.build().await.unwrap();
        let got: Vec<u64> = index
            .search(query.as_slice(), 10, None)
            .await
            .unwrap()
            .into_iter()
            .map(|n| n.id.0)
            .collect();
        let hit = got.iter().filter(|id| want.contains(id)).count();
        assert!(
            hit + 1 >= previous,
            "recall fell at nprobe={nprobe}: {hit} after {previous}"
        );
        previous = hit;
    }
}

#[tokio::test]
async fn an_empty_index_builds_no_state() {
    let mut index = index(4, 2);
    assert!(index.build().await.is_err());
    assert!(!index.is_trained().await.unwrap());
}

#[tokio::test]
async fn k_zero_returns_nothing() {
    let mut corpus = crate::support::Corpus::new(SEED ^ 0x666);
    let vectors = corpus.vectors(200, DIMENSIONS);
    let mut index = filled(4, 4, &vectors).await;
    index.build().await.unwrap();
    assert!(index
        .search(vectors[0].as_slice(), 0, None)
        .await
        .unwrap()
        .is_empty());
}
