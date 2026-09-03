//! The IVF_FLAT recall target: a regression gate on a named distribution.
//!
//! Scoped deliberately. These thresholds hold for the synthetic clustered
//! corpus below and say nothing about a real embedding corpus, which has
//! never been measured here. What they catch is the index getting worse than
//! it is today.
//!
//! Unlike `ivfpq_recall_gate.rs` there is no quantization error to isolate:
//! every recall lost here is a true neighbour sitting in an unprobed list, so
//! `nprobe == nlist` is not a loose target but an exact one, proven generally
//! in `ivfflat_exactness.rs` and echoed here as a floor on this corpus.

use db::index::vector::engine::ann::VectorIndexEngine;
use db::index::vector::engine::flat::Flat;
use db::index::vector::engine::hnsw::Hnsw;
use db::index::vector::engine::ivfflat::IvfFlat;
use db::index::vector::engine::ivfflat::IvfFlatParams;
use db::index::vector::engine::ivfpq::IvfPq;
use db::index::vector::engine::ivfpq::IvfPqParams;
use db::index::vector::params::Params;
use db::index::vector::params::DEFAULT_EF_SEARCH;
use db::index::vector::params::DEFAULT_M;
use db::index::vector::store::MemoryNodeStore;
use db::index::vector::store::NodeId;
use defra_core::vector::Metric;

const SEED: u64 = 0x09C0_F1A7;
const K: usize = 10;
const QUERIES: usize = 12;
const DIMENSIONS: usize = 16;

struct Measured {
    recall: f64,
}

async fn oracle(vectors: &[Vec<f32>]) -> Flat<MemoryNodeStore> {
    let mut flat = Flat::new(MemoryNodeStore::new(), Metric::Cosine);
    for (i, vector) in vectors.iter().enumerate() {
        flat.insert(NodeId(i as u64 + 1), vector).await.unwrap();
    }
    flat
}

async fn exact(flat: &Flat<MemoryNodeStore>, query: &[f32]) -> Vec<u64> {
    flat.search(query, K, None)
        .await
        .unwrap()
        .into_iter()
        .map(|n| n.id.0)
        .collect()
}

async fn measure(nprobe: u32) -> Measured {
    let mut corpus = crate::support::Corpus::new(SEED);
    let vectors = corpus.clustered(600, DIMENSIONS, 8, 0.4);
    let queries = corpus.vectors(QUERIES, DIMENSIONS);

    let mut index = IvfFlat::try_new(
        MemoryNodeStore::new(),
        Metric::Cosine,
        IvfFlatParams {
            nlist: 8,
            nprobe,
            ..IvfFlatParams::default()
        },
        SEED,
    )
    .unwrap();
    for (i, vector) in vectors.iter().enumerate() {
        index.insert(NodeId(i as u64 + 1), vector).await.unwrap();
    }
    index.build().await.unwrap();

    // Built once, not per query: the oracle is the same for every one of them.
    let flat = oracle(&vectors).await;
    let (mut hit, mut total) = (0usize, 0usize);
    for query in &queries {
        let want = exact(&flat, query).await;
        let got = index.search(query.as_slice(), K, None).await.unwrap();
        hit += got.iter().filter(|n| want.contains(&n.id.0)).count();
        total += want.len();
    }

    Measured {
        recall: hit as f64 / total as f64,
    }
}

/// Every list probed, so the scan is exhaustive and there is no partial-probe
/// error left at all: this must be a perfect recall against the same oracle,
/// on this corpus, not merely a high one.
#[tokio::test]
async fn probing_every_list_matches_the_oracle_exactly() {
    let m = measure(8).await;
    println!("nprobe=8 (of 8): recall@10={:.4}", m.recall);
    assert_eq!(
        m.recall, 1.0,
        "full probe must be exact, recall@10 was {:.4}",
        m.recall
    );
}

/// One list probed: most of the loss is the coarse step missing a neighbour
/// it never scanned. A floor, so a broken probe ordering is caught.
#[tokio::test]
async fn a_single_probe_still_finds_most_neighbours() {
    let m = measure(1).await;
    println!("nprobe=1 (of 8): recall@10={:.4}", m.recall);
    assert!(m.recall >= 0.55, "recall@10 fell to {:.4}", m.recall);
}

/// Probing more lists can only see more candidates.
#[tokio::test]
async fn recall_rises_with_nprobe() {
    let one = measure(1).await.recall;
    let half = measure(4).await.recall;
    let all = measure(8).await.recall;
    assert!(
        half >= one,
        "probing half the lists ({half:.4}) was worse than one ({one:.4})"
    );
    assert!(
        all >= half,
        "probing every list ({all:.4}) was worse than half ({half:.4})"
    );
}

/// Recall at each engine's own default settings, on one shared corpus: what
/// #1516 asks the PR to report rather than assume. Not a regression gate
/// (each engine already has its own), just the comparison written down.
///
/// Every parameter left at `default()`: `nlist` and `m` derive from the
/// corpus, `nprobe` is `DEFAULT_NPROBE`, `ef_search` is `DEFAULT_EF_SEARCH`.
#[tokio::test]
async fn recall_against_ivfpq_and_hnsw_at_default_settings() {
    const COUNT: usize = 2_000;
    const DIMS: usize = 32;
    const CLUSTERS: usize = 40;
    const COMPARE_SEED: u64 = 0x9E37_79B9;

    let mut corpus = crate::support::Corpus::new(COMPARE_SEED);
    let vectors = corpus.clustered(COUNT, DIMS, CLUSTERS, 0.15);

    let flat = oracle(&vectors).await;

    let mut ivfflat = IvfFlat::try_new(
        MemoryNodeStore::new(),
        Metric::Cosine,
        IvfFlatParams::default(),
        COMPARE_SEED,
    )
    .unwrap();
    let mut ivfpq = IvfPq::try_new(
        MemoryNodeStore::new(),
        Metric::Cosine,
        IvfPqParams::default(),
        COMPARE_SEED,
    )
    .unwrap();
    let mut hnsw = Hnsw::new(
        MemoryNodeStore::new(),
        Metric::Cosine,
        Params::new(DEFAULT_M),
        COMPARE_SEED,
    );
    for (i, vector) in vectors.iter().enumerate() {
        let id = NodeId(i as u64 + 1);
        ivfflat.insert(id, vector).await.unwrap();
        ivfpq.insert(id, vector).await.unwrap();
        hnsw.insert(id, vector).await.unwrap();
    }
    ivfflat.build().await.unwrap();
    ivfpq.build().await.unwrap();

    let mut queries = crate::support::Corpus::new(COMPARE_SEED ^ 0xFFFF);
    let (mut hit_ivfflat, mut hit_ivfpq, mut hit_hnsw, mut total) =
        (0usize, 0usize, 0usize, 0usize);
    for _ in 0..QUERIES {
        let query = queries.vector(DIMS);
        let want = exact(&flat, &query).await;

        let from_ivfflat = ivfflat.search(&query, K, None).await.unwrap();
        let from_ivfpq = ivfpq.search(&query, K, None).await.unwrap();
        let from_hnsw = hnsw
            .search(&query, K, Some(DEFAULT_EF_SEARCH))
            .await
            .unwrap();

        hit_ivfflat += from_ivfflat
            .iter()
            .filter(|n| want.contains(&n.id.0))
            .count();
        hit_ivfpq += from_ivfpq.iter().filter(|n| want.contains(&n.id.0)).count();
        hit_hnsw += from_hnsw.iter().filter(|n| want.contains(&n.id.0)).count();
        total += want.len();
    }

    let recall_ivfflat = hit_ivfflat as f64 / total as f64;
    let recall_ivfpq = hit_ivfpq as f64 / total as f64;
    let recall_hnsw = hit_hnsw as f64 / total as f64;

    println!(
        "corpus: clustered N={COUNT} d={DIMS} clusters={CLUSTERS}, defaults, recall@{K}:\n\
         \x20 IVF_FLAT (nlist={}, nprobe={}) = {recall_ivfflat:.4}\n\
         \x20 IVF_PQ   (nlist={}, nprobe={}, m={}) = {recall_ivfpq:.4}\n\
         \x20 HNSW     (m={DEFAULT_M}, ef_search={DEFAULT_EF_SEARCH}) = {recall_hnsw:.4}",
        IvfFlatParams::default().resolved_nlist(COUNT as u64),
        IvfFlatParams::default().nprobe,
        IvfPqParams::default().resolved_nlist(COUNT as u64),
        IvfPqParams::default().nprobe,
        IvfPqParams::default().resolved_m(DIMS),
    );

    // A floor, not a race: IVF_FLAT has no quantization error, so it must not
    // be the worst of the three on the same corpus at default settings.
    assert!(
        recall_ivfflat >= recall_ivfpq - 0.02,
        "IVF_FLAT ({recall_ivfflat:.4}) fell meaningfully behind IVF_PQ ({recall_ivfpq:.4}) \
         despite paying no quantization error"
    );
}
