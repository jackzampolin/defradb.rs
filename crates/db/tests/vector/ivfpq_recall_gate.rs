//! The IVF-PQ recall target: a regression gate on a named distribution.
//!
//! Scoped deliberately. These thresholds hold for the synthetic clustered
//! corpus below and say nothing about a real embedding corpus, which has never
//! been measured here. What they catch is the index getting worse than it is
//! today.
//!
//! Sized to run in the ordinary suite; the full curves live in
//! `ivfpq_recall_baseline.rs`, which is `#[ignore]`d.

use db::index::vector::core::Metric;
use db::index::vector::engine::ann::VectorIndexEngine;
use db::index::vector::engine::flat::Flat;
use db::index::vector::engine::ivfpq::{IvfPq, IvfPqParams};
use db::index::vector::store::{MemoryNodeStore, NodeId};

const SEED: u64 = 0x09C0_D1F4;
const K: usize = 10;
const QUERIES: usize = 12;
const DIMENSIONS: usize = 16;

struct Measured {
    recall: f64,
    ratio: f64,
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

async fn measure(nprobe: u32, m: u32) -> Measured {
    let mut corpus = crate::support::Corpus::new(SEED);
    let vectors = corpus.clustered(600, DIMENSIONS, 8, 0.4);
    let queries = corpus.vectors(QUERIES, DIMENSIONS);

    let mut index = IvfPq::try_new(
        MemoryNodeStore::new(),
        Metric::Cosine,
        IvfPqParams {
            nlist: 8,
            nprobe,
            m,
            ..IvfPqParams::default()
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
    let (mut hit, mut total, mut ratio_sum) = (0usize, 0usize, 0.0f64);
    for query in &queries {
        let want = exact(&flat, query).await;
        let got = index.search(query.as_slice(), K, None).await.unwrap();
        hit += got.iter().filter(|n| want.contains(&n.id.0)).count();
        total += want.len();

        let ideal: f64 = want
            .iter()
            .map(|id| Metric::Cosine.distance(query.as_slice(), &vectors[*id as usize - 1]))
            .sum();
        let actual: f64 = got
            .iter()
            .map(|n| Metric::Cosine.distance(query.as_slice(), &vectors[n.id.0 as usize - 1]))
            .sum();
        ratio_sum += if ideal.abs() > f64::EPSILON {
            actual / ideal
        } else {
            1.0
        };
    }

    Measured {
        recall: hit as f64 / total as f64,
        ratio: ratio_sum / QUERIES as f64,
    }
}

/// Every list probed, so the coarse step is exhaustive and the only error left
/// is the codec's. This is the row that gates the quantizer.
#[tokio::test]
async fn quantization_error_alone_stays_bounded() {
    let m = measure(8, 8).await;
    println!(
        "nprobe=8 m=8: recall@10={:.4} dist/best={:.4}",
        m.recall, m.ratio
    );
    assert!(m.recall >= 0.90, "recall@10 fell to {:.4}", m.recall);
    assert!(
        m.ratio <= 1.02,
        "returned neighbours are {:.4}x ideal",
        m.ratio
    );
}

/// One list probed: most of the loss is the coarse step missing a neighbour it
/// never scanned. A floor, so a broken probe ordering is caught.
#[tokio::test]
async fn a_single_probe_still_finds_most_neighbours() {
    let m = measure(1, 8).await;
    println!(
        "nprobe=1 m=8: recall@10={:.4} dist/best={:.4}",
        m.recall, m.ratio
    );
    assert!(m.recall >= 0.55, "recall@10 fell to {:.4}", m.recall);
}

/// Probing more lists can only see more candidates.
#[tokio::test]
async fn recall_rises_with_nprobe() {
    let one = measure(1, 8).await.recall;
    let all = measure(8, 8).await.recall;
    assert!(
        all > one,
        "probing every list ({all:.4}) was no better than one ({one:.4})"
    );
}

/// Finer codes can only rank better.
#[tokio::test]
async fn recall_rises_with_finer_codes() {
    let coarse = measure(8, 2).await;
    let fine = measure(8, 8).await;
    assert!(
        fine.recall >= coarse.recall,
        "m=8 ({:.4}) was worse than m=2 ({:.4})",
        fine.recall,
        coarse.recall
    );
    assert!(fine.ratio <= coarse.ratio);
}
