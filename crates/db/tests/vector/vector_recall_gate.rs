//! The recall target: a regression gate on a clustered corpus.
//!
//! Scoped deliberately. These thresholds hold for the synthetic clustered
//! distribution below and say nothing about recall on a real embedding corpus,
//! which has never been measured here. What they catch is the graph getting
//! worse than it is today.
//!
//! Sized to run in the ordinary suite, which makes it a proxy: at N=2000 the
//! top-10 is well separated and recall is ~0.98. The hard end of the curve
//! (N=20000, d=128, where recall@10 at these defaults is 0.594) lives in
//! `vector_recall_baseline.rs`, which is `#[ignore]`d because it takes minutes.
//!
//! `dist/best` is the load-bearing one. Recall@k counts a swap between two
//! near-equidistant cluster members as a full miss, so it understates quality
//! on clustered data; the ratio of returned to ideal total distance does not.

use db::index::vector::core::Metric;
use db::index::vector::engine::ann::VectorIndexEngine;
use db::index::vector::store::NodeId;

const SEED: u64 = 0x9E37_79B9;
const QUERIES: usize = 30;
const K: usize = 10;

struct Measured {
    recall: f64,
    ratio: f64,
}

async fn measure(dimensions: usize, count: usize, clusters: usize, ef: usize) -> Measured {
    let mut corpus = crate::support::Corpus::new(SEED);
    let vectors = corpus.clustered(count, dimensions, clusters, 0.15);

    let mut graph = crate::support::graph(SEED);
    for (i, vector) in vectors.iter().enumerate() {
        graph.insert(NodeId(i as u64), vector).await.unwrap();
    }

    let mut queries = crate::support::Corpus::new(SEED ^ 0xFFFF);
    let (mut hit, mut total, mut ratio_sum) = (0usize, 0usize, 0.0f64);
    for _ in 0..QUERIES {
        let query = queries.vector(dimensions);
        let want = crate::support::scored(&vectors, &query, K);
        let got = graph.search(&query, K, Some(ef)).await.unwrap();

        hit += got.iter().filter(|n| want.contains(&n.id)).count();
        total += want.len();

        let ideal: f64 = want
            .iter()
            .map(|id| Metric::Cosine.distance(&query, &vectors[id.0 as usize]))
            .sum();
        let actual: f64 = got
            .iter()
            .map(|n| Metric::Cosine.distance(&query, &vectors[n.id.0 as usize]))
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

#[tokio::test]
async fn clustered_low_dimension_recall_holds() {
    let m = measure(16, 2_000, 40, 64).await;
    println!(
        "clustered d=16 N=2000 ef=64: recall@10={:.4} dist/best={:.4}",
        m.recall, m.ratio
    );
    assert!(m.recall >= 0.98, "recall@10 fell to {:.4}", m.recall);
    assert!(
        m.ratio <= 1.005,
        "returned neighbours are {:.4}x ideal",
        m.ratio
    );
}

#[tokio::test]
async fn clustered_high_dimension_recall_holds() {
    let m = measure(128, 2_000, 40, 64).await;
    println!(
        "clustered d=128 N=2000 ef=64: recall@10={:.4} dist/best={:.4}",
        m.recall, m.ratio
    );
    assert!(m.recall >= 0.95, "recall@10 fell to {:.4}", m.recall);
    assert!(
        m.ratio <= 1.005,
        "returned neighbours are {:.4}x ideal",
        m.ratio
    );
}
