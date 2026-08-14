//! IVF-PQ recall and cost against `Flat`, the exact oracle.
//!
//! `#[ignore]` because it takes minutes. Its output is what the plan records,
//! so the numbers there stay reproducible:
//!
//! ```text
//! cargo test --release -p db-index --test ivfpq_recall_baseline -- --ignored --nocapture
//! ```
//!
//! Two errors compound here and the table separates them. Probing fewer than
//! every list can miss a neighbour whose code was never scanned; quantization
//! can rank a scanned code wrongly. At `nprobe = nlist` only the second remains,
//! which is the row that isolates the codec.

use db_index::vector::core::Metric;
use db_index::vector::engine::ann::VectorIndexEngine;
use db_index::vector::engine::flat::Flat;
use db_index::vector::engine::ivfpq::{IvfPq, IvfPqParams};
use db_index::vector::store::{MemoryNodeStore, NodeId};

mod common;

const SEED: u64 = 0x01F4_9C0D;
const K: usize = 10;
const QUERIES: usize = 30;

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

struct Row {
    recall: f64,
    ratio: f64,
    code_bytes: usize,
    vector_bytes: usize,
}

async fn measure(
    vectors: &[Vec<f32>],
    queries: &[Vec<f32>],
    dimensions: usize,
    nlist: u32,
    nprobe: u32,
    m: u32,
) -> Row {
    let mut index = IvfPq::try_new(
        MemoryNodeStore::new(),
        Metric::Cosine,
        IvfPqParams {
            nlist,
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
    let report = index.build().await.unwrap();

    let (mut hit, mut total, mut ratio_sum) = (0usize, 0usize, 0.0f64);
    for query in queries {
        let want = exact(vectors, query, K).await;
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

    Row {
        recall: hit as f64 / total as f64,
        ratio: ratio_sum / QUERIES as f64,
        code_bytes: report.state.m as usize,
        vector_bytes: dimensions * size_of::<f32>(),
    }
}

fn header() {
    println!(
        "{:>22} {:>7} {:>6} {:>7} {:>4} {:>10} {:>9} {:>12}",
        "corpus", "N", "nlist", "nprobe", "m", "recall@10", "dist/best", "bytes/vector"
    );
}

fn report(label: &str, n: usize, nlist: u32, nprobe: u32, m: u32, row: &Row) {
    println!(
        "{label:>22} {n:>7} {nlist:>6} {nprobe:>7} {m:>4} {:>10.4} {:>9.4} {:>5} -> {:<4}",
        row.recall, row.ratio, row.vector_bytes, row.code_bytes
    );
}

/// The full curve: how recall trades against how many lists are probed.
///
/// Queries are **held out**, not corpus members, and the clusters overlap. With
/// tight clusters and a query drawn from the corpus, its true top-10 all sit in
/// its own list and `nprobe` cannot matter, which measures nothing.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement; run with --ignored --nocapture"]
async fn recall_vs_nprobe() {
    header();
    for dimensions in [32usize, 128] {
        let mut corpus = common::Corpus::new(SEED);
        let vectors = corpus.clustered(5_000, dimensions, 64, 0.45);
        let queries = corpus.vectors(QUERIES, dimensions);
        for nprobe in [1u32, 2, 4, 8, 16, 32, 64] {
            let row = measure(&vectors, &queries, dimensions, 64, nprobe, 32).await;
            report(
                &format!("clustered d={dimensions}"),
                vectors.len(),
                64,
                nprobe,
                32,
                &row,
            );
        }
    }
}

/// Compression against accuracy, with the coarse step exhaustive so the only
/// error left is the codec's.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement; run with --ignored --nocapture"]
async fn recall_vs_compression() {
    header();
    let dimensions = 128;
    let mut corpus = common::Corpus::new(SEED ^ 0xC0DE);
    let vectors = corpus.clustered(5_000, dimensions, 32, 0.12);

    let queries = corpus.vectors(QUERIES, dimensions);
    for m in [4u32, 8, 16, 32, 64] {
        let row = measure(&vectors, &queries, dimensions, 32, 32, m).await;
        report("clustered d=128", vectors.len(), 32, 32, m, &row);
    }
}

/// What the untrained state costs: exact, and linear.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement; run with --ignored --nocapture"]
async fn the_untrained_state_is_exact() {
    let dimensions = 64;
    let mut corpus = common::Corpus::new(SEED ^ 0xFEED);
    let vectors = corpus.clustered(2_000, dimensions, 16, 0.12);

    let mut index = IvfPq::try_new(
        MemoryNodeStore::new(),
        Metric::Cosine,
        IvfPqParams::default(),
        SEED,
    )
    .unwrap();
    for (i, vector) in vectors.iter().enumerate() {
        index.insert(NodeId(i as u64 + 1), vector).await.unwrap();
    }

    let mut agreed = 0usize;
    for q in 0..QUERIES {
        let query = &vectors[q * 7 % vectors.len()];
        let got: Vec<u64> = index
            .search(query.as_slice(), K, None)
            .await
            .unwrap()
            .into_iter()
            .map(|n| n.id.0)
            .collect();
        if got == exact(&vectors, query, K).await {
            agreed += 1;
        }
    }
    println!("untrained: {agreed}/{QUERIES} queries identical to an exact scan");
    assert_eq!(agreed, QUERIES, "the untrained state must be exact");
}
