//! SIFT-small: search cost and recall on real vectors with published ground
//! truth.
//!
//! ```text
//! just setup-sift
//! cargo bench -p db-index --bench sift
//! ```
//!
//! Recall is printed rather than asserted, because a benchmark is not a gate.
//! It is here so a number that describes real data sits next to the cost of
//! producing it.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use db::index::vector::store::NodeId;
use std::hint::black_box;
use tokio::runtime::Runtime;

mod common;

use common::sift::{skip_notice, SiftSmall};
use common::{Index, ALL_KINDS};
use db::index::vector::core::Metric;

/// SIFT's ground truth is Euclidean.
const METRIC: Metric = Metric::Euclidean;

const K: usize = 10;

fn runtime() -> Runtime {
    Runtime::new().expect("a tokio runtime")
}

/// Recall against the corpus's own ground truth, which is the whole reason to
/// use it: every other number in this crate is measured against an oracle over
/// vectors we generated.
fn recall(c: &mut Criterion) {
    let Some(sift) = SiftSmall::load() else {
        skip_notice("sift/recall");
        return;
    };
    let rt = runtime();

    println!(
        "\nSIFT-small: {} base vectors, {} dimensions, {} queries, recall@{K} against published ground truth",
        sift.base.len(),
        sift.dimensions(),
        sift.queries.len()
    );

    for kind in ALL_KINDS {
        let index = rt.block_on(Index::filled_with(
            kind,
            METRIC,
            &sift.base,
            kind.is_batch_built(),
        ));
        let mut hit = 0usize;
        let mut total = 0usize;
        for (q, query) in sift.queries.iter().enumerate() {
            let want: Vec<u64> = sift.groundtruth[q]
                .iter()
                .take(K)
                .map(|i| *i as u64 + 1)
                .collect();
            let got = rt.block_on(index.search_ids(query, K));
            hit += got.iter().filter(|id| want.contains(id)).count();
            total += want.len();
        }
        println!(
            "  {:<6} recall@{K} = {:.4}",
            kind.name(),
            hit as f64 / total as f64
        );
    }
    println!();

    // The cost of the same queries, so recall is never reported without it.
    let mut group = c.benchmark_group("sift/search");
    for kind in ALL_KINDS {
        let index = rt.block_on(Index::filled_with(
            kind,
            METRIC,
            &sift.base,
            kind.is_batch_built(),
        ));
        let query = sift.queries[0].clone();
        group.bench_with_input(
            BenchmarkId::from_parameter(kind.name()),
            &kind,
            |b, _kind| {
                b.to_async(&rt)
                    .iter(|| async { black_box(index.search(black_box(&query), K).await) });
            },
        );
    }
    group.finish();
}

/// Building each kind over the real corpus.
fn bulk_load(c: &mut Criterion) {
    let Some(sift) = SiftSmall::load() else {
        skip_notice("sift/bulk_load");
        return;
    };
    let rt = runtime();

    let mut group = c.benchmark_group("sift/bulk_load");
    group.sample_size(10);
    for kind in ALL_KINDS {
        group.bench_with_input(
            BenchmarkId::from_parameter(kind.name()),
            &kind,
            |b, &kind| {
                b.to_async(&rt).iter(|| async {
                    black_box(
                        Index::filled_with(kind, METRIC, &sift.base, kind.is_batch_built()).await,
                    )
                });
            },
        );
    }
    group.finish();
}

/// Inserting into an index already holding the whole corpus.
fn insert(c: &mut Criterion) {
    let Some(sift) = SiftSmall::load() else {
        skip_notice("sift/insert");
        return;
    };
    let rt = runtime();
    let incoming = &sift.queries;
    let existing = sift.base.len();

    let mut group = c.benchmark_group("sift/insert");
    for kind in ALL_KINDS {
        let fixture = rt.block_on(Index::filled_with(
            kind,
            METRIC,
            &sift.base,
            kind.is_batch_built(),
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(kind.name()),
            &kind,
            |b, _kind| {
                b.to_async(&rt).iter_batched(
                    || fixture.clone(),
                    |mut index| async move {
                        for (i, vector) in incoming.iter().enumerate() {
                            index
                                .insert(NodeId((existing + i + 1) as u64), vector)
                                .await;
                        }
                        index
                    },
                    criterion::BatchSize::LargeInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(sift, recall, bulk_load, insert);
criterion_main!(sift);
