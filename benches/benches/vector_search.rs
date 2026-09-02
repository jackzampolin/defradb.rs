//! Query cost across every vector index kind, and the kernels underneath.
//!
//! ```text
//! cargo bench -p db --bench vector_search
//! ```

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use defra_core::vector::{dot, squared_euclidean, Metric, Tier};
use std::hint::black_box;

mod common;

use common::vector::{Corpus, Index, ALL_KINDS, SEED};

const DIMENSIONS: usize = 128;
const K: usize = 10;

/// The batch-built kinds are measured in their built state, which is the one a
/// production query hits. Their untrained state is a flat scan and is already
/// covered by the `flat` row.
fn search(c: &mut Criterion) {
    let rt = common::owned_runtime();
    let mut corpus = Corpus::new(SEED);
    let vectors = corpus.clustered(5_000, DIMENSIONS, 50, 0.25);
    let query = corpus.vector(DIMENSIONS);

    let mut group = c.benchmark_group("search");
    for kind in ALL_KINDS {
        let index = rt.block_on(Index::filled(kind, &vectors, kind.is_batch_built()));
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

/// How query cost grows with the corpus. The exhaustive kind is linear by
/// construction; the point of the others is that they are not.
fn search_by_corpus_size(c: &mut Criterion) {
    let rt = common::owned_runtime();
    let mut group = c.benchmark_group("search_by_corpus");
    group.sample_size(20);

    for size in [1_000usize, 5_000, 20_000] {
        let mut corpus = Corpus::new(SEED ^ size as u64);
        let vectors = corpus.clustered(size, DIMENSIONS, 50, 0.25);
        let query = corpus.vector(DIMENSIONS);

        for kind in ALL_KINDS {
            let index = rt.block_on(Index::filled(kind, &vectors, kind.is_batch_built()));
            group.bench_with_input(BenchmarkId::new(kind.name(), size), &kind, |b, _kind| {
                b.to_async(&rt)
                    .iter(|| async { black_box(index.search(black_box(&query), K).await) });
            });
        }
    }
    group.finish();
}

/// The kernels every kind sits on. Reported next to the tier that ran them, so
/// a number from a scalar fallback is never mistaken for a SIMD one.
fn kernels(c: &mut Criterion) {
    let mut corpus = Corpus::new(SEED ^ 0xEEE);
    let tier = Tier::active().name();

    let mut group = c.benchmark_group(format!("kernel[{tier}]"));
    for dimensions in [16usize, 128, 768] {
        let a = corpus.vector(dimensions);
        let b_vec = corpus.vector(dimensions);
        let wide_a: Vec<f64> = a.iter().map(|x| *x as f64).collect();
        let wide_b: Vec<f64> = b_vec.iter().map(|x| *x as f64).collect();

        group.throughput(Throughput::Elements(dimensions as u64));

        group.bench_with_input(
            BenchmarkId::new("dot_f32", dimensions),
            &dimensions,
            |bencher, _| bencher.iter(|| black_box(dot(black_box(&a), black_box(&b_vec)))),
        );
        group.bench_with_input(
            BenchmarkId::new("dot_f64", dimensions),
            &dimensions,
            |bencher, _| bencher.iter(|| black_box(dot(black_box(&wide_a), black_box(&wide_b)))),
        );
        group.bench_with_input(
            BenchmarkId::new("squared_euclidean_f32", dimensions),
            &dimensions,
            |bencher, _| {
                bencher.iter(|| black_box(squared_euclidean(black_box(&a), black_box(&b_vec))))
            },
        );
        group.bench_with_input(
            BenchmarkId::new("cosine_f32", dimensions),
            &dimensions,
            |bencher, _| {
                bencher
                    .iter(|| black_box(Metric::Cosine.distance(black_box(&a), black_box(&b_vec))))
            },
        );
        group.bench_with_input(
            BenchmarkId::new("cosine_normalized_f32", dimensions),
            &dimensions,
            |bencher, _| {
                bencher.iter(|| {
                    black_box(Metric::Cosine.distance_normalized(black_box(&a), black_box(&b_vec)))
                })
            },
        );
    }
    group.finish();
}

/// Every tier the running machine can execute, on the same input, so the
/// speedup a SIMD tier buys is a measured ratio rather than a claim.
fn kernel_tiers(c: &mut Criterion) {
    let mut corpus = Corpus::new(SEED ^ 0xFFF);
    let a = corpus.vector(768);
    let b_vec = corpus.vector(768);

    let mut group = c.benchmark_group("kernel_tiers[768]");
    group.throughput(Throughput::Elements(768));

    for tier in defra_core::vector::ALL_TIERS.iter().copied() {
        if !tier.is_available() {
            continue;
        }
        group.bench_with_input(
            BenchmarkId::new("dot_f32", tier.name()),
            &tier,
            |bencher, &tier| bencher.iter(|| black_box(tier.dot(black_box(&a), black_box(&b_vec)))),
        );
    }
    group.finish();
}

criterion_group!(
    queries,
    search,
    search_by_corpus_size,
    kernels,
    kernel_tiers
);
criterion_main!(queries);
