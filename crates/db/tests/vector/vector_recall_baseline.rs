//! The Phase 1 recall baseline: a measurement, not a check.
//!
//! `#[ignore]` because it takes minutes and asserts almost nothing. Its output
//! is what the plan's status table records, so the numbers there stay
//! reproducible rather than being a claim nobody can re-derive:
//!
//! ```text
//! cargo test --release -p db --test vector -- vector_recall_baseline --ignored --nocapture
//! ```
//!
//! The fast gate lives in `vector_engine.rs`. What this adds is scale and the
//! shape of the corpus, and the second is what matters most: uniformly random
//! vectors in high dimensions are all near-orthogonal, which is the degenerate
//! case for *any* approximate index. Real embeddings are clustered. Reporting a
//! number from uniform random data as though it were a recall result would be
//! the fabrication this repo's honesty rules exist to prevent.

use db::index::error::Result;
use db::index::vector::core::Metric;
use db::index::vector::engine::hnsw::Hnsw;
use db::index::vector::params::Params;
use db::index::vector::params::DEFAULT_M;
use db::index::vector::store::MemoryNodeStore;
use db::index::vector::store::Meta;
use db::index::vector::store::Node;
use db::index::vector::store::NodeId;
use db::index::vector::store::VectorNodeStore;
use defra_core::thread_bounds::MaybeSend;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

const CORPUS_SEED: u64 = 0x5EED_C0FF_EE00_1234;
const GRAPH_SEED: u64 = 0x0000_1234_5678_9ABC;
const QUERY_SEED: u64 = 0xABCD_EF01_2345_6789;
const K: usize = 10;
const QUERIES: usize = 50;

struct Corpus {
    state: u64,
}

impl Corpus {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    fn vector(&mut self, dimensions: usize) -> Vec<f32> {
        (0..dimensions)
            .map(|_| (self.unit() * 2.0 - 1.0) as f32)
            .collect()
    }

    fn vectors(&mut self, count: usize, dimensions: usize) -> Vec<Vec<f32>> {
        (0..count).map(|_| self.vector(dimensions)).collect()
    }

    fn gaussian(&mut self) -> f32 {
        let u1 = self.unit().max(f64::MIN_POSITIVE);
        let u2 = self.unit();
        ((-2.0 * u1.ln()).sqrt() * (core::f64::consts::TAU * u2).cos()) as f32
    }

    /// `clusters` centers with gaussian noise around each: the shape a real
    /// embedding corpus has, and the one a graph index is built for.
    fn clustered(
        &mut self,
        count: usize,
        dimensions: usize,
        clusters: usize,
        spread: f32,
    ) -> Vec<Vec<f32>> {
        let centers: Vec<Vec<f32>> = (0..clusters).map(|_| self.vector(dimensions)).collect();
        (0..count)
            .map(|i| {
                let center = &centers[i % clusters];
                (0..dimensions)
                    .map(|j| center[j] + self.gaussian() * spread)
                    .collect()
            })
            .collect()
    }
}

/// Counts node reads, so recall is always reported next to how much of the
/// graph the walk touched to earn it.
#[derive(Debug, Default)]
struct Counting<S> {
    inner: S,
    reads: AtomicUsize,
}

impl<S> Counting<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            reads: AtomicUsize::new(0),
        }
    }

    fn take(&self) -> usize {
        self.reads.swap(0, Ordering::Relaxed)
    }
}

#[async_trait::async_trait]
impl<S: VectorNodeStore> VectorNodeStore for Counting<S> {
    async fn get_node(&self, id: NodeId) -> Result<Option<Node>> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.get_node(id).await
    }

    async fn put_node(&mut self, node: Node) -> Result<()> {
        self.inner.put_node(node).await
    }

    async fn get_meta(&self) -> Result<Option<Meta>> {
        self.inner.get_meta().await
    }

    async fn put_meta(&mut self, meta: Meta) -> Result<()> {
        self.inner.put_meta(meta).await
    }

    async fn iterate_nodes<F>(&self, visit: F) -> Result<()>
    where
        F: FnMut(Node) -> Result<()> + MaybeSend,
    {
        self.inner.iterate_nodes(visit).await
    }

    async fn get_aux(&self, kind: u8, key: &[u8]) -> Result<Option<bytes::Bytes>> {
        self.inner.get_aux(kind, key).await
    }

    async fn put_aux(&mut self, kind: u8, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put_aux(kind, key, value).await
    }

    async fn iterate_aux<F>(&self, kind: u8, key_prefix: &[u8], visit: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<()> + MaybeSend,
    {
        self.inner.iterate_aux(kind, key_prefix, visit).await
    }
}

fn exact(metric: Metric, vectors: &[Vec<f32>], query: &[f32]) -> Vec<NodeId> {
    let mut scored: Vec<(NodeId, f64)> = vectors
        .iter()
        .enumerate()
        .map(|(i, v)| (NodeId(i as u64), metric.distance(query, v)))
        .collect();
    scored.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    scored.into_iter().take(K).map(|(id, _)| id).collect()
}

async fn measure(label: &str, vectors: &[Vec<f32>], dimensions: usize, efs: &[usize]) {
    measure_with(Metric::Cosine, label, vectors, dimensions, efs).await
}

async fn measure_with(
    metric: Metric,
    label: &str,
    vectors: &[Vec<f32>],
    dimensions: usize,
    efs: &[usize],
) {
    measure_m(metric, DEFAULT_M, label, vectors, dimensions, efs).await
}

async fn measure_m(
    metric: Metric,
    m: usize,
    label: &str,
    vectors: &[Vec<f32>],
    dimensions: usize,
    efs: &[usize],
) {
    let mut index = Hnsw::new(
        Counting::new(MemoryNodeStore::new()),
        metric,
        Params::new(m),
        GRAPH_SEED,
    );
    for (i, vector) in vectors.iter().enumerate() {
        index.insert(NodeId(i as u64), vector).await.unwrap();
    }

    for &ef in efs {
        let mut queries = Corpus::new(QUERY_SEED);
        let (mut hit, mut total, mut reads) = (0usize, 0usize, 0usize);
        let mut ratio_sum = 0.0f64;
        for _ in 0..QUERIES {
            let query = queries.vector(dimensions);
            let want = exact(metric, vectors, &query);
            index.store().take();
            let got = index.search_with_ef(&query, K, ef).await.unwrap();
            reads += index.store().take();
            hit += got.iter().filter(|n| want.contains(&n.id)).count();
            total += want.len();

            // Distance ratio separates a genuinely worse answer from a swap
            // between near-ties, which recall@k counts as a full miss.
            let ideal: f64 = want
                .iter()
                .map(|id| metric.distance(&query, &vectors[id.0 as usize]))
                .sum();
            let actual: f64 = got
                .iter()
                .map(|n| metric.distance(&query, &vectors[n.id.0 as usize]))
                .sum();
            ratio_sum += if ideal.abs() > f64::EPSILON {
                actual / ideal
            } else {
                1.0
            };
        }
        let per_query = reads as f64 / QUERIES as f64;
        println!(
            "{label:>26} {:>7} {ef:>4} {:>10.4} {:>9.4} {:>8.0} {:>7.1}%",
            vectors.len(),
            hit as f64 / total as f64,
            ratio_sum / QUERIES as f64,
            per_query,
            100.0 * per_query / vectors.len() as f64
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement, minutes long; run with --ignored --nocapture"]
async fn recall_baseline() {
    println!(
        "{:>26} {:>7} {:>4} {:>10} {:>9} {:>8} {:>8}",
        "corpus", "N", "ef", "recall@10", "dist/best", "reads", "%corpus"
    );
    for dimensions in [16usize, 128] {
        for count in [4_000usize, 20_000] {
            let mut corpus = Corpus::new(CORPUS_SEED);
            let uniform = corpus.vectors(count, dimensions);
            measure(
                &format!("uniform d={dimensions}"),
                &uniform,
                dimensions,
                &[32, 64],
            )
            .await;

            let mut corpus = Corpus::new(CORPUS_SEED);
            let clustered = corpus.clustered(count, dimensions, 100, 0.15);
            measure(
                &format!("clustered d={dimensions}"),
                &clustered,
                dimensions,
                &[32, 64],
            )
            .await;
        }
    }
}

/// `M` is the connectivity knob. 16 is tuned for low dimensions; the published
/// HNSW guidance is 32-48 once the data is high-dimensional, and this measures
/// what the default costs at d=128.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement; run with --ignored --nocapture"]
async fn recall_vs_m() {
    println!(
        "{:>26} {:>7} {:>4} {:>10} {:>9} {:>8} {:>8}",
        "corpus", "N", "ef", "recall@10", "dist/best", "reads", "%corpus"
    );
    let dimensions = 128;
    let mut corpus = Corpus::new(CORPUS_SEED);
    let clustered = corpus.clustered(20_000, dimensions, 100, 0.15);
    for m in [16usize, 32, 48, 64] {
        measure_m(
            Metric::Cosine,
            m,
            &format!("clustered d=128 M={m}"),
            &clustered,
            dimensions,
            &[64],
        )
        .await;
    }
}

/// Whether recall is bounded by search effort or by the graph. If it climbs
/// towards 1.0 with `ef` the structure is sound and the rest is a knob; if it
/// plateaus, construction is the limit.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement; run with --ignored --nocapture"]
async fn recall_vs_effort() {
    println!(
        "{:>26} {:>7} {:>4} {:>10} {:>9} {:>8} {:>8}",
        "corpus", "N", "ef", "recall@10", "dist/best", "reads", "%corpus"
    );
    let dimensions = 128;
    let mut corpus = Corpus::new(CORPUS_SEED);
    let clustered = corpus.clustered(20_000, dimensions, 100, 0.15);
    measure(
        "clustered d=128",
        &clustered,
        dimensions,
        &[64, 128, 256, 512, 1024],
    )
    .await;
}

/// Dot product is not a metric, and HNSW's greedy descent assumes one. This
/// measures what that costs against cosine on the same corpus, scaled so
/// magnitudes actually differ; without the spread the two are the same query.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement; run with --ignored --nocapture"]
async fn dot_recall() {
    println!(
        "{:>26} {:>7} {:>4} {:>10} {:>9} {:>8} {:>8}",
        "corpus", "N", "ef", "recall@10", "dist/best", "reads", "%corpus"
    );
    for dimensions in [16usize, 128] {
        let mut corpus = Corpus::new(CORPUS_SEED);
        let mut vectors = corpus.clustered(20_000, dimensions, 100, 0.15);
        let mut scale = Corpus::new(QUERY_SEED);
        for vector in &mut vectors {
            let factor = 0.25 + 4.0 * scale.unit() as f32;
            for component in vector.iter_mut() {
                *component *= factor;
            }
        }

        measure_with(
            Metric::Cosine,
            &format!("cosine d={dimensions}"),
            &vectors,
            dimensions,
            &[32, 64],
        )
        .await;
        measure_with(
            Metric::NegativeDot,
            &format!("dot d={dimensions}"),
            &vectors,
            dimensions,
            &[32, 64],
        )
        .await;
    }
}

/// Graph health, independent of recall. A layer-0 degree far under `m_max0`, an
/// isolated node, or a level distribution away from `1/m` is a structural
/// defect; low recall on near-orthogonal data is not.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement; run with --ignored --nocapture"]
async fn graph_shape() {
    for dimensions in [16usize, 128] {
        let mut corpus = Corpus::new(CORPUS_SEED);
        let vectors = corpus.vectors(20_000, dimensions);
        let params = Params::new(DEFAULT_M);
        let mut index = Hnsw::new(MemoryNodeStore::new(), Metric::Cosine, params, GRAPH_SEED);
        for (i, vector) in vectors.iter().enumerate() {
            index.insert(NodeId(i as u64), vector).await.unwrap();
        }

        let mut degrees = Vec::new();
        let mut above_layer_zero = 0usize;
        index
            .store()
            .iterate_nodes(|node| {
                degrees.push(node.layers[0].len());
                if node.layers.len() > 1 {
                    above_layer_zero += 1;
                }
                Ok(())
            })
            .await
            .unwrap();
        degrees.sort_unstable();

        let mean = degrees.iter().sum::<usize>() as f64 / degrees.len() as f64;
        let isolated = degrees.iter().filter(|&&d| d == 0).count();
        println!(
            "d={dimensions}: layer-0 degree mean={mean:.1} median={} p10={} cap={} | isolated={isolated} \
             | above layer 0 = {:.1}% (theory {:.1}%)",
            degrees[degrees.len() / 2],
            degrees[degrees.len() / 10],
            params.m_max0,
            100.0 * above_layer_zero as f64 / degrees.len() as f64,
            100.0 / DEFAULT_M as f64
        );
        assert_eq!(isolated, 0, "d={dimensions}: the graph has isolated nodes");
    }
}
