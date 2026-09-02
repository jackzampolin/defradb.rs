//! Does SSG earn its place?
//!
//! SSG here derives its kNN graph from HNSW layer 0 rather than NN-Descent,
//! which is a deviation from the paper taken because NN-Descent holds the whole
//! graph resident. The question that decides whether this kind ships is whether
//! the deviation still beats the graph it was built from, measured as **recall
//! per node read** so a win bought by reading more of the corpus does not count.
//!
//! ```text
//! cargo test --release -p db --test vector -- ssg_vs_hnsw --ignored --nocapture
//! ```

use async_trait::async_trait;
use db::index::error::Result;
use db::index::vector::engine::ann::VectorIndexEngine;
use db::index::vector::engine::flat::Flat;
use db::index::vector::engine::hnsw::Hnsw;
use db::index::vector::engine::ssg::Ssg;
use db::index::vector::engine::ssg::SsgParams;
use db::index::vector::params::Params;
use db::index::vector::params::DEFAULT_M;
use db::index::vector::store::MemoryNodeStore;
use db::index::vector::store::Meta;
use db::index::vector::store::Node;
use db::index::vector::store::NodeId;
use db::index::vector::store::VectorNodeStore;
use defra_core::thread_bounds::MaybeSend;
use defra_core::vector::Metric;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

const SEED: u64 = 0x0559_C0DE;
const K: usize = 10;
const QUERIES: usize = 30;

/// Counts node reads, so recall is always reported next to what it cost.
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

#[async_trait]
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

    async fn delete_aux(&mut self, kind: u8, key: &[u8]) -> Result<()> {
        self.inner.delete_aux(kind, key).await
    }

    async fn iterate_aux<F>(&self, kind: u8, key_prefix: &[u8], visit: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<()> + MaybeSend,
    {
        self.inner.iterate_aux(kind, key_prefix, visit).await
    }
}

async fn oracle(vectors: &[Vec<f32>]) -> Flat<MemoryNodeStore> {
    let mut flat = Flat::new(MemoryNodeStore::new(), Metric::Cosine);
    for (i, vector) in vectors.iter().enumerate() {
        flat.insert(NodeId(i as u64 + 1), vector).await.unwrap();
    }
    flat
}

struct Score {
    recall: f64,
    reads: f64,
}

impl Score {
    fn per_read(&self) -> f64 {
        if self.reads <= 0.0 {
            0.0
        } else {
            self.recall / self.reads * 1000.0
        }
    }
}

async fn score_hnsw(vectors: &[Vec<f32>], queries: &[Vec<f32>], ef: usize) -> Score {
    let mut index = Hnsw::new(
        Counting::new(MemoryNodeStore::new()),
        Metric::Cosine,
        Params::new(DEFAULT_M),
        SEED,
    );
    for (i, vector) in vectors.iter().enumerate() {
        index.insert(NodeId(i as u64 + 1), vector).await.unwrap();
    }

    let flat = oracle(vectors).await;
    let (mut hit, mut total, mut reads) = (0usize, 0usize, 0usize);
    for query in queries {
        let want: Vec<u64> = flat
            .search(query.as_slice(), K, None)
            .await
            .unwrap()
            .into_iter()
            .map(|n| n.id.0)
            .collect();
        index.store().take();
        let got = index.search_with_ef(query.as_slice(), K, ef).await.unwrap();
        reads += index.store().take();
        hit += got.iter().filter(|n| want.contains(&n.id.0)).count();
        total += want.len();
    }

    Score {
        recall: hit as f64 / total as f64,
        reads: reads as f64 / queries.len() as f64,
    }
}

async fn score_ssg(vectors: &[Vec<f32>], queries: &[Vec<f32>], pool: u32) -> Score {
    let mut index = Ssg::try_new(
        Counting::new(MemoryNodeStore::new()),
        Metric::Cosine,
        Params::new(DEFAULT_M),
        SsgParams {
            pool,
            ..SsgParams::default()
        },
        SEED,
    )
    .unwrap();
    for (i, vector) in vectors.iter().enumerate() {
        index.insert(NodeId(i as u64 + 1), vector).await.unwrap();
    }
    index.build().await.unwrap();

    let flat = oracle(vectors).await;
    let (mut hit, mut total, mut reads) = (0usize, 0usize, 0usize);
    for query in queries {
        let want: Vec<u64> = flat
            .search(query.as_slice(), K, None)
            .await
            .unwrap()
            .into_iter()
            .map(|n| n.id.0)
            .collect();
        index.store().take();
        let got = index.search(query.as_slice(), K, None).await.unwrap();
        reads += index.store().take();
        hit += got.iter().filter(|n| want.contains(&n.id.0)).count();
        total += want.len();
    }

    Score {
        recall: hit as f64 / total as f64,
        reads: reads as f64 / queries.len() as f64,
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement; run with --ignored --nocapture"]
async fn recall_per_node_read() {
    println!(
        "{:>18} {:>7} {:>6} {:>10} {:>8} {:>14}",
        "index", "N", "effort", "recall@10", "reads", "recall/1k reads"
    );

    for dimensions in [32usize, 128] {
        let mut corpus = crate::support::Corpus::new(SEED);
        let vectors = corpus.clustered(5_000, dimensions, 50, 0.35);
        let queries = corpus.vectors(QUERIES, dimensions);

        for effort in [32usize, 64, 128, 256] {
            let hnsw = score_hnsw(&vectors, &queries, effort).await;
            println!(
                "{:>18} {:>7} {effort:>6} {:>10.4} {:>8.0} {:>14.4}",
                format!("hnsw d={dimensions}"),
                vectors.len(),
                hnsw.recall,
                hnsw.reads,
                hnsw.per_read()
            );

            let ssg = score_ssg(&vectors, &queries, effort as u32).await;
            println!(
                "{:>18} {:>7} {effort:>6} {:>10.4} {:>8.0} {:>14.4}",
                format!("ssg d={dimensions}"),
                vectors.len(),
                ssg.recall,
                ssg.reads,
                ssg.per_read()
            );
        }
    }
}
