//! What a search and an insert actually cost against storage.
//!
//! `#[ignore]`: a measurement, not a check. Run with
//! `cargo test --release -p db --test vector -- vector_kv_cost --ignored --nocapture`.
//!
//! The in-memory baseline counts node reads; this counts **key reads against a
//! real KV store**, which is the number that matters for a persisted index and
//! which the honesty table has carried as unmeasured since the KV adapter
//! landed.

use crate::support::Corpus;
use crate::support::CORPUS_SEED;
use crate::support::GRAPH_SEED;
use crate::support::QUERY_SEED;
use db::index::error::Result;
use db::index::vector::core::Metric;
use db::index::vector::engine::hnsw::Hnsw;
use db::index::vector::kv_store::KvNodeStore;
use db::index::vector::params::Params;
use db::index::vector::params::DEFAULT_M;
use db::index::vector::store::Meta;
use db::index::vector::store::Node;
use db::index::vector::store::NodeId;
use db::index::vector::store::VectorNodeStore;
use defra_core::thread_bounds::MaybeSend;
use std::collections::HashSet;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Mutex;
use storage::backends::MemoryStore;
use storage::corekv::Store;
use storage::corekv::Txn;

/// Counts every key read and write reaching the store.
#[derive(Debug, Default)]
struct Counting<S> {
    inner: S,
    reads: AtomicUsize,
    writes: AtomicUsize,
    /// Distinct keys read, so a repeat read of the same node is visible
    /// separately from a genuinely new one.
    distinct: Mutex<HashSet<u64>>,
}

impl<S> Counting<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            reads: AtomicUsize::new(0),
            writes: AtomicUsize::new(0),
            distinct: Mutex::new(HashSet::new()),
        }
    }

    /// Reads, writes, distinct keys read.
    fn take(&self) -> (usize, usize, usize) {
        let distinct = {
            let mut seen = self.distinct.lock().unwrap();
            let count = seen.len();
            seen.clear();
            count
        };
        (
            self.reads.swap(0, Ordering::Relaxed),
            self.writes.swap(0, Ordering::Relaxed),
            distinct,
        )
    }
}

#[async_trait::async_trait]
impl<S: VectorNodeStore> VectorNodeStore for Counting<S> {
    async fn get_node(&self, id: NodeId) -> Result<Option<Node>> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.distinct.lock().unwrap().insert(id.0);
        self.inner.get_node(id).await
    }

    async fn put_node(&mut self, node: Node) -> Result<()> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.inner.put_node(node).await
    }

    async fn get_meta(&self) -> Result<Option<Meta>> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.get_meta().await
    }

    async fn put_meta(&mut self, meta: Meta) -> Result<()> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.inner.put_meta(meta).await
    }

    async fn iterate_nodes<F>(&self, visit: F) -> Result<()>
    where
        F: FnMut(Node) -> Result<()> + MaybeSend,
    {
        self.inner.iterate_nodes(visit).await
    }

    async fn get_aux(&self, kind: u8, key: &[u8]) -> Result<Option<Vec<u8>>> {
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

#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement; run with --ignored --nocapture"]
async fn cost_against_a_kv_store() {
    const K: usize = 10;
    const QUERIES: usize = 50;

    println!(
        "{:>7} {:>4} {:>4} {:>9} {:>9} {:>7} {:>10} {:>10}",
        "N", "dim", "ef", "reads/q", "distinct", "repeat", "writes/ins", "reads/ins"
    );

    for (count, dimensions) in [(1_000usize, 16usize), (5_000, 16), (5_000, 128)] {
        let mut corpus = Corpus::new(CORPUS_SEED);
        let vectors = corpus.vectors(count, dimensions);

        let store = MemoryStore::new();
        let mut write: Box<dyn Txn> = store.new_txn(false).await.unwrap();
        let (insert_reads, insert_writes, _) = {
            let mut index = Hnsw::new(
                Counting::new(KvNodeStore::new(&mut write, 1, 1, 0)),
                Metric::Cosine,
                Params::new(DEFAULT_M),
                GRAPH_SEED,
            );
            for (i, vector) in vectors.iter().enumerate() {
                index.insert(NodeId(i as u64), vector).await.unwrap();
            }
            index.store().take()
        };
        write.commit().await.unwrap();

        let mut read: Box<dyn Txn> = store.new_txn(false).await.unwrap();
        let index = Hnsw::new(
            Counting::new(KvNodeStore::new(&mut read, 1, 1, 0)),
            Metric::Cosine,
            Params::new(DEFAULT_M),
            GRAPH_SEED,
        );

        for ef in [32usize, 64] {
            let mut queries = Corpus::new(QUERY_SEED);
            let (mut reads, mut distinct) = (0usize, 0usize);
            index.store().take();
            for _ in 0..QUERIES {
                let query = queries.vector(dimensions);
                index.search_with_ef(&query, K, ef).await.unwrap();
                // Per query, so a repeat read within one search is visible and
                // a node touched by two different searches is not miscounted
                // as one.
                let (query_reads, _, query_distinct) = index.store().take();
                reads += query_reads;
                distinct += query_distinct;
            }
            println!(
                "{count:>7} {dimensions:>4} {ef:>4} {:>9.0} {:>9.0} {:>6.0}% {:>10.1} {:>10.1}",
                reads as f64 / QUERIES as f64,
                distinct as f64 / QUERIES as f64,
                100.0 * (1.0 - distinct as f64 / reads as f64),
                insert_writes as f64 / count as f64,
                insert_reads as f64 / count as f64,
            );
        }
    }
}
