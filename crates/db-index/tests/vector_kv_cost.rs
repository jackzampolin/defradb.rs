//! What a search and an insert actually cost against storage.
//!
//! `#[ignore]`: a measurement, not a check. Run with
//! `cargo test --release -p db-index --test vector_kv_cost -- --ignored --nocapture`.
//!
//! The in-memory baseline counts node reads; this counts **key reads against a
//! real KV store**, which is the number that matters for a persisted index and
//! which the honesty table has carried as unmeasured since the KV adapter
//! landed.

mod common;

use common::{Corpus, CORPUS_SEED, GRAPH_SEED, QUERY_SEED};

use std::sync::atomic::{AtomicUsize, Ordering};

use db_index::error::Result;
use db_index::vector::core::Metric;
use db_index::vector::engine::hnsw::Hnsw;
use db_index::vector::kv_store::KvNodeStore;
use db_index::vector::params::{Params, DEFAULT_M};
use db_index::vector::store::{Meta, Node, NodeId, VectorNodeStore};
use defra_core::thread_bounds::MaybeSend;
use storage::backends::MemoryStore;
use storage::corekv::{Store, Txn};

/// Counts every key read and write reaching the store.
#[derive(Debug, Default)]
struct Counting<S> {
    inner: S,
    reads: AtomicUsize,
    writes: AtomicUsize,
}

impl<S> Counting<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            reads: AtomicUsize::new(0),
            writes: AtomicUsize::new(0),
        }
    }

    fn take(&self) -> (usize, usize) {
        (
            self.reads.swap(0, Ordering::Relaxed),
            self.writes.swap(0, Ordering::Relaxed),
        )
    }
}

#[async_trait::async_trait]
impl<S: VectorNodeStore> VectorNodeStore for Counting<S> {
    async fn get_node(&self, id: NodeId) -> Result<Option<Node>> {
        self.reads.fetch_add(1, Ordering::Relaxed);
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
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement; run with --ignored --nocapture"]
async fn cost_against_a_kv_store() {
    const K: usize = 10;
    const QUERIES: usize = 50;

    println!(
        "{:>7} {:>4} {:>4} {:>12} {:>12} {:>12}",
        "N", "dim", "ef", "reads/search", "writes/insert", "reads/insert"
    );

    for (count, dimensions) in [(1_000usize, 16usize), (5_000, 16), (5_000, 128)] {
        let mut corpus = Corpus::new(CORPUS_SEED);
        let vectors = corpus.vectors(count, dimensions);

        let store = MemoryStore::new();
        let mut write: Box<dyn Txn> = store.new_txn(false).await.unwrap();
        let (insert_reads, insert_writes) = {
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
            index.store().take();
            for _ in 0..QUERIES {
                let query = queries.vector(dimensions);
                index.search_with_ef(&query, K, ef).await.unwrap();
            }
            let (reads, _) = index.store().take();
            println!(
                "{count:>7} {dimensions:>4} {ef:>4} {:>12.0} {:>12.1} {:>12.1}",
                reads as f64 / QUERIES as f64,
                insert_writes as f64 / count as f64,
                insert_reads as f64 / count as f64,
            );
        }
    }
}
