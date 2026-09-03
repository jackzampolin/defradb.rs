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
use db::index::vector::engine::ann::VectorIndexEngine;
use db::index::vector::engine::hnsw::Hnsw;
use db::index::vector::engine::ivfflat::IvfFlat;
use db::index::vector::engine::ivfflat::IvfFlatParams;
use db::index::vector::kv_store::KvNodeStore;
use db::index::vector::params::Params;
use db::index::vector::params::DEFAULT_M;
use db::index::vector::store::Meta;
use db::index::vector::store::Node;
use db::index::vector::store::NodeId;
use db::index::vector::store::VectorNodeStore;
use defra_core::thread_bounds::MaybeSend;
use defra_core::vector::Metric;
use std::collections::HashSet;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Mutex;
use storage::corekv::Store;
use storage::corekv::Txn;
use storage::RegolithStore;

/// Counts every key read and write reaching the store, node and aux alike.
#[derive(Debug, Default)]
struct Counting<S> {
    inner: S,
    reads: AtomicUsize,
    writes: AtomicUsize,
    /// Distinct keys read, so a repeat read of the same node is visible
    /// separately from a genuinely new one.
    distinct: Mutex<HashSet<u64>>,
    /// Entries handed to an `iterate_aux` visitor: what a list scan actually
    /// touches, as opposed to the number of `iterate_aux` calls (one per
    /// probed list) or the corpus size.
    aux_entries_visited: AtomicUsize,
    /// Largest single value handed to `put_aux`, so a build's per-write cost
    /// can be checked against one vector's width rather than assumed.
    max_aux_write_bytes: AtomicUsize,
}

impl<S> Counting<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            reads: AtomicUsize::new(0),
            writes: AtomicUsize::new(0),
            distinct: Mutex::new(HashSet::new()),
            aux_entries_visited: AtomicUsize::new(0),
            max_aux_write_bytes: AtomicUsize::new(0),
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

    /// Aux entries visited, and the largest single write, since the last
    /// call.
    fn take_aux(&self) -> (usize, usize) {
        (
            self.aux_entries_visited.swap(0, Ordering::Relaxed),
            self.max_aux_write_bytes.swap(0, Ordering::Relaxed),
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

    async fn get_aux(&self, kind: u8, key: &[u8]) -> Result<Option<bytes::Bytes>> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.get_aux(kind, key).await
    }

    async fn put_aux(&mut self, kind: u8, key: &[u8], value: &[u8]) -> Result<()> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.max_aux_write_bytes
            .fetch_max(value.len(), Ordering::Relaxed);
        self.inner.put_aux(kind, key, value).await
    }

    async fn delete_aux(&mut self, kind: u8, key: &[u8]) -> Result<()> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.inner.delete_aux(kind, key).await
    }

    async fn iterate_aux<F>(&self, kind: u8, key_prefix: &[u8], mut visit: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<()> + MaybeSend,
    {
        let entries = &self.aux_entries_visited;
        self.inner
            .iterate_aux(kind, key_prefix, |key, value| {
                entries.fetch_add(1, Ordering::Relaxed);
                visit(key, value)
            })
            .await
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

        let store = RegolithStore::in_memory().unwrap();
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

/// IVF_FLAT's build cost: whether it holds anything sized by the corpus.
///
/// The structure passed between the read pass that computes a document's list
/// and the write pass that stores it is `(u32, NodeId)`, never the vector
/// itself; the vector is re-read from the node it is already durable in. That
/// is checked two ways rather than assumed: the number of aux writes a build
/// performs is exactly `nlist` centroids plus one list entry per document
/// plus one state marker (linear in the corpus, never more), and no single
/// write handed to the store ever exceeds one vector's encoded width, at any
/// corpus size or dimension.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement; run with --ignored --nocapture"]
async fn ivfflat_build_holds_nothing_corpus_sized() {
    println!(
        "{:>7} {:>4} {:>6} {:>12} {:>12} {:>14} {:>14}",
        "N", "dim", "nlist", "expect_writes", "writes", "max_write(B)", "vector(B)"
    );

    for (count, dimensions, nlist) in [(200usize, 16usize, 8u32), (2_000, 16, 32), (2_000, 256, 32)]
    {
        let mut corpus = Corpus::new(CORPUS_SEED);
        let vectors = corpus.vectors(count, dimensions);

        let store = RegolithStore::in_memory().unwrap();
        let mut write: Box<dyn Txn> = store.new_txn(false).await.unwrap();
        let params = IvfFlatParams {
            nlist,
            nprobe: nlist,
            ..IvfFlatParams::default()
        };
        let mut index = IvfFlat::try_new(
            Counting::new(KvNodeStore::new(&mut write, 1, 1, 0)),
            Metric::Cosine,
            params,
            GRAPH_SEED,
        )
        .unwrap();
        for (i, vector) in vectors.iter().enumerate() {
            index.insert(NodeId(i as u64 + 1), vector).await.unwrap();
        }
        // Discard the insert phase's own counts: only the build matters here.
        index.store().take();
        index.store().take_aux();

        let report = index.build().await.unwrap();
        let (_reads, actual_writes, _distinct) = index.store().take();
        let (_visited, max_write) = index.store().take_aux();

        let expected_writes = report.state.nlist as usize + report.indexed as usize + 1;
        let one_vector_bytes = dimensions * 4;

        println!(
            "{count:>7} {dimensions:>4} {nlist:>6} {expected_writes:>12} {actual_writes:>12} \
             {max_write:>14} {one_vector_bytes:>14}"
        );
        assert_eq!(
            actual_writes, expected_writes,
            "build performed {actual_writes} aux writes, expected exactly \
             nlist + corpus + 1 = {expected_writes}: a build is not linear in the corpus"
        );
        assert_eq!(
            max_write, one_vector_bytes,
            "the largest single aux write was {max_write}B, expected exactly one \
             encoded vector ({one_vector_bytes}B): some write scaled with the corpus"
        );
    }
}

/// IVF_FLAT's search cost: whether a probe reads only the lists it probed,
/// rather than assuming it from the design.
///
/// The entries an `iterate_aux` scan hands to its visitor during a search are
/// counted directly. Probing every list must visit exactly one entry per live
/// document, since every document lives in exactly one list; probing fewer
/// must visit strictly fewer than the whole corpus, or the entire premise of
/// probing a handful of lists instead of scanning everything does not hold.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement; run with --ignored --nocapture"]
async fn ivfflat_search_reads_only_the_probed_lists() {
    const COUNT: usize = 3_000;
    const DIMENSIONS: usize = 32;
    const NLIST: u32 = 24;
    const K: usize = 10;
    const QUERIES: usize = 30;

    let mut corpus = Corpus::new(CORPUS_SEED);
    let vectors = corpus.vectors(COUNT, DIMENSIONS);

    let store = RegolithStore::in_memory().unwrap();
    let mut write: Box<dyn Txn> = store.new_txn(false).await.unwrap();
    {
        let params = IvfFlatParams {
            nlist: NLIST,
            nprobe: NLIST,
            ..IvfFlatParams::default()
        };
        let mut index = IvfFlat::try_new(
            KvNodeStore::new(&mut write, 2, 2, 0),
            Metric::Cosine,
            params,
            GRAPH_SEED,
        )
        .unwrap();
        for (i, vector) in vectors.iter().enumerate() {
            index.insert(NodeId(i as u64 + 1), vector).await.unwrap();
        }
        index.build().await.unwrap();
    }
    write.commit().await.unwrap();

    println!("{:>6} {:>12} {:>10}", "nprobe", "visited/q", "of_corpus");

    for nprobe in [1u32, 4, 12, NLIST] {
        let mut read: Box<dyn Txn> = store.new_txn(false).await.unwrap();
        let params = IvfFlatParams {
            nlist: NLIST,
            nprobe,
            ..IvfFlatParams::default()
        };
        let index = IvfFlat::try_new(
            Counting::new(KvNodeStore::new(&mut read, 2, 2, 0)),
            Metric::Cosine,
            params,
            GRAPH_SEED,
        )
        .unwrap();

        let mut queries = Corpus::new(QUERY_SEED);
        let mut visited_total = 0usize;
        for _ in 0..QUERIES {
            let query = queries.vector(DIMENSIONS);
            index.store().take_aux();
            let hits = index.search(&query, K, None).await.unwrap();
            assert!(!hits.is_empty(), "nprobe={nprobe}: an empty answer");

            let (visited, _) = index.store().take_aux();
            if nprobe >= NLIST {
                assert_eq!(
                    visited, COUNT,
                    "probing every list must visit exactly one entry per live document"
                );
            } else {
                assert!(
                    visited < COUNT,
                    "nprobe={nprobe} visited {visited} of {COUNT}: the probe did not narrow \
                     the scan below the whole corpus"
                );
            }
            visited_total += visited;
        }

        let visited_per_query = visited_total as f64 / QUERIES as f64;
        println!(
            "{nprobe:>6} {visited_per_query:>12.1} {:>9.1}%",
            100.0 * visited_per_query / COUNT as f64
        );
    }
}
