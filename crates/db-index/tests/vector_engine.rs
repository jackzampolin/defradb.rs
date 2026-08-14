//! The HNSW engine against an in-memory store: no KV, no transactions, no
//! defradb types.
//!
//! Recall is *recorded*, not asserted against a target. The assertions here are
//! floors that catch a broken graph (a disconnected component, a dropped
//! back-link, a tombstone leaking into results); the measured number is printed
//! and belongs in the plan's status table, not in an assertion that would turn
//! a quality figure into a pass mark.

use std::sync::atomic::{AtomicUsize, Ordering};

use defra_core::thread_bounds::MaybeSend;

use db_index::error::{Error, Result};
use db_index::vector::core::Metric;
use db_index::vector::engine::ann::{AdmitAll, EngineKind, VectorIndexEngine};
use db_index::vector::engine::flat::Flat;
use db_index::vector::engine::hnsw::{Hnsw, LevelSampler};
use db_index::vector::params::{
    Params, DEFAULT_EF_CONSTRUCTION, DEFAULT_EF_SEARCH, DEFAULT_M, MAX_EF_CONSTRUCTION,
    MAX_EF_SEARCH, MAX_M,
};
use db_index::vector::store::{MemoryNodeStore, Meta, Node, NodeId, VectorNodeStore};

/// Counts node reads, so a recall figure can be reported next to how much of
/// the graph the walk actually touched. A recall of 1.0 means very little if
/// the search visited most of the corpus.
#[derive(Debug, Default)]
struct Counting<S> {
    inner: S,
    node_reads: AtomicUsize,
}

impl<S> Counting<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            node_reads: AtomicUsize::new(0),
        }
    }

    fn take_reads(&self) -> usize {
        self.node_reads.swap(0, Ordering::Relaxed)
    }
}

#[async_trait::async_trait]
impl<S: VectorNodeStore> VectorNodeStore for Counting<S> {
    async fn get_node(&self, id: NodeId) -> Result<Option<Node>> {
        self.node_reads.fetch_add(1, Ordering::Relaxed);
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

mod common;

use common::{build, flat, graph, scored, Corpus, CORPUS_SEED, GRAPH_SEED, QUERY_SEED};

#[tokio::test]
async fn an_empty_graph_searches_without_error() {
    let index = graph(GRAPH_SEED);
    assert!(index
        .search_with_ef(&[1.0, 0.0, 0.0], 10, DEFAULT_EF_SEARCH)
        .await
        .expect("empty graph is not an error")
        .is_empty());
}

#[tokio::test]
async fn k_of_zero_returns_nothing() {
    let mut corpus = Corpus::new(CORPUS_SEED);
    let vectors = corpus.vectors(32, 8);
    let index = build(&vectors, GRAPH_SEED).await;
    assert!(index
        .search_with_ef(&vectors[0], 0, DEFAULT_EF_SEARCH)
        .await
        .unwrap()
        .is_empty());
}

/// Every indexed vector must find itself first when used as its own query.
/// This is exact, not approximate: a graph that cannot retrieve a point it
/// contains is broken, whatever its recall on unseen queries.
#[tokio::test]
async fn every_indexed_vector_retrieves_itself_first() {
    let mut corpus = Corpus::new(CORPUS_SEED);
    let vectors = corpus.vectors(200, 16);
    let index = build(&vectors, GRAPH_SEED).await;

    let mut misses = Vec::new();
    for (i, vector) in vectors.iter().enumerate() {
        let hits = index
            .search_with_ef(vector, 1, DEFAULT_EF_SEARCH)
            .await
            .unwrap();
        match hits.first() {
            Some(hit) if hit.id == NodeId(i as u64) => {}
            other => misses.push((i, other.map(|h| h.id))),
        }
    }
    assert!(
        misses.is_empty(),
        "{} of {} vectors did not retrieve themselves (seeds corpus={CORPUS_SEED:#x} graph={GRAPH_SEED:#x}): {:?}",
        misses.len(),
        vectors.len(),
        &misses[..misses.len().min(5)]
    );
}

#[tokio::test]
async fn results_are_ordered_nearest_first() {
    let mut corpus = Corpus::new(CORPUS_SEED);
    let vectors = corpus.vectors(200, 16);
    let index = build(&vectors, GRAPH_SEED).await;

    let query = Corpus::new(QUERY_SEED).vector(16);
    let hits = index
        .search_with_ef(&query, 10, DEFAULT_EF_SEARCH)
        .await
        .unwrap();
    assert!(hits.len() > 1, "expected several hits, got {}", hits.len());
    for pair in hits.windows(2) {
        assert!(
            pair[0].distance <= pair[1].distance,
            "out of order: {:?} then {:?}",
            pair[0],
            pair[1]
        );
    }
}

/// The Phase 1 gate. Records recall against an exact scan, alongside how much
/// of the graph the walk read to get it, because recall alone says nothing if
/// the search visited everything.
///
/// The corpus is sized so the search is genuinely approximate: at 1,000 vectors
/// of 32 dimensions an earlier version of this test read 74% of the corpus per
/// query and scored a meaningless 1.0000. The assertion is a floor a broken
/// graph would fail, not the figure being measured.
#[tokio::test]
async fn recall_against_an_exact_scan_is_recorded() {
    const CORPUS: usize = 4000;
    const DIMENSIONS: usize = 16;
    const QUERIES: usize = 50;
    const K: usize = 10;
    const EF: usize = 32;
    /// Well below what the graph should reach; present so a graph that stopped
    /// working fails the suite. Not a quality target.
    const BROKEN_BELOW: f64 = 0.75;
    /// Above this the walk is a scan wearing a graph's clothes, and any recall
    /// it reports is meaningless.
    const NOT_APPROXIMATING_ABOVE: f64 = 0.35;

    let mut corpus = Corpus::new(CORPUS_SEED);
    let vectors = corpus.vectors(CORPUS, DIMENSIONS);

    let exact = flat(&vectors).await;
    let mut index = Hnsw::new(
        Counting::new(MemoryNodeStore::new()),
        Metric::Cosine,
        Params::new(DEFAULT_M),
        GRAPH_SEED,
    );
    for (i, vector) in vectors.iter().enumerate() {
        index.insert(NodeId(i as u64), vector).await.unwrap();
    }

    let mut queries = Corpus::new(QUERY_SEED);
    let (mut hit, mut total, mut reads) = (0usize, 0usize, 0usize);
    for _ in 0..QUERIES {
        let query = queries.vector(DIMENSIONS);

        let want: Vec<NodeId> = exact
            .search(&query, K, None)
            .await
            .unwrap()
            .into_iter()
            .map(|n| n.id)
            .collect();
        assert_eq!(
            want,
            scored(&vectors, &query, K),
            "the Flat oracle is not exact, so it cannot judge the graph"
        );

        index.store().take_reads();
        let got = index.search_with_ef(&query, K, EF).await.unwrap();
        reads += index.store().take_reads();
        hit += got.iter().filter(|n| want.contains(&n.id)).count();
        total += want.len();
    }

    let recall = hit as f64 / total as f64;
    let read_fraction = reads as f64 / QUERIES as f64 / CORPUS as f64;
    println!(
        "recall@{K} = {recall:.4} ({hit}/{total}) reading {:.1}% of {CORPUS} vectors per query, \
         {DIMENSIONS} dimensions, m={DEFAULT_M} ef_construction={DEFAULT_EF_CONSTRUCTION} \
         ef_search={EF}, seeds corpus={CORPUS_SEED:#x} graph={GRAPH_SEED:#x} query={QUERY_SEED:#x}",
        100.0 * read_fraction
    );
    assert!(
        recall > BROKEN_BELOW,
        "recall {recall:.4} is below the broken-graph floor {BROKEN_BELOW}"
    );
    assert!(
        read_fraction < NOT_APPROXIMATING_ABOVE,
        "the search read {:.1}% of the corpus, so it is a scan and its recall means nothing",
        100.0 * read_fraction
    );
}

/// Both kinds must be usable through the trait alone, with no knowledge of
/// which one is behind it. A trait one type implements is a guess; this is what
/// makes it an abstraction.
#[tokio::test]
async fn both_kinds_satisfy_the_engine_trait() {
    async fn exercise<E: VectorIndexEngine>(
        engine: &mut E,
        vectors: &[Vec<f32>],
        kind: EngineKind,
    ) {
        assert_eq!(engine.kind(), kind);
        for (i, vector) in vectors.iter().enumerate() {
            engine.insert(NodeId(i as u64), vector).await.unwrap();
        }

        let hits = engine.search(&vectors[0], 5, None).await.unwrap();
        assert_eq!(
            hits.first().map(|h| h.id),
            Some(NodeId(0)),
            "{kind:?}: a stored vector must retrieve itself"
        );
        for pair in hits.windows(2) {
            assert!(pair[0].distance <= pair[1].distance, "{kind:?}: unordered");
        }

        assert!(engine.delete(NodeId(0)).await.unwrap(), "{kind:?}: existed");
        assert!(!engine.delete(NodeId(0)).await.unwrap(), "{kind:?}: no-op");
        let hits = engine.search(&vectors[0], 5, None).await.unwrap();
        assert!(
            hits.iter().all(|h| h.id != NodeId(0)),
            "{kind:?}: a deleted node was returned"
        );

        assert!(engine
            .search(&vectors[1], 0, None)
            .await
            .unwrap()
            .is_empty());
        assert!(engine.search(&vectors[1], 5, Some(64)).await.unwrap().len() > 1);

        // Filtering is part of the contract, so every kind answers it, and the
        // defaulted `search` must agree with an all-admitting `search_where`.
        let filtered = engine
            .search_where(&vectors[1], 5, None, &|id: NodeId| !id.0.is_multiple_of(2))
            .await
            .unwrap();
        assert!(
            filtered.iter().all(|h| !h.id.0.is_multiple_of(2)),
            "{kind:?}: a rejected node was returned"
        );
        assert_eq!(
            engine.search(&vectors[1], 5, None).await.unwrap(),
            engine
                .search_where(&vectors[1], 5, None, &AdmitAll)
                .await
                .unwrap(),
            "{kind:?}: the defaulted search disagrees with an all-admitting one"
        );
    }

    let mut corpus = Corpus::new(CORPUS_SEED);
    let vectors = corpus.vectors(200, 16);

    exercise(
        &mut Hnsw::new(
            MemoryNodeStore::new(),
            Metric::Cosine,
            Params::new(DEFAULT_M),
            GRAPH_SEED,
        ),
        &vectors,
        EngineKind::Hnsw,
    )
    .await;
    exercise(
        &mut Flat::new(MemoryNodeStore::new(), Metric::Cosine),
        &vectors,
        EngineKind::Flat,
    )
    .await;
}

/// A query arriving as `f64`, which is what JSON and GraphQL deliver, must rank
/// identically to the same query as `f32`. The engine narrows to the stored
/// width once; nothing about the caller's width may change the answer.
#[tokio::test]
async fn the_query_width_does_not_change_the_answer() {
    let mut corpus = Corpus::new(CORPUS_SEED);
    let vectors = corpus.vectors(300, 16);

    // Built from f64 input, which is what a document field carries.
    let wide: Vec<Vec<f64>> = vectors
        .iter()
        .map(|v| v.iter().map(|x| *x as f64).collect())
        .collect();
    let mut from_wide = graph(GRAPH_SEED);
    for (i, vector) in wide.iter().enumerate() {
        from_wide.insert(NodeId(i as u64), vector).await.unwrap();
    }
    let from_narrow = build(&vectors, GRAPH_SEED).await;

    let mut queries = Corpus::new(QUERY_SEED);
    for _ in 0..25 {
        let narrow = queries.vector(16);
        let widened: Vec<f64> = narrow.iter().map(|x| *x as f64).collect();
        assert_eq!(
            from_wide.search_with_ef(&widened, 10, 64).await.unwrap(),
            from_narrow.search_with_ef(&narrow, 10, 64).await.unwrap(),
            "an f64 round trip through the index changed the ranking"
        );
        assert_eq!(
            from_narrow.search_with_ef(&widened, 10, 64).await.unwrap(),
            from_narrow.search_with_ef(&narrow, 10, 64).await.unwrap(),
            "an f64 query ranked differently from the same f32 query"
        );
    }
}

/// A search hit carries an `f64` distance no matter what the query was made of.
/// An integer query against a stored corpus still separates by irrational
/// angles, and the hit must be able to say so.
#[tokio::test]
async fn search_hits_carry_a_floating_point_distance_for_an_integer_query() {
    let mut index = graph(GRAPH_SEED);
    for (id, vector) in [
        (1u64, [1.0f32, 0.0, 0.0]),
        (2, [1.0, 1.0, 0.0]),
        (3, [0.0, 1.0, 0.0]),
    ] {
        index.insert(NodeId(id), &vector).await.unwrap();
    }

    // The same query as i32, i64 and f32: one direction, three widths.
    let hits_i32 = index.search_with_ef(&[1i32, 0, 0], 3, 64).await.unwrap();
    let hits_i64 = index.search_with_ef(&[1i64, 0, 0], 3, 64).await.unwrap();
    let hits_f32 = index
        .search_with_ef(&[1.0f32, 0.0, 0.0], 3, 64)
        .await
        .unwrap();
    assert_eq!(hits_i32, hits_i64, "the integer widths must agree");
    assert_eq!(
        hits_i32, hits_f32,
        "an integer query must match its float twin"
    );

    assert_eq!(hits_i32.first().map(|h| h.id), Some(NodeId(1)));
    assert!(
        hits_i32[0].distance.abs() < 1e-6,
        "the identical direction is distance 0, got {}",
        hits_i32[0].distance
    );

    // Node 2 sits at 45 degrees: a distance no integer type could express.
    let at_45 = hits_i32
        .iter()
        .find(|h| h.id == NodeId(2))
        .expect("node 2 is reachable");
    let want = 1.0 - std::f64::consts::FRAC_1_SQRT_2;
    assert!(
        (at_45.distance - want).abs() < 1e-6,
        "expected {want}, got {}",
        at_45.distance
    );
}

/// The exact kind must agree with a direct scan on every query, or it is not an
/// oracle.
#[tokio::test]
async fn the_flat_kind_is_exact() {
    let mut corpus = Corpus::new(CORPUS_SEED);
    let vectors = corpus.vectors(400, 16);
    let exact = flat(&vectors).await;

    let mut queries = Corpus::new(QUERY_SEED);
    for _ in 0..25 {
        let query = queries.vector(16);
        let got: Vec<NodeId> = exact
            .search(&query, 10, None)
            .await
            .unwrap()
            .into_iter()
            .map(|n| n.id)
            .collect();
        assert_eq!(got, scored(&vectors, &query, 10));
    }
}

/// A wider `ef_search` explores more of layer 0, so it cannot find less.
#[tokio::test]
async fn a_wider_search_does_not_lose_hits() {
    const K: usize = 10;
    let mut corpus = Corpus::new(CORPUS_SEED);
    let vectors = corpus.vectors(500, 16);
    let index = build(&vectors, GRAPH_SEED).await;

    let mut queries = Corpus::new(QUERY_SEED);
    let (mut narrow_hits, mut wide_hits, mut total) = (0usize, 0usize, 0usize);
    for _ in 0..50 {
        let query = queries.vector(16);
        let want = scored(&vectors, &query, K);
        let narrow = index.search_with_ef(&query, K, K).await.unwrap();
        let wide = index.search_with_ef(&query, K, 256).await.unwrap();
        narrow_hits += narrow.iter().filter(|n| want.contains(&n.id)).count();
        wide_hits += wide.iter().filter(|n| want.contains(&n.id)).count();
        total += want.len();
    }
    println!(
        "recall@{K}: ef_search={K} -> {:.4}, ef_search=256 -> {:.4}",
        narrow_hits as f64 / total as f64,
        wide_hits as f64 / total as f64
    );
    assert!(
        wide_hits >= narrow_hits,
        "a wider search found fewer true neighbors: {wide_hits} < {narrow_hits}"
    );
}

#[tokio::test]
async fn tombstoned_nodes_are_never_returned_but_still_route() {
    let mut corpus = Corpus::new(CORPUS_SEED);
    let vectors = corpus.vectors(300, 16);
    let mut index = build(&vectors, GRAPH_SEED).await;

    let deleted: Vec<NodeId> = (0..300).step_by(3).map(|i| NodeId(i as u64)).collect();
    for &id in &deleted {
        assert!(
            index.delete(id).await.unwrap(),
            "{id:?} should have existed"
        );
        assert!(
            !index.delete(id).await.unwrap(),
            "deleting twice is a no-op"
        );
    }

    // Every live vector must still be reachable, which is only true if the walk
    // continues through the tombstones rather than stopping at them.
    let mut unreachable = Vec::new();
    for (i, vector) in vectors.iter().enumerate() {
        let id = NodeId(i as u64);
        if deleted.contains(&id) {
            continue;
        }
        let hits = index
            .search_with_ef(vector, 5, DEFAULT_EF_SEARCH)
            .await
            .unwrap();
        assert!(
            hits.iter().all(|h| !deleted.contains(&h.id)),
            "a tombstone was returned for {id:?}"
        );
        if !hits.iter().any(|h| h.id == id) {
            unreachable.push(id);
        }
    }
    assert!(
        unreachable.is_empty(),
        "{} live vectors became unreachable after deleting a third of the graph: {:?}",
        unreachable.len(),
        &unreachable[..unreachable.len().min(5)]
    );
}

/// Deleting everything leaves a graph whose every reachable node is a
/// tombstone. The next insert has nothing live to link to, so it must become
/// the entry point or it would be invisible to every later search.
#[tokio::test]
async fn an_insert_into_a_fully_tombstoned_graph_is_findable() {
    let mut corpus = Corpus::new(CORPUS_SEED);
    let vectors = corpus.vectors(50, 8);
    let mut index = build(&vectors, GRAPH_SEED).await;

    for i in 0..vectors.len() {
        index.delete(NodeId(i as u64)).await.unwrap();
    }
    assert!(
        index
            .search_with_ef(&vectors[0], 10, DEFAULT_EF_SEARCH)
            .await
            .unwrap()
            .is_empty(),
        "a fully tombstoned graph has no live hits"
    );

    let fresh = corpus.vector(8);
    let fresh_id = NodeId(999);
    index.insert(fresh_id, &fresh).await.unwrap();

    let hits = index
        .search_with_ef(&fresh, 1, DEFAULT_EF_SEARCH)
        .await
        .unwrap();
    assert_eq!(
        hits.first().map(|h| h.id),
        Some(fresh_id),
        "the only live node must be findable"
    );
}

/// Node heights are the only thing the seed controls, so they are what proves
/// it is plumbed through. Search results are a weaker signal: with `ef_search`
/// this wide over a corpus this small the walk is near-exhaustive, and two
/// differently-shaped graphs return the same top-k anyway.
async fn heights(index: &Hnsw<MemoryNodeStore>) -> Vec<(NodeId, usize)> {
    let mut out = Vec::new();
    index
        .store()
        .iterate_nodes(|node| {
            out.push((node.id, node.layers.len()));
            Ok(())
        })
        .await
        .unwrap();
    out.sort();
    out
}

#[tokio::test]
async fn the_same_seed_builds_the_same_graph() {
    let mut corpus = Corpus::new(CORPUS_SEED);
    let vectors = corpus.vectors(200, 16);

    let left = build(&vectors, GRAPH_SEED).await;
    let right = build(&vectors, GRAPH_SEED).await;
    let other = build(&vectors, GRAPH_SEED ^ 0xFFFF).await;

    assert_eq!(
        heights(&left).await,
        heights(&right).await,
        "the same seed must build the same graph"
    );
    assert_ne!(
        heights(&left).await,
        heights(&other).await,
        "a different seed produced identical node heights, so the seed is not reaching level generation"
    );

    let mut queries = Corpus::new(QUERY_SEED);
    for _ in 0..20 {
        let query = queries.vector(16);
        assert_eq!(
            left.search_with_ef(&query, 10, DEFAULT_EF_SEARCH)
                .await
                .unwrap(),
            right
                .search_with_ef(&query, 10, DEFAULT_EF_SEARCH)
                .await
                .unwrap(),
            "the same seed must give the same results"
        );
    }
}

#[tokio::test]
async fn reinserting_an_id_replaces_its_vector() {
    let mut corpus = Corpus::new(CORPUS_SEED);
    let vectors = corpus.vectors(100, 8);
    let mut index = build(&vectors, GRAPH_SEED).await;

    let replacement = corpus.vector(8);
    index.insert(NodeId(7), &replacement).await.unwrap();

    let hits = index
        .search_with_ef(&replacement, 1, DEFAULT_EF_SEARCH)
        .await
        .unwrap();
    assert_eq!(hits.first().map(|h| h.id), Some(NodeId(7)));

    let node = index
        .store()
        .get_node(NodeId(7))
        .await
        .unwrap()
        .expect("node 7 exists");
    // Stored normalized, so compare directions rather than components.
    assert!(
        Metric::Cosine.distance(&node.vector, &replacement) < 1e-6,
        "the stored vector is not the replacement"
    );
}

/// A meta pointing at a node that is not stored is corruption. Continuing would
/// quietly build a second component that no search can reach, so it fails loud.
#[tokio::test]
async fn a_dangling_entry_point_fails_the_insert() {
    let mut corpus = Corpus::new(CORPUS_SEED);
    let vectors = corpus.vectors(10, 8);
    let mut index = build(&vectors, GRAPH_SEED).await;

    index
        .store_mut()
        .put_meta(Meta {
            entry_point: NodeId(4242),
            top_layer: 0,
        })
        .await
        .unwrap();

    let err = index.insert(NodeId(11), &vectors[0]).await.unwrap_err();
    assert!(
        matches!(err, Error::VectorEntryPointNotFound { entry_point: 4242 }),
        "expected a dangling-entry-point error, got {err:?}"
    );

    // Search is the read path and must degrade rather than fail: an unreadable
    // entry point means nothing is findable, not that the query is invalid.
    assert!(index
        .search_with_ef(&vectors[0], 5, DEFAULT_EF_SEARCH)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn iterate_nodes_skips_tombstones() {
    let mut corpus = Corpus::new(CORPUS_SEED);
    let vectors = corpus.vectors(40, 8);
    let mut index = build(&vectors, GRAPH_SEED).await;
    for i in (0..40).step_by(4) {
        index.delete(NodeId(i as u64)).await.unwrap();
    }

    let mut seen = Vec::new();
    index
        .store()
        .iterate_nodes(|node| {
            seen.push(node.id);
            Ok(())
        })
        .await
        .unwrap();

    assert_eq!(seen.len(), 30, "10 of 40 were tombstoned");
    assert!(seen.iter().all(|id| id.0 % 4 != 0));
}

/// A node's height is drawn from an unbounded distribution, but the sampler
/// only draws 53 bits, so the tallest node it can ask for is bounded and a
/// node's layer vector cannot grow without limit.
#[test]
fn sampled_levels_stay_within_the_bound() {
    for m in [2usize, 4, 16, 48] {
        let params = Params::new(m);
        let bound = LevelSampler::max_level(params.ml);
        let sampler = LevelSampler::new(GRAPH_SEED);
        let mut highest = 0;
        for id in 0..200_000u64 {
            let level = sampler.level(id, params.ml);
            assert!(
                level <= bound,
                "m={m}: drew level {level} above the bound {bound}"
            );
            highest = highest.max(level);
        }
        // Layer 0 alone would mean no hierarchy at all.
        assert!(highest > 0, "m={m}: every node landed on layer 0");
        println!(
            "m={m} ml={:.4} bound={bound} highest drawn={highest}",
            params.ml
        );
    }
}

#[test]
fn params_match_the_go_defaults() {
    let params = Params::new(DEFAULT_M);
    assert_eq!(params.m, DEFAULT_M);
    assert_eq!(params.m_max0, 2 * DEFAULT_M);
    assert_eq!(params.ef_construction, DEFAULT_EF_CONSTRUCTION);
    assert_eq!(params.ef_search, DEFAULT_EF_SEARCH);
    assert!((params.ml - 1.0 / (DEFAULT_M as f64).ln()).abs() < f64::EPSILON);
    assert_eq!(params.max_links(0), params.m_max0);
    assert_eq!(params.max_links(1), params.m);

    // `ml` is `1 / ln(m)`, so m < 2 would be infinite and every node would ask
    // for an unbounded number of layers.
    for degenerate in [0, 1] {
        assert_eq!(
            Params::new(degenerate).m,
            DEFAULT_M,
            "m = {degenerate} must fall back"
        );
    }
    assert!(Params::new(2).ml.is_finite());
}

/// The bounds exist because any client can set these when Node Access Control
/// is off, so an unbounded value makes one write do unbounded work. The
/// defaults must sit inside them, and anything past them must be refused.
#[test]
fn out_of_range_parameters_are_refused() {
    assert!(Params::new(DEFAULT_M).validate().is_ok());
    assert!(Params::new(MAX_M).validate().is_ok());

    for over in [
        Params {
            m: MAX_M + 1,
            ..Params::new(DEFAULT_M)
        },
        Params {
            ef_construction: MAX_EF_CONSTRUCTION + 1,
            ..Params::new(DEFAULT_M)
        },
        Params {
            ef_search: MAX_EF_SEARCH + 1,
            ..Params::new(DEFAULT_M)
        },
    ] {
        assert!(over.validate().is_err(), "{over:?} should be refused");
    }
}
