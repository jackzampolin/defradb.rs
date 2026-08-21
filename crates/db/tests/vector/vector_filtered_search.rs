//! Filtered nearest-neighbour search.
//!
//! The contract under test: a predicate decides what may be *returned*, never
//! what may be *traversed*, and `k` results still come back whenever `k`
//! admitted nodes exist.

use crate::support::{build, flat, graph, Corpus, CORPUS_SEED, GRAPH_SEED, QUERY_SEED};

use db::index::vector::core::Metric;
use db::index::vector::engine::ann::{AdmitAll, VectorIndexEngine};
use db::index::vector::engine::hnsw::Hnsw;
use db::index::vector::kv_store::KvNodeStore;
use db::index::vector::params::{Params, DEFAULT_EF_SEARCH, DEFAULT_M};
use db::index::vector::store::{MemoryNodeStore, NodeId};
use storage::backends::MemoryStore;
use storage::corekv::{Store, Txn};

const K: usize = 10;

/// Nearest `k` among the admitted, computed exhaustively.
fn exhaustive_admitted(
    vectors: &[Vec<f32>],
    query: &[f32],
    k: usize,
    admit: impl Fn(NodeId) -> bool,
) -> Vec<NodeId> {
    let mut scored: Vec<(NodeId, f64)> = vectors
        .iter()
        .enumerate()
        .map(|(i, v)| (NodeId(i as u64), Metric::Cosine.distance(query, v)))
        .filter(|(id, _)| admit(*id))
        .collect();
    scored.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    scored.into_iter().take(k).map(|(id, _)| id).collect()
}

/// A filter narrows what may be returned, never how many come back.
#[tokio::test]
async fn a_full_k_survives_every_selectivity() {
    let mut corpus = Corpus::new(CORPUS_SEED);
    let vectors = corpus.vectors(2000, 16);
    let index = build(&vectors, GRAPH_SEED).await;

    for divisor in [2u64, 3, 5, 10, 25, 50, 100] {
        let admit = move |id: NodeId| id.0.is_multiple_of(divisor);
        let admitted = (0..vectors.len() as u64)
            .filter(|id| admit(NodeId(*id)))
            .count();
        assert!(
            admitted >= K,
            "test setup: 1 in {divisor} gives only {admitted}"
        );

        let mut queries = Corpus::new(QUERY_SEED);
        for _ in 0..10 {
            let query = queries.vector(16);
            let hits = index
                .search_with_ef_where(&query, K, DEFAULT_EF_SEARCH, &admit)
                .await
                .unwrap();
            assert_eq!(
                hits.len(),
                K,
                "1 in {divisor} admitted ({admitted} of {}) returned {}",
                vectors.len(),
                hits.len()
            );
            assert!(
                hits.iter().all(|h| admit(h.id)),
                "a rejected node came back"
            );
        }
    }
}

/// The hits must be the nearest *among the admitted*, not the admitted subset
/// of an unfiltered top-k.
#[tokio::test]
async fn the_hits_are_the_nearest_admitted() {
    let mut corpus = Corpus::new(CORPUS_SEED);
    let vectors = corpus.vectors(1000, 16);
    let index = build(&vectors, GRAPH_SEED).await;

    for divisor in [2u64, 7, 20] {
        let admit = move |id: NodeId| id.0.is_multiple_of(divisor);
        let mut queries = Corpus::new(QUERY_SEED);
        let (mut hit, mut total) = (0usize, 0usize);
        for _ in 0..25 {
            let query = queries.vector(16);
            let want = exhaustive_admitted(&vectors, &query, K, admit);
            let got = index
                .search_with_ef_where(&query, K, 128, &admit)
                .await
                .unwrap();
            hit += got.iter().filter(|n| want.contains(&n.id)).count();
            total += want.len();
        }
        let recall = hit as f64 / total as f64;
        println!("filtered recall@{K}, 1 in {divisor} admitted = {recall:.4}");
        assert!(recall > 0.9, "1 in {divisor}: recall {recall:.4}");
    }
}

/// If the only admitted documents sit behind rejected ones, the walk must still
/// reach them. This is what a traverse-but-do-not-admit filter buys.
#[tokio::test]
async fn traversal_passes_through_rejected_nodes() {
    let mut corpus = Corpus::new(CORPUS_SEED);
    let vectors = corpus.vectors(500, 16);
    let index = build(&vectors, GRAPH_SEED).await;

    for admit in [
        &(|id: NodeId| id.0 >= 490) as &(dyn Fn(NodeId) -> bool + Sync),
        &|id: NodeId| id.0 < 10,
        &|id: NodeId| (240..250).contains(&id.0),
    ] {
        let mut queries = Corpus::new(QUERY_SEED);
        for _ in 0..10 {
            let query = queries.vector(16);
            let hits = index
                .search_with_ef_where(&query, 5, DEFAULT_EF_SEARCH, &admit)
                .await
                .unwrap();
            assert_eq!(hits.len(), 5, "the walk did not reach the admitted region");
            assert!(hits.iter().all(|h| admit(h.id)));
        }
    }
}

/// Exactly as many admitted as asked for, one short, and one spare.
#[tokio::test]
async fn boundary_counts_are_exact() {
    let mut corpus = Corpus::new(CORPUS_SEED);
    let vectors = corpus.vectors(400, 16);
    let index = build(&vectors, GRAPH_SEED).await;
    let query = Corpus::new(QUERY_SEED).vector(16);

    for admitted in [K - 1, K, K + 1] {
        let admit = move |id: NodeId| (id.0 as usize) < admitted;
        let hits = index
            .search_with_ef_where(&query, K, DEFAULT_EF_SEARCH, &admit)
            .await
            .unwrap();
        assert_eq!(
            hits.len(),
            admitted.min(K),
            "{admitted} admitted, asked for {K}"
        );
    }
}

#[tokio::test]
async fn the_degenerate_filters_behave() {
    let mut corpus = Corpus::new(CORPUS_SEED);
    let vectors = corpus.vectors(300, 16);
    let index = build(&vectors, GRAPH_SEED).await;
    let query = Corpus::new(QUERY_SEED).vector(16);

    assert!(
        index
            .search_with_ef_where(&query, K, DEFAULT_EF_SEARCH, &|_: NodeId| false)
            .await
            .unwrap()
            .is_empty(),
        "admitting nothing must return nothing"
    );
    assert_eq!(
        index
            .search_with_ef_where(&query, K, DEFAULT_EF_SEARCH, &|_: NodeId| true)
            .await
            .unwrap(),
        index
            .search_with_ef(&query, K, DEFAULT_EF_SEARCH)
            .await
            .unwrap(),
        "admitting everything must equal an unfiltered search"
    );
    assert!(
        index
            .search_with_ef_where(&query, 0, DEFAULT_EF_SEARCH, &|_: NodeId| true)
            .await
            .unwrap()
            .is_empty(),
        "k of zero returns nothing whatever the filter"
    );
    assert_eq!(
        index
            .search_with_ef_where(&query, K, DEFAULT_EF_SEARCH, &|id: NodeId| id.0 == 42)
            .await
            .unwrap()
            .len(),
        1,
        "a filter admitting one node returns exactly that node"
    );

    // An empty graph is not an error, with or without a filter.
    let empty = graph(GRAPH_SEED);
    assert!(empty
        .search_with_ef_where(&query, K, DEFAULT_EF_SEARCH, &|_: NodeId| true)
        .await
        .unwrap()
        .is_empty());
}

/// A tombstone outranks the filter: an admitted but deleted node stays out.
#[tokio::test]
async fn a_filter_cannot_resurrect_a_tombstone() {
    let mut corpus = Corpus::new(CORPUS_SEED);
    let vectors = corpus.vectors(300, 16);
    let mut index = build(&vectors, GRAPH_SEED).await;

    for id in (0..300).step_by(2) {
        index.delete(NodeId(id as u64)).await.unwrap();
    }

    let mut queries = Corpus::new(QUERY_SEED);
    for _ in 0..10 {
        let query = queries.vector(16);
        // Admits everything, including the tombstoned half.
        let hits = index
            .search_with_ef_where(&query, K, DEFAULT_EF_SEARCH, &AdmitAll)
            .await
            .unwrap();
        assert!(
            hits.iter().all(|h| !h.id.0.is_multiple_of(2)),
            "a tombstoned node was returned"
        );
        assert_eq!(hits.len(), K, "the live half must still fill k");

        // Admits only the tombstoned half: nothing can come back.
        assert!(
            index
                .search_with_ef_where(&query, K, DEFAULT_EF_SEARCH, &|id: NodeId| id
                    .0
                    .is_multiple_of(2))
                .await
                .unwrap()
                .is_empty(),
            "admitting only tombstones must return nothing"
        );
    }
}

#[tokio::test]
async fn results_stay_ordered_and_distinct_under_a_filter() {
    let mut corpus = Corpus::new(CORPUS_SEED);
    let vectors = corpus.vectors(800, 16);
    let index = build(&vectors, GRAPH_SEED).await;
    let admit = |id: NodeId| id.0.is_multiple_of(3);

    let mut queries = Corpus::new(QUERY_SEED);
    for _ in 0..20 {
        let query = queries.vector(16);
        let hits = index
            .search_with_ef_where(&query, K, DEFAULT_EF_SEARCH, &admit)
            .await
            .unwrap();
        for pair in hits.windows(2) {
            assert!(pair[0].distance <= pair[1].distance, "out of order");
        }
        let mut ids: Vec<u64> = hits.iter().map(|h| h.id.0).collect();
        ids.sort_unstable();
        let count = ids.len();
        ids.dedup();
        assert_eq!(ids.len(), count, "a node was returned twice");
    }
}

/// A wider search explores more of layer 0, so it cannot find fewer true
/// neighbours -- filtered no less than unfiltered.
#[tokio::test]
async fn a_wider_filtered_search_does_not_lose_hits() {
    let mut corpus = Corpus::new(CORPUS_SEED);
    let vectors = corpus.vectors(1000, 16);
    let index = build(&vectors, GRAPH_SEED).await;
    let admit = |id: NodeId| id.0.is_multiple_of(11);

    let mut queries = Corpus::new(QUERY_SEED);
    let (mut narrow, mut wide, mut total) = (0usize, 0usize, 0usize);
    for _ in 0..25 {
        let query = queries.vector(16);
        let want = exhaustive_admitted(&vectors, &query, K, admit);
        let a = index
            .search_with_ef_where(&query, K, K, &admit)
            .await
            .unwrap();
        let b = index
            .search_with_ef_where(&query, K, 256, &admit)
            .await
            .unwrap();
        narrow += a.iter().filter(|n| want.contains(&n.id)).count();
        wide += b.iter().filter(|n| want.contains(&n.id)).count();
        total += want.len();
    }
    println!(
        "filtered recall@{K}: ef={K} -> {:.4}, ef=256 -> {:.4}",
        narrow as f64 / total as f64,
        wide as f64 / total as f64
    );
    assert!(
        wide >= narrow,
        "a wider filtered search found fewer: {wide} < {narrow}"
    );
}

/// Same seed, same filter, same answer.
#[tokio::test]
async fn a_filtered_search_is_deterministic() {
    let mut corpus = Corpus::new(CORPUS_SEED);
    let vectors = corpus.vectors(400, 16);
    let left = build(&vectors, GRAPH_SEED).await;
    let right = build(&vectors, GRAPH_SEED).await;
    let admit = |id: NodeId| id.0.is_multiple_of(4);

    let mut queries = Corpus::new(QUERY_SEED);
    for _ in 0..20 {
        let query = queries.vector(16);
        assert_eq!(
            left.search_with_ef_where(&query, K, DEFAULT_EF_SEARCH, &admit)
                .await
                .unwrap(),
            right
                .search_with_ef_where(&query, K, DEFAULT_EF_SEARCH, &admit)
                .await
                .unwrap()
        );
    }
}

/// Every element width a query can arrive in must filter identically.
#[tokio::test]
async fn every_query_width_filters_identically() {
    let mut corpus = Corpus::new(CORPUS_SEED);
    let vectors = corpus.vectors(300, 8);
    let index = build(&vectors, GRAPH_SEED).await;
    let admit = |id: NodeId| id.0.is_multiple_of(3);

    // Whole numbers, so every width holds the query exactly.
    let base = [3.0f64, -2.0, 6.0, 1.0, 0.0, -5.0, 4.0, 2.0];
    let as_f32: Vec<f32> = base.iter().map(|x| *x as f32).collect();
    let as_i32: Vec<i32> = base.iter().map(|x| *x as i32).collect();
    let as_i64: Vec<i64> = base.iter().map(|x| *x as i64).collect();

    let from_f64 = index
        .search_with_ef_where(&base, K, 64, &admit)
        .await
        .unwrap();
    for (label, hits) in [
        (
            "f32",
            index
                .search_with_ef_where(&as_f32, K, 64, &admit)
                .await
                .unwrap(),
        ),
        (
            "i32",
            index
                .search_with_ef_where(&as_i32, K, 64, &admit)
                .await
                .unwrap(),
        ),
        (
            "i64",
            index
                .search_with_ef_where(&as_i64, K, 64, &admit)
                .await
                .unwrap(),
        ),
    ] {
        assert_eq!(hits, from_f64, "{label} filtered differently from f64");
    }
    assert!(from_f64.iter().all(|h| admit(h.id)));
}

/// Both kinds must agree under the same filter, which is what makes the exact
/// kind an oracle for the filtered path too.
#[tokio::test]
async fn both_kinds_agree_under_a_filter() {
    let mut corpus = Corpus::new(CORPUS_SEED);
    let vectors = corpus.vectors(400, 16);
    let graph = build(&vectors, GRAPH_SEED).await;
    let exact = flat(&vectors).await;

    for divisor in [2u64, 3, 9] {
        let admit = move |id: NodeId| id.0.is_multiple_of(divisor);
        let mut queries = Corpus::new(QUERY_SEED);
        for _ in 0..15 {
            let query = queries.vector(16);
            let from_graph = graph
                .search_with_ef_where(&query, 5, 200, &admit)
                .await
                .unwrap();
            let from_scan = exact.search_where(&query, 5, None, &admit).await.unwrap();
            assert_eq!(
                from_graph.iter().map(|h| h.id).collect::<Vec<_>>(),
                from_scan.iter().map(|h| h.id).collect::<Vec<_>>(),
                "1 in {divisor}: the graph and the exact scan disagree"
            );
        }
    }
}

/// The filter must behave the same against a persisted graph as an in-memory
/// one: nothing about it lives in the store.
#[tokio::test]
async fn filtering_works_against_a_persisted_graph() {
    let store = MemoryStore::new();
    let mut corpus = Corpus::new(CORPUS_SEED);
    let vectors = corpus.vectors(300, 8);
    let admit = |id: NodeId| id.0.is_multiple_of(5);

    let mut write: Box<dyn Txn> = store.new_txn(false).await.unwrap();
    {
        let mut index = Hnsw::new(
            KvNodeStore::new(&mut write, 7, 3, 0),
            Metric::Cosine,
            Params::new(DEFAULT_M),
            GRAPH_SEED,
        );
        for (i, vector) in vectors.iter().enumerate() {
            index.insert(NodeId(i as u64), vector).await.unwrap();
        }
    }
    write.commit().await.unwrap();

    let mut read: Box<dyn Txn> = store.new_txn(false).await.unwrap();
    let persisted = Hnsw::new(
        KvNodeStore::new(&mut read, 7, 3, 0),
        Metric::Cosine,
        Params::new(DEFAULT_M),
        GRAPH_SEED,
    );
    let in_memory = build(&vectors, GRAPH_SEED).await;

    let mut queries = Corpus::new(QUERY_SEED);
    for _ in 0..15 {
        let query = queries.vector(8);
        let from_disk = persisted
            .search_with_ef_where(&query, K, DEFAULT_EF_SEARCH, &admit)
            .await
            .unwrap();
        assert_eq!(
            from_disk,
            in_memory
                .search_with_ef_where(&query, K, DEFAULT_EF_SEARCH, &admit)
                .await
                .unwrap(),
            "a persisted graph filtered differently"
        );
        assert!(from_disk.iter().all(|h| admit(h.id)));
        assert_eq!(from_disk.len(), K);
    }
}

/// The predicate is consulted during the walk, not applied to a finished
/// result set: a selective filter must read far more nodes than an
/// all-admitting one, because it cannot stop as early.
#[tokio::test]
async fn the_filter_is_applied_during_the_walk() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let mut corpus = Corpus::new(CORPUS_SEED);
    let vectors = corpus.vectors(1000, 16);
    let mut index = Hnsw::new(
        MemoryNodeStore::new(),
        Metric::Cosine,
        Params::new(DEFAULT_M),
        GRAPH_SEED,
    );
    for (i, vector) in vectors.iter().enumerate() {
        index.insert(NodeId(i as u64), vector).await.unwrap();
    }
    let query = Corpus::new(QUERY_SEED).vector(16);

    let calls = AtomicUsize::new(0);
    let counted = |id: NodeId| {
        calls.fetch_add(1, Ordering::Relaxed);
        id.0.is_multiple_of(50)
    };
    let hits = index
        .search_with_ef_where(&query, K, DEFAULT_EF_SEARCH, &counted)
        .await
        .unwrap();
    let selective_calls = calls.load(Ordering::Relaxed);

    assert_eq!(hits.len(), K);
    assert!(
        selective_calls >= hits.len(),
        "the predicate was consulted {selective_calls} times for {} hits",
        hits.len()
    );
    assert!(
        selective_calls < vectors.len() * 2,
        "the predicate was consulted {selective_calls} times, which is a scan"
    );

    let permissive_calls = AtomicUsize::new(0);
    let permissive = |_: NodeId| {
        permissive_calls.fetch_add(1, Ordering::Relaxed);
        true
    };
    index
        .search_with_ef_where(&query, K, DEFAULT_EF_SEARCH, &permissive)
        .await
        .unwrap();
    let permissive_calls = permissive_calls.load(Ordering::Relaxed);

    println!("predicate calls: 1-in-50 filter = {selective_calls}, admit-all = {permissive_calls}");
    assert!(
        selective_calls > permissive_calls,
        "a selective filter should widen the walk: {selective_calls} !> {permissive_calls}"
    );
}
