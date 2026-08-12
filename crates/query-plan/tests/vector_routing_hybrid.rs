//! Whether the hybrid retrieval query in `db-search` actually routes to a
//! vector index.
//!
//! The query is built here in the shape `render_dense_ranked_query` emits, then
//! driven through the real parser and the real extraction, because every past
//! break in this path was in the translation rather than in the rule: an
//! `_alias` ordering that kept its wrapper, or a second scored field counted as
//! a second similarity.

use query_plan::planner::vector_routing::{route, similarity_query, NotRouted};
use schema::{
    DistanceMetric, HnswParams, IndexDescription, IndexedFieldDescription, VectorAlgorithm,
    VectorIndexDescription,
};

/// The aliases `db-search` renders. Duplicated deliberately: if they change
/// there, this test must fail rather than quietly follow.
const BM25_ALIAS: &str = "dense_v1_bm25_score";
const SIMILARITY_ALIAS: &str = "dense_v1_similarity_score";

const VECTOR: [f64; 4] = [0.1, 0.2, 0.3, 0.4];

fn vector_index() -> Vec<IndexDescription> {
    vec![IndexDescription {
        name: "by_embedding".to_string(),
        id: 5,
        fields: vec![IndexedFieldDescription {
            name: "embedding".to_string(),
            descending: false,
        }],
        unique: false,
        kind: None,
        auto_generated: false,
    }
    .as_vector(VectorIndexDescription {
        algorithm: VectorAlgorithm::Hnsw,
        metric: DistanceMetric::Cosine,
        dimensions: VECTOR.len() as u32,
        hnsw: Some(HnswParams::default()),
    })]
}

/// The query `render_dense_ranked_query` produces, with the pieces that vary
/// per call left open.
fn hybrid_query(filter: Option<&str>, order_alias: &str, limit: usize) -> String {
    let vector = VECTOR
        .iter()
        .map(|component| component.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let filter = filter.map_or(String::new(), |filter| format!("filter: {filter}\n    "));
    format!(
        r#"{{
  Article(
    {filter}order: {{ _alias: {{ {order_alias}: DESC }} }}
    limit: {limit}
  ) {{
    _docID
    {BM25_ALIAS}: BM25(query: "neural retrieval", fields: [title, body])
    {SIMILARITY_ALIAS}: SIMILARITY(embedding: {{vector: [{vector}]}})
    title
  }}
}}"#
    )
}

fn parse(query: &str) -> query_types::mapper::Select {
    let mut selects = query_parse::parse_query(query).expect("the hybrid query must parse");
    assert_eq!(selects.len(), 1, "one top-level field");
    selects.remove(0)
}

fn route_query(query: &str) -> Result<query_plan::planner::vector_routing::VectorRoute, NotRouted> {
    route(&similarity_query(&parse(query)), &vector_index())
}

/// The case that matters: `exclude_doc_ids` folds into a filter on essentially
/// every hybrid call, and Go refuses a filtered query outright (its issue
/// 5071). This engine filters during the walk, so the filter must not stop it.
#[test]
fn the_dense_ranked_hybrid_query_routes() {
    let filter = r#"{_and: [{status: {_eq: "published"}}, {_docID: {_nin: ["bae-1", "bae-2"]}}]}"#;
    let query = hybrid_query(Some(filter), SIMILARITY_ALIAS, 20);

    // Without this the test would still pass if the parser dropped the filter,
    // and would then be proving nothing at all.
    assert!(
        parse(&query).filter.is_some(),
        "the filter must survive parsing, or this test is vacuous"
    );

    let routed = route_query(&query).expect("the hybrid query must route");

    assert_eq!(routed.index_id, 5);
    assert_eq!(routed.query_vector, VECTOR);
    assert_eq!(routed.k, 20);
}

/// Without a filter it must route identically, so the filter is proven to be
/// the thing that does not matter.
#[test]
fn the_filter_does_not_change_the_route() {
    let filtered = route_query(&hybrid_query(
        Some(r#"{status: {_eq: "published"}}"#),
        SIMILARITY_ALIAS,
        20,
    ));
    let plain = route_query(&hybrid_query(None, SIMILARITY_ALIAS, 20));
    assert_eq!(filtered, plain);
}

/// A `BM25` field is scored too, but it is not a similarity, so it must not
/// make the query ambiguous.
#[test]
fn the_bm25_field_is_not_a_second_similarity() {
    let query = similarity_query(&parse(&hybrid_query(None, SIMILARITY_ALIAS, 20)));
    assert_eq!(
        query.similarities.len(),
        1,
        "BM25 must not be counted as a similarity: {:?}",
        query.similarities
    );
    assert_eq!(query.similarities[0].output_name, SIMILARITY_ALIAS);
    assert_eq!(query.similarities[0].target_field, "embedding");
}

/// The `_alias` wrapper must not survive into the order key, or the name
/// comparison against the similarity's alias can never match.
#[test]
fn the_alias_ordering_resolves_to_the_bare_alias() {
    let query = similarity_query(&parse(&hybrid_query(None, SIMILARITY_ALIAS, 20)));
    let order = query.sole_order.expect("a sole order key");
    assert_eq!(order.field, SIMILARITY_ALIAS);
    assert!(order.descending);
}

/// The BM25-ranked variant of the same query orders by relevance, not by
/// distance, so its top-k is not the graph's top-k. It must fall back to a
/// scan rather than return the wrong rows.
#[test]
fn the_bm25_ranked_variant_does_not_route() {
    assert_eq!(
        route_query(&hybrid_query(None, BM25_ALIAS, 20)),
        Err(NotRouted::NotOrderedBySimilarity)
    );
}

/// `limit` is what the caller asked for; the offset is added on top because the
/// graph has to produce the rows the offset skips.
#[test]
fn the_offset_is_added_to_k() {
    let query =
        hybrid_query(None, SIMILARITY_ALIAS, 20).replace("limit: 20", "limit: 20, offset: 5");
    assert_eq!(route_query(&query).expect("routes").k, 25);
}

/// A vector whose length disagrees with the index would be scored on its shared
/// leading elements alone, which is wrong rather than approximate.
#[test]
fn a_wrong_length_vector_is_refused() {
    let query = hybrid_query(None, SIMILARITY_ALIAS, 20).replace("0.3, 0.4", "0.3");
    assert_eq!(
        route_query(&query),
        Err(NotRouted::DimensionMismatch {
            expected: 4,
            actual: 3
        })
    );
}
