//! A `_similarity` query must reach the vector index, on every fetcher and
//! every algorithm.
//!
//! The engines and the planner routing were both correct in isolation while the
//! production fetchers dropped `supports_vector_search`, so every query silently
//! full-scanned. A full scan returns the same documents in the same order, so
//! asserting on results proves nothing: these tests assert the index was
//! consulted, via the `indexFetches` an execute-explain reports.

use defra_node::EmbeddedNode;
use query::QueryRequest;
use serde_json::Value;

const DIMENSIONS: usize = 8;
const CORPUS: usize = 40;

/// No metric argument: `SIMILARITY` scores by whichever metric the index
/// declares, so the default has to route like any other.
fn schema(algorithm: &str) -> String {
    let args = if algorithm.is_empty() {
        format!("dimensions: {DIMENSIONS}")
    } else {
        format!("dimensions: {DIMENSIONS}, alg: {algorithm}")
    };
    format!(
        "type Note {{ title: String  tag: String  embedding: [Float32!] @index(vector: {{{args}}}) }}"
    )
}

/// `@index(vector: {...})` for one algorithm and metric. The metric lives in
/// the algorithm's own block, so the block name has to come from the algorithm.
fn vector_index_sdl(algorithm: schema::VectorAlgorithm, metric: schema::DistanceMetric) -> String {
    format!(
        "@index(vector: {{dimensions: {DIMENSIONS}, {}: {{metric: {}}}}})",
        algorithm.sdl_block(),
        metric.as_str()
    )
}

/// Deterministic and well spread, so nearest-neighbour order is unambiguous.
fn vector_for(index: usize) -> Vec<f64> {
    (0..DIMENSIONS)
        .map(|slot| ((index + slot) % 17) as f64)
        .collect()
}

fn render(vector: &[f64]) -> String {
    vector
        .iter()
        .map(|value| format!("{value:?}"))
        .collect::<Vec<_>>()
        .join(", ")
}

async fn query_data(node: &EmbeddedNode, query: &str, context: &str) -> Value {
    let response = node.execute(query).await;
    assert!(!response.has_errors(), "{context}: {:?}", response.errors);
    response.data.unwrap_or(Value::Null)
}

async fn seed(node: &EmbeddedNode) {
    for index in 0..CORPUS {
        let tag = if index % 2 == 0 { "even" } else { "odd" };
        let mutation = format!(
            r#"mutation {{ create_Note(input: {{ title: "note-{index}", tag: "{tag}", embedding: [{}] }}) {{ _docID }} }}"#,
            render(&vector_for(index))
        );
        query_data(node, &mutation, "seed").await;
    }
}

fn similarity_query(vector: &[f64], limit: usize, filter: Option<&str>) -> String {
    let filter = filter.map_or(String::new(), |f| format!("filter: {f}, "));
    format!(
        r#"{{ Note({filter}order: {{ _alias: {{ sim: DESC }} }}, limit: {limit}) {{ title tag sim: SIMILARITY(embedding: {{vector: [{}]}}) }} }}"#,
        render(vector)
    )
}

/// Same shape as `similarity_query`, with a `metric` argument alongside the
/// vector.
fn similarity_query_with_metric(vector: &[f64], limit: usize, metric: &str) -> String {
    format!(
        r#"{{ Note(order: {{ _alias: {{ sim: DESC }} }}, limit: {limit}) {{ title tag sim: SIMILARITY(embedding: {{vector: [{}], metric: {metric}}}) }} }}"#,
        render(vector)
    )
}

/// Sum a counter across every node of an execute-explain tree.
fn total(explain: &Value, counter: &str) -> u64 {
    match explain {
        Value::Object(map) => {
            map.get(counter).and_then(Value::as_u64).unwrap_or(0)
                + map.values().map(|value| total(value, counter)).sum::<u64>()
        }
        Value::Array(items) => items.iter().map(|item| total(item, counter)).sum(),
        _ => 0,
    }
}

/// `indexFetches` counts a vector-index hit as one. A full-scan fallback leaves
/// it at zero, which is exactly the regression being guarded.
fn index_fetches(explain: &Value) -> u64 {
    total(explain, "indexFetches")
}

/// The vector index explain says served the scan, if any. Unlike `indexFetches`
/// this cannot be confused with a scalar index scan.
fn vector_index(explain: &Value) -> Option<String> {
    match explain {
        Value::Object(map) => map
            .get("vectorIndex")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| map.values().find_map(vector_index)),
        Value::Array(items) => items.iter().find_map(vector_index),
        _ => None,
    }
}

async fn node_with(algorithm: &str) -> EmbeddedNode {
    let node = EmbeddedNode::builder().build().await.unwrap();
    node.add_schema(&schema(algorithm))
        .await
        .unwrap_or_else(|e| panic!("add schema for {algorithm:?}: {e}"));
    node
}

async fn assert_routes(node: &EmbeddedNode, query: &str, context: &str) {
    let explain = query_data(
        node,
        &format!("query @explain(type: execute) {query}"),
        context,
    )
    .await;

    assert!(
        vector_index(&explain).is_some() && index_fetches(&explain) > 0,
        "{context}: the query full-scanned instead of using the vector index\n{explain:#}"
    );
}

#[tokio::test]
async fn similarity_reaches_the_index_on_the_autocommit_path() {
    let node = node_with("").await;
    seed(&node).await;

    let query = similarity_query(&vector_for(3), 5, None);
    assert_routes(&node, &query, "autocommit").await;

    let data = query_data(&node, &query, "similarity").await;
    let rows = data["Note"].as_array().expect("Note array");
    assert_eq!(rows.len(), 5, "the index must return the requested count");
}

#[tokio::test]
async fn similarity_reaches_the_index_inside_a_transaction() {
    let node = node_with("").await;
    seed(&node).await;

    let handle = node
        .begin_transaction(true)
        .await
        .expect("begin read transaction");
    let query = format!(
        "query @explain(type: execute) {}",
        similarity_query(&vector_for(3), 5, None)
    );
    let response = node
        .execute_request_in_txn(QueryRequest::new(&query), &handle)
        .await;
    assert!(
        !response.has_errors(),
        "in-transaction explain: {:?}",
        response.errors
    );
    let explain = response.data.unwrap_or(Value::Null);

    assert!(
        vector_index(&explain).is_some(),
        "an in-transaction similarity query full-scanned instead of using the index\n{explain:#}"
    );
}

/// The routing resolves a descriptor and calls `VectorIndex::search`, so it is
/// engine-agnostic by construction. This pins that, so a new algorithm cannot
/// land reachable only through its own engine tests.
#[tokio::test]
async fn similarity_reaches_the_index_for_every_algorithm() {
    for algorithm in schema::VectorAlgorithm::ALL {
        // Every algorithm supports the default metric, so none is skipped here;
        // the pairs an algorithm refuses are covered by
        // `an_unsupported_metric_is_refused_when_the_schema_is_written`.
        assert!(algorithm.supports_metric(schema::DistanceMetric::default()));
        let node = node_with(algorithm.as_str()).await;
        seed(&node).await;

        let query = similarity_query(&vector_for(5), 5, None);
        assert_routes(&node, &query, algorithm.as_str()).await;

        let data = query_data(&node, &query, algorithm.as_str()).await;
        assert_eq!(
            data["Note"].as_array().expect("Note array").len(),
            5,
            "{} returned a short page",
            algorithm.as_str()
        );
    }
}

/// A filter must not silently shrink the result set: the answer is the nearest
/// `limit` documents *that match*, not whatever survives filtering the nearest
/// `limit` overall.
#[tokio::test]
async fn a_filtered_similarity_query_returns_a_full_page() {
    let node = node_with("").await;
    seed(&node).await;

    let query = similarity_query(&vector_for(3), 5, Some(r#"{tag: {_eq: "even"}}"#));
    assert_routes(&node, &query, "filtered").await;

    let data = query_data(&node, &query, "filtered similarity").await;
    let rows = data["Note"].as_array().expect("Note array");
    assert_eq!(
        rows.len(),
        5,
        "half the corpus matches the filter, so a full page must be reachable; \
         a short page means the index returned the nearest 5 overall and the \
         filter then dropped the non-matching ones"
    );
    assert!(
        rows.iter().all(|row| row["tag"] == "even"),
        "every row must match the filter"
    );
}

/// A cosine index serves a `SIMILARITY` ranking, and serves the same answer the
/// exhaustive scan gives.
///
/// This is the whole point of scoring by the index's metric. `SIMILARITY` used
/// to compute a raw dot product no matter what the index was built with, so a
/// cosine index ranked one way while the scan scored another; routing to it
/// would have returned different documents, and the planner declined for that
/// reason. Now both resolve one metric, so the routed page and the exhaustive
/// page must be the same page.
#[tokio::test]
async fn a_cosine_index_routes_and_agrees_with_the_exhaustive_scan() {
    let node = EmbeddedNode::builder().build().await.unwrap();
    node.add_schema(&format!(
        "type Note {{ title: String  tag: String  embedding: [Float32!] @index(vector: {{dimensions: {DIMENSIONS}, hnsw: {{metric: COSINE}}}}) }}"
    ))
    .await
    .expect("add cosine schema");
    seed(&node).await;

    let query = similarity_query(&vector_for(3), 5, None);
    assert_routes(&node, &query, "cosine").await;

    let routed = query_data(&node, &query, "cosine similarity").await;
    let rows = routed["Note"].as_array().expect("Note array");
    assert_eq!(rows.len(), 5);

    let routed_titles: Vec<&str> = rows
        .iter()
        .filter_map(|row| row["title"].as_str())
        .collect();
    let routed_scores: Vec<f64> = rows.iter().filter_map(|row| row["sim"].as_f64()).collect();

    // Unlimited, so nothing can narrow it: this is every document scored by the
    // scan, which is the answer the routed page has to reproduce.
    let all = query_data(
        &node,
        &format!(
            r#"{{ Note(order: {{_alias: {{sim: DESC}}}}, limit: {CORPUS}) {{ title sim: SIMILARITY(embedding: {{vector: [{}]}}) }} }}"#,
            render(&vector_for(3))
        ),
        "exhaustive scores",
    )
    .await;
    let exhaustive = all["Note"].as_array().expect("Note array");
    let expected_titles: Vec<&str> = exhaustive
        .iter()
        .filter_map(|row| row["title"].as_str())
        .take(5)
        .collect();
    let expected_scores: Vec<f64> = exhaustive
        .iter()
        .filter_map(|row| row["sim"].as_f64())
        .take(5)
        .collect();

    assert_eq!(
        routed_titles, expected_titles,
        "the routed page must be the exhaustive page"
    );
    assert_eq!(
        routed_scores, expected_scores,
        "the routed scores must be the exhaustive scores"
    );

    // A cosine score is a true cosine, so it stays within [-1, 1] up to
    // rounding: the quotient is deliberately not clamped, matching the
    // reference, so an exact match can land a ULP past 1. A raw dot product
    // over this corpus runs to the hundreds, which is what the old scoring
    // produced here, so the bound separates the two by three orders of
    // magnitude even with the slop.
    const SLOP: f64 = 4.0 * f64::EPSILON;
    assert!(
        routed_scores
            .iter()
            .all(|score| (-1.0 - SLOP..=1.0 + SLOP).contains(score)),
        "cosine scores must be cosines: {routed_scores:?}"
    );
}

/// A scalar index on the filtered field must not displace the vector index.
/// Only the vector index can serve `ORDER BY similarity` with a limit; a scalar
/// index narrows the filter and then leaves every match to be scored.
#[tokio::test]
async fn a_scalar_index_on_the_filter_does_not_displace_the_vector_index() {
    let node = EmbeddedNode::builder().build().await.unwrap();
    node.add_schema(&format!(
        "type Note {{ title: String  tag: String @index  embedding: [Float32!] @index(vector: {{dimensions: {DIMENSIONS}, hnsw: {{metric: DOT}}}}) }}"
    ))
    .await
    .expect("add schema with an indexed tag");
    seed(&node).await;

    let query = similarity_query(&vector_for(3), 5, Some(r#"{tag: {_eq: "even"}}"#));
    let explain = query_data(
        &node,
        &format!("query @explain(type: execute) {query}"),
        "scalar index present",
    )
    .await;

    // The vector path reads exactly the candidates it asked for. A scalar index
    // scan instead reads every matching entry, so the document count separates
    // the two.
    let doc_fetches = total(&explain, "docFetches");

    assert!(
        doc_fetches < CORPUS as u64,
        "the vector index must bound the read; {doc_fetches} of {CORPUS} documents were fetched\n{explain:#}"
    );

    let rows = query_data(&node, &query, "scalar index present rows").await;
    let rows = rows["Note"].as_array().expect("Note array");
    assert_eq!(rows.len(), 5);
    assert!(rows.iter().all(|row| row["tag"] == "even"));
}

/// Every combination that reaches the read path, checked against one invariant:
/// the answer must be the same one an exhaustive scan gives, and the index must
/// be used whatever the metric.
///
/// The scan scores by the index's own metric, so the two rank identically by
/// construction. Before that, only a `DOT` index routed and the other metrics
/// silently full-scanned.
///
/// Axes: algorithm x metric x filter shape x fetcher. A new engine or metric
/// enters the matrix by appearing in `ALL`, so coverage cannot fall behind the
/// enum.
#[tokio::test]
async fn every_combination_agrees_with_an_exhaustive_scan() {
    const LIMIT: usize = 5;

    for algorithm in schema::VectorAlgorithm::ALL {
        for metric in schema::DistanceMetric::ALL {
            for (filter_name, filter, indexed_tag) in [
                ("none", None, false),
                ("unindexed field", Some(r#"{tag: {_eq: "even"}}"#), false),
                ("indexed field", Some(r#"{tag: {_eq: "even"}}"#), true),
            ] {
                for in_transaction in [false, true] {
                    if !algorithm.supports_metric(*metric) {
                        continue;
                    }
                    let case = format!(
                        "{} / {} / filter: {filter_name} / txn: {in_transaction}",
                        algorithm.as_str(),
                        metric.as_str()
                    );

                    let tag_field = if indexed_tag {
                        "tag: String @index"
                    } else {
                        "tag: String"
                    };
                    let node = EmbeddedNode::builder().build().await.unwrap();
                    node.add_schema(&format!(
                        "type Note {{ title: String  {tag_field}  embedding: [Float32!] {} }}",
                        vector_index_sdl(*algorithm, *metric)
                    ))
                    .await
                    .unwrap_or_else(|e| panic!("{case}: add schema: {e}"));
                    seed(&node).await;

                    let probe = vector_for(3);
                    let query = similarity_query(&probe, LIMIT, filter);

                    let explain = query_data(
                        &node,
                        &format!("query @explain(type: execute) {query}"),
                        &case,
                    )
                    .await;
                    assert!(
                        vector_index(&explain).is_some(),
                        "{case}: every metric must route\n{explain:#}"
                    );

                    let data = if in_transaction {
                        let handle = node
                            .begin_transaction(true)
                            .await
                            .unwrap_or_else(|e| panic!("{case}: begin txn: {e}"));
                        let response = node
                            .execute_request_in_txn(QueryRequest::new(&query), &handle)
                            .await;
                        assert!(!response.has_errors(), "{case}: {:?}", response.errors);
                        response.data.unwrap_or(Value::Null)
                    } else {
                        query_data(&node, &query, &case).await
                    };
                    let rows = data["Note"]
                        .as_array()
                        .unwrap_or_else(|| panic!("{case}: rows"));

                    assert_eq!(rows.len(), LIMIT, "{case}: short page");
                    if filter.is_some() {
                        assert!(
                            rows.iter().all(|row| row["tag"] == "even"),
                            "{case}: a row escaped the filter"
                        );
                    }

                    let scores: Vec<f64> = rows
                        .iter()
                        .map(|row| row["sim"].as_f64().unwrap_or_else(|| panic!("{case}: sim")))
                        .collect();
                    assert!(
                        scores.windows(2).all(|pair| pair[0] >= pair[1]),
                        "{case}: rows are not ordered by similarity: {scores:?}"
                    );

                    // The exhaustive answer, computed without a limit so no
                    // index can narrow it.
                    let exhaustive = query_data(
                        &node,
                        &format!(
                            r#"{{ Note({}limit: {CORPUS}) {{ sim: SIMILARITY(embedding: {{vector: [{}]}}) }} }}"#,
                            filter.map_or(String::new(), |f| format!("filter: {f}, ")),
                            render(&probe)
                        ),
                        &case,
                    )
                    .await;
                    let mut expected: Vec<f64> = exhaustive["Note"]
                        .as_array()
                        .unwrap_or_else(|| panic!("{case}: exhaustive rows"))
                        .iter()
                        .filter_map(|row| row["sim"].as_f64())
                        .collect();
                    expected.sort_by(|a, b| b.partial_cmp(a).expect("scores are finite"));
                    expected.truncate(LIMIT);

                    assert_eq!(
                        scores, expected,
                        "{case}: the indexed answer differs from the exhaustive one"
                    );
                }
            }
        }
    }
}

/// Dimensions are required and must be greater than zero, which Go made
/// unconditional in sourcenetwork/defradb#5188 by dropping the inference from an
/// `@embedding` on the same field. A zero-dimension index cannot check a query
/// vector's length, so a wrong-length query would be scored on its shared
/// leading elements alone: silently wrong rather than merely approximate.
#[tokio::test]
async fn dimensions_are_required_when_the_schema_is_written() {
    for sdl in [
        "type Note { embedding: [Float32!] @index(vector: {}) }".to_string(),
        "type Note { embedding: [Float32!] @index(vector: {dimensions: 0}) }".to_string(),
        // An @embedding fixes the length, and used to supply it. It no longer does.
        "type Note { title: String  embedding: [Float32!] @index(vector: {}) \
             @embedding(provider: \"openai\", model: \"m\", fields: [\"title\"]) }"
            .to_string(),
    ] {
        let node = EmbeddedNode::builder().build().await.unwrap();
        let error = node
            .add_schema(&sdl)
            .await
            .expect_err(&format!("must be refused: {sdl}"))
            .to_string();
        assert!(
            error.contains("dimensions"),
            "the error must name dimensions, got: {error}"
        );
    }
}

/// An algorithm that cannot rank by a metric must be refused when the schema is
/// written, not when the first document is indexed.
#[tokio::test]
async fn an_unsupported_metric_is_refused_when_the_schema_is_written() {
    for algorithm in schema::VectorAlgorithm::ALL {
        for metric in schema::DistanceMetric::ALL {
            if algorithm.supports_metric(*metric) {
                continue;
            }
            let node = EmbeddedNode::builder().build().await.unwrap();
            let result = node
                .add_schema(&format!(
                    "type Note {{ embedding: [Float32!] {} }}",
                    vector_index_sdl(*algorithm, *metric)
                ))
                .await;
            let error = result.expect_err(&format!(
                "{} must refuse {}",
                algorithm.as_str(),
                metric.as_str()
            ));
            assert!(
                error.to_string().contains(metric.as_str()),
                "the error must name the metric, got: {error}"
            );
        }
    }
}

/// A filter that runs *above* the scan must still fill the page.
///
/// A relation filter is applied at a `SelectNode` after the joins, not at the
/// scan, so counting what the scan emitted says nothing about what survived.
/// The nearest documents here all fail the filter, so a scan that stops after
/// its first `k` candidates returns nothing at all.
#[tokio::test]
async fn a_relation_filter_above_the_scan_still_fills_the_page() {
    let node = EmbeddedNode::builder().build().await.unwrap();
    node.add_schema(
        "type Owner { name: String  notes: [Note] }
         type Note { title: String  owner: Owner  embedding: [Float32!] @index(vector: {dimensions: 4, hnsw: {metric: DOT}}) }",
    )
    .await
    .expect("add schema");

    let mut owners = Vec::new();
    for who in ["keep", "drop"] {
        let data = query_data(
            &node,
            &format!(r#"mutation {{ add_Owner(input: {{name: "{who}"}}) {{ _docID }} }}"#),
            "seed owner",
        )
        .await;
        owners.push(
            data["add_Owner"][0]["_docID"]
                .as_str()
                .expect("owner docID")
                .to_string(),
        );
    }

    // The 20 nearest belong to "drop"; the 20 that match the filter are further.
    for index in 0..40 {
        let near = index < 20;
        let owner = if near { &owners[1] } else { &owners[0] };
        let vector: Vec<f64> = (0..4)
            .map(|slot| if near { 9.0 - slot as f64 } else { 1.0 })
            .collect();
        query_data(
            &node,
            &format!(
                r#"mutation {{ add_Note(input: {{title: "n{index}", owner: "{owner}", embedding: [{}]}}) {{ _docID }} }}"#,
                render(&vector)
            ),
            "seed note",
        )
        .await;
    }

    let query = r#"{ Note(filter: {owner: {name: {_eq: "keep"}}}, order: {_alias: {sim: DESC}}, limit: 5) { title sim: SIMILARITY(embedding: {vector: [9.0, 8.0, 7.0, 6.0]}) } }"#;
    let data = query_data(&node, query, "relation-filtered similarity").await;
    let rows = data["Note"].as_array().expect("Note array");

    assert_eq!(
        rows.len(),
        5,
        "20 documents match the filter, so a page of 5 must be fillable; a short \
         page means the scan stopped counting its own output while the filter \
         above it rejected every row"
    );
}

/// An empty index must not be reported as having served the scan.
///
/// With no candidates the scan falls through to reading the collection, so
/// naming a vector index in the explain output would describe a search that
/// never happened.
#[tokio::test]
async fn an_empty_index_is_not_reported_as_serving_the_scan() {
    let node = node_with("").await;

    let query = similarity_query(&vector_for(1), 5, None);
    let explain = query_data(
        &node,
        &format!("query @explain(type: execute) {query}"),
        "empty index",
    )
    .await;

    assert_eq!(
        vector_index(&explain),
        None,
        "an index that returned no candidates must not be named\n{explain:#}"
    );
    assert_eq!(
        index_fetches(&explain),
        0,
        "nor counted as a fetch\n{explain:#}"
    );
}

/// Exhausting the index is not exhausting the collection.
///
/// A vector index holds an entry per indexed vector. A document whose embedding
/// is null is never inserted, so a routed scan that stops when the index runs
/// dry drops rows the caller asked for. With one indexed vector among three
/// documents, `limit: 3` returned one row.
#[tokio::test]
async fn documents_without_a_vector_still_fill_the_page() {
    let node = node_with("").await;

    query_data(
        &node,
        &format!(
            r#"mutation {{ create_Note(input: {{ title: "has-vector", tag: "even", embedding: [{}] }}) {{ _docID }} }}"#,
            render(&vector_for(0))
        ),
        "seed indexed",
    )
    .await;
    for title in ["no-vector-a", "no-vector-b"] {
        query_data(
            &node,
            &format!(r#"mutation {{ create_Note(input: {{ title: "{title}", tag: "even" }}) {{ _docID }} }}"#),
            "seed unindexed",
        )
        .await;
    }

    let data = query_data(
        &node,
        &similarity_query(&vector_for(0), 3, None),
        "similarity over a partly indexed collection",
    )
    .await;
    let rows = data["Note"].as_array().expect("rows");

    assert_eq!(
        rows.len(),
        3,
        "every document must be returned, not only the indexed one: {rows:?}"
    );

    let titles: Vec<&str> = rows
        .iter()
        .filter_map(|row| row["title"].as_str())
        .collect();
    for expected in ["has-vector", "no-vector-a", "no-vector-b"] {
        assert!(
            titles.contains(&expected),
            "{expected} missing from {titles:?}"
        );
    }
}

/// A document is returned once, not twice, when the fallback runs.
#[tokio::test]
async fn the_fallback_does_not_duplicate_indexed_documents() {
    let node = node_with("").await;

    for index in 0..3 {
        query_data(
            &node,
            &format!(
                r#"mutation {{ create_Note(input: {{ title: "indexed-{index}", tag: "even", embedding: [{}] }}) {{ _docID }} }}"#,
                render(&vector_for(index))
            ),
            "seed indexed",
        )
        .await;
    }
    query_data(
        &node,
        r#"mutation { create_Note(input: { title: "unindexed", tag: "even" }) { _docID } }"#,
        "seed unindexed",
    )
    .await;

    let data = query_data(
        &node,
        &similarity_query(&vector_for(0), 10, None),
        "over-wide limit",
    )
    .await;
    let rows = data["Note"].as_array().expect("rows");

    let mut titles: Vec<&str> = rows
        .iter()
        .filter_map(|row| row["title"].as_str())
        .collect();
    titles.sort_unstable();
    let mut unique = titles.clone();
    unique.dedup();
    assert_eq!(
        titles, unique,
        "a document must not be returned twice: {titles:?}"
    );
    assert_eq!(titles.len(), 4, "every document exactly once: {titles:?}");
}

/// A routed similarity query stays silent: `extensions` must be entirely
/// absent, not merely an empty warnings list. Explain already says the index
/// was used; nothing here needs a second channel to repeat that.
#[tokio::test]
async fn a_routed_similarity_query_has_no_extensions() {
    let node = node_with("").await;
    seed(&node).await;

    let query = similarity_query(&vector_for(3), 5, None);
    let response = node.execute(&query).await;
    assert!(!response.has_errors(), "{:?}", response.errors);
    assert!(
        response.extensions.is_none(),
        "a routed query must not carry extensions: {:?}",
        response.extensions
    );
}

/// Without a limit, `route()` never gets far enough to ask for an index, so
/// the scan falls back to scoring everything. The results are still correct;
/// only the cost changed, which is exactly what the warning is for.
#[tokio::test]
async fn dropping_the_limit_warns_vector_index_unused() {
    let node = node_with("").await;
    seed(&node).await;

    let query = format!(
        r#"{{ Note(order: {{ _alias: {{ sim: DESC }} }}) {{ title tag sim: SIMILARITY(embedding: {{vector: [{}]}}) }} }}"#,
        render(&vector_for(3))
    );
    let response = node.execute(&query).await;
    assert!(!response.has_errors(), "{:?}", response.errors);

    let extensions = response
        .extensions
        .as_ref()
        .expect("dropping the limit must warn");
    assert_eq!(extensions.warnings.len(), 1);
    let warning = &extensions.warnings[0];
    assert_eq!(warning.code, "VECTOR_INDEX_UNUSED");
    let detail = warning.detail.as_ref().expect("detail");
    assert_eq!(detail["reason"], "noLimit");
    assert_eq!(detail["field"], "embedding");

    let rows = response.data.as_ref().unwrap()["Note"]
        .as_array()
        .expect("Note array")
        .clone();
    assert_eq!(rows.len(), CORPUS, "no limit means every document");
    let scores: Vec<f64> = rows
        .iter()
        .map(|row| row["sim"].as_f64().expect("sim"))
        .collect();
    assert!(
        scores.windows(2).all(|pair| pair[0] >= pair[1]),
        "rows are not ordered by similarity: {scores:?}"
    );
}

/// Same shape as `dropping_the_limit_warns_vector_index_unused`, but the
/// defect is the order direction instead of the missing limit.
#[tokio::test]
async fn ascending_order_warns_vector_index_unused() {
    let node = node_with("").await;
    seed(&node).await;

    let query = format!(
        r#"{{ Note(order: {{ _alias: {{ sim: ASC }} }}, limit: 5) {{ title tag sim: SIMILARITY(embedding: {{vector: [{}]}}) }} }}"#,
        render(&vector_for(3))
    );
    let response = node.execute(&query).await;
    assert!(!response.has_errors(), "{:?}", response.errors);

    let extensions = response
        .extensions
        .as_ref()
        .expect("ascending order must warn");
    let warning = &extensions.warnings[0];
    assert_eq!(warning.code, "VECTOR_INDEX_UNUSED");
    let detail = warning.detail.as_ref().expect("detail");
    assert_eq!(detail["reason"], "notOrderedBySimilarityDesc");
    assert_eq!(detail["field"], "embedding");
}

/// A metric no index on the field carries must still answer correctly: the
/// query falls back to a full scan scored by the metric it named, and warns
/// that the vector index went unused rather than silently ranking by the
/// index's own metric instead.
#[tokio::test]
async fn a_metric_no_index_carries_scans_and_warns() {
    let node = EmbeddedNode::builder().build().await.unwrap();
    node.add_schema(&format!(
        "type Note {{ title: String  tag: String  embedding: [Float32!] @index(vector: {{dimensions: {DIMENSIONS}, hnsw: {{metric: COSINE}}}}) }}"
    ))
    .await
    .expect("add cosine schema");
    seed(&node).await;

    let query = similarity_query_with_metric(&vector_for(3), 5, "DOT");
    let explain = query_data(
        &node,
        &format!("query @explain(type: execute) {query}"),
        "metric mismatch explain",
    )
    .await;
    assert_eq!(
        vector_index(&explain),
        None,
        "a metric no index carries must not be served by the index\n{explain:#}"
    );

    let response = node.execute(&query).await;
    assert!(!response.has_errors(), "{:?}", response.errors);

    let extensions = response
        .extensions
        .as_ref()
        .expect("a metric mismatch must warn");
    assert_eq!(extensions.warnings.len(), 1);
    let warning = &extensions.warnings[0];
    assert_eq!(warning.code, "VECTOR_INDEX_UNUSED");
    let detail = warning.detail.as_ref().expect("detail");
    assert_eq!(detail["reason"], "metricMismatch");
    assert_eq!(detail["field"], "embedding");

    let rows = response.data.as_ref().unwrap()["Note"]
        .as_array()
        .expect("Note array")
        .clone();
    assert_eq!(rows.len(), 5, "the scan must still fill the page");
    let titles: Vec<&str> = rows
        .iter()
        .filter_map(|row| row["title"].as_str())
        .collect();
    let scores: Vec<f64> = rows
        .iter()
        .map(|row| row["sim"].as_f64().expect("sim"))
        .collect();
    assert!(
        scores.windows(2).all(|pair| pair[0] >= pair[1]),
        "rows are not ordered by similarity: {scores:?}"
    );

    // The exhaustive answer under the same named metric, computed without a
    // limit so no index can narrow it: the scanned page must be its top slice.
    let exhaustive = query_data(
        &node,
        &similarity_query_with_metric(&vector_for(3), CORPUS, "DOT"),
        "exhaustive dot scores",
    )
    .await;
    let exhaustive_rows = exhaustive["Note"].as_array().expect("Note array");
    let expected_titles: Vec<&str> = exhaustive_rows
        .iter()
        .filter_map(|row| row["title"].as_str())
        .take(5)
        .collect();

    assert_eq!(
        titles, expected_titles,
        "the scanned page must be the exhaustive DOT-scored page"
    );
}

/// The precision condition: a field with no vector index never warns, however
/// the query is shaped. Written with the limit removed, the one shape that
/// *would* warn if this field had an index (see
/// `dropping_the_limit_warns_vector_index_unused`).
#[tokio::test]
async fn a_field_without_a_vector_index_never_warns() {
    let node = EmbeddedNode::builder().build().await.unwrap();
    node.add_schema(&format!(
        "type Note {{ title: String  tag: String  embedding: [Float32!] @index(vector: {{dimensions: {DIMENSIONS}}})  plainEmbedding: [Float32!] }}"
    ))
    .await
    .expect("add schema with an unindexed vector field");

    for index in 0..3 {
        let mutation = format!(
            r#"mutation {{ create_Note(input: {{ title: "note-{index}", tag: "even", plainEmbedding: [{}] }}) {{ _docID }} }}"#,
            render(&vector_for(index))
        );
        query_data(&node, &mutation, "seed").await;
    }

    let query = format!(
        r#"{{ Note(order: {{ _alias: {{ sim: DESC }} }}) {{ title sim: SIMILARITY(plainEmbedding: {{vector: [{}]}}) }} }}"#,
        render(&vector_for(0))
    );
    let response = node.execute(&query).await;
    assert!(!response.has_errors(), "{:?}", response.errors);
    assert!(
        response.extensions.is_none(),
        "a similarity query on an unindexed field must never warn: {:?}",
        response.extensions
    );
}

/// Two vector indexes on one field, and a query that says which it means.
///
/// This is the whole point of the `metric` argument, and it was unreachable
/// from SDL until a repeated `@index` on a field stopped overwriting: the
/// parser kept the last directive, so a field could never carry two indexes and
/// there was nothing for the argument to choose between. Each metric must reach
/// its own index, and the routed page must be the exhaustive page for that
/// metric, not for the other one.
#[tokio::test]
async fn a_named_metric_reaches_its_own_index_among_several() {
    let node = EmbeddedNode::builder().build().await.unwrap();
    node.add_schema(&format!(
        "type Note {{ title: String  tag: String  embedding: [Float32!] \
         @index(name: \"by_cosine\", vector: {{dimensions: {DIMENSIONS}, hnsw: {{metric: COSINE}}}}) \
         @index(name: \"by_dot\", vector: {{dimensions: {DIMENSIONS}, hnsw: {{metric: DOT}}}}) }}"
    ))
    .await
    .expect("two vector indexes on one field");
    seed(&node).await;

    let probe = vector_for(3);
    for (metric, index_name) in [("COSINE", "Note_by_cosine"), ("DOT", "Note_by_dot")] {
        let query = format!(
            r#"{{ Note(order: {{ _alias: {{ sim: DESC }} }}, limit: 5) {{ title sim: SIMILARITY(embedding: {{vector: [{}], metric: {metric}}}) }} }}"#,
            render(&probe)
        );

        let explain = query_data(
            &node,
            &format!("query @explain(type: execute) {query}"),
            metric,
        )
        .await;
        let served = vector_index(&explain)
            .unwrap_or_else(|| panic!("{metric}: no vector index served the scan\n{explain:#}"));
        assert!(
            served.contains(index_name) || served.contains(metric.to_lowercase().as_str()),
            "{metric}: served by {served}, not the index built with that metric"
        );

        let routed = query_data(&node, &query, metric).await;
        let rows = routed["Note"].as_array().expect("Note array");
        assert_eq!(rows.len(), 5, "{metric}");
        let titles: Vec<&str> = rows.iter().filter_map(|r| r["title"].as_str()).collect();

        // Unlimited, so nothing narrows it: the answer the routed page must
        // reproduce, scored by the metric this query named.
        let all = query_data(
            &node,
            &format!(
                r#"{{ Note(order: {{_alias: {{sim: DESC}}}}, limit: {CORPUS}) {{ title sim: SIMILARITY(embedding: {{vector: [{}], metric: {metric}}}) }} }}"#,
                render(&probe)
            ),
            metric,
        )
        .await;
        let expected: Vec<&str> = all["Note"]
            .as_array()
            .expect("Note array")
            .iter()
            .filter_map(|r| r["title"].as_str())
            .take(5)
            .collect();
        assert_eq!(
            titles, expected,
            "{metric}: routed page differs from the exhaustive one"
        );
    }
}

/// Naming no metric when the field carries several is ambiguous, so the query
/// declines to route and says so rather than letting index order decide the
/// results.
#[tokio::test]
async fn several_indexes_without_a_metric_warn_ambiguous() {
    let node = EmbeddedNode::builder().build().await.unwrap();
    node.add_schema(&format!(
        "type Note {{ title: String  tag: String  embedding: [Float32!] \
         @index(name: \"by_cosine\", vector: {{dimensions: {DIMENSIONS}, hnsw: {{metric: COSINE}}}}) \
         @index(name: \"by_dot\", vector: {{dimensions: {DIMENSIONS}, hnsw: {{metric: DOT}}}}) }}"
    ))
    .await
    .expect("two vector indexes on one field");
    seed(&node).await;

    let query = similarity_query(&vector_for(3), 5, None);
    let response = node.execute(&query).await;
    assert!(!response.has_errors(), "{:?}", response.errors);

    let extensions = response
        .extensions
        .as_ref()
        .expect("an ambiguous metric must warn");
    let detail = extensions.warnings[0].detail.as_ref().expect("detail");
    assert_eq!(detail["reason"], "ambiguousMetric");
    assert_eq!(detail["field"], "embedding");

    assert_eq!(
        response.data.as_ref().unwrap()["Note"]
            .as_array()
            .expect("Note array")
            .len(),
        5,
        "declining to route must still fill the page"
    );
}
