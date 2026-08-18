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

/// `SIMILARITY` ranks by dot product, so the index must too or routing declines.
fn schema(algorithm: &str) -> String {
    let args = if algorithm.is_empty() {
        format!("dimensions: {DIMENSIONS}")
    } else {
        format!("dimensions: {DIMENSIONS}, algorithm: \"{algorithm}\"")
    };
    format!(
        "type Note {{ title: String  tag: String  embedding: [Float32!] @vectorIndex({args}, metric: \"DOT\") }}"
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
        // SIMILARITY ranks by dot product, so an algorithm that cannot order one
        // is covered by `an_unsupported_metric_is_refused_when_the_schema_is_written`.
        if !algorithm.supports_metric(schema::DistanceMetric::Dot) {
            continue;
        }
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

/// A cosine index ranks by a different measure than `SIMILARITY` does, so
/// routing to it would return the wrong documents. It must decline and scan,
/// and the answer must still be right.
#[tokio::test]
async fn a_mismatched_metric_declines_to_route_and_stays_correct() {
    let node = EmbeddedNode::builder().build().await.unwrap();
    node.add_schema(&format!(
        "type Note {{ title: String  tag: String  embedding: [Float32!] @vectorIndex(dimensions: {DIMENSIONS}, metric: \"COSINE\") }}"
    ))
    .await
    .expect("add cosine schema");
    seed(&node).await;

    let query = similarity_query(&vector_for(3), 5, None);
    let explain = query_data(
        &node,
        &format!("query @explain(type: execute) {query}"),
        "cosine explain",
    )
    .await;
    assert_eq!(
        vector_index(&explain),
        None,
        "a cosine index must not serve a dot-product ranking\n{explain:#}"
    );

    let routed = query_data(&node, &query, "cosine similarity").await;
    let rows = routed["Note"].as_array().expect("Note array");
    assert_eq!(rows.len(), 5);

    // The scan is exhaustive, so its top score is the true maximum.
    let best = rows[0]["sim"].as_f64().expect("sim");
    let all = query_data(
        &node,
        &format!(
            r#"{{ Note(limit: {CORPUS}) {{ sim: SIMILARITY(embedding: {{vector: [{}]}}) }} }}"#,
            render(&vector_for(3))
        ),
        "exhaustive scores",
    )
    .await;
    let true_best = all["Note"]
        .as_array()
        .expect("Note array")
        .iter()
        .filter_map(|row| row["sim"].as_f64())
        .fold(f64::MIN, f64::max);
    assert!(
        (best - true_best).abs() <= true_best.abs() * 1e-9,
        "declining to route must still return the true nearest: got {best}, best is {true_best}"
    );
}

/// A scalar index on the filtered field must not displace the vector index.
/// Only the vector index can serve `ORDER BY similarity` with a limit; a scalar
/// index narrows the filter and then leaves every match to be scored.
#[tokio::test]
async fn a_scalar_index_on_the_filter_does_not_displace_the_vector_index() {
    let node = EmbeddedNode::builder().build().await.unwrap();
    node.add_schema(&format!(
        "type Note {{ title: String  tag: String @index  embedding: [Float32!] @vectorIndex(dimensions: {DIMENSIONS}, metric: \"DOT\") }}"
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
/// be used exactly when the metric allows it.
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
                        "type Note {{ title: String  {tag_field}  embedding: [Float32!] @vectorIndex(dimensions: {DIMENSIONS}, algorithm: \"{}\", metric: \"{}\") }}",
                        algorithm.as_str(),
                        metric.as_str()
                    ))
                    .await
                    .unwrap_or_else(|e| panic!("{case}: add schema: {e}"));
                    seed(&node).await;

                    let probe = vector_for(3);
                    let query = similarity_query(&probe, LIMIT, filter);

                    // Only a dot-product index ranks the way SIMILARITY does.
                    let explain = query_data(
                        &node,
                        &format!("query @explain(type: execute) {query}"),
                        &case,
                    )
                    .await;
                    let routed = vector_index(&explain).is_some();
                    assert_eq!(
                        routed,
                        *metric == schema::DistanceMetric::Dot,
                        "{case}: routing must follow the metric\n{explain:#}"
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
                    "type Note {{ embedding: [Float32!] @vectorIndex(dimensions: {DIMENSIONS}, algorithm: \"{}\", metric: \"{}\") }}",
                    algorithm.as_str(),
                    metric.as_str()
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
         type Note { title: String  owner: Owner  embedding: [Float32!] @vectorIndex(dimensions: 4, metric: \"DOT\") }",
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
