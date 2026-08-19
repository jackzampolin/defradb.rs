//! An indexed DateTime field must accept the whole RFC3339 range.
//!
//! A timestamp encoded as a single i64 nanosecond count only spans 1677-2262,
//! so writing `"9999-12-31T23:59:59Z"` into an indexed DateTime field failed
//! the mutation outright while Go accepted it. Go encodes `(t.Unix(),
//! t.Nanosecond())` as two order-preserving varints and covers the full second
//! range; these tests run against both runtimes.

use integration_test::{for_each_runtime, TestCluster};
use serde_json::Value;

const SCHEMA: &str = "type Reading { name: String  observed_at: DateTime }";

/// Chronological. The window the old encoder could represent is roughly
/// 1677-2262, so everything outside it is a regression case.
const STAMPS: [(&str, &str); 7] = [
    ("ancient", "1000-06-15T12:30:45Z"),
    ("pre_window", "1600-01-01T00:00:00Z"),
    ("pre_epoch", "1969-12-31T23:59:59Z"),
    ("epoch", "1970-01-01T00:00:00Z"),
    ("modern", "2024-06-01T12:00:00Z"),
    ("post_window", "2263-01-01T00:00:00Z"),
    ("far_future", "9999-12-31T23:59:59Z"),
];

fn names_in_order(rows: &[Value]) -> Vec<&str> {
    rows.iter()
        .map(|row| row["name"].as_str().expect("name"))
        .collect()
}

async fn datetime_index_spans_the_full_range(cluster: TestCluster) {
    let node = cluster.client(0);
    node.schema_add(SCHEMA).expect("schema add");
    node.index_create("Reading", &["observed_at"], Some("idx_observed_at"), false)
        .expect("create observed_at index");

    // The reported failure: the mutation itself, once the field is indexed.
    for (name, stamp) in STAMPS {
        node.query(&format!(
            r#"mutation {{ add_Reading(input: {{ name: "{name}", observed_at: "{stamp}" }}) {{ _docID }} }}"#
        ))
        .unwrap_or_else(|e| panic!("writing {stamp} into an indexed DateTime field must succeed: {e}"));
    }

    let ascending: Value = node
        .query(r#"{ Reading(order: [{observed_at: ASC}]) { name observed_at } }"#)
        .expect("ascending query");
    let rows = ascending["Reading"].as_array().expect("Reading array");
    assert_eq!(rows.len(), STAMPS.len(), "every row must be indexed");

    let expected: Vec<&str> = STAMPS.iter().map(|(name, _)| *name).collect();
    assert_eq!(
        names_in_order(rows),
        expected,
        "index keys must sort chronologically across the whole range"
    );

    let descending: Value = node
        .query(r#"{ Reading(order: [{observed_at: DESC}]) { name observed_at } }"#)
        .expect("descending query");
    let rows = descending["Reading"].as_array().expect("Reading array");
    let mut reversed = expected.clone();
    reversed.reverse();
    assert_eq!(names_in_order(rows), reversed, "DESC must mirror ASC");
}

async fn datetime_range_filter_reaches_beyond_the_window(cluster: TestCluster) {
    let node = cluster.client(0);
    node.schema_add(SCHEMA).expect("schema add");
    node.index_create("Reading", &["observed_at"], Some("idx_observed_at"), false)
        .expect("create observed_at index");

    for (name, stamp) in STAMPS {
        node.query(&format!(
            r#"mutation {{ add_Reading(input: {{ name: "{name}", observed_at: "{stamp}" }}) {{ _docID }} }}"#
        ))
        .expect("seed reading");
    }

    let result: Value = node
        .query(
            r#"{ Reading(filter: {observed_at: {_gt: "2262-04-11T23:47:16Z"}}, order: [{observed_at: ASC}]) { name } }"#,
        )
        .expect("range filter query");
    let rows = result["Reading"].as_array().expect("Reading array");
    assert_eq!(
        names_in_order(rows),
        vec!["post_window", "far_future"],
        "a bound past the old nanosecond ceiling must still select the rows above it"
    );

    let result: Value = node
        .query(
            r#"{ Reading(filter: {observed_at: {_lt: "1677-09-21T00:12:44Z"}}, order: [{observed_at: ASC}]) { name } }"#,
        )
        .expect("range filter query");
    let rows = result["Reading"].as_array().expect("Reading array");
    assert_eq!(
        names_in_order(rows),
        vec!["ancient", "pre_window"],
        "a bound below the old nanosecond floor must still select the rows under it"
    );
}

for_each_runtime!(
    datetime_index_spans_the_full_range,
    datetime_index_spans_the_full_range
);
for_each_runtime!(
    datetime_range_filter_reaches_beyond_the_window,
    datetime_range_filter_reaches_beyond_the_window
);
