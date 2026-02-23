use integration_test::{for_each_runtime, TestCluster};

/// Build a query with `sub_levels` nested sub-selections below the `Book` collection.
///
/// `Book` is depth 1. Each additional level is one more `Requestable::Select` in
/// the parsed tree, incrementing the depth counter by 1.
///
/// - sub_levels = 19 → deepest Select is at depth 20 (exactly at limit)
/// - sub_levels = 20 → deepest Select is at depth 21 (one over the limit)
fn build_depth_query(sub_levels: usize) -> String {
    let mut query = "query { Book { ".to_string();
    for i in 0..sub_levels {
        query.push_str(&format!("l{} {{ ", i));
    }
    query.push_str("_docID");
    for _ in 0..sub_levels {
        query.push_str(" }");
    }
    query.push_str(" } }");
    query
}

/// Build a query that selects `field_count` aliased `title` fields on `Book`.
///
/// Uses aliases `f1: title`, `f2: title`, … to produce exactly `field_count`
/// entries in the selection set without schema duplication errors.
fn build_width_query(field_count: usize) -> String {
    let fields: String = (1..=field_count)
        .map(|i| format!("f{}: title", i))
        .collect::<Vec<_>>()
        .join(" ");
    format!("query {{ Book {{ {} }} }}", fields)
}

async fn query_depth_width_limit(cluster: TestCluster) {
    let client = cluster.client(0);

    client
        .schema_add("type Book { title: String }")
        .expect("schema add failed");

    // --- Depth limit ---

    // Depth exactly at the limit (depth 20): Book is depth 1, plus 19 sub-selects
    // reaching depth 20. The depth check passes; schema validation may reject
    // unknown fields, but the error must NOT be a depth-exceeded error.
    let depth_ok_query = build_depth_query(19);
    match client.query(&depth_ok_query) {
        Ok(_) => {}
        Err(e) => {
            let msg = e.to_string();
            assert!(
                !msg.contains("query exceeds maximum nesting depth"),
                "depth-20 query should not trigger the depth limit, but got: {}",
                msg
            );
        }
    }

    // Depth one over the limit (depth 21): Book is depth 1, plus 20 sub-selects.
    // Must fail with the depth-exceeded error.
    let depth_fail_query = build_depth_query(20);
    let depth_err = client
        .query(&depth_fail_query)
        .expect_err("depth-21 query should be rejected");
    assert!(
        depth_err
            .to_string()
            .contains("query exceeds maximum nesting depth of 20"),
        "depth-21 error should mention nesting depth limit, got: {}",
        depth_err
    );

    // --- Width limit ---

    // Exactly 100 fields at top level: must succeed (exit 0).
    let width_ok_query = build_width_query(100);
    client
        .query(&width_ok_query)
        .expect("width-100 query should succeed");

    // 101 fields: must be rejected with the width-exceeded error.
    let width_fail_query = build_width_query(101);
    let width_err = client
        .query(&width_fail_query)
        .expect_err("width-101 query should be rejected");
    assert!(
        width_err
            .to_string()
            .contains("query exceeds maximum field width of 100"),
        "width-101 error should mention field width limit, got: {}",
        width_err
    );

    // Confirm the node is still healthy after the rejected queries.
    client
        .query("query { Book { _docID } }")
        .expect("node should be healthy after rejected queries");
}

for_each_runtime!(query_depth_width_limit, query_depth_width_limit);

async fn query_timeout_under_load(_cluster: TestCluster) {
    // Audit gap: Query timeout fires correctly even when the
    // node is under heavy concurrent load.
    //
    // Blocked on: TestClusterBuilder does not yet expose --query-timeout,
    // so there is no way to start a node with a custom timeout from the
    // integration-test harness.
    todo!("implement: query timeout under load test")
}

for_each_runtime!(query_timeout_under_load, query_timeout_under_load);
