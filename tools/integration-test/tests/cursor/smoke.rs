//! Smoke tests: basic forward + backward cursor pagination, _pageInfo shape.

use serde_json::Value;

use super::common::{seed_users, setup_indexed_cluster, setup_unindexed_cluster};

#[tokio::test]
async fn rust_forward_first() {
    let (_cluster, node) = setup_indexed_cluster().await;
    seed_users(
        &node,
        &[
            ("alice", 20),
            ("bob", 30),
            ("carol", 40),
            ("dave", 50),
            ("eve", 60),
        ],
    )
    .await;

    let result: Value = node
        .query(r#"{ _cursor { User(first: 2, order: [{age: ASC}]) { name } } }"#)
        .expect("forward cursor query");

    let users = result["_cursor"]["User"]
        .as_array()
        .expect("_cursor.User is an array");
    assert_eq!(users.len(), 2, "expected 2 users in first page");
    assert_eq!(users[0]["name"], "alice");
    assert_eq!(users[1]["name"], "bob");
}

#[tokio::test]
async fn rust_forward_with_page_info() {
    let (_cluster, node) = setup_indexed_cluster().await;
    seed_users(&node, &[("alice", 20), ("bob", 30), ("carol", 40)]).await;

    let result: Value = node
        .query(
            r#"{ _cursor {
            User(first: 2, order: [{age: ASC}]) { name }
            _pageInfo { hasNext startCursor endCursor }
        } }"#,
        )
        .expect("forward cursor query with pageInfo");

    let page_info = &result["_cursor"]["_pageInfo"];
    assert_eq!(
        page_info["hasNext"],
        Value::Bool(true),
        "hasNext should be true (carol exists)"
    );
    assert!(
        page_info["startCursor"].is_string(),
        "startCursor populated"
    );
    assert!(page_info["endCursor"].is_string(), "endCursor populated");
    // hasPrev was not selected — must be absent from the response
    assert!(
        page_info.get("hasPrev").is_none(),
        "hasPrev not selected, must be absent from response"
    );
}

#[tokio::test]
async fn rust_backward_last() {
    let (_cluster, node) = setup_indexed_cluster().await;
    seed_users(
        &node,
        &[("alice", 20), ("bob", 30), ("carol", 40), ("dave", 50)],
    )
    .await;

    let result: Value = node
        .query(r#"{ _cursor { User(last: 2, order: [{age: ASC}]) { name } } }"#)
        .expect("backward cursor query");

    let users = result["_cursor"]["User"]
        .as_array()
        .expect("_cursor.User is an array");
    assert_eq!(users.len(), 2, "expected 2 users in last page");
    // last:2 in ASC order → the two highest ages: carol (40) and dave (50)
    let names: Vec<&str> = users
        .iter()
        .map(|u| u["name"].as_str().expect("name field"))
        .collect();
    assert!(
        names.contains(&"carol") && names.contains(&"dave"),
        "expected carol and dave, got {:?}",
        names
    );
}

#[tokio::test]
async fn rust_no_order_uses_doc_id_fallback() {
    // No order specified → docID ordering — no index required.
    // Use an unindexed schema to confirm this path works without any index.
    let (_cluster, node) = setup_unindexed_cluster().await;
    seed_users(&node, &[("alpha", 1), ("beta", 2)]).await;

    let result: Value = node
        .query(r#"{ _cursor { User(first: 2) { name } } }"#)
        .expect("no-order cursor query");

    let users = result["_cursor"]["User"]
        .as_array()
        .expect("_cursor.User is an array");
    assert_eq!(users.len(), 2, "expected 2 users");
}

#[tokio::test]
async fn rust_pagination_round_trip() {
    // Page 1 → get endCursor → use as `after` → Page 2 → expect next rows.
    let (_cluster, node) = setup_indexed_cluster().await;
    seed_users(
        &node,
        &[
            ("alice", 20),
            ("bob", 30),
            ("carol", 40),
            ("dave", 50),
            ("eve", 60),
        ],
    )
    .await;

    let page1: Value = node
        .query(
            r#"{ _cursor {
            User(first: 2, order: [{age: ASC}]) { name }
            _pageInfo { endCursor hasNext }
        } }"#,
        )
        .expect("page 1");

    let end_cursor = page1["_cursor"]["_pageInfo"]["endCursor"]
        .as_str()
        .expect("endCursor present")
        .to_string();
    assert_eq!(
        page1["_cursor"]["_pageInfo"]["hasNext"],
        Value::Bool(true),
        "hasNext should be true on page 1"
    );

    let page2_query = format!(
        r#"{{ _cursor {{
            User(first: 2, after: "{end_cursor}", order: [{{age: ASC}}]) {{ name }}
        }} }}"#
    );
    let page2: Value = node.query(&page2_query).expect("page 2");

    let users2 = page2["_cursor"]["User"].as_array().expect("page 2 users");
    assert_eq!(users2.len(), 2, "expected 2 users on page 2");
    assert_eq!(users2[0]["name"], "carol");
    assert_eq!(users2[1]["name"], "dave");
}

#[tokio::test]
async fn rust_forward_first_after_doc_id_desc() {
    // Regression: forward slow path with _docID DESC used `row > after` (ASC comparison).
    // For DESC order, smaller doc_ids come next, so the correct comparison is `row < after`.
    let cluster = integration_test::TestCluster::builder()
        .rust_nodes(1)
        .build()
        .await
        .unwrap();
    let node = cluster.client(0);
    node.schema_add(super::common::USER_SCHEMA).expect("schema");

    for name in ["a", "b", "c", "d", "e"] {
        node.query(&format!(
            r#"mutation {{ add_User(input: {{ name: "{name}" }}) {{ _docID }} }}"#
        ))
        .expect("seed");
    }

    // Page 1: first 2 in _docID DESC.
    let p1: Value = node
        .query(
            r#"{ _cursor {
            User(first: 2, order: [{_docID: DESC}]) { _docID name }
            _pageInfo { endCursor hasNext }
        } }"#,
        )
        .expect("page 1 _docID DESC");

    let p1_users = p1["_cursor"]["User"]
        .as_array()
        .expect("_cursor.User page 1");
    assert_eq!(p1_users.len(), 2);
    assert_eq!(
        p1["_cursor"]["_pageInfo"]["hasNext"],
        Value::Bool(true),
        "hasNext must be true with 5 users and page size 2"
    );

    let end_cursor = p1["_cursor"]["_pageInfo"]["endCursor"]
        .as_str()
        .expect("endCursor present")
        .to_string();

    let p1_ids: Vec<String> = p1_users
        .iter()
        .map(|u| u["_docID"].as_str().unwrap().to_string())
        .collect();

    // Page 2: must return rows with SMALLER doc_ids (DESC order) than the endCursor row.
    let p2: Value = node
        .query(&format!(
            r#"{{ _cursor {{
            User(first: 2, after: "{end_cursor}", order: [{{_docID: DESC}}]) {{ _docID name }}
        }} }}"#
        ))
        .expect("page 2 _docID DESC");

    let p2_users = p2["_cursor"]["User"]
        .as_array()
        .expect("_cursor.User page 2");
    assert_eq!(p2_users.len(), 2);

    let p2_ids: Vec<String> = p2_users
        .iter()
        .map(|u| u["_docID"].as_str().unwrap().to_string())
        .collect();

    // Every id on page 2 must be strictly less than every id on page 1 (DESC).
    for id2 in &p2_ids {
        for id1 in &p1_ids {
            assert!(
                id2 < id1,
                "page 2 _docID {id2} must be < page 1 _docID {id1} (DESC order)"
            );
        }
    }

    // Combined 4 ids must be distinct (no overlap).
    let mut all: Vec<String> = p1_ids.into_iter().chain(p2_ids).collect();
    all.sort();
    all.dedup();
    assert_eq!(all.len(), 4, "pages must not overlap; got: {all:?}");
}

#[tokio::test]
async fn rust_backward_last_before_doc_id_desc() {
    // Regression: backward slow path with _docID DESC used `>= boundary` (ASC stop condition).
    // For DESC order the iterator yields larger doc_ids first; stop when `<= boundary`.
    let cluster = integration_test::TestCluster::builder()
        .rust_nodes(1)
        .build()
        .await
        .unwrap();
    let node = cluster.client(0);
    node.schema_add(super::common::USER_SCHEMA).expect("schema");

    for name in ["a", "b", "c", "d", "e"] {
        node.query(&format!(
            r#"mutation {{ add_User(input: {{ name: "{name}" }}) {{ _docID }} }}"#
        ))
        .expect("seed");
    }

    // Get a middle cursor: first 3 in _docID DESC.
    let p1: Value = node
        .query(
            r#"{ _cursor {
            User(first: 3, order: [{_docID: DESC}]) { _docID }
            _pageInfo { endCursor }
        } }"#,
        )
        .expect("page 1 _docID DESC");

    let end_cursor = p1["_cursor"]["_pageInfo"]["endCursor"]
        .as_str()
        .expect("endCursor present")
        .to_string();

    let p1_ids: Vec<String> = p1["_cursor"]["User"]
        .as_array()
        .unwrap()
        .iter()
        .map(|u| u["_docID"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(p1_ids.len(), 3, "page 1 must have 3 rows");
    let boundary_id = &p1_ids[2]; // the smallest id on page 1 (last in DESC)

    // last:2 + before:<cursor for 3rd row> in DESC order.
    // The 2 rows that come BEFORE the boundary in DESC order are the 2 largest doc_ids
    // on page 1 (rows 0 and 1).
    let result: Value = node
        .query(&format!(
            r#"{{ _cursor {{
            User(last: 2, before: "{end_cursor}", order: [{{_docID: DESC}}]) {{ _docID }}
        }} }}"#
        ))
        .expect("backward _docID DESC");

    let rows: Vec<String> = result["_cursor"]["User"]
        .as_array()
        .expect("_cursor.User")
        .iter()
        .map(|u| u["_docID"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(rows.len(), 2, "expected exactly 2 rows before the cursor");

    // All returned rows must have strictly larger doc_ids than the boundary (DESC order).
    for row_id in &rows {
        assert!(
            row_id > boundary_id,
            "row {row_id} must be > boundary {boundary_id} in DESC order"
        );
    }
}

#[tokio::test]
async fn rust_page_info_field_aliases_are_ignored() {
    // Go parity: aliases on _pageInfo sub-fields are DISCARDED — the response
    // always uses canonical names (hasNext/startCursor/...), matching Go's
    // planner which keys PageInfo() by request.HasNextFieldName etc.
    let (_cluster, node) = setup_indexed_cluster().await;
    seed_users(&node, &[("a", 20), ("b", 30)]).await;

    let result: Value = node
        .query(
            r#"{ _cursor {
            User(first: 1, order: [{age: ASC}]) { name }
            _pageInfo {
                next: hasNext
                start: startCursor
            }
        } }"#,
        )
        .expect("query with aliased _pageInfo fields");

    let pi = &result["_cursor"]["_pageInfo"];
    assert!(
        pi.get("hasNext").is_some(),
        "canonical 'hasNext' must appear regardless of alias: {pi}"
    );
    assert!(
        pi.get("next").is_none(),
        "alias 'next' must NOT appear (Go discards _pageInfo aliases): {pi}"
    );
    assert!(
        pi.get("startCursor").is_some(),
        "canonical 'startCursor' must appear regardless of alias: {pi}"
    );
    assert!(
        pi.get("start").is_none(),
        "alias 'start' must NOT appear (Go discards _pageInfo aliases): {pi}"
    );
    // hasPrev / endCursor were not selected — must be absent.
    assert!(
        pi.get("hasPrev").is_none(),
        "unselected 'hasPrev' must not appear: {pi}"
    );
    assert!(
        pi.get("endCursor").is_none(),
        "unselected 'endCursor' must not appear: {pi}"
    );
}

#[tokio::test]
async fn rust_page_info_block_alias_is_ignored() {
    // Go parity: an alias on the _pageInfo block is DISCARDED — the block always
    // renders under the literal "_pageInfo" key (Go uses request.PageInfoFieldName).
    let (_cluster, node) = setup_indexed_cluster().await;
    seed_users(&node, &[("a", 20), ("b", 30)]).await;

    let result: Value = node
        .query(
            r#"{ _cursor {
            User(first: 1, order: [{age: ASC}]) { name }
            info: _pageInfo { hasNext }
        } }"#,
        )
        .expect("query with aliased _pageInfo block");

    let cursor = &result["_cursor"];

    // The _pageInfo block must render under "_pageInfo", not the alias "info".
    assert!(
        cursor.get("_pageInfo").is_some(),
        "_pageInfo must render under canonical key regardless of alias: {cursor}"
    );
    assert!(
        cursor.get("info").is_none(),
        "alias 'info' must NOT appear (Go discards the _pageInfo block alias): {cursor}"
    );
    assert!(
        cursor["_pageInfo"].get("hasNext").is_some(),
        "hasNext must be present inside the _pageInfo block: {cursor}"
    );
}
