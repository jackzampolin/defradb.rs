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
