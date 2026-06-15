//! Cursor pagination over composite indexes.
//!
//! Validates the composite prefix rule from Go's `isUnsupportedCursorCompositePrefix`:
//! - A non-unique composite index requires ordering by ALL its fields (cursor errors otherwise).
//! - A unique composite index allows ordering by a prefix of its fields.
//!
//! Also validates:
//! - Non-unique index entries with duplicate field values (P1.2 duplicate-keys regression).
//! - `before` cursor is exclusive on the fast path (PR#961 round-2 Fix 2 regression).
//! - Unique composite prefix cursors fall back to the slow path safely (PR#961 round-2 Fix 4).

use integration_test::TestCluster;
use serde_json::Value;

const USER_COMPOSITE_SCHEMA: &str = "type User { name: String  age: Int  score: Int }";

async fn seed_composite_users(node: &integration_test::DefraClient, users: &[(&str, i32, i32)]) {
    for (name, age, score) in users {
        let mutation = format!(
            r#"mutation {{ add_User(input: {{ name: "{name}", age: {age}, score: {score} }}) {{ _docID }} }}"#
        );
        node.query(&mutation).expect("seed user");
    }
}

#[tokio::test]
async fn rust_composite_index_full_field_order_succeeds() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let node = cluster.client(0);
    node.schema_add(USER_COMPOSITE_SCHEMA)
        .expect("add User schema");

    node.index_create("User", &["age", "score"], Some("idx_age_score"), false)
        .expect("create composite index");

    seed_composite_users(
        &node,
        &[("alice", 20, 90), ("bob", 30, 80), ("carol", 40, 70)],
    )
    .await;

    // Order by both fields — satisfies the composite prefix rule for a non-unique index.
    let result: Value = node
        .query(
            r#"{ _cursor { User(first: 2, order: [{age: ASC}, {score: ASC}]) { name age score } } }"#,
        )
        .expect("composite full-field cursor query");

    let users = result["_cursor"]["User"]
        .as_array()
        .expect("_cursor.User is an array");
    assert_eq!(users.len(), 2, "expected 2 users in first page");
    assert_eq!(users[0]["name"], "alice");
    assert_eq!(users[1]["name"], "bob");
}

#[tokio::test]
async fn rust_non_unique_composite_prefix_only_errors() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let node = cluster.client(0);
    node.schema_add(USER_COMPOSITE_SCHEMA)
        .expect("add User schema");

    // Non-unique composite index on (age, score).
    node.index_create("User", &["age", "score"], Some("idx_age_score"), false)
        .expect("create composite index");

    seed_composite_users(&node, &[("alice", 20, 90)]).await;

    // Order only by `age` — partial coverage of a non-unique composite index.
    // Go's rule: !index.Unique && len(ordering) < len(index.Fields) → no supporting index.
    let err = node
        .query(r#"{ _cursor { User(first: 2, order: [{age: ASC}]) { name } } }"#)
        .unwrap_err()
        .to_string();

    assert!(
        err.contains("no supporting index"),
        "expected no-supporting-index error for non-unique composite prefix, got: {err}"
    );
}

#[tokio::test]
async fn rust_unique_composite_prefix_succeeds() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let node = cluster.client(0);
    node.schema_add(USER_COMPOSITE_SCHEMA)
        .expect("add User schema");

    // Unique composite index on (age, score).
    node.index_create("User", &["age", "score"], Some("idx_age_score"), true)
        .expect("create unique composite index");

    seed_composite_users(&node, &[("alice", 20, 90), ("bob", 30, 80)]).await;

    // Order only by `age` — partial coverage is allowed for UNIQUE composite indexes.
    let result: Value = node
        .query(r#"{ _cursor { User(first: 2, order: [{age: ASC}]) { name } } }"#)
        .expect("unique composite prefix cursor query");

    let users = result["_cursor"]["User"]
        .as_array()
        .expect("_cursor.User is an array");
    assert_eq!(users.len(), 2, "expected 2 users");
}

#[tokio::test]
async fn rust_cursor_paginates_through_duplicate_index_keys() {
    // Regression test for P1.2: when multiple documents share the same indexed field
    // value, the cursor seek key must include the doc_id suffix for non-unique indexes.
    // Without the suffix, a forward-exclusive seek at [prefix][age=30] rejects ALL rows
    // with age=30 (not just the boundary doc), causing page 2 to be empty.
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let node = cluster.client(0);

    node.schema_add(USER_COMPOSITE_SCHEMA)
        .expect("add User schema");
    node.index_create("User", &["age"], Some("idx_age"), false)
        .expect("create age index");

    // 4 users all at age=30 (same indexed value), with distinct names.
    for name in ["alice", "bob", "carol", "dave"] {
        let mutation = format!(
            r#"mutation {{ add_User(input: {{ name: "{name}", age: 30, score: 0 }}) {{ _docID }} }}"#
        );
        node.query(&mutation).expect("seed user");
    }

    // Page 1: first 2 users ordered by age ASC. All have age=30, so order is by doc_id.
    let p1: Value = node
        .query(
            r#"{ _cursor {
            User(first: 2, order: [{age: ASC}]) { name }
            _pageInfo { endCursor hasNext }
        } }"#,
        )
        .expect("page 1");

    let p1_users = p1["_cursor"]["User"]
        .as_array()
        .expect("_cursor.User is an array");
    assert_eq!(p1_users.len(), 2, "expected 2 users in page 1");
    assert_eq!(
        p1["_cursor"]["_pageInfo"]["hasNext"],
        Value::Bool(true),
        "hasNext should be true (2 more users remain)"
    );

    let end_cursor = p1["_cursor"]["_pageInfo"]["endCursor"]
        .as_str()
        .expect("endCursor present")
        .to_string();

    let p1_names: Vec<String> = p1_users
        .iter()
        .map(|u| u["name"].as_str().unwrap().to_string())
        .collect();

    // Page 2: must return the OTHER two users with age=30, not skip them.
    // Before the P1.2 fix, seek at [prefix][age=30] (exclusive) excluded ALL age=30 rows,
    // making page 2 empty.
    let p2_query = format!(
        r#"{{ _cursor {{
            User(first: 2, after: "{end_cursor}", order: [{{age: ASC}}]) {{ name }}
        }} }}"#
    );
    let p2: Value = node.query(&p2_query).expect("page 2");

    let p2_users = p2["_cursor"]["User"]
        .as_array()
        .expect("_cursor.User page 2 is an array");
    assert_eq!(
        p2_users.len(),
        2,
        "expected 2 remaining users on page 2 (was 0 before the P1.2 fix)"
    );

    let p2_names: Vec<String> = p2_users
        .iter()
        .map(|u| u["name"].as_str().unwrap().to_string())
        .collect();

    // The two pages combined must account for all 4 users with no duplicates.
    let mut all_names: Vec<String> = p1_names.into_iter().chain(p2_names).collect();
    all_names.sort();
    assert_eq!(
        all_names,
        vec!["alice", "bob", "carol", "dave"],
        "pages 1 and 2 must cover all 4 users with no duplicates or missing entries"
    );
}

/// Regression: `before` cursor must be EXCLUSIVE on the fast path (index seek active).
///
/// Before PR#961 round-2 Fix 2, `inclusive: matches!(direction, CursorDirection::Backward)`
/// made the `before` boundary inclusive — the boundary row appeared in the result.
#[tokio::test]
async fn rust_backward_before_excludes_boundary() {
    let cluster = integration_test::TestCluster::builder()
        .rust_nodes(1)
        .build()
        .await
        .unwrap();
    let node = cluster.client(0);

    node.schema_add(super::common::USER_SCHEMA)
        .expect("add schema");
    node.index_create("User", &["age"], Some("idx_age"), false)
        .expect("create age index");

    for (name, age) in [("a", 20), ("b", 30), ("c", 40), ("d", 50)] {
        let mutation = format!(
            r#"mutation {{ add_User(input: {{ name: "{name}", age: {age} }}) {{ _docID }} }}"#
        );
        node.query(&mutation).expect("seed");
    }

    // Get the cursor for "c" (age=40) — the last row of a first:3 page.
    let page1: serde_json::Value = node
        .query(
            r#"{ _cursor {
            User(first: 3, order: [{age: ASC}]) { name age }
            _pageInfo { endCursor }
        } }"#,
        )
        .expect("page 1");
    let c_cursor = page1["_cursor"]["_pageInfo"]["endCursor"]
        .as_str()
        .expect("endCursor")
        .to_string();

    // last:2, before:<cursor for c (age=40)> — must return [a(20), b(30)].
    // The boundary c MUST NOT appear in the result.
    let result: serde_json::Value = node
        .query(&format!(
            r#"{{ _cursor {{
            User(last: 2, before: "{c_cursor}", order: [{{age: ASC}}]) {{ name age }}
        }} }}"#
        ))
        .expect("backward before query");

    let users = result["_cursor"]["User"]
        .as_array()
        .expect("_cursor.User is an array");
    let names: Vec<&str> = users
        .iter()
        .map(|u| u["name"].as_str().expect("name"))
        .collect();
    assert!(
        !names.contains(&"c"),
        "boundary row 'c' (age=40) must be excluded by `before` cursor; got {:?}",
        names
    );
    assert_eq!(
        names,
        vec!["a", "b"],
        "expected [a, b] before the cursor at c(40)"
    );
}

/// Regression: unique composite prefix cursor must fall back to slow path, not corrupt results.
///
/// A unique index on (age, name) with ORDER BY age only is a "unique composite prefix" case.
/// Before PR#961 round-2 Fix 4, the planner built a seek key covering only the `age` prefix,
/// which is ambiguous for unique indexes (doc_id is in the value, not the key). The fix makes
/// the planner fall back to the slow path, which uses doc_id comparison and works correctly
/// when ordering is docID/empty (no `after`/`before` token with non-empty keys).
#[tokio::test]
async fn rust_unique_composite_prefix_falls_back_to_slow_path() {
    let cluster = integration_test::TestCluster::builder()
        .rust_nodes(1)
        .build()
        .await
        .unwrap();
    let node = cluster.client(0);

    node.schema_add(USER_COMPOSITE_SCHEMA)
        .expect("add composite schema");
    // Unique composite index on (age, name).
    node.index_create("User", &["age", "score"], Some("idx_age_score"), true)
        .expect("create unique composite index");

    seed_composite_users(
        &node,
        &[("alice", 20, 90), ("bob", 30, 80), ("carol", 40, 70)],
    )
    .await;

    // ORDER BY age only (prefix of unique composite index). Page 1 works without a cursor token.
    let result: serde_json::Value = node
        .query(r#"{ _cursor { User(first: 2, order: [{age: ASC}]) { name } } }"#)
        .expect("unique composite prefix first page");

    let users = result["_cursor"]["User"]
        .as_array()
        .expect("_cursor.User is an array");
    assert_eq!(users.len(), 2, "expected 2 users on page 1");
    // Alice and bob have the two lowest ages.
    let names: Vec<&str> = users
        .iter()
        .map(|u| u["name"].as_str().expect("name"))
        .collect();
    assert!(
        names.contains(&"alice") && names.contains(&"bob"),
        "expected alice and bob on page 1, got {:?}",
        names
    );
}

/// Regression: page 2 of a unique composite-prefix cursor query falls back to the slow path.
///
/// The slow path errors with an actionable message when `after` is present and ORDER is not
/// `_docID`/empty — because the slow path can only compare doc_ids, not arbitrary field tuples.
/// This test closes the gap flagged in PR#961 round-3: the previous test only exercised page 1
/// (no cursor token) and never reached the fallback branch.
#[tokio::test]
async fn rust_unique_composite_prefix_after_paginates_via_slow_path() {
    let cluster = integration_test::TestCluster::builder()
        .rust_nodes(1)
        .build()
        .await
        .unwrap();
    let node = cluster.client(0);
    node.schema_add(USER_COMPOSITE_SCHEMA)
        .expect("add composite schema");
    node.index_create("User", &["age", "score"], Some("idx_age_score"), true)
        .expect("create unique composite index");

    seed_composite_users(
        &node,
        &[("alice", 20, 90), ("bob", 30, 80), ("carol", 40, 70)],
    )
    .await;

    // Page 1 — no cursor token, returns the first row in age order.
    let p1: Value = node
        .query(
            r#"{ _cursor {
            User(first: 1, order: [{age: ASC}]) { name age }
            _pageInfo { endCursor }
        } }"#,
        )
        .expect("page 1 unique composite prefix");
    assert_eq!(
        p1["_cursor"]["User"][0]["name"],
        Value::String("alice".into())
    );
    let end_cursor = p1["_cursor"]["_pageInfo"]["endCursor"]
        .as_str()
        .expect("endCursor present")
        .to_string();

    // Page 2 with `after`: the unique-composite-prefix cursor uses the slow path
    // (seek not applicable for a partial index prefix). The slow path locates the
    // boundary by exact docID equality and relies on the order node's ordering —
    // matching Go's drain fallback — so it returns the NEXT row, not an error.
    let p2: Value = node
        .query(&format!(
            r#"{{ _cursor {{
            User(first: 1, after: "{end_cursor}", order: [{{age: ASC}}]) {{ name age }}
        }} }}"#
        ))
        .expect("page 2 unique composite prefix slow-path pagination");
    assert_eq!(
        p2["_cursor"]["User"][0]["name"],
        Value::String("bob".into()),
        "page 2 must continue past alice to bob, got: {}",
        p2["_cursor"]["User"]
    );
}

/// Regression test for PR#961 round-4 P1.1: cursor seek applied to wrong index.
///
/// When two single-field indexes exist (one for filtering, one for ordering),
/// `try_select_index` may pick the filter index (e.g. `idx_score`) while
/// `validate_cursor_index` matched the order index (e.g. `idx_age`). Before the fix,
/// the seek bytes encoded for `idx_age`'s field positions were applied to the
/// `idx_score` scan, producing garbage page 2 results.
///
/// The planner must not switch from the filter index to the order index only
/// because an `after` token is present. Keeping the same filter-first scan lets
/// `OrderByNode` provide a stable order on every page, while cursor slow-path
/// skipping advances past the boundary docID.
#[tokio::test]
async fn rust_cursor_with_filter_and_order_keeps_filter_index_consistent() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let node = cluster.client(0);
    node.schema_add(USER_COMPOSITE_SCHEMA)
        .expect("add User schema");

    // Two single-field indexes: one for filtering (score), one for ordering (age).
    node.index_create("User", &["score"], Some("idx_score"), false)
        .expect("score idx");
    node.index_create("User", &["age"], Some("idx_age"), false)
        .expect("age idx");

    // 5 users; filter by score=90, order by age ASC.
    for (name, age, score) in [
        ("a", 20, 90),
        ("b", 30, 90),
        ("c", 40, 90),
        ("d", 50, 80),
        ("e", 60, 90),
    ] {
        let mutation = format!(
            r#"mutation {{ add_User(input: {{ name: "{name}", age: {age}, score: {score} }}) {{ _docID }} }}"#
        );
        node.query(&mutation).expect("seed");
    }

    // Page 1: filter by score=90, order by age ASC, first 2.
    let p1: Value = node
        .query(
            r#"{ _cursor {
            User(first: 2, filter: { score: { _eq: 90 } }, order: [{age: ASC}]) { name age score }
            _pageInfo { endCursor hasNext }
        } }"#,
        )
        .expect("page 1");

    let names1: Vec<String> = p1["_cursor"]["User"]
        .as_array()
        .unwrap()
        .iter()
        .map(|u| u["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(names1, vec!["a", "b"], "page 1: ages 20, 30");
    assert_eq!(
        p1["_cursor"]["_pageInfo"]["hasNext"],
        Value::Bool(true),
        "more rows exist"
    );

    let end_cursor = p1["_cursor"]["_pageInfo"]["endCursor"]
        .as_str()
        .expect("endCursor present")
        .to_string();

    // Page 2: continue from cursor. The filter index is scanned again, the
    // `OrderByNode` applies the same ordering as page 1, and the cursor slow
    // path skips past `b`.
    let p2: Value = node
        .query(&format!(
        r#"{{ _cursor {{
            User(first: 2, after: "{end_cursor}", filter: {{ score: {{ _eq: 90 }} }}, order: [{{age: ASC}}]) {{ name age score }}
        }} }}"#
    ))
        .expect("page 2 filter/order cursor should continue after the boundary");

    let names2: Vec<String> = p2["_cursor"]["User"]
        .as_array()
        .unwrap()
        .iter()
        .map(|u| u["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(names2, vec!["c", "e"]);

    for row in p2["_cursor"]["User"].as_array().unwrap() {
        assert_eq!(row["score"], Value::Number(90.into()));
    }
}

/// Regression: filtered cursor pagination must not change tie ordering between
/// page 1 and active-token pages.
///
/// A previous order-index override used the filter index on page 1, then used
/// the order index on page 2+. For non-unique order fields, page 1 ties followed
/// filter-scan input order while later pages followed order-index docID order,
/// which could drop or duplicate rows at the page boundary.
#[tokio::test]
async fn rust_filtered_cursor_order_ties_keep_page_boundary_stable() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let node = cluster.client(0);
    node.schema_add(USER_COMPOSITE_SCHEMA)
        .expect("add User schema");

    node.index_create("User", &["score"], Some("idx_score"), false)
        .expect("score idx");
    node.index_create("User", &["age"], Some("idx_age"), false)
        .expect("age idx");

    for (name, age, score) in [
        ("a", 40, 80),
        ("b", 50, 80),
        ("c", 50, 85),
        ("d", 50, 90),
        ("e", 50, 95),
        ("f", 60, 100),
    ] {
        let mutation = format!(
            r#"mutation {{ add_User(input: {{ name: "{name}", age: {age}, score: {score} }}) {{ _docID }} }}"#
        );
        node.query(&mutation).expect("seed");
    }

    let all: Value = node
        .query(
            r#"{ _cursor {
            User(first: 6, filter: { score: { _gte: 80 } }, order: [{age: ASC}]) { name }
        } }"#,
        )
        .expect("baseline filtered order");
    let expected: Vec<String> = all["_cursor"]["User"]
        .as_array()
        .unwrap()
        .iter()
        .map(|u| u["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(expected.len(), 6);

    let p1: Value = node
        .query(
            r#"{ _cursor {
            User(first: 3, filter: { score: { _gte: 80 } }, order: [{age: ASC}]) { name }
            _pageInfo { endCursor hasNext }
        } }"#,
        )
        .expect("page 1");
    assert_eq!(p1["_cursor"]["_pageInfo"]["hasNext"], Value::Bool(true));
    let end_cursor = p1["_cursor"]["_pageInfo"]["endCursor"]
        .as_str()
        .expect("endCursor present")
        .to_string();

    let p2: Value = node
        .query(&format!(
            r#"{{ _cursor {{
            User(first: 3, after: "{end_cursor}", filter: {{ score: {{ _gte: 80 }} }}, order: [{{age: ASC}}]) {{ name }}
        }} }}"#
        ))
        .expect("page 2");

    let mut paged: Vec<String> = p1["_cursor"]["User"]
        .as_array()
        .unwrap()
        .iter()
        .chain(p2["_cursor"]["User"].as_array().unwrap())
        .map(|u| u["name"].as_str().unwrap().to_string())
        .collect();
    paged.truncate(expected.len());

    assert_eq!(
        paged, expected,
        "paginated tie order must match the no-token filtered order"
    );
}
