use integration_test::{DefraClient, TestCluster};

const USER_SCHEMA: &str = "type User { name: String tag: String seq: Int }";

/// Create `count` `User` documents in batches, using `field_json` to build each
/// document's input fields (e.g. `r#"name: "User0""#`). Batching keeps the
/// number of CLI subprocess round-trips small for large document counts.
/// Returns the created docIDs in creation order.
fn seed_users(
    client: &DefraClient,
    count: usize,
    mut field_json: impl FnMut(usize) -> String,
) -> Vec<String> {
    const CHUNK: usize = 200;
    let mut doc_ids = Vec::with_capacity(count);
    for chunk_start in (0..count).step_by(CHUNK) {
        let chunk_end = (chunk_start + CHUNK).min(count);
        let items: Vec<String> = (chunk_start..chunk_end)
            .map(|i| format!("{{{}}}", field_json(i)))
            .collect();
        let result = client
            .query(&format!(
                "mutation {{ add_User(input: [{}]) {{ _docID }} }}",
                items.join(", ")
            ))
            .expect("batch create users");
        let rows = result["add_User"]
            .as_array()
            .expect("batch create result not array");
        for row in rows {
            doc_ids.push(row["_docID"].as_str().expect("docID").to_string());
        }
    }
    doc_ids
}

/// A limit query over a large collection must return exactly the requested
/// page. This asserts correctness only: the harness runs a real node over
/// HTTP, so it cannot observe how many documents storage was asked for. The
/// read count is asserted in `db`'s `limit_pushdown_tests`.
#[tokio::test]
async fn limit_query_returns_exact_page_from_large_collection() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let client = cluster.client(0);
    client.schema_add(USER_SCHEMA).expect("add schema");

    seed_users(&client, 2_000, |i| format!(r#"name: "User{i}""#));

    let res = client
        .query("query { User(limit: 10) { name } }")
        .expect("limit query");
    let docs = res["User"].as_array().expect("User array");
    assert_eq!(
        docs.len(),
        10,
        "limit must still return exactly 10 documents"
    );
}

/// The negative case the spec requires: a filtered query returns N MATCHING
/// documents, never N raw rows.
#[tokio::test]
async fn filtered_limit_returns_matching_not_raw_rows() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let client = cluster.client(0);
    client.schema_add(USER_SCHEMA).expect("add schema");

    // Spread ~25 matches evenly across 2000 docs (2000 / 80 = 25).
    seed_users(&client, 2_000, |i| {
        let tag = if i % 80 == 0 { "match" } else { "nomatch" };
        format!(r#"name: "User{i}", tag: "{tag}""#)
    });

    let res = client
        .query(r#"query { User(filter: {tag: {_eq: "match"}}, limit: 10) { name tag } }"#)
        .expect("filtered limit query");
    let docs = res["User"].as_array().expect("User array");

    assert_eq!(docs.len(), 10, "expected 10 MATCHING docs, not 10 raw rows");
    assert!(docs.iter().all(|d| d["tag"] == "match"));
}

/// limit+offset on a NON-INDEXED field. No Rust-native test covers this today.
#[tokio::test]
async fn limit_offset_on_non_indexed_field_is_correct() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let client = cluster.client(0);
    client.schema_add(USER_SCHEMA).expect("add schema");

    seed_users(&client, 50, |i| format!(r#"name: "User{i}", seq: {i}"#));

    let all = client
        .query("query { User(order: {seq: ASC}) { name } }")
        .expect("all docs query");
    let all_docs = all["User"].as_array().expect("User array").clone();

    let page = client
        .query("query { User(order: {seq: ASC}, limit: 10, offset: 5) { name } }")
        .expect("paged query");
    let page_docs = page["User"].as_array().expect("User array").clone();

    assert_eq!(page_docs.len(), 10);
    assert_eq!(
        page_docs,
        all_docs[5..15],
        "offset+limit must be a contiguous slice"
    );
}

/// Deleted documents interleaved must not shorten a page.
#[tokio::test]
async fn deleted_documents_do_not_shorten_a_limited_page() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let client = cluster.client(0);
    client.schema_add(USER_SCHEMA).expect("add schema");

    let doc_ids = seed_users(&client, 100, |i| format!(r#"name: "User{i}""#));

    // Delete every third document.
    let to_delete: Vec<String> = doc_ids
        .iter()
        .enumerate()
        .filter(|(i, _)| (i + 1) % 3 == 0)
        .map(|(_, id)| format!(r#""{id}""#))
        .collect();
    client
        .query(&format!(
            "mutation {{ delete_User(docIDs: [{}]) {{ _docID }} }}",
            to_delete.join(", ")
        ))
        .expect("delete every third document");

    let res = client
        .query("query { User(limit: 10) { name } }")
        .expect("limit query after deletes");
    let docs = res["User"].as_array().expect("User array");
    assert_eq!(docs.len(), 10, "a full page must still be returned");
}
