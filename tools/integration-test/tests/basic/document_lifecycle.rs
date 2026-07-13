use integration_test::{for_each_runtime, TestCluster};
use serde_json::Value;

/// Extract indexes array from index_list output.
/// Rust returns `[...]`, Go returns `{"CollectionName": [...]}`.
fn extract_indexes(val: &Value) -> Vec<&Value> {
    if let Some(arr) = val.as_array() {
        return arr.iter().collect();
    }
    if let Some(obj) = val.as_object() {
        for v in obj.values() {
            if let Some(arr) = v.as_array() {
                return arr.iter().collect();
            }
        }
    }
    vec![]
}

async fn document_lifecycle_test(cluster: TestCluster) {
    let client = cluster.client(0);

    // 1. Deploy schema
    client
        .schema_add("type Article { title: String  author: String  views: Int }")
        .expect("failed to add schema");

    // 2. Verify Article type exists via GraphQL introspection (works on both Go and Rust)
    let introspection = client
        .query(r#"{ __type(name: "Article") { name fields { name } } }"#)
        .expect("introspection query failed");
    assert_eq!(
        introspection["__type"]["name"], "Article",
        "Article type should exist in schema"
    );

    // 3. Create 3 articles via mutation
    let a1 = client
        .query(r#"mutation { add_Article(input: {title: "Rust 101", author: "Alice", views: 10}) { _docID } }"#)
        .expect("create article 1");
    let id1 = a1["add_Article"][0]["_docID"]
        .as_str()
        .expect("missing _docID for article 1")
        .to_string();

    client
        .query(r#"mutation { add_Article(input: {title: "Go Patterns", author: "Bob", views: 20}) { _docID } }"#)
        .expect("create article 2");

    client
        .query(r#"mutation { add_Article(input: {title: "P2P Networking", author: "Alice", views: 5}) { _docID } }"#)
        .expect("create article 3");

    // 4. Verify 3 documents exist
    let doc_ids = client
        .collection_doc_ids("Article")
        .expect("collection_doc_ids failed");
    assert_eq!(
        doc_ids.len(),
        3,
        "expected 3 doc IDs, got {}",
        doc_ids.len()
    );

    // 5. Collection describe — verify field info present
    let desc = client
        .collection_describe("Article")
        .expect("collection_describe failed");
    let desc_str = serde_json::to_string(&desc).unwrap();
    assert!(
        desc_str.contains("title"),
        "collection describe should mention 'title', got: {}",
        desc_str
    );

    // 6. Index create (non-unique)
    let idx1 = client
        .index_create("Article", &["author"], Some("idx_author"), false)
        .expect("index_create idx_author failed");
    let idx1_str = serde_json::to_string(&idx1).unwrap();
    assert!(
        idx1_str.contains("idx_author"),
        "index create should return index metadata with name, got: {}",
        idx1_str
    );

    // 7. Index create (unique)
    let idx2 = client
        .index_create("Article", &["views"], Some("idx_views"), true)
        .expect("index_create idx_views failed");
    let idx2_str = serde_json::to_string(&idx2).unwrap();
    assert!(
        idx2_str.contains("idx_views"),
        "index create should return index metadata with name, got: {}",
        idx2_str
    );

    // 8. Index list — verify 2 indexes
    let indexes = client
        .index_list(Some("Article"))
        .expect("index_list failed");
    let indexes_arr = extract_indexes(&indexes);
    assert!(
        indexes_arr.len() >= 2,
        "expected at least 2 indexes, got {}",
        indexes_arr.len()
    );

    // 9. Collection update — set views to 100 on first article
    client
        .collection_update("Article", &id1, r#"{"views": 100}"#)
        .expect("collection_update failed");

    // 10. Collection get — verify views == 100
    let doc = client
        .collection_get("Article", &id1)
        .expect("collection_get after update failed");
    assert_eq!(
        doc["views"], 100,
        "expected views=100 after update, got: {:?}",
        doc
    );

    // 11. Index delete
    client
        .index_delete("Article", "idx_author")
        .expect("index_delete failed");

    // 12. Index list — verify 1 index remains
    let indexes_after = client
        .index_list(Some("Article"))
        .expect("index_list after drop failed");
    let indexes_after_arr = extract_indexes(&indexes_after);
    assert!(
        indexes_after_arr.len() < indexes_arr.len(),
        "expected fewer indexes after drop"
    );

    // 13. Collection truncate
    client
        .collection_truncate("Article")
        .expect("collection_truncate failed");

    // 14. Query — verify 0 documents remain
    let data = client
        .query("query { Article { _docID } }")
        .expect("query after truncate failed");
    let articles = data["Article"]
        .as_array()
        .expect("Article result not array");
    assert_eq!(
        articles.len(),
        0,
        "expected 0 articles after truncate, got {}",
        articles.len()
    );
}

for_each_runtime!(document_lifecycle, document_lifecycle_test);

/// Deletion is terminal (Go parity: ErrDocumentDeleted). Recreating a document
/// with IDENTICAL content regenerates the same content-addressed docID, and
/// creating onto a tombstone must fail loudly — previously it silently wrote a
/// "zombie": the create reported success while the row stayed deleted, so
/// desired-state appliers looped forever on a create that never materialized
/// (sourcenetwork/defra-agent#700). Distinct content mints a new identity and
/// must succeed.
async fn recreate_after_delete_semantics(cluster: TestCluster) {
    let client = cluster.client(0);
    client
        .schema_add("type Note { label: String  body: String }")
        .expect("failed to add schema");

    let created = client
        .query(r#"mutation { add_Note(input: {label: "config", body: "v1"}) { _docID } }"#)
        .expect("create note");
    let doc_id = created["add_Note"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();

    client
        .query(&format!(
            r#"mutation {{ delete_Note(docID: "{doc_id}") {{ _docID }} }}"#
        ))
        .expect("delete note");

    // Identical content => identical docID => terminal-deletion error, not a
    // silent zombie.
    let identical =
        client.query(r#"mutation { add_Note(input: {label: "config", body: "v1"}) { _docID } }"#);
    match identical {
        Err(error) => {
            let message = format!("{error:#}");
            assert!(
                message.contains("deleted"),
                "recreating identical content over a tombstone must name the deletion, got: {message}"
            );
        }
        Ok(result) => {
            panic!("recreating identical content over a tombstone must fail loudly, got: {result}")
        }
    }

    // Distinct content mints a new identity and succeeds.
    let recreated = client
        .query(r#"mutation { add_Note(input: {label: "config", body: "v2"}) { _docID } }"#)
        .expect("recreate with distinct content");
    let new_id = recreated["add_Note"][0]["_docID"]
        .as_str()
        .expect("missing recreated _docID");
    assert_ne!(new_id, doc_id, "distinct content must produce a new docID");

    let live = client
        .query(r#"query { Note { body } }"#)
        .expect("query notes");
    let rows = live["Note"].as_array().expect("not array");
    assert_eq!(rows.len(), 1, "exactly the recreated doc must be live");
    assert_eq!(rows[0]["body"].as_str(), Some("v2"));
}

for_each_runtime!(recreate_after_delete, recreate_after_delete_semantics);

async fn filter_mutations_return_post_update_docs(cluster: TestCluster) {
    let client = cluster.client(0);

    client
        .schema_add("type FilterMutationUser { name: String  age: Int }")
        .expect("failed to add schema");

    client
        .query(r#"mutation { add_FilterMutationUser(input: {name: "Alice", age: 20}) { _docID } }"#)
        .expect("create Alice");

    let update = client
        .query(
            r#"mutation {
                update_FilterMutationUser(
                    filter: {age: {_lt: 30}},
                    input: {age: 50}
                ) {
                    name
                    age
                }
            }"#,
        )
        .expect("update by filter");
    let updated = update["update_FilterMutationUser"]
        .as_array()
        .expect("update result not array");
    assert_eq!(updated.len(), 1, "expected updated doc to be returned");
    assert_eq!(updated[0]["name"], "Alice");
    assert_eq!(updated[0]["age"], 50);

    let no_longer_matching = client
        .query("query { FilterMutationUser(filter: {age: {_lt: 30}}) { name age } }")
        .expect("query age filter after update");
    assert_eq!(
        no_longer_matching["FilterMutationUser"]
            .as_array()
            .expect("query result not array")
            .len(),
        0,
        "updated doc should no longer match the original update filter"
    );

    client
        .query(r#"mutation { add_FilterMutationUser(input: {name: "Bob", age: 25}) { _docID } }"#)
        .expect("create Bob");

    let upsert = client
        .query(
            r#"mutation {
                upsert_FilterMutationUser(
                    filter: {name: {_eq: "Bob"}},
                    add: {name: "Bob", age: 25},
                    update: {name: "Robert", age: 51}
                ) {
                    name
                    age
                }
            }"#,
        )
        .expect("upsert by filter");
    let upserted = upsert["upsert_FilterMutationUser"]
        .as_array()
        .expect("upsert result not array");
    assert_eq!(upserted.len(), 1, "expected upserted doc to be returned");
    assert_eq!(upserted[0]["name"], "Robert");
    assert_eq!(upserted[0]["age"], 51);

    let old_name = client
        .query(r#"query { FilterMutationUser(filter: {name: {_eq: "Bob"}}) { name age } }"#)
        .expect("query old name after upsert");
    assert_eq!(
        old_name["FilterMutationUser"]
            .as_array()
            .expect("old-name query result not array")
            .len(),
        0,
        "upserted doc should no longer match the original upsert filter"
    );
}

#[tokio::test]
async fn rust_filter_mutations_return_post_update_docs() {
    let _root = integration_test::workspace_root();
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    filter_mutations_return_post_update_docs(cluster).await;
}
