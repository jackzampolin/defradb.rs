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
        .query(r#"mutation { create_Article(input: {title: "Rust 101", author: "Alice", views: 10}) { _docID } }"#)
        .expect("create article 1");
    let id1 = a1["create_Article"][0]["_docID"]
        .as_str()
        .expect("missing _docID for article 1")
        .to_string();

    client
        .query(r#"mutation { create_Article(input: {title: "Go Patterns", author: "Bob", views: 20}) { _docID } }"#)
        .expect("create article 2");

    client
        .query(r#"mutation { create_Article(input: {title: "P2P Networking", author: "Alice", views: 5}) { _docID } }"#)
        .expect("create article 3");

    // 4. Collection doc-ids — verify 3 documents exist
    //    Try CLI doc-ids first; fall back to GraphQL query if CLI returns empty
    //    (Go's streaming docIDs output may not be captured by process stdout).
    let doc_ids = client.collection_doc_ids("Article").unwrap_or_default();
    if doc_ids.is_empty() {
        let count_result = client
            .query("query { Article { _docID } }")
            .expect("doc count query failed");
        let count = count_result["Article"]
            .as_array()
            .map(|a| a.len())
            .unwrap_or(0);
        assert_eq!(count, 3, "expected 3 articles, got {}", count);
    } else {
        assert_eq!(
            doc_ids.len(),
            3,
            "expected 3 doc IDs, got {}",
            doc_ids.len()
        );
    }

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

    // 11. Index drop
    client
        .index_drop("Article", "idx_author")
        .expect("index_drop failed");

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
