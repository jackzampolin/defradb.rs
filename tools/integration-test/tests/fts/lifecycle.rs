use integration_test::TestCluster;

const SCHEMA: &str = r#"type Article {
    title: String @fulltext
    body: String @fulltext
    category: String
}"#;

#[tokio::test]
async fn bm25_document_update_reflects_new_content() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let client = cluster.client(0);
    client.schema_add(SCHEMA).unwrap();

    // Create a doc about "rust"
    let data = client
        .query(r#"mutation { add_Article(input: {title: "Learning Rust", body: "Rust is great", category: "tech"}) { _docID } }"#)
        .unwrap();
    let doc_id = data["add_Article"][0]["_docID"]
        .as_str()
        .unwrap()
        .to_string();

    // Verify it scores for "rust"
    let data = client
        .query(r#"query { Article { title BM25(query: "rust", fields: ["title", "body"]) } }"#)
        .unwrap();
    let score_before = data["Article"].as_array().unwrap()[0]["BM25"]
        .as_f64()
        .unwrap_or(0.0);
    assert!(score_before > 0.0, "Should score for 'rust' before update");

    // Update the doc to be about "python" instead
    client
        .query(&format!(
            r#"mutation {{ update_Article(docID: "{}", input: {{title: "Learning Python", body: "Python is great"}}) {{ _docID }} }}"#,
            doc_id
        ))
        .unwrap();

    // Now "rust" should score 0
    let data = client
        .query(r#"query { Article { title BM25(query: "rust", fields: ["title", "body"]) } }"#)
        .unwrap();
    let score_rust_after = data["Article"].as_array().unwrap()[0]["BM25"]
        .as_f64()
        .unwrap_or(0.0);
    assert_eq!(
        score_rust_after, 0.0,
        "Should score 0 for 'rust' after update to python"
    );

    // And "python" should score positive
    let data = client
        .query(r#"query { Article { title BM25(query: "python", fields: ["title", "body"]) } }"#)
        .unwrap();
    let score_python = data["Article"].as_array().unwrap()[0]["BM25"]
        .as_f64()
        .unwrap_or(0.0);
    assert!(score_python > 0.0, "Should score for 'python' after update");
}

#[tokio::test]
async fn bm25_document_deletion_removes_from_results() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let client = cluster.client(0);
    client.schema_add(SCHEMA).unwrap();

    // Create two docs
    let data = client
        .query(r#"mutation { add_Article(input: {title: "Rust Guide", body: "A rust tutorial", category: "tech"}) { _docID } }"#)
        .unwrap();
    let doc_id = data["add_Article"][0]["_docID"]
        .as_str()
        .unwrap()
        .to_string();

    client
        .query(r#"mutation { add_Article(input: {title: "Rust Reference", body: "Rust language reference", category: "tech"}) { _docID } }"#)
        .unwrap();

    // Both should score for "rust"
    let data = client
        .query(r#"query { Article { title BM25(query: "rust", fields: ["title", "body"]) } }"#)
        .unwrap();
    let articles = data["Article"].as_array().unwrap();
    assert_eq!(articles.len(), 2);
    let positive_count = articles
        .iter()
        .filter(|a| a["BM25"].as_f64().unwrap_or(0.0) > 0.0)
        .count();
    assert_eq!(positive_count, 2, "Both docs should score for 'rust'");

    // Delete the first doc
    client
        .query(&format!(
            r#"mutation {{ delete_Article(docID: "{}") {{ _docID }} }}"#,
            doc_id
        ))
        .unwrap();

    // Only one doc should remain
    let data = client
        .query(r#"query { Article { title BM25(query: "rust", fields: ["title", "body"]) } }"#)
        .unwrap();
    let articles = data["Article"].as_array().unwrap();
    assert_eq!(
        articles.len(),
        1,
        "Only one doc should remain after deletion"
    );
    assert!(
        articles[0]["BM25"].as_f64().unwrap_or(0.0) > 0.0,
        "Remaining doc should still score for 'rust'"
    );
}
