use integration_test::TestCluster;

const SCHEMA: &str = r#"type Article {
    title: String @fulltext
    body: String @fulltext
    category: String
}"#;

#[tokio::test]
async fn bm25_large_document() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let client = cluster.client(0);
    client.schema_add(SCHEMA).unwrap();

    // Generate a 10KB+ body with "needle" buried in the middle
    let filler = "lorem ipsum dolor sit amet consectetur adipiscing elit ";
    let mut large_body = String::with_capacity(12_000);
    while large_body.len() < 5_000 {
        large_body.push_str(filler);
    }
    large_body.push_str("the needle is hidden here ");
    while large_body.len() < 11_000 {
        large_body.push_str(filler);
    }

    // Escape for JSON embedding
    let escaped_body = large_body.replace('"', r#"\""#);
    client
        .query(&format!(
            r#"mutation {{ add_Article(input: {{title: "Large Document", body: "{}", category: "test"}}) {{ _docID }} }}"#,
            escaped_body
        ))
        .unwrap();

    let data = client
        .query(r#"query { Article { title BM25(query: "needle", fields: ["body"]) } }"#)
        .unwrap();

    let articles = data["Article"].as_array().unwrap();
    assert_eq!(articles.len(), 1);
    assert!(
        articles[0]["BM25"].as_f64().unwrap_or(0.0) > 0.0,
        "Should find 'needle' in large document"
    );
}

#[tokio::test]
async fn bm25_unicode_and_special_characters() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let client = cluster.client(0);
    client.schema_add(SCHEMA).unwrap();

    client
        .query(r#"mutation { add_Article(input: {title: "Caf\u00e9 Guide", body: "Visit the caf\u00e9 for cr\u00e8me br\u00fbl\u00e9e", category: "food"}) { _docID } }"#)
        .unwrap();
    client
        .query(r#"mutation { add_Article(input: {title: "Normal Article", body: "Just a normal article here", category: "misc"}) { _docID } }"#)
        .unwrap();

    // Search should not crash on unicode content
    let data = client
        .query(r#"query { Article { title BM25(query: "normal", fields: ["title", "body"]) } }"#)
        .unwrap();

    let articles = data["Article"].as_array().unwrap();
    assert_eq!(articles.len(), 2);

    let normal_score = articles
        .iter()
        .find(|a| a["title"].as_str().unwrap().contains("Normal"))
        .unwrap()["BM25"]
        .as_f64()
        .unwrap_or(0.0);
    assert!(normal_score > 0.0, "Should find 'normal' in Normal Article");
}

#[tokio::test]
async fn bm25_empty_field_does_not_crash() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let client = cluster.client(0);
    client.schema_add(SCHEMA).unwrap();

    // Doc with empty body
    client
        .query(r#"mutation { add_Article(input: {title: "Empty Body", body: "", category: "test"}) { _docID } }"#)
        .unwrap();
    // Doc with content
    client
        .query(r#"mutation { add_Article(input: {title: "Has Content", body: "searchable content here", category: "test"}) { _docID } }"#)
        .unwrap();

    let data = client
        .query(
            r#"query { Article { title BM25(query: "searchable", fields: ["title", "body"]) } }"#,
        )
        .unwrap();

    let articles = data["Article"].as_array().unwrap();
    assert_eq!(articles.len(), 2);

    let empty_score = articles
        .iter()
        .find(|a| a["title"].as_str().unwrap() == "Empty Body")
        .unwrap()["BM25"]
        .as_f64()
        .unwrap_or(0.0);
    let content_score = articles
        .iter()
        .find(|a| a["title"].as_str().unwrap() == "Has Content")
        .unwrap()["BM25"]
        .as_f64()
        .unwrap_or(0.0);

    assert_eq!(empty_score, 0.0, "Empty body should score 0");
    assert!(
        content_score > 0.0,
        "Doc with content should score positive"
    );
}
