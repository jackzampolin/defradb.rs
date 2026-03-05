use integration_test::TestCluster;

const SCHEMA: &str = r#"type Article {
    title: String @fulltext
    body: String @fulltext
    category: String
}"#;

#[tokio::test]
async fn bm25_basic_search() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let client = cluster.client(0);
    client.schema_add(SCHEMA).unwrap();

    client
        .query(r#"mutation { create_Article(input: {title: "Introduction to Rust", body: "Rust is a systems programming language", category: "tech"}) { _docID } }"#)
        .unwrap();
    client
        .query(r#"mutation { create_Article(input: {title: "Cooking with Python", body: "Python is a great scripting language", category: "food"}) { _docID } }"#)
        .unwrap();
    client
        .query(r#"mutation { create_Article(input: {title: "Advanced Rust Patterns", body: "Rust ownership and borrowing patterns", category: "tech"}) { _docID } }"#)
        .unwrap();

    let data = client
        .query(r#"query { Article { title BM25(query: "rust", fields: ["title"]) } }"#)
        .unwrap();

    let articles = data["Article"].as_array().unwrap();
    assert_eq!(articles.len(), 3);

    // Articles mentioning "rust" in their title should have non-zero scores
    let mut rust_articles = Vec::new();
    let mut non_rust_articles = Vec::new();
    for article in articles {
        let score = article["BM25"].as_f64().unwrap_or(0.0);
        let title = article["title"].as_str().unwrap();
        if title.contains("Rust") {
            rust_articles.push((title.to_string(), score));
        } else {
            non_rust_articles.push((title.to_string(), score));
        }
    }

    assert_eq!(rust_articles.len(), 2);
    assert_eq!(non_rust_articles.len(), 1);

    for (title, score) in &rust_articles {
        assert!(
            *score > 0.0,
            "Rust article '{}' should have positive score, got {}",
            title,
            score
        );
    }
    for (title, score) in &non_rust_articles {
        assert_eq!(
            *score, 0.0,
            "Non-rust article '{}' should have zero score, got {}",
            title, score
        );
    }
}

#[tokio::test]
async fn bm25_multi_field_search() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let client = cluster.client(0);
    client.schema_add(SCHEMA).unwrap();

    client
        .query(r#"mutation { create_Article(input: {title: "Hello World", body: "This article discusses databases", category: "tech"}) { _docID } }"#)
        .unwrap();
    client
        .query(r#"mutation { create_Article(input: {title: "Database Design", body: "Principles of good design", category: "tech"}) { _docID } }"#)
        .unwrap();

    // Search across both title and body fields
    let data = client
        .query(r#"query { Article { title body BM25(query: "database", fields: ["title", "body"]) } }"#)
        .unwrap();

    let articles = data["Article"].as_array().unwrap();
    assert_eq!(articles.len(), 2);

    // Both articles mention "database" in either title or body
    for article in articles {
        let score = article["BM25"].as_f64().unwrap_or(0.0);
        assert!(
            score > 0.0,
            "Article should have positive score for 'database' search"
        );
    }
}

#[tokio::test]
async fn bm25_no_results_returns_zero_scores() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let client = cluster.client(0);
    client.schema_add(SCHEMA).unwrap();

    client
        .query(r#"mutation { create_Article(input: {title: "Hello World", body: "A simple greeting", category: "misc"}) { _docID } }"#)
        .unwrap();

    let data = client
        .query(
            r#"query { Article { title BM25(query: "nonexistent_term_xyz", fields: ["title"]) } }"#,
        )
        .unwrap();

    let articles = data["Article"].as_array().unwrap();
    assert_eq!(articles.len(), 1);
    let score = articles[0]["BM25"].as_f64().unwrap_or(0.0);
    assert_eq!(score, 0.0, "Non-matching query should return zero score");
}

#[tokio::test]
async fn bm25_with_alias() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let client = cluster.client(0);
    client.schema_add(SCHEMA).unwrap();

    client
        .query(r#"mutation { create_Article(input: {title: "Rust Programming", body: "Learn Rust", category: "tech"}) { _docID } }"#)
        .unwrap();

    let data = client
        .query(r#"query { Article { title relevance: BM25(query: "rust", fields: ["title"]) } }"#)
        .unwrap();

    let articles = data["Article"].as_array().unwrap();
    assert_eq!(articles.len(), 1);
    // Score should be under the alias name
    let score = articles[0]["relevance"].as_f64().unwrap_or(0.0);
    assert!(score > 0.0, "Aliased BM25 score should be positive");
}
