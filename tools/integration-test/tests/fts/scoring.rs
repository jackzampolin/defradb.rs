use integration_test::TestCluster;

const SCHEMA: &str = r#"type Article {
    title: String @fulltext
    body: String @fulltext
    category: String
}"#;

#[tokio::test]
async fn bm25_score_ordering() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let client = cluster.client(0);
    client.schema_add(SCHEMA).unwrap();

    // Doc with "rust" appearing once
    client
        .query(r#"mutation { add_Article(input: {title: "Learning Rust", body: "A guide to getting started", category: "tech"}) { _docID } }"#)
        .unwrap();
    // Doc with "rust" appearing multiple times
    client
        .query(r#"mutation { add_Article(input: {title: "Rust Rust Rust", body: "Rust everywhere in this rust article about rust", category: "tech"}) { _docID } }"#)
        .unwrap();
    // Doc without "rust"
    client
        .query(r#"mutation { add_Article(input: {title: "Python Guide", body: "Learn Python scripting", category: "tech"}) { _docID } }"#)
        .unwrap();

    let data = client
        .query(r#"query { Article { title BM25(query: "rust", fields: ["title", "body"]) } }"#)
        .unwrap();

    let articles = data["Article"].as_array().unwrap();
    let mut scored: Vec<(String, f64)> = articles
        .iter()
        .map(|a| {
            (
                a["title"].as_str().unwrap().to_string(),
                a["BM25"].as_f64().unwrap_or(0.0),
            )
        })
        .collect();
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    // "Rust Rust Rust" should rank highest (most occurrences)
    assert_eq!(scored[0].0, "Rust Rust Rust");
    assert!(
        scored[0].1 > scored[1].1,
        "Higher TF should produce higher score"
    );
    // Python guide should score 0
    assert_eq!(scored[2].1, 0.0);
}

#[tokio::test]
async fn bm25_idf_rare_term_scores_higher() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let client = cluster.client(0);
    client.schema_add(SCHEMA).unwrap();

    // Create several docs mentioning "common" but only one mentioning "rare"
    for i in 0..5 {
        client
            .query(&format!(
                r#"mutation {{ add_Article(input: {{title: "Common topic {}", body: "This is about common things", category: "misc"}}) {{ _docID }} }}"#,
                i
            ))
            .unwrap();
    }
    // One doc with both "common" and "rare"
    client
        .query(r#"mutation { add_Article(input: {title: "Rare and common", body: "This article has both rare and common terms", category: "misc"}) { _docID } }"#)
        .unwrap();

    // Search for "rare" — IDF should be high since it appears in few docs
    let data_rare = client
        .query(r#"query { Article { title BM25(query: "rare", fields: ["title", "body"]) } }"#)
        .unwrap();
    let rare_scores: Vec<f64> = data_rare["Article"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["BM25"].as_f64().unwrap_or(0.0))
        .filter(|s| *s > 0.0)
        .collect();

    // Search for "common" — IDF should be low since it appears in all docs
    let data_common = client
        .query(r#"query { Article { title BM25(query: "common", fields: ["title", "body"]) } }"#)
        .unwrap();
    let common_scores: Vec<f64> = data_common["Article"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["BM25"].as_f64().unwrap_or(0.0))
        .filter(|s| *s > 0.0)
        .collect();

    assert!(
        !rare_scores.is_empty(),
        "rare term should match at least one doc"
    );
    assert!(!common_scores.is_empty(), "common term should match docs");

    // The max score for the rare term should be higher than the max for common
    let max_rare = rare_scores.iter().cloned().fold(0.0f64, f64::max);
    let max_common = common_scores.iter().cloned().fold(0.0f64, f64::max);
    assert!(
        max_rare > max_common,
        "Rare term (IDF higher) should score higher than common term. rare={}, common={}",
        max_rare,
        max_common
    );
}

#[tokio::test]
async fn bm25_multi_field_combination() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let client = cluster.client(0);
    client.schema_add(SCHEMA).unwrap();

    // Doc with "database" in title only
    client
        .query(r#"mutation { add_Article(input: {title: "Database Systems", body: "An introduction to storage engines", category: "tech"}) { _docID } }"#)
        .unwrap();
    // Doc with "database" in both title and body
    client
        .query(r#"mutation { add_Article(input: {title: "Database Design", body: "How to design a good database schema", category: "tech"}) { _docID } }"#)
        .unwrap();
    // Doc with "database" in body only
    client
        .query(r#"mutation { add_Article(input: {title: "Storage Guide", body: "This guide covers database internals", category: "tech"}) { _docID } }"#)
        .unwrap();

    let data = client
        .query(r#"query { Article { title BM25(query: "database", fields: ["title", "body"]) } }"#)
        .unwrap();

    let articles = data["Article"].as_array().unwrap();
    let mut scored: Vec<(String, f64)> = articles
        .iter()
        .map(|a| {
            (
                a["title"].as_str().unwrap().to_string(),
                a["BM25"].as_f64().unwrap_or(0.0),
            )
        })
        .collect();
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    // "Database Design" has "database" in both title and body, should rank first
    assert_eq!(
        scored[0].0, "Database Design",
        "Doc matching in both fields should rank highest"
    );
    // All three should have positive scores
    for (title, score) in &scored {
        assert!(score > &0.0, "'{}' should have positive score", title);
    }
}
