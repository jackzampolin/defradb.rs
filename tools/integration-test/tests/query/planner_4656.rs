use integration_test::TestCluster;

async fn inverted_join_scalar_and_relation_filters_are_anded_test(cluster: TestCluster) {
    let node = cluster.client(0);

    node.schema_add(
        r#"
        type Author {
            name: String @index
            published: [Book]
        }

        type Book {
            title: String
            rating: Float
            genre: String
            author: Author
        }
        "#,
    )
    .expect("add schema");

    let john = node
        .query(r#"mutation { add_Author(input: {name: "John Grisham"}) { _docID } }"#)
        .expect("add John");
    let john_id = john["add_Author"][0]["_docID"].as_str().unwrap();
    let cornelia = node
        .query(r#"mutation { add_Author(input: {name: "Cornelia Funke"}) { _docID } }"#)
        .expect("add Cornelia");
    let cornelia_id = cornelia["add_Author"][0]["_docID"].as_str().unwrap();

    node.query(&format!(
        r#"mutation {{ add_Book(input: {{title: "Painted House", rating: 4.9, genre: "drama", author: "{}"}}) {{ _docID }} }}"#,
        john_id
    ))
    .expect("add Painted House");
    node.query(&format!(
        r#"mutation {{ add_Book(input: {{title: "A Time to Kill", rating: 4.0, genre: "thriller", author: "{}"}}) {{ _docID }} }}"#,
        john_id
    ))
    .expect("add A Time to Kill");
    node.query(&format!(
        r#"mutation {{ add_Book(input: {{title: "The Firm", rating: 4.5, genre: "thriller", author: "{}"}}) {{ _docID }} }}"#,
        john_id
    ))
    .expect("add The Firm");
    node.query(&format!(
        r#"mutation {{ add_Book(input: {{title: "The Thief Lord", rating: 4.8, genre: "fantasy", author: "{}"}}) {{ _docID }} }}"#,
        cornelia_id
    ))
    .expect("add The Thief Lord");

    let result = node
        .query(
            r#"
            query {
                Book(
                    filter: {
                        genre: {_eq: "thriller"}
                        rating: {_gt: 4.0}
                        author: {name: {_eq: "John Grisham"}}
                    }
                ) {
                    title
                    rating
                    genre
                    author { name }
                }
            }
            "#,
        )
        .expect("query books");

    assert_eq!(
        result["Book"],
        serde_json::json!([
            {
                "title": "The Firm",
                "rating": 4.5,
                "genre": "thriller",
                "author": {"name": "John Grisham"}
            }
        ])
    );
}

#[tokio::test]
async fn rust_inverted_join_scalar_and_relation_filters_are_anded() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    inverted_join_scalar_and_relation_filters_are_anded_test(cluster).await;
}
