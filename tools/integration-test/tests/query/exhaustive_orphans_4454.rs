use integration_test::TestCluster;

async fn exhaustive_relation_order_includes_orphans_test(cluster: TestCluster) {
    let node = cluster.client(0);

    node.schema_add(
        r#"
        type Author {
            name: String
            published: [Book]
        }

        type Book {
            title: String
            author: Author
            publisher: Publisher
        }

        type Publisher {
            name: String
            establishedYear: Int @index
            book: Book @primary
        }
        "#,
    )
    .expect("add schema");

    let author = node
        .query(r#"mutation { add_Author(input: {name: "John"}) { _docID } }"#)
        .expect("add author");
    let author_id = author["add_Author"][0]["_docID"].as_str().unwrap();

    let add_book = |title: &str| {
        let result = node
            .query(&format!(
                r#"mutation {{ add_Book(input: {{title: "{title}", author: "{author_id}"}}) {{ _docID }} }}"#
            ))
            .expect("add book");
        result["add_Book"][0]["_docID"]
            .as_str()
            .unwrap()
            .to_string()
    };

    let book_2020 = add_book("Book2020");
    let book_2010 = add_book("Book2010");
    let book_2000 = add_book("Book2000");
    add_book("OrphanBook");

    let add_publisher = |name: &str, year: i64, book_id: &str| {
        node.query(&format!(
            r#"mutation {{ add_Publisher(input: {{name: "{name}", establishedYear: {year}, book: "{book_id}"}}) {{ _docID }} }}"#
        ))
        .expect("add publisher");
    };

    add_publisher("Publisher2020", 2020, &book_2020);
    add_publisher("Publisher2010", 2010, &book_2010);
    add_publisher("Publisher2000", 2000, &book_2000);

    let default_result = node
        .query(
            r#"
            query {
                Book(order: {publisher: {establishedYear: ASC}}) {
                    title
                }
            }
            "#,
        )
        .expect("query non-exhaustive books");
    assert_eq!(
        default_result["Book"],
        serde_json::json!([
            {"title": "Book2000"},
            {"title": "Book2010"},
            {"title": "Book2020"}
        ])
    );

    let exhaustive_result = node
        .query(
            r#"
            query @exhaustive {
                Book(order: {publisher: {establishedYear: ASC}}) {
                    title
                }
            }
            "#,
        )
        .expect("query exhaustive books");
    assert_eq!(
        exhaustive_result["Book"],
        serde_json::json!([
            {"title": "OrphanBook"},
            {"title": "Book2000"},
            {"title": "Book2010"},
            {"title": "Book2020"}
        ])
    );

    let nested_result = node
        .query(
            r#"
            query @exhaustive {
                Author {
                    name
                    published(order: {publisher: {establishedYear: ASC}}, limit: 2) {
                        title
                    }
                }
            }
            "#,
        )
        .expect("query exhaustive nested books");
    assert_eq!(
        nested_result["Author"],
        serde_json::json!([
            {
                "name": "John",
                "published": [
                    {"title": "OrphanBook"},
                    {"title": "Book2000"}
                ]
            }
        ])
    );
}

#[tokio::test]
async fn rust_exhaustive_relation_order_includes_orphans() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    exhaustive_relation_order_includes_orphans_test(cluster).await;
}
