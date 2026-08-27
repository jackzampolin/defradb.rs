//! Shared Book/Author fixture mirroring the Go `one_to_many` package
//! (`tests/integration/query/one_to_many/utils.go`).

use integration_test::DefraClient;

pub fn add_schema(node: &DefraClient) {
    node.schema_add(
        r#"
        type Book {
            name: String
            rating: Float
            author: Author
        }

        type Author {
            name: String
            age: Int
            verified: Boolean
            published: [Book]
        }
        "#,
    )
    .expect("add schema");
}

pub fn add_author(node: &DefraClient, name: &str, age: i64, verified: bool) -> String {
    let result = node
        .query(&format!(
            r#"mutation {{ add_Author(input: {{name: "{name}", age: {age}, verified: {verified}}}) {{ _docID }} }}"#
        ))
        .unwrap_or_else(|e| panic!("add author {name}: {e}"));
    result["add_Author"][0]["_docID"]
        .as_str()
        .expect("author _docID")
        .to_string()
}

pub fn add_book(node: &DefraClient, name: &str, rating: f64, author: &str) {
    node.query(&format!(
        r#"mutation {{ add_Book(input: {{name: "{name}", rating: {rating}, author: "{author}"}}) {{ _docID }} }}"#
    ))
    .unwrap_or_else(|e| panic!("add book {name}: {e}"));
}
