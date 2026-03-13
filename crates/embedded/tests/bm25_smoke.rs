use anyhow::{bail, Context, Result};
use embedded::NodeBuilder;

const ARTICLE_SDL: &str = r#"
type Article {
    title: String @fulltext
    body: String @fulltext
}
"#;

const RELATION_SDL: &str = r#"
type File {
    name: String @fulltext
    path: String @fulltext
    content: String @fulltext
    functions: [Function]
}

type Function {
    name: String @fulltext
    content: String @fulltext
    qualifiedName: String
    startLine: Int
    file: File @primary
}
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn embedded_node_executes_bm25_queries() -> Result<()> {
    let node = NodeBuilder::default().build().await?;

    node.add_schema(ARTICLE_SDL).await?;

    let auth_insert = node
        .execute(
            r#"mutation {
                add_Article(input: {
                    title: "Auth middleware"
                    body: "Authenticate requests through shared middleware"
                }) {
                    _docID
                    title
                }
            }"#,
        )
        .await;
    ensure_success(&auth_insert, "add_Article auth")?;

    let util_insert = node
        .execute(
            r#"mutation {
                add_Article(input: {
                    title: "Utility formatters"
                    body: "Pretty print shared output for the CLI"
                }) {
                    _docID
                    title
                }
            }"#,
        )
        .await;
    ensure_success(&util_insert, "add_Article utility")?;

    let response = node
        .execute(
            r#"query {
                Article(order: {_alias: {score: DESC}}) {
                    title
                    score: BM25(query: "auth middleware", fields: ["title", "body"])
                }
            }"#,
        )
        .await;
    ensure_success(&response, "Article BM25 query")?;

    let articles = response
        .data
        .as_ref()
        .and_then(|data| data.get("Article"))
        .and_then(|articles| articles.as_array())
        .context("Article query response missing data")?;

    assert_eq!(articles.len(), 2);
    assert_eq!(articles[0]["title"].as_str(), Some("Auth middleware"));

    let top_score = articles[0]["score"].as_f64().unwrap_or(0.0);
    let second_score = articles[1]["score"].as_f64().unwrap_or(0.0);
    assert!(
        top_score > 0.0,
        "matching article should receive a positive BM25 score"
    );
    assert!(
        top_score > second_score,
        "BM25 ordering should rank the matching article ahead of unrelated content"
    );

    node.database.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn embedded_node_executes_nested_bm25_queries() -> Result<()> {
    let node = NodeBuilder::default().build().await?;

    node.add_schema(RELATION_SDL).await?;

    let auth_file = node
        .execute(
            r#"mutation {
                add_File(input: {
                    name: "auth.rs"
                    path: "src/auth.rs"
                    content: "auth middleware hooks"
                }) {
                    _docID
                }
            }"#,
        )
        .await;
    ensure_success(&auth_file, "add_File auth")?;

    let utils_file = node
        .execute(
            r#"mutation {
                add_File(input: {
                    name: "utils.rs"
                    path: "src/utils.rs"
                    content: "shared helpers"
                }) {
                    _docID
                }
            }"#,
        )
        .await;
    ensure_success(&utils_file, "add_File utils")?;

    let files = node
        .execute(
            r#"query {
                File(order: {name: ASC}) {
                    _docID
                    name
                }
            }"#,
        )
        .await;
    ensure_success(&files, "File lookup")?;

    let file_rows = files
        .data
        .as_ref()
        .and_then(|data| data.get("File"))
        .and_then(|rows| rows.as_array())
        .context("File lookup response missing data")?;
    let auth_file_id = file_rows
        .iter()
        .find(|row| row["name"].as_str() == Some("auth.rs"))
        .and_then(|row| row.get("_docID"))
        .and_then(|id| id.as_str())
        .context("auth file doc id missing")?;
    let utils_file_id = file_rows
        .iter()
        .find(|row| row["name"].as_str() == Some("utils.rs"))
        .and_then(|row| row.get("_docID"))
        .and_then(|id| id.as_str())
        .context("utils file doc id missing")?;

    let auth_fn = node
        .execute(&format!(
            r#"mutation {{
                add_Function(input: {{
                    name: "handle_request"
                    qualifiedName: "auth::handle_request"
                    startLine: 10
                    content: "handles inbound requests"
                    _fileID: "{auth_file_id}"
                }}) {{
                    _docID
                }}
            }}"#,
        ))
        .await;
    ensure_success(&auth_fn, "add_Function auth")?;

    let utils_fn = node
        .execute(&format!(
            r#"mutation {{
                add_Function(input: {{
                    name: "handle_request"
                    qualifiedName: "utils::handle_request"
                    startLine: 20
                    content: "handles inbound requests"
                    _fileID: "{utils_file_id}"
                }}) {{
                    _docID
                }}
            }}"#,
        ))
        .await;
    ensure_success(&utils_fn, "add_Function utils")?;

    let response = node
        .execute(
            r#"query {
                File(order: {name: ASC}) {
                    name
                    functions(order: {qualifiedName: ASC}) {
                        qualifiedName
                        score: BM25(query: "auth", fields: ["name", "content", "file.content"])
                    }
                }
            }"#,
        )
        .await;
    ensure_success(&response, "nested BM25 query")?;

    let files = response
        .data
        .as_ref()
        .and_then(|data| data.get("File"))
        .and_then(|files| files.as_array())
        .context("File query response missing data")?;

    assert_eq!(files.len(), 2);
    assert_eq!(files[0]["name"].as_str(), Some("auth.rs"));
    assert_eq!(files[1]["name"].as_str(), Some("utils.rs"));

    let auth_score = files[0]["functions"]
        .as_array()
        .and_then(|items| items.first())
        .and_then(|item| item.get("score"))
        .and_then(|score| score.as_f64())
        .unwrap_or(0.0);
    let utils_score = files[1]["functions"]
        .as_array()
        .and_then(|items| items.first())
        .and_then(|item| item.get("score"))
        .and_then(|score| score.as_f64())
        .unwrap_or(0.0);

    assert!(
        auth_score > 0.0,
        "matching nested function should receive a positive BM25 score"
    );
    assert_eq!(
        utils_score, 0.0,
        "non-matching nested function should not receive a BM25 score"
    );

    node.database.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn embedded_node_executes_nested_bm25_alias_filter_and_order_queries() -> Result<()> {
    let node = NodeBuilder::default().build().await?;

    node.add_schema(RELATION_SDL).await?;

    let auth_file = node
        .execute(
            r#"mutation {
                add_File(input: {
                    name: "auth.rs"
                    path: "src/auth.rs"
                    content: "auth middleware hooks"
                }) {
                    _docID
                }
            }"#,
        )
        .await;
    ensure_success(&auth_file, "add_File auth")?;

    let utils_file = node
        .execute(
            r#"mutation {
                add_File(input: {
                    name: "utils.rs"
                    path: "src/utils.rs"
                    content: "shared helpers"
                }) {
                    _docID
                }
            }"#,
        )
        .await;
    ensure_success(&utils_file, "add_File utils")?;

    let files = node
        .execute(
            r#"query {
                File(order: {name: ASC}) {
                    _docID
                    name
                }
            }"#,
        )
        .await;
    ensure_success(&files, "File lookup")?;

    let file_rows = files
        .data
        .as_ref()
        .and_then(|data| data.get("File"))
        .and_then(|rows| rows.as_array())
        .context("File lookup response missing data")?;
    let auth_file_id = file_rows
        .iter()
        .find(|row| row["name"].as_str() == Some("auth.rs"))
        .and_then(|row| row.get("_docID"))
        .and_then(|id| id.as_str())
        .context("auth file doc id missing")?;
    let utils_file_id = file_rows
        .iter()
        .find(|row| row["name"].as_str() == Some("utils.rs"))
        .and_then(|row| row.get("_docID"))
        .and_then(|id| id.as_str())
        .context("utils file doc id missing")?;

    let auth_guard = node
        .execute(&format!(
            r#"mutation {{
                add_Function(input: {{
                    name: "auth_guard"
                    qualifiedName: "auth::auth_guard"
                    startLine: 5
                    content: "auth guard middleware"
                    _fileID: "{auth_file_id}"
                }}) {{
                    _docID
                }}
            }}"#,
        ))
        .await;
    ensure_success(&auth_guard, "add_Function auth_guard")?;

    let auth_handler = node
        .execute(&format!(
            r#"mutation {{
                add_Function(input: {{
                    name: "handle_request"
                    qualifiedName: "auth::handle_request"
                    startLine: 10
                    content: "handles inbound requests"
                    _fileID: "{auth_file_id}"
                }}) {{
                    _docID
                }}
            }}"#,
        ))
        .await;
    ensure_success(&auth_handler, "add_Function auth handler")?;

    let utils_handler = node
        .execute(&format!(
            r#"mutation {{
                add_Function(input: {{
                    name: "handle_request"
                    qualifiedName: "utils::handle_request"
                    startLine: 20
                    content: "handles inbound requests"
                    _fileID: "{utils_file_id}"
                }}) {{
                    _docID
                }}
            }}"#,
        ))
        .await;
    ensure_success(&utils_handler, "add_Function utils handler")?;

    let response = node
        .execute(
            r#"query {
                File(order: {name: ASC}) {
                    name
                    functions(
                        filter: {_alias: {score: {_gt: 0}}}
                        order: {_alias: {score: DESC}}
                    ) {
                        qualifiedName
                        score: BM25(query: "auth guard", fields: ["name", "content", "file.content"])
                    }
                }
            }"#,
        )
        .await;
    ensure_success(&response, "nested BM25 alias filter/order query")?;

    let files = response
        .data
        .as_ref()
        .and_then(|data| data.get("File"))
        .and_then(|files| files.as_array())
        .context("File query response missing data")?;

    assert_eq!(files.len(), 2);
    assert_eq!(files[0]["name"].as_str(), Some("auth.rs"));
    assert_eq!(files[1]["name"].as_str(), Some("utils.rs"));

    let auth_functions = files[0]["functions"]
        .as_array()
        .context("auth.rs functions missing")?;
    let utils_functions = files[1]["functions"]
        .as_array()
        .context("utils.rs functions missing")?;

    assert_eq!(auth_functions.len(), 2);
    assert!(auth_functions
        .iter()
        .any(|function| function["qualifiedName"].as_str() == Some("auth::auth_guard")));
    assert!(auth_functions
        .iter()
        .any(|function| function["qualifiedName"].as_str() == Some("auth::handle_request")));
    assert!(
        auth_functions[0]["score"].as_f64().unwrap_or(0.0)
            > auth_functions[1]["score"].as_f64().unwrap_or(0.0)
    );
    assert!(auth_functions
        .iter()
        .all(|function| function["score"].as_f64().unwrap_or(0.0) > 0.0));
    assert!(utils_functions.is_empty());

    node.database.close().await?;
    Ok(())
}

fn ensure_success(response: &query::QueryResponse, operation: &str) -> Result<()> {
    if response.has_errors() {
        bail!("{operation} returned errors: {:?}", response.errors);
    }
    Ok(())
}
