use integration_test::TestCluster;

const RELATION_SCHEMA: &str = r#"
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

#[tokio::test]
async fn bm25_relation_path_scores_parent_context() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let client = cluster.client(0);
    client.schema_add(RELATION_SCHEMA).unwrap();

    client
        .query(
            r#"mutation { add_File(input: {name: "auth.rs", path: "src/auth.rs", content: "auth middleware hooks"}) { _docID } }"#,
        )
        .unwrap();
    client
        .query(
            r#"mutation { add_File(input: {name: "utils.rs", path: "src/utils.rs", content: "shared helpers"}) { _docID } }"#,
        )
        .unwrap();

    let files = client
        .query(r#"query { File(order: {name: ASC}) { _docID name } }"#)
        .unwrap();
    let file_rows = files["File"].as_array().unwrap();
    let auth_file_id = file_rows[0]["_docID"].as_str().unwrap();
    let utils_file_id = file_rows[1]["_docID"].as_str().unwrap();

    client
        .query(&format!(
            r#"mutation {{ add_Function(input: {{name: "handle_request", qualifiedName: "auth::handle_request", startLine: 10, content: "handles inbound requests", _fileID: "{auth_file_id}"}}) {{ _docID }} }}"#,
        ))
        .unwrap();
    client
        .query(&format!(
            r#"mutation {{ add_Function(input: {{name: "handle_request", qualifiedName: "utils::handle_request", startLine: 20, content: "handles inbound requests", _fileID: "{utils_file_id}"}}) {{ _docID }} }}"#,
        ))
        .unwrap();

    let data = client
        .query(
            r#"query {
                Function {
                    _docID
                    name
                    qualifiedName
                    startLine
                    file { name path }
                    score: BM25(query: "auth", fields: ["name", "content", "file.content"])
                }
            }"#,
        )
        .unwrap();

    let functions = data["Function"].as_array().unwrap();
    assert_eq!(functions.len(), 2);

    let auth_fn = functions
        .iter()
        .find(|function| function["file"]["name"].as_str() == Some("auth.rs"))
        .unwrap();
    let utils_fn = functions
        .iter()
        .find(|function| function["file"]["name"].as_str() == Some("utils.rs"))
        .unwrap();

    assert!(
        auth_fn["score"].as_f64().unwrap_or(0.0) > 0.0,
        "Function in auth.rs should get relation-path BM25 score"
    );
    assert_eq!(
        utils_fn["score"].as_f64().unwrap_or(0.0),
        0.0,
        "Function in utils.rs should not score for auth-related file context"
    );
}

#[tokio::test]
async fn bm25_relation_path_scores_reverse_one_to_many_and_orders_by_alias() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let client = cluster.client(0);
    client.schema_add(RELATION_SCHEMA).unwrap();

    client
        .query(
            r#"mutation { add_File(input: {name: "auth.rs", path: "src/auth.rs", content: "auth middleware hooks"}) { _docID } }"#,
        )
        .unwrap();
    client
        .query(
            r#"mutation { add_File(input: {name: "utils.rs", path: "src/utils.rs", content: "shared helpers"}) { _docID } }"#,
        )
        .unwrap();

    let files = client
        .query(r#"query { File(order: {name: ASC}) { _docID name } }"#)
        .unwrap();
    let file_rows = files["File"].as_array().unwrap();
    let auth_file_id = file_rows[0]["_docID"].as_str().unwrap();
    let utils_file_id = file_rows[1]["_docID"].as_str().unwrap();

    client
        .query(&format!(
            r#"mutation {{ add_Function(input: {{name: "parse token", qualifiedName: "auth::parse_token", startLine: 15, content: "parse token auth flow", _fileID: "{auth_file_id}"}}) {{ _docID }} }}"#,
        ))
        .unwrap();
    client
        .query(&format!(
            r#"mutation {{ add_Function(input: {{name: "format_output", qualifiedName: "utils::format_output", startLine: 30, content: "format shared output", _fileID: "{utils_file_id}"}}) {{ _docID }} }}"#,
        ))
        .unwrap();

    let data = client
        .query(
            r#"query {
                File(order: {_alias: {score: DESC}}) {
                    _docID
                    name
                    path
                    score: BM25(query: "parse token", fields: ["name", "functions.name", "functions.content"])
                }
            }"#,
        )
        .unwrap();

    let files = data["File"].as_array().unwrap();
    assert_eq!(files.len(), 2);

    assert_eq!(
        files[0]["name"].as_str(),
        Some("auth.rs"),
        "Alias ordering should rank the file containing the matching function text first"
    );
    assert!(
        files[0]["score"].as_f64().unwrap_or(0.0) > 0.0,
        "auth.rs should receive a positive reverse relation BM25 score"
    );
    assert_eq!(
        files[1]["name"].as_str(),
        Some("utils.rs"),
        "Non-matching file should sort after the matching file"
    );
    assert_eq!(
        files[1]["score"].as_f64().unwrap_or(0.0),
        0.0,
        "utils.rs should not receive a score for parse token"
    );
}

#[tokio::test]
async fn bm25_nested_child_selection_scores_one_to_many_children() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let client = cluster.client(0);
    client.schema_add(RELATION_SCHEMA).unwrap();

    client
        .query(
            r#"mutation { add_File(input: {name: "auth.rs", path: "src/auth.rs", content: "auth middleware hooks"}) { _docID } }"#,
        )
        .unwrap();
    client
        .query(
            r#"mutation { add_File(input: {name: "utils.rs", path: "src/utils.rs", content: "shared helpers"}) { _docID } }"#,
        )
        .unwrap();

    let files = client
        .query(r#"query { File(order: {name: ASC}) { _docID name } }"#)
        .unwrap();
    let file_rows = files["File"].as_array().unwrap();
    let auth_file_id = file_rows[0]["_docID"].as_str().unwrap();
    let utils_file_id = file_rows[1]["_docID"].as_str().unwrap();

    client
        .query(&format!(
            r#"mutation {{ add_Function(input: {{name: "handle_request", qualifiedName: "auth::handle_request", startLine: 10, content: "handles inbound requests", _fileID: "{auth_file_id}"}}) {{ _docID }} }}"#,
        ))
        .unwrap();
    client
        .query(&format!(
            r#"mutation {{ add_Function(input: {{name: "handle_request", qualifiedName: "utils::handle_request", startLine: 20, content: "handles inbound requests", _fileID: "{utils_file_id}"}}) {{ _docID }} }}"#,
        ))
        .unwrap();

    let data = client
        .query(
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
        .unwrap();

    let files = data["File"].as_array().unwrap();
    assert_eq!(files.len(), 2);
    assert_eq!(files[0]["name"].as_str(), Some("auth.rs"));
    assert_eq!(files[1]["name"].as_str(), Some("utils.rs"));

    let auth_fn = files[0]["functions"].as_array().unwrap().first().unwrap();
    let utils_fn = files[1]["functions"].as_array().unwrap().first().unwrap();

    assert!(
        auth_fn["score"].as_f64().unwrap_or(0.0) > 0.0,
        "Nested Function selection under auth.rs should receive a BM25 score"
    );
    assert_eq!(
        utils_fn["score"].as_f64().unwrap_or(0.0),
        0.0,
        "Nested Function selection under utils.rs should not score for auth"
    );
}

#[tokio::test]
async fn bm25_nested_child_selection_scores_one_to_one_children() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let client = cluster.client(0);
    client.schema_add(RELATION_SCHEMA).unwrap();

    client
        .query(
            r#"mutation { add_File(input: {name: "auth.rs", path: "src/auth.rs", content: "auth middleware hooks"}) { _docID } }"#,
        )
        .unwrap();
    client
        .query(
            r#"mutation { add_File(input: {name: "utils.rs", path: "src/utils.rs", content: "shared helpers"}) { _docID } }"#,
        )
        .unwrap();

    let files = client
        .query(r#"query { File(order: {name: ASC}) { _docID name } }"#)
        .unwrap();
    let file_rows = files["File"].as_array().unwrap();
    let auth_file_id = file_rows[0]["_docID"].as_str().unwrap();
    let utils_file_id = file_rows[1]["_docID"].as_str().unwrap();

    client
        .query(&format!(
            r#"mutation {{ add_Function(input: {{name: "parse token", qualifiedName: "auth::parse_token", startLine: 15, content: "parse token auth flow", _fileID: "{auth_file_id}"}}) {{ _docID }} }}"#,
        ))
        .unwrap();
    client
        .query(&format!(
            r#"mutation {{ add_Function(input: {{name: "format_output", qualifiedName: "utils::format_output", startLine: 30, content: "format shared output", _fileID: "{utils_file_id}"}}) {{ _docID }} }}"#,
        ))
        .unwrap();

    let data = client
        .query(
            r#"query {
                Function(order: {qualifiedName: ASC}) {
                    qualifiedName
                    file {
                        name
                        score: BM25(query: "parse token", fields: ["name", "functions.name", "functions.content"])
                    }
                }
            }"#,
        )
        .unwrap();

    let functions = data["Function"].as_array().unwrap();
    assert_eq!(functions.len(), 2);
    assert_eq!(
        functions[0]["qualifiedName"].as_str(),
        Some("auth::parse_token")
    );
    assert_eq!(
        functions[1]["qualifiedName"].as_str(),
        Some("utils::format_output")
    );

    assert!(
        functions[0]["file"]["score"].as_f64().unwrap_or(0.0) > 0.0,
        "Nested File selection under auth::parse_token should receive a BM25 score"
    );
    assert_eq!(
        functions[1]["file"]["score"].as_f64().unwrap_or(0.0),
        0.0,
        "Nested File selection under utils::format_output should not score for parse token"
    );
}

#[tokio::test]
async fn bm25_nested_child_selection_filters_and_orders_by_alias() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let client = cluster.client(0);
    client.schema_add(RELATION_SCHEMA).unwrap();

    client
        .query(
            r#"mutation { add_File(input: {name: "auth.rs", path: "src/auth.rs", content: "auth middleware hooks"}) { _docID } }"#,
        )
        .unwrap();
    client
        .query(
            r#"mutation { add_File(input: {name: "utils.rs", path: "src/utils.rs", content: "shared helpers"}) { _docID } }"#,
        )
        .unwrap();

    let files = client
        .query(r#"query { File(order: {name: ASC}) { _docID name } }"#)
        .unwrap();
    let file_rows = files["File"].as_array().unwrap();
    let auth_file_id = file_rows[0]["_docID"].as_str().unwrap();
    let utils_file_id = file_rows[1]["_docID"].as_str().unwrap();

    client
        .query(&format!(
            r#"mutation {{ add_Function(input: {{name: "auth_guard", qualifiedName: "auth::auth_guard", startLine: 5, content: "auth guard middleware", _fileID: "{auth_file_id}"}}) {{ _docID }} }}"#,
        ))
        .unwrap();
    client
        .query(&format!(
            r#"mutation {{ add_Function(input: {{name: "handle_request", qualifiedName: "auth::handle_request", startLine: 10, content: "handles inbound requests", _fileID: "{auth_file_id}"}}) {{ _docID }} }}"#,
        ))
        .unwrap();
    client
        .query(&format!(
            r#"mutation {{ add_Function(input: {{name: "handle_request", qualifiedName: "utils::handle_request", startLine: 20, content: "handles inbound requests", _fileID: "{utils_file_id}"}}) {{ _docID }} }}"#,
        ))
        .unwrap();

    let data = client
        .query(
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
        .unwrap();

    let files = data["File"].as_array().unwrap();
    assert_eq!(files.len(), 2);
    assert_eq!(files[0]["name"].as_str(), Some("auth.rs"));
    assert_eq!(files[1]["name"].as_str(), Some("utils.rs"));

    let auth_functions = files[0]["functions"].as_array().unwrap();
    let utils_functions = files[1]["functions"].as_array().unwrap();

    assert_eq!(
        auth_functions.len(),
        2,
        "Both auth.rs functions should remain after filtering by positive nested BM25 score"
    );
    assert!(
        auth_functions
            .iter()
            .any(|function| function["qualifiedName"].as_str() == Some("auth::auth_guard")),
        "Nested alias filtering should retain auth::auth_guard"
    );
    assert!(
        auth_functions
            .iter()
            .any(|function| function["qualifiedName"].as_str() == Some("auth::handle_request")),
        "Nested alias filtering should retain auth::handle_request"
    );
    assert!(
        auth_functions[0]["score"].as_f64().unwrap_or(0.0)
            > auth_functions[1]["score"].as_f64().unwrap_or(0.0),
        "Nested alias ordering should sort scores descending"
    );
    assert!(
        auth_functions
            .iter()
            .all(|function| function["score"].as_f64().unwrap_or(0.0) > 0.0),
        "Nested alias filtering should keep only positive-score auth.rs functions"
    );
    assert!(
        utils_functions.is_empty(),
        "Nested alias filtering should remove utils.rs children with zero BM25 score"
    );
}
