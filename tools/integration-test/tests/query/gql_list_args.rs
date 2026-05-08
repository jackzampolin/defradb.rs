use integration_test::{for_each_runtime, TestCluster};
use serde_json::{json, Value};

const SCHEMA: &str = "type User { name: String  age: Int }";

fn extract_doc_id(data: &Value, mutation_name: &str) -> String {
    data[mutation_name]
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|value| value["_docID"].as_str())
        .or_else(|| data[mutation_name]["_docID"].as_str())
        .expect("missing _docID")
        .to_string()
}

fn rows<'a>(data: &'a Value, field: &str) -> &'a [Value] {
    data[field]
        .as_array()
        .unwrap_or_else(|| panic!("{field} array missing from response: {data}"))
        .as_slice()
}

fn commit_cid(data: &Value) -> String {
    rows(data, "_commits")
        .iter()
        .find(|commit| commit["height"].as_u64() == Some(1))
        .or_else(|| rows(data, "_commits").first())
        .and_then(|commit| commit["cid"].as_str())
        .expect("missing commit cid")
        .to_string()
}

fn find_field<'a>(type_value: &'a Value, name: &str) -> &'a Value {
    type_value["fields"]
        .as_array()
        .and_then(|fields| {
            fields
                .iter()
                .find(|field| field["name"].as_str() == Some(name))
        })
        .unwrap_or_else(|| panic!("missing field {name} in {type_value}"))
}

fn find_arg<'a>(field: &'a Value, name: &str) -> &'a Value {
    field["args"]
        .as_array()
        .and_then(|args| args.iter().find(|arg| arg["name"].as_str() == Some(name)))
        .unwrap_or_else(|| panic!("missing arg {name} in {field}"))
}

fn assert_id_list_arg(arg: &Value) {
    let ty = &arg["type"];
    assert_eq!(
        ty["kind"].as_str(),
        Some("LIST"),
        "arg is not a list: {arg}"
    );
    assert_eq!(
        ty["ofType"]["kind"].as_str(),
        Some("NON_NULL"),
        "list item is not non-null: {arg}"
    );
    assert_eq!(
        ty["ofType"]["ofType"]["name"].as_str(),
        Some("ID"),
        "list item is not ID: {arg}"
    );
}

async fn graphql_raw(api_url: &str, query: &str) -> Value {
    reqwest::Client::new()
        .post(format!("{api_url}/api/v0/graphql"))
        .json(&json!({ "query": query }))
        .send()
        .await
        .expect("send graphql request")
        .json()
        .await
        .expect("decode graphql response")
}

fn assert_graphql_error(response: &Value, expected: &str) {
    let response_text = serde_json::to_string(response).expect("serialize response");
    assert!(
        response_text.contains(expected),
        "expected error containing {expected:?}, got {response_text}"
    );
}

async fn gql_list_args_schema_introspection_test(cluster: TestCluster) {
    let node = cluster.client(0);
    node.schema_add(SCHEMA).expect("add schema");

    let schema = node
        .query(
            r#"query {
                queryType: __type(name: "Query") {
                    fields {
                        name
                        args {
                            name
                            type {
                                kind
                                name
                                ofType {
                                    kind
                                    name
                                    ofType {
                                        kind
                                        name
                                    }
                                }
                            }
                        }
                    }
                }
                commitType: __type(name: "Commit") {
                    fields {
                        name
                        args {
                            name
                            type {
                                kind
                                name
                                ofType {
                                    kind
                                    name
                                    ofType {
                                        kind
                                        name
                                    }
                                }
                            }
                        }
                    }
                }
            }"#,
        )
        .expect("introspection query");

    let user = find_field(&schema["queryType"], "User");
    assert_id_list_arg(find_arg(user, "cid"));

    let commits = find_field(&schema["queryType"], "_commits");
    assert_id_list_arg(find_arg(commits, "cid"));
    assert_id_list_arg(find_arg(commits, "docID"));

    let heads = find_field(&schema["commitType"], "heads");
    assert_id_list_arg(find_arg(heads, "cid"));
    assert_id_list_arg(find_arg(heads, "docID"));

    let links = find_field(&schema["commitType"], "links");
    assert_id_list_arg(find_arg(links, "cid"));
    assert_id_list_arg(find_arg(links, "docID"));
}

async fn gql_list_args_single_value_compat_test(cluster: TestCluster) {
    let node = cluster.client(0);
    node.schema_add(SCHEMA).expect("add schema");

    let create = node
        .query(r#"mutation { add_User(input: {name: "Alice", age: 30}) { _docID } }"#)
        .expect("create user");
    let doc_id = extract_doc_id(&create, "add_User");

    let commits = node
        .query(&format!(
            r#"query {{
                _commits(docID: ["{doc_id}"], filter: {{fieldName: {{_eq: "_C"}}}}) {{
                    cid
                    height
                    docID
                }}
            }}"#,
        ))
        .expect("query commits by docID list");
    let cid = commit_cid(&commits);

    for query in [
        format!(r#"query {{ User(cid: "{cid}") {{ _docID name }} }}"#),
        format!(r#"query {{ User(cid: ["{cid}"]) {{ _docID name }} }}"#),
    ] {
        let result = node.query(&query).expect("query user by cid");
        let users = rows(&result, "User");
        assert_eq!(
            users.len(),
            1,
            "unexpected User result for {query}: {result}"
        );
        assert_eq!(users[0]["_docID"].as_str(), Some(doc_id.as_str()));
        assert_eq!(users[0]["name"].as_str(), Some("Alice"));
    }

    for query in [
        format!(r#"query {{ _commits(docID: "{doc_id}") {{ cid docID }} }}"#),
        format!(r#"query {{ _commits(docID: ["{doc_id}"]) {{ cid docID }} }}"#),
        format!(r#"query {{ _commits(cid: "{cid}") {{ cid docID }} }}"#),
        format!(r#"query {{ _commits(cid: ["{cid}"]) {{ cid docID }} }}"#),
    ] {
        let result = node.query(&query).expect("query commits by list arg");
        assert!(
            !rows(&result, "_commits").is_empty(),
            "expected commits for {query}: {result}"
        );
    }
}

async fn gql_list_args_multi_value_errors_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let api_url = cluster.api_url(0).to_string();
    node.schema_add(SCHEMA).expect("add schema");

    let cid_error = graphql_raw(
        &api_url,
        r#"query {
                User(cid: [
                    "bafyreifldhofx6cwi6ashk24rcefsuiqje5a2rziwcyte54z27wmgv4pey"
                    "bafyreic2vrbl344kkc7h5d7e2hpnwvffta4ck73bvjs5acgjtvqubvvioe"
                ]) {
                    _docID
                }
            }"#,
    )
    .await;
    assert_graphql_error(&cid_error, "querying by multiple cids is not yet supported");

    let commit_cid_error = graphql_raw(
        &api_url,
        r#"query {
                _commits(cid: [
                    "bafyreifldhofx6cwi6ashk24rcefsuiqje5a2rziwcyte54z27wmgv4pey"
                    "bafyreic2vrbl344kkc7h5d7e2hpnwvffta4ck73bvjs5acgjtvqubvvioe"
                ]) {
                    cid
                }
            }"#,
    )
    .await;
    assert_graphql_error(
        &commit_cid_error,
        "querying by multiple cids is not yet supported",
    );

    let doc_id_error = graphql_raw(
        &api_url,
        r#"query {
                _commits(docID: ["bae-one", "bae-two"]) {
                    cid
                }
            }"#,
    )
    .await;
    assert_graphql_error(
        &doc_id_error,
        "querying by multiple docIDs is not yet supported",
    );
}

for_each_runtime!(
    gql_list_args_schema_introspection,
    gql_list_args_schema_introspection_test
);
for_each_runtime!(
    gql_list_args_single_value_compat,
    gql_list_args_single_value_compat_test
);
for_each_runtime!(
    gql_list_args_multi_value_errors,
    gql_list_args_multi_value_errors_test
);
