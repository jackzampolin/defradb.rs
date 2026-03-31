use integration_test::TestCluster;
use serde_json::Value;

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

fn extract_commits(data: &Value) -> &[Value] {
    data["_commits"]
        .as_array()
        .unwrap_or_else(|| {
            panic!(
                "_commits array missing from response: {}",
                serde_json::to_string_pretty(data).unwrap()
            )
        })
        .as_slice()
}

fn commit_heights(commits: &[Value]) -> Vec<i64> {
    let mut heights: Vec<_> = commits
        .iter()
        .map(|commit| commit["height"].as_i64().expect("height"))
        .collect();
    heights.sort_unstable();
    heights
}

fn assert_field_name(commits: &[Value], expected: &str) {
    for commit in commits {
        assert_eq!(
            commit["fieldName"].as_str(),
            Some(expected),
            "unexpected fieldName in commit: {}",
            serde_json::to_string(commit).unwrap()
        );
    }
}

async fn commits_height_filter_test(cluster: TestCluster) {
    let node = cluster.client(0);
    node.schema_add(SCHEMA).expect("add schema");

    let create = node
        .query(r#"mutation { add_User(input: {name: "Alice", age: 30}) { _docID } }"#)
        .expect("create user");
    let doc_id = extract_doc_id(&create, "add_User");

    node.query(&format!(
        r#"mutation {{ update_User(docID: "{doc_id}", input: {{age: 31}}) {{ _docID }} }}"#,
    ))
    .expect("update age");
    node.query(&format!(
        r#"mutation {{ update_User(docID: "{doc_id}", input: {{name: "Alicia"}}) {{ _docID }} }}"#,
    ))
    .expect("update name");

    let composite_commits = node
        .query(&format!(
            r#"query {{
                _commits(
                    docID: ["{doc_id}"]
                    filter: {{fieldName: {{_eq: "_C"}}}}
                ) {{
                    cid
                    fieldName
                    height
                }}
            }}"#,
        ))
        .expect("query composite commits");
    let composite_arr = extract_commits(&composite_commits);
    assert_eq!(
        commit_heights(composite_arr),
        vec![1, 2, 3],
        "expected one composite commit per document version"
    );
    assert_field_name(composite_arr, "_C");

    let range = node
        .query(&format!(
            r#"query {{
                _commits(
                    docID: ["{doc_id}"]
                    filter: {{height: {{_gte: 2, _lte: 3}}, fieldName: {{_eq: "_C"}}}}
                ) {{
                    fieldName
                    height
                }}
            }}"#,
        ))
        .expect("query inclusive range");
    let range_arr = extract_commits(&range);
    assert_eq!(commit_heights(range_arr), vec![2, 3]);
    assert_field_name(range_arr, "_C");

    let exclusive = node
        .query(&format!(
            r#"query {{
                _commits(
                    docID: ["{doc_id}"]
                    filter: {{height: {{_gt: 1, _lt: 3}}, fieldName: {{_eq: "_C"}}}}
                ) {{
                    fieldName
                    height
                }}
            }}"#,
        ))
        .expect("query exclusive range");
    let exclusive_arr = extract_commits(&exclusive);
    assert_eq!(commit_heights(exclusive_arr), vec![2]);
    assert_field_name(exclusive_arr, "_C");

    let exact_field = node
        .query(&format!(
            r#"query {{
                _commits(
                    docID: ["{doc_id}"]
                    filter: {{height: {{_eq: 2}}, fieldName: {{_eq: "age"}}}}
                ) {{
                    fieldName
                    height
                }}
            }}"#,
        ))
        .expect("query exact field commit");
    let exact_field_arr = extract_commits(&exact_field);
    assert_eq!(commit_heights(exact_field_arr), vec![2]);
    assert_field_name(exact_field_arr, "age");

    let and_filter = node
        .query(&format!(
            r#"query {{
                _commits(
                    docID: ["{doc_id}"]
                    filter: {{
                        _and: [
                            {{height: {{_gte: 2}}}}
                            {{height: {{_lt: 4}}}}
                            {{fieldName: {{_eq: "_C"}}}}
                        ]
                    }}
                ) {{
                    fieldName
                    height
                }}
            }}"#,
        ))
        .expect("query _and filter");
    let and_arr = extract_commits(&and_filter);
    assert_eq!(commit_heights(and_arr), vec![2, 3]);
    assert_field_name(and_arr, "_C");

    let or_filter = node
        .query(&format!(
            r#"query {{
                _commits(
                    docID: ["{doc_id}"]
                    filter: {{
                        _or: [
                            {{
                                _and: [
                                    {{height: {{_eq: 1}}}}
                                    {{fieldName: {{_eq: "_C"}}}}
                                ]
                            }}
                            {{
                                _and: [
                                    {{height: {{_eq: 3}}}}
                                    {{fieldName: {{_eq: "_C"}}}}
                                ]
                            }}
                        ]
                    }}
                ) {{
                    fieldName
                    height
                }}
            }}"#,
        ))
        .expect("query _or filter");
    let or_arr = extract_commits(&or_filter);
    assert_eq!(commit_heights(or_arr), vec![1, 3]);
    assert_field_name(or_arr, "_C");

    let empty = node
        .query(&format!(
            r#"query {{
                _commits(
                    docID: ["{doc_id}"]
                    filter: {{height: {{_gt: 10}}, fieldName: {{_eq: "_C"}}}}
                ) {{
                    height
                }}
            }}"#,
        ))
        .expect("query empty range");
    assert!(
        extract_commits(&empty).is_empty(),
        "expected empty result set"
    );
}

#[tokio::test]
async fn rust_commits_height_filter() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    commits_height_filter_test(cluster).await;
}

/// Known upstream gap: Go DefraDB returns `{"data": null}` for `_commits`
/// height-filter queries that pass on Rust.
#[tokio::test]
#[ignore]
async fn go_commits_height_filter() {
    let cluster = TestCluster::builder().go_nodes(1).build().await.unwrap();
    commits_height_filter_test(cluster).await;
}
