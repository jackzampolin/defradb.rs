use integration_test::{query_both, DefraClient, TestCluster};
use serde_json::Value;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Commit {
    field_name: String,
    height: i64,
    cid: String,
}

fn doc_id(response: &Value, mutation: &str) -> String {
    response[mutation][0]["_docID"]
        .as_str()
        .unwrap_or_else(|| panic!("{mutation} response has no document ID: {response}"))
        .to_string()
}

fn commits(client: &DefraClient, doc_id: &str) -> Vec<Commit> {
    let response = client
        .query(&format!(
            r#"query {{
                _commits(docID: ["{doc_id}"]) {{
                    cid
                    fieldName
                    height
                }}
            }}"#
        ))
        .expect("query commits");
    let mut commits = response["_commits"]
        .as_array()
        .unwrap_or_else(|| panic!("_commits response is not an array: {response}"))
        .iter()
        .map(|commit| Commit {
            field_name: commit["fieldName"].as_str().expect("fieldName").to_string(),
            height: commit["height"].as_i64().expect("height"),
            cid: commit["cid"].as_str().expect("cid").to_string(),
        })
        .collect::<Vec<_>>();
    commits.sort();
    commits
}

fn assert_commits_match(
    rust: &DefraClient,
    go: &DefraClient,
    doc_id: &str,
    expected_count: usize,
    stage: &str,
) {
    let rust_commits = commits(rust, doc_id);
    let go_commits = commits(go, doc_id);
    assert_eq!(
        rust_commits.len(),
        expected_count,
        "{stage}: Rust commit count"
    );
    assert_eq!(go_commits.len(), expected_count, "{stage}: Go commit count");
    assert_eq!(rust_commits, go_commits, "{stage}: block CIDs diverged");
}

fn assert_counter_history_matches(
    rust: &DefraClient,
    go: &DefraClient,
    doc_id: &str,
    expected_count: usize,
    stage: &str,
) {
    let rust_commits = commits(rust, doc_id);
    let go_commits = commits(go, doc_id);
    assert_eq!(
        rust_commits.len(),
        expected_count,
        "{stage}: Rust commit count"
    );
    assert_eq!(go_commits.len(), expected_count, "{stage}: Go commit count");

    let shape = |commits: &[Commit]| {
        commits
            .iter()
            .map(|commit| (commit.field_name.clone(), commit.height))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        shape(&rust_commits),
        shape(&go_commits),
        "{stage}: history shape"
    );

    let deterministic = |commits: &[Commit]| {
        commits
            .iter()
            .filter(|commit| commit.field_name == "label")
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(
        deterministic(&rust_commits),
        deterministic(&go_commits),
        "{stage}: LWW field CIDs diverged"
    );
}

#[tokio::test]
async fn go_cross_runtime_block_cid_equivalence() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .build()
        .await
        .unwrap();
    let rust = cluster.client(0);
    let go = cluster.client(1);
    let schema = r#"
        type Pair { left: String  right: String }
        type Counted { label: String  hits: Int @crdt(type: pncounter) }
    "#;
    rust.schema_add(schema).expect("add Rust schema");
    go.schema_add(schema).expect("add Go schema");

    let (rust_create, go_create) = query_both(
        &rust,
        &go,
        r#"mutation { add_Pair(input: {left: "a0", right: "b0"}) { _docID } }"#,
    );
    let pair_id = doc_id(&rust_create, "add_Pair");
    assert_eq!(pair_id, doc_id(&go_create, "add_Pair"));
    assert_commits_match(&rust, &go, &pair_id, 3, "create Pair");

    for (round, (field, value)) in [
        ("left", "a1"),
        ("right", "b1"),
        ("left", "a2"),
        ("right", "b2"),
    ]
    .into_iter()
    .enumerate()
    {
        query_both(
            &rust,
            &go,
            &format!(
                r#"mutation {{ update_Pair(docID: "{pair_id}", input: {{{field}: "{value}"}}) {{ _docID }} }}"#
            ),
        );
        assert_commits_match(
            &rust,
            &go,
            &pair_id,
            5 + round * 2,
            &format!("update Pair.{field}"),
        );
    }

    let (rust_counted, go_counted) = query_both(
        &rust,
        &go,
        r#"mutation { add_Counted(input: {label: "first", hits: 1}) { _docID } }"#,
    );
    let counted_id = doc_id(&rust_counted, "add_Counted");
    assert_eq!(counted_id, doc_id(&go_counted, "add_Counted"));
    assert_commits_match(&rust, &go, &counted_id, 3, "create Counted");

    query_both(
        &rust,
        &go,
        &format!(
            r#"mutation {{ update_Counted(docID: "{counted_id}", input: {{label: "second"}}) {{ _docID }} }}"#
        ),
    );
    assert_commits_match(&rust, &go, &counted_id, 5, "update Counted.label");

    query_both(
        &rust,
        &go,
        &format!(
            r#"mutation {{ update_Counted(docID: "{counted_id}", input: {{hits: 2}}) {{ _docID }} }}"#
        ),
    );
    // Counter updates carry random nonces, so their new field and composite
    // CIDs intentionally differ between independent executions.
    assert_counter_history_matches(&rust, &go, &counted_id, 7, "update Counted.hits");

    query_both(
        &rust,
        &go,
        &format!(
            r#"mutation {{ update_Counted(docID: "{counted_id}", input: {{label: "third"}}) {{ _docID }} }}"#
        ),
    );
    assert_counter_history_matches(&rust, &go, &counted_id, 9, "update Counted.label again");

    query_both(
        &rust,
        &go,
        &format!(r#"mutation {{ delete_Pair(docID: "{pair_id}") {{ _docID }} }}"#),
    );
    assert_commits_match(&rust, &go, &pair_id, 12, "delete Pair");

    let (rust_recreate, go_recreate) = query_both(
        &rust,
        &go,
        r#"mutation { add_Pair(input: {left: "recreated", right: "document"}) { _docID } }"#,
    );
    let recreated_id = doc_id(&rust_recreate, "add_Pair");
    assert_eq!(recreated_id, doc_id(&go_recreate, "add_Pair"));
    assert_ne!(recreated_id, pair_id);
    assert_commits_match(&rust, &go, &recreated_id, 3, "recreate Pair");
}
