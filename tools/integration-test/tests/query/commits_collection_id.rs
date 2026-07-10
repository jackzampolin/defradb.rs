use integration_test::TestCluster;
use serde_json::Value;

const SCHEMA: &str = "type IncidentReport { title: String status: String }";

fn extract_doc_id(data: &Value) -> String {
    data["add_IncidentReport"]
        .as_array()
        .and_then(|rows| rows.first())
        .and_then(|row| row["_docID"].as_str())
        .expect("missing _docID")
        .to_string()
}

fn extract_collection_id(description: &Value) -> String {
    description
        .get("CollectionID")
        .or_else(|| description.get("collection_id"))
        .and_then(Value::as_str)
        .expect("missing CollectionID")
        .to_string()
}

fn extract_commits(data: &Value) -> &[Value] {
    data["_commits"].as_array().expect("missing _commits array")
}

fn assert_collection_id(commits: &[Value], expected: &str) {
    for commit in commits {
        assert_eq!(
            commit["collectionID"].as_str(),
            Some(expected),
            "unexpected collectionID for commit {}",
            commit
        );
    }
}

#[tokio::test]
async fn commits_include_collection_id_for_doc_id_and_cid_queries() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let node = cluster.client(0);

    node.schema_add(SCHEMA).expect("add schema");
    let collection_id = extract_collection_id(
        &node
            .collection_describe_version("IncidentReport")
            .expect("describe collection"),
    );

    let created = node
        .query(
            r#"mutation {
                add_IncidentReport(input: {title: "Database alert", status: "open"}) {
                    _docID
                }
            }"#,
        )
        .expect("create incident report");
    let doc_id = extract_doc_id(&created);

    node.query(&format!(
        r#"mutation {{
            update_IncidentReport(docID: "{doc_id}", input: {{status: "resolved"}}) {{
                _docID
            }}
        }}"#,
    ))
    .expect("update incident report");

    let by_doc_id = node
        .query(&format!(
            r#"query {{
                _commits(docID: ["{doc_id}"]) {{
                    cid
                    collectionID
                    fieldName
                }}
            }}"#,
        ))
        .expect("query commits by docID");
    let doc_commits = extract_commits(&by_doc_id);
    assert_collection_id(doc_commits, &collection_id);

    let composite_cid = doc_commits
        .iter()
        .find(|commit| commit["fieldName"] == "_C")
        .and_then(|commit| commit["cid"].as_str())
        .expect("missing composite commit");
    let field_cid = doc_commits
        .iter()
        .find(|commit| commit["fieldName"] == "status")
        .and_then(|commit| commit["cid"].as_str())
        .expect("missing field commit");

    let by_cid = node
        .query(&format!(
            r#"query {{
                _commits(cid: ["{composite_cid}", "{field_cid}"]) {{
                    cid
                    collectionID
                    fieldName
                }}
            }}"#,
        ))
        .expect("query composite and field commits by cid");
    let cid_commits = extract_commits(&by_cid);
    assert_eq!(cid_commits.len(), 2, "expected both requested commits");
    assert_collection_id(cid_commits, &collection_id);
}
