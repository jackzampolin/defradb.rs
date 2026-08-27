//! CID query collection scoping (#1604). Mirrors Go's
//! TestQuerySimple_WithCidFromAnotherCollection_ReturnsEmpty,
//! TestQuerySimpleWithCidOfBranchableCollectionAndDocID, and
//! TestQuerySimple_UnknownCid.

use integration_test::{for_each_runtime, DefraClient, TestCluster};
use serde_json::Value;

fn extract_doc_id(data: &Value, mutation_name: &str) -> String {
    data[mutation_name]
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|value| value["_docID"].as_str())
        .or_else(|| data[mutation_name]["_docID"].as_str())
        .unwrap_or_else(|| panic!("missing _docID in {data}"))
        .to_string()
}

fn create_commit_cid_for_doc(node: &DefraClient, doc_id: &str) -> String {
    let result = node
        .query(&format!(
            r#"query {{
                _commits(docID: ["{doc_id}"], filter: {{fieldName: {{_eq: "_C"}}}}) {{
                    cid
                    height
                }}
            }}"#
        ))
        .expect("query doc commit cid");
    result["_commits"]
        .as_array()
        .and_then(|commits| {
            commits
                .iter()
                .find(|commit| commit["height"].as_u64() == Some(1))
        })
        .and_then(|commit| commit["cid"].as_str())
        .unwrap_or_else(|| panic!("missing create commit cid in {result}"))
        .to_string()
}

fn collection_commit_cid_at_height(node: &DefraClient, height: u64) -> String {
    let result = node
        .query("query { _commits { cid height docID } }")
        .expect("query collection commits");
    result["_commits"]
        .as_array()
        .and_then(|commits| {
            commits.iter().find(|commit| {
                commit["docID"].is_null() && commit["height"].as_u64() == Some(height)
            })
        })
        .and_then(|commit| commit["cid"].as_str())
        .unwrap_or_else(|| panic!("missing collection commit at height {height} in {result}"))
        .to_string()
}

async fn cid_from_another_collection_returns_empty_test(cluster: TestCluster) {
    let node = cluster.client(0);
    node.schema_add("type Users { name: String }  type Pets { name: String }")
        .expect("add schema");

    let created = node
        .query(r#"mutation { add_Users(input: {name: "John"}) { _docID name } }"#)
        .expect("add John");
    let john_id = extract_doc_id(&created, "add_Users");
    let john_cid = create_commit_cid_for_doc(&node, &john_id);

    let result = node
        .query(&format!(
            r#"query {{ Pets(cid: "{john_cid}") {{ name }} }}"#
        ))
        .expect("query Pets by Users CID");

    assert_eq!(
        result["Pets"],
        Value::Array(vec![]),
        "a Users commit CID queried as Pets must yield an empty result: {result}"
    );
}

async fn cid_of_branchable_collection_with_doc_id_test(cluster: TestCluster) {
    let node = cluster.client(0);
    node.schema_add("type Users @branchable { name: String }")
        .expect("add schema");

    let created = node
        .query(r#"mutation { add_Users(input: {name: "Fred"}) { _docID name } }"#)
        .expect("add Fred");
    let fred_id = extract_doc_id(&created, "add_Users");
    node.query(r#"mutation { add_Users(input: {name: "John"}) { _docID name } }"#)
        .expect("add John");
    node.query(&format!(
        r#"mutation {{ update_Users(docID: "{fred_id}", input: {{name: "Freddddd"}}) {{ _docID name }} }}"#
    ))
    .expect("update Fred");

    // Collection commit created when John was added (Go: CollectionCID0_1).
    let collection_cid = collection_commit_cid_at_height(&node, 2);

    let result = node
        .query(&format!(
            r#"query {{ Users(cid: "{collection_cid}", docID: "{fred_id}") {{ name }} }}"#
        ))
        .expect("query Users by collection CID and docID");

    let users = result["Users"]
        .as_array()
        .unwrap_or_else(|| panic!("Users array missing from response: {result}"));
    assert_eq!(
        users.len(),
        1,
        "docID must post-filter the collection snapshot to one document: {result}"
    );
    assert_eq!(
        users[0]["name"], "Fred",
        "expected Fred's historical value at that collection state: {result}"
    );
}

async fn unknown_cid_errors_test(cluster: TestCluster) {
    let node = cluster.client(0);
    node.schema_add("type Users { name: String }")
        .expect("add schema");

    let result = node.query(
        r#"query { Users(cid: "bafybeid57gpbwi4i6bg7g35hhhhhhhhhhhhhhhhhhhhhhhdoesnotexist") { name } }"#,
    );

    let error_text = match result {
        Err(err) => format!("{err:#}"),
        Ok(value) => {
            let errors = value["errors"].as_array().cloned().unwrap_or_default();
            assert!(
                !errors.is_empty(),
                "unknown CID must error, got success: {value}"
            );
            Value::Array(errors).to_string()
        }
    };
    assert!(
        error_text.contains("failed to get block in blockstore: ipld: could not find"),
        "unknown CID must surface the blockstore miss, got: {error_text}"
    );
}

for_each_runtime!(
    cid_from_another_collection_returns_empty,
    cid_from_another_collection_returns_empty_test
);
for_each_runtime!(
    cid_of_branchable_collection_with_doc_id,
    cid_of_branchable_collection_with_doc_id_test
);
for_each_runtime!(unknown_cid_errors, unknown_cid_errors_test);
