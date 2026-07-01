use integration_test::{for_each_runtime, generate_identity, TestCluster};
use serde_json::Value;

// Mirrors the multi-CID vector cases added in Go PR #4794. Document CID tests
// use the same literal CIDs; _commits tests derive runtime CIDs because commit
// CID bytes are not stable across the Rust and Go integration harness paths.
const SIMPLE_USERS_SCHEMA: &str = "type Users { name: String }";
const COMMIT_USERS_SCHEMA: &str = "type Users { name: String  age: Int  verified: Boolean }";

const SIMPLE_JOHN_CID: &str = "bafyreifldhofx6cwi6ashk24rcefsuiqje5a2rziwcyte54z27wmgv4pey";
const SIMPLE_FRED_CID: &str = "bafyreihufziq5m2i6sgw2ls45uratin7eudhjplfg23qtj2lv6g6knevha";
const SIMPLE_JOHNNN_CID: &str = "bafyreiecis4aqmvr4effzlb74cwflphkykfnibpdnnftdyp6o2cneqy57q";

const USERS_ACP_POLICY: &str = r#"description: a test policy which marks a collection in a database as a resource
name: test
resources:
- name: users
  permissions:
  - name: delete
  - expr: reader
    name: read
  - name: update
  relations:
  - manages:
    - reader
    name: admin
    types:
    - actor
  - name: reader
    types:
    - actor"#;

fn rows<'a>(data: &'a Value, field: &str) -> &'a [Value] {
    data[field]
        .as_array()
        .unwrap_or_else(|| panic!("{field} array missing from response: {data}"))
        .as_slice()
}

fn extract_doc_id(data: &Value, mutation_name: &str) -> String {
    data[mutation_name]
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|value| value["_docID"].as_str())
        .or_else(|| data[mutation_name]["_docID"].as_str())
        .unwrap_or_else(|| panic!("missing _docID in {data}"))
        .to_string()
}

fn extract_policy_id(data: &Value) -> String {
    data["PolicyID"]
        .as_str()
        .or_else(|| data["policyID"].as_str())
        .unwrap_or_else(|| panic!("missing PolicyID in {data}"))
        .to_string()
}

fn users_policy_schema(policy_id: &str) -> String {
    format!(
        r#"type Users @policy(id: "{}", resource: "users") {{ name: String  age: Int }}"#,
        policy_id
    )
}

fn names(data: &Value) -> Vec<String> {
    rows(data, "Users")
        .iter()
        .map(|user| {
            user["name"]
                .as_str()
                .unwrap_or_else(|| panic!("missing name in {user}"))
                .to_string()
        })
        .collect()
}

fn commit_cids(data: &Value) -> Vec<String> {
    rows(data, "_commits")
        .iter()
        .map(|commit| {
            commit["cid"]
                .as_str()
                .unwrap_or_else(|| panic!("missing cid in {commit}"))
                .to_string()
        })
        .collect()
}

fn commit_cid_at_height(data: &Value, height: u64) -> String {
    rows(data, "_commits")
        .iter()
        .find(|commit| commit["height"].as_u64() == Some(height))
        .and_then(|commit| commit["cid"].as_str())
        .unwrap_or_else(|| panic!("missing commit cid at height {height} in {data}"))
        .to_string()
}

fn add_simple_user(node: &integration_test::DefraClient, name: &str) -> Value {
    node.query(&format!(
        r#"mutation {{ add_Users(input: {{name: "{name}"}}) {{ _docID name }} }}"#
    ))
    .unwrap_or_else(|err| panic!("add {name}: {err:#}"))
}

fn add_commit_user(node: &integration_test::DefraClient, name: &str, age: i64) -> Value {
    node.query(&format!(
        r#"mutation {{ add_Users(input: {{name: "{name}", age: {age}}}) {{ _docID name age }} }}"#
    ))
    .unwrap_or_else(|err| panic!("add {name}: {err:#}"))
}

fn commit_cid_for_doc_field(
    node: &integration_test::DefraClient,
    doc_id: &str,
    field_name: &str,
    height: u64,
) -> String {
    let result = node
        .query(&format!(
            r#"query {{
                _commits(docID: ["{doc_id}"], filter: {{fieldName: {{_eq: "{field_name}"}}}}) {{
                    cid
                    height
                }}
            }}"#
        ))
        .expect("query doc commit cid");
    commit_cid_at_height(&result, height)
}

fn create_commit_cid_for_doc(node: &integration_test::DefraClient, doc_id: &str) -> String {
    commit_cid_for_doc_field(node, doc_id, "_C", 1)
}

async fn multi_cid_simple_multiple_cids_test(cluster: TestCluster) {
    let node = cluster.client(0);
    node.schema_add(SIMPLE_USERS_SCHEMA).expect("add schema");

    add_simple_user(&node, "John");
    add_simple_user(&node, "Fred");
    add_simple_user(&node, "Shahzad");

    let result = node
        .query(&format!(
            r#"query {{
                Users(cid: ["{SIMPLE_JOHN_CID}", "{SIMPLE_FRED_CID}"]) {{
                    name
                }}
            }}"#
        ))
        .expect("query Users by multiple CIDs");

    assert_eq!(
        names(&result),
        ["John", "Fred"],
        "unexpected Users result for CIDs {SIMPLE_JOHN_CID}, {SIMPLE_FRED_CID}: {result}"
    );
}

async fn multi_cid_simple_duplicate_cids_for_same_doc_test(cluster: TestCluster) {
    let node = cluster.client(0);
    node.schema_add(SIMPLE_USERS_SCHEMA).expect("add schema");

    add_simple_user(&node, "John");
    add_simple_user(&node, "Fred");

    let result = node
        .query(&format!(
            r#"query {{
                Users(cid: ["{SIMPLE_JOHN_CID}", "{SIMPLE_JOHN_CID}"]) {{
                    name
                }}
            }}"#
        ))
        .expect("query Users by duplicate CIDs");

    assert_eq!(
        names(&result),
        ["John"],
        "unexpected Users result for duplicate CID {SIMPLE_JOHN_CID}: {result}"
    );
}

async fn multi_cid_simple_multiple_cids_for_same_doc_test(cluster: TestCluster) {
    let node = cluster.client(0);
    node.schema_add(SIMPLE_USERS_SCHEMA).expect("add schema");

    let created = add_simple_user(&node, "John");
    let john_id = extract_doc_id(&created, "add_Users");
    node.query(&format!(
        r#"mutation {{ update_Users(docID: "{john_id}", input: {{name: "Johnnn"}}) {{ _docID name }} }}"#
    ))
    .expect("update John");
    add_simple_user(&node, "Fred");

    let result = node
        .query(&format!(
            r#"query {{
                Users(cid: ["{SIMPLE_JOHN_CID}", "{SIMPLE_JOHNNN_CID}"]) {{
                    name
                }}
            }}"#
        ))
        .expect("query Users by multiple CIDs for one doc");

    assert_eq!(
        names(&result),
        ["John", "Johnnn"],
        "unexpected Users result for CIDs {SIMPLE_JOHN_CID}, {SIMPLE_JOHNNN_CID}: {result}"
    );
}

async fn multi_cid_commits_different_docs_test(cluster: TestCluster) {
    let node = cluster.client(0);
    node.schema_add(COMMIT_USERS_SCHEMA).expect("add schema");

    let john = add_commit_user(&node, "John", 21);
    let john_id = extract_doc_id(&john, "add_Users");
    let fred = add_commit_user(&node, "Fred", 21);
    let fred_id = extract_doc_id(&fred, "add_Users");
    let john_cid = create_commit_cid_for_doc(&node, &john_id);
    let fred_cid = create_commit_cid_for_doc(&node, &fred_id);

    let result = node
        .query(&format!(
            r#"query {{
                _commits(cid: ["{john_cid}", "{fred_cid}"]) {{
                    cid
                }}
            }}"#
        ))
        .expect("query _commits by multiple CIDs");

    assert_eq!(
        commit_cids(&result),
        [john_cid, fred_cid],
        "unexpected _commits result: {result}"
    );
}

async fn multi_cid_commits_same_doc_test(cluster: TestCluster) {
    let node = cluster.client(0);
    node.schema_add(COMMIT_USERS_SCHEMA).expect("add schema");

    let created = add_commit_user(&node, "John", 21);
    let john_id = extract_doc_id(&created, "add_Users");
    node.query(&format!(
        r#"mutation {{ update_Users(docID: "{john_id}", input: {{age: 22}}) {{ _docID age }} }}"#
    ))
    .expect("update John");
    let create_cid = create_commit_cid_for_doc(&node, &john_id);
    let update_cid = commit_cid_for_doc_field(&node, &john_id, "age", 2);

    let result = node
        .query(&format!(
            r#"query {{
                _commits(cid: ["{create_cid}", "{update_cid}"]) {{
                    cid
                }}
            }}"#
        ))
        .expect("query _commits by multiple CIDs for one doc");

    assert_eq!(
        commit_cids(&result),
        [create_cid, update_cid],
        "unexpected _commits result: {result}"
    );
}

async fn multi_cid_commits_overlapping_depth_dedups_test(cluster: TestCluster) {
    let node = cluster.client(0);
    node.schema_add(COMMIT_USERS_SCHEMA).expect("add schema");

    let created = add_commit_user(&node, "John", 21);
    let john_id = extract_doc_id(&created, "add_Users");
    node.query(&format!(
        r#"mutation {{ update_Users(docID: "{john_id}", input: {{age: 22}}) {{ _docID age }} }}"#
    ))
    .expect("update John");

    let age_create_cid = commit_cid_for_doc_field(&node, &john_id, "age", 1);
    let age_update_cid = commit_cid_for_doc_field(&node, &john_id, "age", 2);

    let result = node
        .query(&format!(
            r#"query {{
                _commits(cid: ["{age_update_cid}", "{age_create_cid}"], depth: 2) {{
                    cid
                }}
            }}"#
        ))
        .expect("query _commits by overlapping CIDs");

    assert_eq!(
        commit_cids(&result),
        [age_update_cid, age_create_cid],
        "unexpected _commits overlap result: {result}"
    );
}

async fn multi_cid_commits_filters_unreadable_cid_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let owner = generate_identity(node.binary_path()).expect("owner identity");

    let policy = node
        .acp_policy_add(USERS_ACP_POLICY, &owner.private_key_hex)
        .expect("add policy");
    let schema = users_policy_schema(&extract_policy_id(&policy));
    node.schema_add_with_identity(&schema, &owner.private_key_hex)
        .expect("add schema");

    let public = node
        .query(r#"mutation { add_Users(input: {name: "John", age: 21}) { _docID } }"#)
        .expect("add public doc");
    let public_id = extract_doc_id(&public, "add_Users");
    let private = node
        .query_with_identity(
            r#"mutation { add_Users(input: {name: "Fred", age: 21}) { _docID } }"#,
            &owner.private_key_hex,
        )
        .expect("add protected doc");
    let private_id = extract_doc_id(&private, "add_Users");

    let public_cid = create_commit_cid_for_doc(&node, &public_id);
    let private_cid = {
        let result = node
            .query_with_identity(
                &format!(
                    r#"query {{
                        _commits(docID: ["{private_id}"], filter: {{fieldName: {{_eq: "_C"}}}}) {{
                            cid
                            height
                        }}
                    }}"#
                ),
                &owner.private_key_hex,
            )
            .expect("query private doc commit cid");
        commit_cid_at_height(&result, 1)
    };

    let result = node
        .query(&format!(
            r#"query {{
                _commits(cid: ["{public_cid}", "{private_cid}"]) {{
                    cid
                }}
            }}"#
        ))
        .expect("query _commits with unreadable CID");

    assert_eq!(
        commit_cids(&result),
        [public_cid],
        "unexpected _commits ACP-filtered result: {result}"
    );
}

// NOTE: the `go_` variants below FAIL ON PURPOSE against a Go v1.0.0 node and are
// left failing as a visible parity signal (not `#[ignore]`d). They assert
// hardcoded commit-CID vectors, but Go v1.0.0 derives DocIDs from the genesis
// composite CID (Go #4838), which the Rust port has not yet ported — so Go and
// Rust compute different document identities and the baked vectors cannot match.
// They go green once Rust ports #4838 and the vectors are refreshed. Tracked in
// defradb.rs#1080. The `rust_` variants pass today.
for_each_runtime!(
    multi_cid_simple_multiple_cids,
    multi_cid_simple_multiple_cids_test
);
for_each_runtime!(
    multi_cid_simple_duplicate_cids_for_same_doc,
    multi_cid_simple_duplicate_cids_for_same_doc_test
);
for_each_runtime!(
    multi_cid_simple_multiple_cids_for_same_doc,
    multi_cid_simple_multiple_cids_for_same_doc_test
);
for_each_runtime!(
    multi_cid_commits_different_docs,
    multi_cid_commits_different_docs_test
);
for_each_runtime!(multi_cid_commits_same_doc, multi_cid_commits_same_doc_test);
for_each_runtime!(
    multi_cid_commits_overlapping_depth_dedups,
    multi_cid_commits_overlapping_depth_dedups_test
);
for_each_runtime!(
    multi_cid_commits_filters_unreadable_cid,
    multi_cid_commits_filters_unreadable_cid_test,
    .with_acp_local()
);
