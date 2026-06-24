//! SourceHub-backed cross-object ACP regression coverage.
//!
//! The local ACP variant proves the Zanzibar engine can evaluate `parent->read`;
//! this test proves the SourceHub transaction path can persist the structured
//! object-edge relationship that makes that inheritance possible.

use integration_test::node::{DefraNode, RustNode};
use integration_test::{generate_identity, TestCluster};

fn fs_policy() -> &'static str {
    r#"
name: filesystem
description: cross-object filesystem policy
resources:
  - name: directory
    permissions:
      - name: read
        expr: reader
      - name: update
        expr: reader
      - name: delete
        expr: reader
    relations:
      - name: reader
        types:
          - actor
  - name: file
    permissions:
      - name: read
        expr: reader + parent->read
      - name: update
        expr: reader
      - name: delete
        expr: reader
    relations:
      - name: reader
        types:
          - actor
      - name: parent
        types:
          - directory
"#
}

fn fs_schema(policy_id: &str) -> String {
    format!(
        r#"type Directory @policy(id: "{pid}", resource: "directory") {{ name: String }}
type File @policy(id: "{pid}", resource: "file") {{ title: String }}"#,
        pid = policy_id
    )
}

fn extract_policy_id(value: &serde_json::Value) -> String {
    value["PolicyID"]
        .as_str()
        .or_else(|| value["policyID"].as_str())
        .expect("missing PolicyID")
        .to_string()
}

fn file_count(node: &integration_test::DefraClient, key: &str) -> usize {
    node.query_with_identity("query { File { _docID title } }", key)
        .expect("query File")["File"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0)
}

#[tokio::test]
#[serial_test::serial]
async fn rust_sourcehub_cross_object_parent_read_inheritance() {
    let binary = RustNode::from_workspace().binary_path().to_path_buf();
    RustNode::build().expect("build rust binary");
    let owner = generate_identity(&binary).expect("owner identity");
    let alice = generate_identity(&binary).expect("alice identity");
    let bob = generate_identity(&binary).expect("bob identity");

    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .skip_build()
        .with_source_hub()
        .with_identity(&owner.private_key_hex)
        .build()
        .await
        .expect("failed to build sourcehub cluster");
    let node = cluster.client(0);

    let policy = node
        .acp_policy_add(fs_policy(), &owner.private_key_hex)
        .expect("add filesystem policy");
    let policy_id = extract_policy_id(&policy);
    node.schema_add_with_identity(&fs_schema(&policy_id), &owner.private_key_hex)
        .expect("add filesystem schema");

    let dir = node
        .query_with_identity(
            r#"mutation { add_Directory(input: {name: "team"}) { _docID } }"#,
            &owner.private_key_hex,
        )
        .expect("create directory");
    let dir_id = dir["add_Directory"][0]["_docID"]
        .as_str()
        .expect("directory _docID")
        .to_string();

    let file = node
        .query_with_identity(
            r#"mutation { add_File(input: {title: "report"}) { _docID } }"#,
            &owner.private_key_hex,
        )
        .expect("create file");
    let file_id = file["add_File"][0]["_docID"]
        .as_str()
        .expect("file _docID")
        .to_string();

    node.acp_relationship_add(
        "Directory",
        &dir_id,
        "reader",
        &alice.did,
        &owner.private_key_hex,
    )
    .expect("grant alice reader on directory");

    assert_eq!(
        file_count(&node, &alice.private_key_hex),
        0,
        "directory reader must not see the file before the parent edge"
    );

    let parent_target = format!("directory:{dir_id}");
    node.acp_relationship_add(
        "File",
        &file_id,
        "parent",
        &parent_target,
        &owner.private_key_hex,
    )
    .expect("seed SourceHub cross-object parent edge");

    assert_eq!(
        file_count(&node, &alice.private_key_hex),
        1,
        "alice must inherit file read through parent->read"
    );
    assert_eq!(
        file_count(&node, &bob.private_key_hex),
        0,
        "bob has no grant and must stay denied"
    );

    node.acp_relationship_delete(
        "File",
        &file_id,
        "parent",
        &parent_target,
        &owner.private_key_hex,
    )
    .expect("delete SourceHub cross-object parent edge");
    assert_eq!(
        file_count(&node, &alice.private_key_hex),
        0,
        "deleting the parent edge must remove inherited access"
    );
}
