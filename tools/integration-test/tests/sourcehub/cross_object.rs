//! SourceHub-backed cross-object ACP regression coverage.
//!
//! The local ACP variant proves the Zanzibar engine can evaluate `parent->read`;
//! these tests prove SourceHub can persist and enforce the structured object and
//! userset relationship subjects through the live provider path.

use std::time::Duration;

use integration_test::node::{DefraNode, RustNode};
use integration_test::{extract_p2p_addr, generate_identity, poll_until, TestCluster};

const P2P_TIMEOUT: Duration = Duration::from_secs(15);
const POLL_INTERVAL: Duration = Duration::from_millis(300);
const REPLICATION_TIMEOUT: Duration = Duration::from_secs(30);

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

fn userset_policy() -> &'static str {
    r#"
name: userset-sharing
description: userset-backed article sharing policy
resources:
  - name: group
    permissions:
      - name: read
        expr: participant
    relations:
      - name: participant
        types:
          - actor
  - name: article
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
          - group->participant
"#
}

fn userset_schema(policy_id: &str) -> String {
    format!(
        r#"type Group @policy(id: "{pid}", resource: "group") {{ name: String }}
type Article @policy(id: "{pid}", resource: "article") {{ title: String }}"#,
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

fn collection_count(node: &integration_test::DefraClient, key: &str, collection: &str) -> usize {
    let query = format!("query {{ {collection} {{ _docID }} }}");
    let result = node
        .query_with_identity(&query, key)
        .unwrap_or_else(|err| panic!("query {collection}: {err}"));
    result
        .get(collection)
        .and_then(|value| value.as_array())
        .unwrap_or_else(|| panic!("query result must contain {collection} array: {result}"))
        .len()
}

fn file_count(node: &integration_test::DefraClient, key: &str) -> usize {
    collection_count(node, key, "File")
}

fn article_count(node: &integration_test::DefraClient, key: &str) -> usize {
    collection_count(node, key, "Article")
}

async fn wait_for_collection_count(
    node: &integration_test::DefraClient,
    key: &str,
    collection: &str,
    expected: usize,
    reason: &str,
) {
    let key = key.to_string();
    poll_until(
        || collection_count(node, &key, collection) == expected,
        REPLICATION_TIMEOUT,
        POLL_INTERVAL,
        reason,
    )
    .await;
}

fn sourcehub_owner() -> (std::path::PathBuf, String) {
    let binary = RustNode::from_workspace().binary_path().to_path_buf();
    RustNode::build().expect("build rust binary");
    let owner = generate_identity(&binary).expect("owner identity");
    (binary, owner.private_key_hex)
}

fn object_target(resource: &str, id: &str) -> String {
    format!("{resource}:\"{id}\"")
}

fn userset_target(resource: &str, id: &str, relation: &str) -> String {
    format!("{resource}:\"{id}\"#{relation}")
}

#[tokio::test]
#[serial_test::serial]
async fn rust_sourcehub_cross_object_parent_read_inheritance() {
    let (binary, owner_key) = sourcehub_owner();
    let alice = generate_identity(&binary).expect("alice identity");
    let bob = generate_identity(&binary).expect("bob identity");

    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .skip_build()
        .with_source_hub()
        .with_identity(&owner_key)
        .build()
        .await
        .expect("failed to build sourcehub cluster");
    let node = cluster.client(0);

    let policy = node
        .acp_policy_add(fs_policy(), &owner_key)
        .expect("add filesystem policy");
    let policy_id = extract_policy_id(&policy);
    node.schema_add_with_identity(&fs_schema(&policy_id), &owner_key)
        .expect("add filesystem schema");

    let dir = node
        .query_with_identity(
            r#"mutation { add_Directory(input: {name: "team"}) { _docID } }"#,
            &owner_key,
        )
        .expect("create directory");
    let dir_id = dir["add_Directory"][0]["_docID"]
        .as_str()
        .expect("directory _docID")
        .to_string();

    let file = node
        .query_with_identity(
            r#"mutation { add_File(input: {title: "report"}) { _docID } }"#,
            &owner_key,
        )
        .expect("create file");
    let file_id = file["add_File"][0]["_docID"]
        .as_str()
        .expect("file _docID")
        .to_string();

    node.acp_relationship_add("Directory", &dir_id, "reader", &alice.did, &owner_key)
        .expect("grant alice reader on directory");

    assert_eq!(
        file_count(&node, &alice.private_key_hex),
        0,
        "directory reader must not see the file before the parent edge"
    );

    let parent_target = object_target("directory", &dir_id);
    node.acp_relationship_add("File", &file_id, "parent", &parent_target, &owner_key)
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

    node.acp_relationship_delete("File", &file_id, "parent", &parent_target, &owner_key)
        .expect("delete SourceHub cross-object parent edge");
    assert_eq!(
        file_count(&node, &alice.private_key_hex),
        0,
        "deleting the parent edge must remove inherited access"
    );
}

#[tokio::test]
#[serial_test::serial]
async fn rust_sourcehub_iroh_cross_object_parent_read_inheritance() {
    let (binary, owner_key) = sourcehub_owner();
    let alice = generate_identity(&binary).expect("alice identity");
    let bob = generate_identity(&binary).expect("bob identity");

    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_source_hub()
        .with_iroh_transport()
        .with_identity(&owner_key)
        .build()
        .await
        .expect("failed to build sourcehub iroh cluster");

    for idx in 0..2 {
        cluster
            .wait_for_log(idx, "p2p_listening", P2P_TIMEOUT)
            .await
            .unwrap_or_else(|_| panic!("node{idx} P2P listener did not start"));
    }

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    let policy = node0
        .acp_policy_add(fs_policy(), &owner_key)
        .expect("add filesystem policy");
    let policy_id = extract_policy_id(&policy);
    let schema = fs_schema(&policy_id);
    node0
        .schema_add_with_identity(&schema, &owner_key)
        .expect("add filesystem schema on node0");
    node1
        .schema_add_with_identity(&schema, &owner_key)
        .expect("add filesystem schema on node1");

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("p2p connect");
    node0
        .p2p_collection_add(&["Directory", "File"])
        .expect("p2p collection add node0");
    node1
        .p2p_collection_add(&["Directory", "File"])
        .expect("p2p collection add node1");
    node0
        .p2p_replicator_set_with_identity(&["Directory", "File"], &addr1, &owner_key)
        .expect("set iroh replicator");

    let file = node0
        .query_with_identity(
            r#"mutation { add_File(input: {title: "report"}) { _docID } }"#,
            &owner_key,
        )
        .expect("create child file before parent directory");
    let file_id = file["add_File"][0]["_docID"]
        .as_str()
        .expect("file _docID")
        .to_string();

    wait_for_collection_count(
        &node1,
        &owner_key,
        "File",
        1,
        "child file did not replicate to node1 before parent directory existed",
    )
    .await;
    assert_eq!(
        file_count(&node1, &alice.private_key_hex),
        0,
        "node1 must fail closed while the child file exists without the parent edge"
    );

    let dir = node0
        .query_with_identity(
            r#"mutation { add_Directory(input: {name: "team"}) { _docID } }"#,
            &owner_key,
        )
        .expect("create parent directory after child file");
    let dir_id = dir["add_Directory"][0]["_docID"]
        .as_str()
        .expect("directory _docID")
        .to_string();

    node0
        .acp_relationship_add("Directory", &dir_id, "reader", &alice.did, &owner_key)
        .expect("grant alice reader on directory");
    let parent_target = object_target("directory", &dir_id);
    node0
        .acp_relationship_add("File", &file_id, "parent", &parent_target, &owner_key)
        .expect("seed parent edge after child file replicated");

    wait_for_collection_count(
        &node1,
        &owner_key,
        "Directory",
        1,
        "parent directory did not replicate to node1 after child file",
    )
    .await;
    wait_for_collection_count(
        &node1,
        &alice.private_key_hex,
        "File",
        1,
        "alice did not inherit file read on node1 after SourceHub parent edge was visible",
    )
    .await;
    assert_eq!(
        file_count(&node1, &bob.private_key_hex),
        0,
        "bob has no grant and must stay denied on the replicated node"
    );

    node0
        .acp_relationship_delete("File", &file_id, "parent", &parent_target, &owner_key)
        .expect("delete SourceHub parent edge");
    wait_for_collection_count(
        &node1,
        &alice.private_key_hex,
        "File",
        0,
        "alice access did not revoke on node1 after deleting the parent edge",
    )
    .await;
}

#[tokio::test]
#[serial_test::serial]
async fn rust_sourcehub_userset_reader_relationship() {
    let (binary, owner_key) = sourcehub_owner();
    let alice = generate_identity(&binary).expect("alice identity");
    let bob = generate_identity(&binary).expect("bob identity");

    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .skip_build()
        .with_source_hub()
        .with_identity(&owner_key)
        .build()
        .await
        .expect("failed to build sourcehub cluster");
    let node = cluster.client(0);

    let policy = node
        .acp_policy_add(userset_policy(), &owner_key)
        .expect("add userset policy");
    let policy_id = extract_policy_id(&policy);
    node.schema_add_with_identity(&userset_schema(&policy_id), &owner_key)
        .expect("add userset schema");

    let group = node
        .query_with_identity(
            r#"mutation { add_Group(input: {name: "eng"}) { _docID } }"#,
            &owner_key,
        )
        .expect("create group");
    let group_id = group["add_Group"][0]["_docID"]
        .as_str()
        .expect("group _docID")
        .to_string();

    let article = node
        .query_with_identity(
            r#"mutation { add_Article(input: {title: "plan"}) { _docID } }"#,
            &owner_key,
        )
        .expect("create article");
    let article_id = article["add_Article"][0]["_docID"]
        .as_str()
        .expect("article _docID")
        .to_string();

    node.acp_relationship_add("Group", &group_id, "participant", &alice.did, &owner_key)
        .expect("grant alice group participant");
    assert_eq!(
        article_count(&node, &alice.private_key_hex),
        0,
        "group membership alone must not read the article before userset edge"
    );

    let target = userset_target("group", &group_id, "participant");
    node.acp_relationship_add("Article", &article_id, "reader", &target, &owner_key)
        .expect("seed SourceHub userset reader edge");

    assert_eq!(
        article_count(&node, &alice.private_key_hex),
        1,
        "alice must read the article through group#participant userset"
    );
    assert_eq!(
        article_count(&node, &bob.private_key_hex),
        0,
        "bob is not in the group userset and must stay denied"
    );

    node.acp_relationship_delete("Article", &article_id, "reader", &target, &owner_key)
        .expect("delete SourceHub userset reader edge");
    assert_eq!(
        article_count(&node, &alice.private_key_hex),
        0,
        "deleting the userset edge must revoke article access"
    );
}
