//! Cross-object (collection-level) ACP — end-to-end over the live HTTP stack.
//!
//! Proves that the embedded node's Zanzibar `DocumentACP` resolves a
//! cross-object `parent->read` inheritance cone, seeded entirely through the
//! public HTTP relationship API (no store backdoor):
//!
//! 1. A `directory` reader is granted to alice.
//! 2. A cross-object `file#parent@directory:<dir>` edge is seeded over HTTP
//!    (target string `directory:<dir_id>` parsed at the edge into a structured
//!    subject).
//! 3. An inheriting `File` query returns the file to alice via
//!    `parent->read -> directory#read -> reader`; a stranger stays denied.
//! 4. Revoking the parent edge removes alice's inherited access.

use integration_test::{generate_identity, TestCluster};

/// Two-resource filesystem policy: `file.read` inherits from its parent
/// directory via the cross-object TTU `parent->read`.
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

fn extract_policy_id(value: &serde_json::Value) -> Option<String> {
    value["PolicyID"]
        .as_str()
        .or_else(|| value["policyID"].as_str())
        .map(|s| s.to_string())
}

fn file_count(node: &integration_test::DefraClient, key: &str) -> usize {
    node.query_with_identity("query { File { _docID title } }", key)
        .expect("query File")["File"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0)
}

async fn cross_object_parent_read_inheritance(cluster: TestCluster) {
    let node = cluster.client(0);
    let owner = generate_identity(node.binary_path()).expect("owner");
    let alice = generate_identity(node.binary_path()).expect("alice");
    let bob = generate_identity(node.binary_path()).expect("bob");

    let policy = node
        .acp_policy_add(fs_policy(), &owner.private_key_hex)
        .expect("add fs policy");
    let policy_id = extract_policy_id(&policy).expect("policy id");
    node.schema_add_with_identity(&fs_schema(&policy_id), &owner.private_key_hex)
        .expect("add fs schema");

    let dir = node
        .query_with_identity(
            r#"mutation { add_Directory(input: {name: "team"}) { _docID } }"#,
            &owner.private_key_hex,
        )
        .expect("create directory");
    let dir_id = dir["add_Directory"][0]["_docID"]
        .as_str()
        .expect("dir _docID")
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

    // alice is a reader on the directory only.
    node.acp_relationship_add(
        "Directory",
        &dir_id,
        "reader",
        &alice.did,
        &owner.private_key_hex,
    )
    .expect("grant alice reader on directory");

    // Before the cross-object edge: directory access must NOT leak to the file.
    assert_eq!(
        file_count(&node, &alice.private_key_hex),
        0,
        "alice must not see the file before the parent edge exists"
    );

    // Seed the cross-object parent edge through the HTTP relationship API. The
    // target `directory:<dir_id>` is parsed at the edge into a structured
    // cross-object subject.
    let parent_target = format!("directory:{}", dir_id);
    node.acp_relationship_add(
        "File",
        &file_id,
        "parent",
        &parent_target,
        &owner.private_key_hex,
    )
    .expect("seed cross-object parent edge");

    // alice now reaches the file via parent->read -> directory#read -> reader.
    assert_eq!(
        file_count(&node, &alice.private_key_hex),
        1,
        "alice must see the file via cross-object parent->read inheritance"
    );
    // A stranger with no grant stays denied through the cone.
    assert_eq!(
        file_count(&node, &bob.private_key_hex),
        0,
        "bob has no grant and must remain denied"
    );

    // Revoke the parent edge — inherited access must disappear.
    node.acp_relationship_delete(
        "File",
        &file_id,
        "parent",
        &parent_target,
        &owner.private_key_hex,
    )
    .expect("revoke cross-object parent edge");
    assert_eq!(
        file_count(&node, &alice.private_key_hex),
        0,
        "revoking the parent edge must remove alice's inherited access"
    );
}

// Rust-only: collection-level / cross-object ACP is unimplemented in Go
// (sourcenetwork/defradb#3883), so there is no Go-parity variant.
#[tokio::test]
async fn rust_cross_object_parent_read_inheritance() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_acp_local()
        .build()
        .await
        .unwrap();
    cross_object_parent_read_inheritance(cluster).await;
}
